"""Restricted shell issuance, expiration, fresh sessions and legacy retirement."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import yaml
from fastapi import HTTPException

from drover.policy import reset_enforcer
from drover.services import cloud_shell as shell
from drover.services.credentials import IssuedKubeconfig, workload_namespace
from drover.services.errors import K3sApiError


def _token(roles=None, **changes):
    return {
        "token": "validated-token", "project_id": "project-1", "user_id": "user-1",
        "roles": roles if roles is not None else [
            "member", "reader", "drover-inventory_reader", "drover-access_user",
            "drover-clusters_editor", "drover-workloads_editor",
        ],
        "is_system_admin": False,
        "expires_at": (datetime.now(UTC) + timedelta(hours=2)).isoformat(),
    } | changes


def _issued():
    namespace = workload_namespace("project-1", "user-1")
    config = yaml.safe_dump({
        "apiVersion": "v1", "kind": "Config", "current-context": "principal",
        "clusters": [{"name": "c", "cluster": {"server": "https://kube.invalid", "certificate-authority-data": "public-ca"}}],
        "users": [{"name": "principal", "user": {"token": "short-lived-workload-token"}}],
        "contexts": [{"name": "principal", "context": {"cluster": "c", "user": "principal", "namespace": namespace}}],
    })
    return IssuedKubeconfig(config, datetime.now(UTC) + timedelta(minutes=20), namespace, "principal")


@pytest.fixture(autouse=True)
def _fresh_policy():
    reset_enforcer()
    yield
    reset_enforcer()


@pytest.fixture
def lifecycle():
    with (
        patch.object(shell.k3s_kube, "ensure_namespace", AsyncMock()),
        patch.object(shell.k3s_kube, "ensure_pvc", AsyncMock()),
        patch.object(shell.k3s_kube, "create_k8s_secret", AsyncMock()) as secret,
        patch.object(shell.k3s_kube, "create_pod", AsyncMock()) as pod,
        patch.object(shell.k3s_kube, "wait_pod_ready", AsyncMock(return_value=True)) as ready,
        patch.object(shell, "retire_legacy_sessions", AsyncMock()) as retire,
        patch.object(shell, "delete_session", AsyncMock()) as cleanup,
        patch.object(shell.credentials, "issue_kubeconfig", AsyncMock(return_value=_issued())) as issue,
    ):
        yield {"secret": secret, "pod": pod, "ready": ready, "retire": retire, "cleanup": cleanup, "issue": issue}


def test_user_hash_and_versioned_names():
    assert shell._user_hash("a") == shell._user_hash("a") != shell._user_hash("b")
    assert len(shell._user_hash("a")) == 12
    assert shell.pod_name("a").startswith("cloud-shell-v2-")
    assert shell.pvc_name("a").startswith("cloud-shell-home-v2-")
    assert len(shell.kubeconfig_secret_name(shell.pod_name("a") + "-" + "a" * 12)) <= 63


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [
    ["member"], ["reader"],
    ["reader", "drover-workloads_editor"],
    ["member", "reader", "drover-inventory_reader", "drover-access_user"],
    ["member", "reader", "drover-inventory_reader"],
])
async def test_plain_reader_and_user_denied_before_provisioning(lifecycle, roles):
    with pytest.raises(HTTPException) as error:
        await shell.ensure_session("c", _token(roles), project_id="project-1")
    assert error.value.status_code == 403
    lifecycle["issue"].assert_not_awaited()
    lifecycle["retire"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"project_id": "other-project"}, {"user_id": ""}])
async def test_session_requires_matching_authenticated_principal(lifecycle, changes):
    with pytest.raises(HTTPException) as error:
        await shell.ensure_session("c", _token(**changes), project_id="project-1")
    assert error.value.status_code == 403
    lifecycle["issue"].assert_not_awaited()


@pytest.mark.parametrize("expiry", ["", "not-a-date", "2020-01-01T00:00:00Z", "2099-01-01T00:00:00"])
def test_session_requires_valid_token_expiration(expiry):
    with pytest.raises(HTTPException) as error:
        shell.session_expires_at(_token(expires_at=expiry))
    assert error.value.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [
    _token(), _token([
        "member", "reader", "drover-inventory_reader", "drover-access_user",
        "drover-clusters_editor", "drover-workloads_editor", "drover-clusters_admin", "drover-access_admin",
    ]), _token([], is_system_admin=True),
])
async def test_shell_always_mounts_restricted_issued_token(lifecycle, token):
    name = await shell.ensure_session("c", token, project_id="project-1")
    lifecycle["retire"].assert_awaited_once_with("c", project_id="project-1")
    lifecycle["issue"].assert_awaited_once_with("c", token, grade="editor")
    args = lifecycle["secret"].call_args.args
    assert args[:3] == ("c", shell.SHELL_NAMESPACE, shell.kubeconfig_secret_name(name))
    config = yaml.safe_load(args[3]["config"])
    assert config["users"][0]["user"] == {"token": "short-lived-workload-token"}
    assert config["contexts"][0]["context"]["namespace"] == workload_namespace("project-1", "user-1")
    assert "client-key-data" not in args[3]["config"]
    assert "client-certificate-data" not in args[3]["config"]
    manifest = lifecycle["pod"].call_args.args[2]
    assert manifest["metadata"]["namespace"] == shell.SHELL_NAMESPACE != config["contexts"][0]["context"]["namespace"]
    assert manifest["metadata"]["annotations"]["drover.io/user-id"] == "user-1"
    assert manifest["spec"]["automountServiceAccountToken"] is False
    assert 0 < manifest["spec"]["activeDeadlineSeconds"] <= 1200
    assert manifest["spec"]["securityContext"]["runAsNonRoot"] is True
    mounts = manifest["spec"]["containers"][0]["volumeMounts"]
    assert mounts[1] == {"name": "kubeconfig", "mountPath": "/etc/drover", "readOnly": True}


@pytest.mark.asyncio
async def test_refresh_creates_distinct_pods_and_secrets(lifecycle):
    first = await shell.ensure_session("c", _token(), project_id="project-1")
    second = await shell.ensure_session("c", _token(), project_id="project-1")
    assert first != second
    secrets = [call.args[2] for call in lifecycle["secret"].call_args_list]
    assert secrets == [shell.kubeconfig_secret_name(first), shell.kubeconfig_secret_name(second)]
    assert lifecycle["issue"].await_count == 2
    lifecycle["cleanup"].assert_not_awaited()


@pytest.mark.asyncio
async def test_issuance_failure_has_no_admin_fallback(lifecycle):
    lifecycle["issue"].side_effect = K3sApiError(502, "issuance unavailable")
    with pytest.raises(K3sApiError):
        await shell.ensure_session("c", _token(), project_id="project-1")
    lifecycle["retire"].assert_awaited_once()
    lifecycle["secret"].assert_not_awaited()
    lifecycle["pod"].assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_retirement_failure_denies_session(lifecycle):
    lifecycle["retire"].side_effect = K3sApiError(504, "old pod still terminating")
    with pytest.raises(K3sApiError):
        await shell.ensure_session("c", _token(), project_id="project-1")
    lifecycle["issue"].assert_not_awaited()


@pytest.mark.asyncio
async def test_unready_session_is_removed(lifecycle):
    lifecycle["ready"].return_value = False
    with pytest.raises(HTTPException) as error:
        await shell.ensure_session("c", _token(), project_id="project-1")
    assert error.value.status_code == 504
    name = lifecycle["pod"].call_args.args[2]["metadata"]["name"]
    lifecycle["cleanup"].assert_awaited_once_with("c", name, project_id="project-1")


@pytest.mark.asyncio
async def test_retirement_sweeps_admin_resources_but_preserves_new_sessions_and_old_homes():
    old_hash, other_hash = shell._user_hash("user-1"), shell._user_hash("other-user")
    collections = {
        f"/api/v1/namespaces/{shell.SHELL_NAMESPACE}/pods": [f"cloud-shell-{old_hash}", f"cloud-shell-{other_hash}", shell.pod_name("user-1") + "-session"],
        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings": [f"afterglow-shell-afterglow-user-{old_hash}", "unrelated-binding"],
        f"/api/v1/namespaces/{shell.SHELL_NAMESPACE}/secrets": [f"cloud-shell-kc-{old_hash}", "other-secret"],
        f"/api/v1/namespaces/{shell.SHELL_NAMESPACE}/persistentvolumeclaims": [f"cloud-shell-home-{old_hash}", shell.pvc_name("user-1")],
    }
    client = MagicMock()
    async def get(url):
        path = url.removeprefix("https://kube.invalid")
        if path in collections:
            return httpx.Response(200, json={"items": [{"metadata": {"name": name}} for name in collections[path]]})
        return httpx.Response(404)
    client.get = AsyncMock(side_effect=get)
    client.delete = AsyncMock(return_value=httpx.Response(202))
    @asynccontextmanager
    async def kube_client(*args, **kwargs):
        assert kwargs == {"project_id": "project-1"}
        yield client, "https://kube.invalid"
    with patch.object(shell.k3s_kube, "_kube_client", kube_client):
        await shell.retire_legacy_sessions("c", project_id="project-1")
    deleted = [call.args[0] for call in client.delete.call_args_list]
    assert len(deleted) == 4
    assert any(f"cloud-shell-{other_hash}" in url for url in deleted)
    assert not any("v2-" in url or "unrelated" in url or "other-secret" in url for url in deleted)
    assert all(client.get.call_args_list.count(call) >= 1 for call in client.delete.call_args_list)
    assert not any("persistentvolumeclaims" in call.args[0] for call in client.get.call_args_list)
    assert not any("persistentvolumeclaims" in url for url in deleted)


@pytest.mark.asyncio
async def test_resource_deletion_must_finish_before_replacement():
    client = MagicMock()
    client.delete = AsyncMock(return_value=httpx.Response(202))
    client.get = AsyncMock(return_value=httpx.Response(200, json={"metadata": {"deletionTimestamp": "pending"}}))
    with patch.object(shell.time, "monotonic", side_effect=[0, 91]):
        with pytest.raises(K3sApiError) as error:
            await shell._delete_resource(client, "https://kube.invalid/old-pod")
    assert error.value.status_code == 504


@pytest.mark.asyncio
async def test_cleanup_uses_connection_specific_resource_names():
    client = MagicMock()
    client.delete = AsyncMock(return_value=httpx.Response(404))
    @asynccontextmanager
    async def kube_client(*args, **kwargs):
        yield client, "https://kube.invalid"
    name = shell.pod_name("user-1") + "-old-session"
    with patch.object(shell.k3s_kube, "_kube_client", kube_client):
        await shell.delete_session("c", name, project_id="project-1")
    assert [call.args[0] for call in client.delete.call_args_list] == [
        f"https://kube.invalid/api/v1/namespaces/{shell.SHELL_NAMESPACE}/pods/{name}",
        f"https://kube.invalid/api/v1/namespaces/{shell.SHELL_NAMESPACE}/secrets/{shell.kubeconfig_secret_name(name)}",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("violation", ["expired", "foreign-namespace"])
async def test_invalid_issued_credential_is_never_mounted(lifecycle, violation):
    issued = _issued()
    lifecycle["issue"].return_value = IssuedKubeconfig(
        issued.kubeconfig,
        datetime.now(UTC) - timedelta(seconds=1) if violation == "expired" else issued.expires_at,
        "kube-system" if violation == "foreign-namespace" else issued.namespace,
        issued.service_account,
    )
    with pytest.raises(HTTPException) as error:
        await shell.ensure_session("c", _token(), project_id="project-1")
    assert error.value.status_code == (401 if violation == "expired" else 403)
    lifecycle["secret"].assert_not_awaited()
    lifecycle["pod"].assert_not_awaited()
