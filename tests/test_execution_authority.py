"""Deterministic authority boundaries: no Keystone, OpenStack, Kubernetes or database network.

Shared conftest has no async DB fixture. Activation/retirement use the queue tests'
async session-double pattern. Native smoke must still prove transaction rollback,
row-lock races, real Keystone owner revocation and guest rollout on a live cluster.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from keystoneauth1 import exceptions as ks_exc
from pydantic import ValidationError

from drover import auth, crypto, db, policy
from drover.config import Settings
from drover.models.orm import DroverClusterCredential, K3sCluster
from drover.services import afterglow, autoscale, cluster_authority, delegation, execution, guest_rollout, jobs

NOW = datetime(2030, 1, 1, tzinfo=UTC)


def snapshot(**changes):
    snap = delegation._DelegationSnapshot(
        id="delegation", project_id="project", cluster_id="cluster", operation_id="operation",
        action=delegation.ACTION_CREATE, trust_id="trust", trustor_user_id="operator",
        trustee_user_id="service", role_ids=("member-id",), expires_at=NOW + timedelta(hours=1),
    )
    return replace(snap, **changes)


def trust_access(**changes):
    values = dict(trust_scoped=True, trust_id="trust", project_id="project", user_id="operator",
                  trustor_user_id="operator", trustee_user_id="service", role_ids=["member-id"],
                  role_names=["member"], expires=NOW + timedelta(hours=1))
    return SimpleNamespace(**{**values, **changes})


def test_trust_token_accepts_exact_snapshot():
    delegation._verify_trust_access(trust_access(), snapshot())


def test_trust_token_accepts_keystone_implied_roles():
    # Keystone expands implied roles (member -> reader) into trust-scoped tokens.
    delegation._verify_trust_access(
        trust_access(role_ids=["member-id", "reader-id"], role_names=["member", "reader"]), snapshot()
    )



def test_trust_token_accepts_provider_lifetime_longer_than_delegation():
    delegation._verify_trust_access(trust_access(expires=NOW + timedelta(days=1)), snapshot())


@pytest.mark.parametrize("seconds", [-1, 0])
def test_trust_token_rejects_expired_token(monkeypatch, seconds):
    monkeypatch.setattr(delegation, "_now", lambda: NOW)
    with pytest.raises(execution.AuthorityRevoked):
        delegation._verify_trust_access(trust_access(expires=NOW + timedelta(seconds=seconds)), snapshot())

@pytest.mark.parametrize("changes", [
    {"project_id": "foreign"}, {"trustee_user_id": "foreign"}, {"user_id": "service"},
    {"trustor_user_id": "foreign"}, {"trust_id": "foreign"}, {"trust_scoped": False},
    {"role_ids": ["member-id", "admin-id"], "role_names": ["member", "admin"]},
    {"role_ids": ["reader-id"], "role_names": ["reader"]}, {"role_ids": [], "role_names": []},
    {"expires": None},
])
def test_trust_token_rejects_authority_drift(changes):
    with pytest.raises(execution.AuthorityRevoked):
        delegation._verify_trust_access(trust_access(**changes), snapshot())


@pytest.fixture
def delegated_settings(monkeypatch):
    settings = Settings(drover_delegated_required_roles=["member"],
                        drover_delegated_optional_roles=["load-balancer_member"])
    monkeypatch.setattr(delegation, "get_settings", lambda: settings)
    return settings


def test_role_selection_requires_member_and_only_held_optional_roles(delegated_settings):
    with pytest.raises(delegation.DelegationDenied):
        delegation.select_delegated_roles({"admin": "admin-id", "manager": "manager-id"})
    assert delegation.select_delegated_roles({"member": "member-id", "admin": "admin-id"}) == (
        ["member"], ["member-id"]
    )
    assert delegation.select_delegated_roles({
        "member": "member-id", "load-balancer_member": "lb-id", "manager": "manager-id", "admin": "admin-id",
    }) == (["member", "load-balancer_member"], ["member-id", "lb-id"])


@pytest.mark.parametrize("field", ["drover_delegated_required_roles", "drover_delegated_optional_roles"])
@pytest.mark.parametrize("role", ["admin", "manager", "ADMIN", "Manager"])
def test_config_rejects_privileged_delegation(field, role):
    with pytest.raises(ValidationError, match="must not include admin or manager"):
        Settings(**{field: [role]})


@pytest.mark.parametrize("role", ["admin", "manager"])
def test_selection_defends_against_invalid_runtime_configuration(monkeypatch, role):
    monkeypatch.setattr(delegation, "get_settings", lambda: SimpleNamespace(
        drover_delegated_required_roles=["member"], drover_delegated_optional_roles=[role],
    ))
    with pytest.raises(delegation.DelegationDenied, match="never delegates"):
        delegation.select_delegated_roles({"member": "member-id", role: "privileged-id"})


@pytest.fixture
def current_principal(monkeypatch):
    enforcer = policy.get_enforcer(policy_file="")
    monkeypatch.setattr(policy, "get_enforcer", lambda **_: enforcer)
    state = SimpleNamespace(enabled=(True, True), roles={"member": "member-id", "drover-clusters_editor": "editor-id"})
    monkeypatch.setattr(auth, "current_principal_state", lambda *_: state.enabled)
    monkeypatch.setattr(auth, "current_project_role_map", lambda *_: state.roles)
    monkeypatch.setattr(auth, "_is_system_admin", lambda *_: False)
    return state


def test_current_principal_accepts_current_capability(current_principal):
    delegation.revalidate_principal_sync("operator", "project", delegation.ACTION_SCALE, ["member-id"])


@pytest.mark.parametrize("boundary", ["disabled_user", "disabled_project", "lost_role", "lost_capability"])
def test_principal_revalidation_revokes_stale_authority(current_principal, boundary):
    if boundary == "disabled_user":
        current_principal.enabled = (False, True)
    elif boundary == "disabled_project":
        current_principal.enabled = (True, False)
    elif boundary == "lost_role":
        current_principal.roles.pop("member")
    else:
        current_principal.roles.pop("drover-clusters_editor")
    with pytest.raises(execution.AuthorityRevoked):
        delegation.revalidate_principal_sync("operator", "project", delegation.ACTION_SCALE, ["member-id"])


@pytest.mark.parametrize("lookup", ["current_principal_state", "current_project_role_map"])
def test_directory_connect_failure_is_retryable(current_principal, monkeypatch, lookup):
    def unavailable(*_):
        raise ks_exc.ConnectFailure("directory unreachable")
    monkeypatch.setattr(auth, lookup, unavailable)
    with pytest.raises(execution.ExecutionAuthorityUnavailable):
        delegation.revalidate_principal_sync("operator", "project", delegation.ACTION_SCALE, ["member-id"])


@pytest.fixture
def trust_boundary(monkeypatch):
    state = SimpleNamespace(snap=snapshot(), conn=object(), connect=MagicMock(), mark=AsyncMock())
    state.connect.return_value = state.conn
    async def load(_):
        return state.snap
    monkeypatch.setattr(delegation, "_load_snapshot", load)
    monkeypatch.setattr(delegation, "_now", lambda: NOW)
    monkeypatch.setattr(delegation, "_mark_state", state.mark)
    monkeypatch.setattr(delegation, "_connect_sync", state.connect)
    monkeypatch.setattr(delegation, "get_settings", lambda: SimpleNamespace(drover_operation_trust_min_remaining_seconds=300))
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["scale", "stampede_provision", "reconcile", "reauthorize"])
async def test_trust_rejects_job_outside_admitted_action(trust_boundary, kind):
    with pytest.raises(execution.AuthorityRevoked):
        await delegation.open_trust_connection(execution.OperationAuthority("delegation", "project", "cluster", kind))
    trust_boundary.connect.assert_not_called()
    trust_boundary.mark.assert_awaited_once_with("delegation", "revoked", "scope mismatch")


@pytest.mark.asyncio
@pytest.mark.parametrize("expired_operation_id,allowed", [(None, False), ("foreign", False), ("operation", True)])
async def test_create_rollback_delete_is_operation_fenced(trust_boundary, expired_operation_id, allowed):
    authority = jobs._authority_for("delete", {
        "_delegation_id": "delegation", "expired_operation_id": expired_operation_id,
    }, "cluster", "project")
    if allowed:
        assert await delegation.open_trust_connection(authority) is trust_boundary.conn
        trust_boundary.connect.assert_called_once_with(trust_boundary.snap)
        trust_boundary.mark.assert_not_awaited()
    else:
        with pytest.raises(execution.AuthorityRevoked):
            await delegation.open_trust_connection(authority)
        trust_boundary.connect.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [-1, 0, 299])
async def test_trust_rejects_expired_or_near_expiry(trust_boundary, seconds):
    trust_boundary.snap = snapshot(expires_at=NOW + timedelta(seconds=seconds))
    with pytest.raises(execution.AuthorityRevoked, match="expiry"):
        await delegation.open_trust_connection(execution.OperationAuthority("delegation", "project", "cluster", "create"))
    trust_boundary.connect.assert_not_called()


@pytest.mark.asyncio
async def test_execution_fails_closed_without_binding_and_on_project_mismatch(monkeypatch):
    trust = AsyncMock()
    control = AsyncMock()
    monkeypatch.setattr(delegation, "open_trust_connection", trust)
    monkeypatch.setattr(cluster_authority, "open_control_connection", control)
    assert execution.current() is None
    with pytest.raises(execution.ExecutionAuthorityError, match="No admitted"):
        await execution.open_connection("project")
    for authority in (
        execution.OperationAuthority("delegation", "project", "cluster", "create"),
        execution.ResourceAuthority("project", "cluster", cluster_authority.CAPABILITY_READ),
    ):
        with execution.bound(authority):
            with pytest.raises(execution.AuthorityRevoked, match="project"):
                await execution.open_connection("foreign")
    trust.assert_not_awaited()
    control.assert_not_awaited()
    assert execution.current() is None


@pytest.mark.parametrize("kind", ["create", "bootstrap_ha", "provision_agents", "scale", "delete", "nodegroup_reconcile"])
def test_mutation_jobs_require_delegation(kind):
    with pytest.raises(execution.AuthorityRevoked, match="no admitted"):
        jobs._authority_for(kind, {}, "cluster", "project")
    payload = {"_delegation_id": "delegation"}
    assert jobs._authority_for(kind, payload, "cluster", "project") == execution.OperationAuthority(
        "delegation", "project", "cluster", kind,
    )
    assert "_delegation_id" not in payload


@pytest.mark.parametrize("kind,payload,capability", [
    ("stampede_provision", {}, cluster_authority.CAPABILITY_SCALE),
    ("nodegroup_reconcile", {"stampede": True}, cluster_authority.CAPABILITY_SCALE),
    ("reconcile", {}, cluster_authority.CAPABILITY_READ),
])
def test_continuous_jobs_use_resource_authority(kind, payload, capability):
    assert jobs._authority_for(kind, payload, "cluster", "project") == execution.ResourceAuthority("project", "cluster", capability)


@pytest.mark.parametrize("kind", ["rotate_certificates", "reauthorize"])
def test_non_openstack_jobs_do_not_bind_authority(kind):
    assert jobs._authority_for(kind, {}, "cluster", "project") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("error_class", [execution.ExecutionAuthorityError, execution.AuthorityRevoked, execution.ReauthorizationRequired])
async def test_worker_terminalizes_authority_errors(monkeypatch, error_class):
    retry = AsyncMock(return_value=True)
    complete = AsyncMock()
    monkeypatch.setattr(jobs, "_claim_one", AsyncMock(return_value=("job", 1, "scale", "cluster", "project", {})))
    monkeypatch.setattr(jobs, "_execute_job_direct", AsyncMock(side_effect=error_class("authority denied")))
    monkeypatch.setattr(jobs, "_retry_or_fail", retry)
    monkeypatch.setattr(jobs, "_complete", complete)
    monkeypatch.setattr(jobs, "_heartbeat_lease", AsyncMock())
    assert await jobs.process_one_job() is True
    retry.assert_awaited_once_with("job", attempt=1, error="authority denied", terminal=True)
    complete.assert_not_awaited()


@pytest.mark.parametrize("format", ["ini", "yaml"])
def test_guest_credentials_replace_exactly_once(format):
    credential = {"id": "new-id", "secret": "new-secret"}
    if format == "ini":
        text = "[Global]\nauth-url=https://identity/v3\napplication-credential-id = old-id\napplication-credential-secret=old-secret\n[Other]\napplication-credential-id=untouched\n"
        expected = "[Global]\nauth-url=https://identity/v3\napplication-credential-id=new-id\napplication-credential-secret=new-secret\n[Other]\napplication-credential-id=untouched\n"
        assert guest_rollout.replace_ini_credential(text, credential) == expected
    else:
        text = "auth:\n  application-credential-id: old-id\n  application-credential-secret: old-secret\nregion: RegionOne\n"
        expected = "auth:\n  application-credential-id: new-id\n  application-credential-secret: new-secret\nregion: RegionOne\n"
        assert guest_rollout.replace_yaml_credential(text, credential) == expected


@pytest.mark.parametrize("format", ["ini", "yaml"])
@pytest.mark.parametrize("key", ["id", "secret"])
@pytest.mark.parametrize("occurrences", [0, 2])
def test_guest_credential_replacement_fails_closed(format, key, occurrences):
    counts = {"id": 1, "secret": 1, key: occurrences}
    sep = "=" if format == "ini" else ": "
    text = "[Global]\n" if format == "ini" else ""
    text += "".join(f"application-credential-{field}{sep}old-{field}\n" * count for field, count in counts.items())
    replacer = guest_rollout.replace_ini_credential if format == "ini" else guest_rollout.replace_yaml_credential
    with pytest.raises(guest_rollout.GuestRolloutError, match="exactly one"):
        replacer(text, {"id": "new-id", "secret": "new-secret"})


@pytest.mark.parametrize("spec", [
    {"volumes": [{"secret": {"secretName": "target"}}]},
    {"volumes": [{"configMap": {"name": "target"}}]},
    {"volumes": [{"projected": {"sources": [{"secret": {"name": "target"}}]}}]},
    {"volumes": [{"projected": {"sources": [{"configMap": {"name": "target"}}]}}]},
    *[{container: [{"env": [{"valueFrom": {ref: {"name": "target", "key": "key"}}}]}]}
      for container in ("containers", "initContainers") for ref in ("secretKeyRef", "configMapKeyRef")],
    *[{container: [{"envFrom": [{ref: {"name": "target"}}]}]}
      for container in ("containers", "initContainers") for ref in ("secretRef", "configMapRef")],
])
def test_rollout_detects_each_credential_reference(spec):
    assert guest_rollout._references(spec, {"secret": {"target"}, "configmap": {"target"}})
    assert not guest_rollout._references(spec, {"secret": {"foreign"}, "configmap": {"foreign"}})


def test_rollout_ignores_empty_and_literal_environment():
    assert not guest_rollout._references({}, {"secret": set(), "configmap": set()})
    assert not guest_rollout._references({"containers": [{"env": [{"value": "target"}]}]}, {
        "secret": {"target"}, "configmap": {"target"},
    })


@pytest.mark.parametrize("kind,status", [
    ("daemonsets", {"desiredNumberScheduled": 2, "updatedNumberScheduled": 2, "numberAvailable": 2}),
    ("statefulsets", {"updatedReplicas": 2, "readyReplicas": 2, "currentRevision": "new", "updateRevision": "new"}),
    ("deployments", {"updatedReplicas": 2, "availableReplicas": 2, "replicas": 2}),
])
def test_rollout_requires_observed_generation_and_every_kind_specific_condition(kind, status):
    item = {"metadata": {"generation": 3}, "spec": {"replicas": 2}, "status": {"observedGeneration": 3, **status}}
    assert guest_rollout._rolled_out(kind, item)
    stale = deepcopy(item)
    stale["status"]["observedGeneration"] = 2
    assert not guest_rollout._rolled_out(kind, stale)
    for field in status:
        if field in {"desiredNumberScheduled", "updateRevision"}:
            continue
        incomplete = deepcopy(item)
        incomplete["status"][field] = "old" if field == "currentRevision" else 1
        assert not guest_rollout._rolled_out(kind, incomplete)


def test_deployment_rollout_defaults_to_one_replica_and_allows_zero():
    assert guest_rollout._rolled_out("deployments", {"status": {"updatedReplicas": 1, "availableReplicas": 1, "replicas": 1}})
    assert guest_rollout._rolled_out("deployments", {"spec": {"replicas": 0}})


@pytest.mark.parametrize("changes", [{"unrestricted": True}, {"project_id": "foreign"},
                                     {"roles": [{"id": "member-id"}, {"id": "admin-id"}]}, {"user_id": "foreign"}])
def test_issued_credentials_reject_excess_authority(changes):
    created = SimpleNamespace(**{**dict(id="credential", secret="secret", unrestricted=False,
                                      project_id="project", user_id="operator", roles=[{"id": "member-id"}]), **changes})
    with pytest.raises(cluster_authority.CredentialIssueError):
        cluster_authority._verify_issued(created, owner_user_id="operator", project_id="project",
                                         required_role_ids=["member-id"], allowed_role_ids={"member-id"})


def test_issued_credentials_accept_restricted_owner_project_and_roles():
    created = SimpleNamespace(id="credential", secret="secret", unrestricted=False, project_id="project",
                              user_id="operator", roles=[{"id": "member-id"}])
    assert cluster_authority._verify_issued(created, owner_user_id="operator", project_id="project",
                                            required_role_ids=["member-id"], allowed_role_ids={"member-id"}) == {"member-id"}


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", [False, True])
async def test_gpu_admission_precedes_every_native_create(monkeypatch, denied):
    order = []
    conn = MagicMock()
    settings = SimpleNamespace(drover_boot_volume_size_gb=30)
    cluster = {"name": "cluster", "server_ip": "192.0.2.1", "network_id": "network", "k3s_version": "v1.34.1+k3s1",
               "resource_policy_snapshot": {"effective_agent_image": {"id": "image"}, "k3s.volume_availability_zone": {"id": "nova"}}}
    monkeypatch.setattr(autoscale, "get_settings", lambda: settings)
    monkeypatch.setattr("drover.services.store.get_cluster_admin", AsyncMock(return_value=cluster))
    monkeypatch.setattr("drover.services.store.get_cluster_node_token", AsyncMock(return_value="token"))
    monkeypatch.setattr(execution, "open_connection", AsyncMock(return_value=conn))
    monkeypatch.setattr("drover.services.keystone.close_connection", AsyncMock())
    monkeypatch.setattr("drover.services.plugins.aggregate_agent_args", lambda _: [])
    monkeypatch.setattr(autoscale, "_find_native_resources", AsyncMock(return_value=(None, None, set())))
    monkeypatch.setattr("drover.services.inventory.record_resource", AsyncMock())
    monkeypatch.setattr("drover.services.nodegroup.add_nodegroup_vms", AsyncMock())
    monkeypatch.setattr("drover.services.cloudinit.generate_agent_userdata", lambda **_: SimpleNamespace(data="userdata", config_drive=False))
    async def admit(*_, **__):
        order.append("admit")
        if denied:
            raise afterglow.GpuAdmissionDenied("quota")
    admission = AsyncMock(side_effect=admit)
    volume = MagicMock(side_effect=lambda *_, **__: order.append("volume") or SimpleNamespace(id="volume", status="available"))
    server = MagicMock(side_effect=lambda *_, **__: order.append("server") or SimpleNamespace(id="server", status="ACTIVE"))
    monkeypatch.setattr(afterglow, "require_gpu_admission", admission)
    monkeypatch.setattr("drover.services.cinder.create_volume_from_image", volume)
    monkeypatch.setattr("drover.services.nova.create_server", server)
    call = autoscale.provision_nodegroup_vms("project", "cluster", "group", 2, flavor_id="gpu",
                                           gpu_required=True, provisioning_key_prefix="key")
    if denied:
        with pytest.raises(afterglow.GpuAdmissionDenied):
            await call
        volume.assert_not_called()
        server.assert_not_called()
        assert order == ["admit"]
    else:
        assert len(await call) == 2
        assert order == ["admit", "volume", "server", "admit", "volume", "server"]
    admission.assert_any_await("project", "gpu", settings=settings)


class _Session:
    """Async session shape used by the existing queue tests, with explicit result batches."""
    def __init__(self, cluster=None, batches=()):
        self.cluster = cluster
        self.batches = list(batches)
        self.added = []
        self.statements = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def begin(self):
        return self

    async def get(self, model, key, **kwargs):
        assert model is K3sCluster and key == "cluster" and kwargs.get("with_for_update") is True
        return self.cluster

    async def scalars(self, statement):
        self.statements.append(statement)
        return SimpleNamespace(all=lambda: self.batches.pop(0))

    async def scalar(self, statement):
        self.statements.append(statement)
        return None

    def add(self, row):
        self.added.append(row)


def credential_row(generation, purpose, state, owner="creator"):
    return DroverClusterCredential(id=f"row-{generation}-{purpose}", cluster_id="cluster", project_id="project",
                                   generation=generation, purpose=purpose, owner_user_id=owner,
                                   app_credential_id=f"credential-{generation}-{purpose}", secret_encrypted="ciphertext",
                                   state=state, last_error="old error")


@pytest.mark.asyncio
async def test_activation_switches_generation_and_erases_superseded_secrets(monkeypatch):
    old = credential_row(1, cluster_authority.CONTROL, "active")
    staged_control = credential_row(2, cluster_authority.CONTROL, "staged", "operator")
    staged_guest = credential_row(2, cluster_authority.GUEST, "staged", "operator")
    cluster = SimpleNamespace(project_id="project", deleted_at=None, app_credential_id="legacy-guest", updated_at=None)
    session = _Session(cluster, [[old, staged_control, staged_guest]])
    monkeypatch.setattr(db, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(cluster_authority, "_now", lambda: NOW)
    await cluster_authority._activate("project", "cluster", 2)
    assert old.state == "retiring" and old.secret_encrypted is None and old.retired_at == NOW
    for row in (staged_control, staged_guest):
        assert row.state == "active" and row.activated_at == NOW and row.last_error is None
        assert row.secret_encrypted == "ciphertext"
    assert cluster.app_credential_id == staged_guest.app_credential_id
    assert len(session.added) == 1
    legacy = session.added[0]
    assert legacy.app_credential_id == "legacy-guest" and legacy.state == "retiring" and legacy.secret_encrypted is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "authentication", "rollout"])
async def test_reauthorization_activates_only_after_complete_guest_rollout(monkeypatch, failure):
    control = cluster_authority._CredentialSnapshot("control", "cluster", "project", 2, "control", "operator",
                                                    "credential-control", ("member-id",), "ciphertext")
    guest = replace(control, id="guest", purpose="guest", app_credential_id="credential-guest")
    async def staged(_, purpose, **kwargs):
        return control if purpose == "control" else guest
    monkeypatch.setattr(cluster_authority, "_snapshot", staged)
    monkeypatch.setattr(crypto, "decrypt_app_credential_secret", lambda _: "safe-secret-value-123")
    monkeypatch.setattr(delegation, "revalidate_principal_sync", MagicMock())
    authenticate = MagicMock(side_effect=[None, RuntimeError("guest auth failed")] if failure == "authentication" else None)
    monkeypatch.setattr(cluster_authority, "_authenticate_sync", authenticate)
    monkeypatch.setattr("drover.services.store.get_cluster", AsyncMock(return_value={"status": "ACTIVE"}))
    monkeypatch.setattr(cluster_authority, "_guest_plugins", AsyncMock(return_value=["occm"]))
    rollout = AsyncMock(side_effect=RuntimeError("partial rollout") if failure == "rollout" else None)
    activate = AsyncMock()
    error = AsyncMock()
    monkeypatch.setattr(guest_rollout, "rollout_guest_credential", rollout)
    monkeypatch.setattr(cluster_authority, "_activate", activate)
    monkeypatch.setattr(cluster_authority, "_record_staged_error", error)
    if failure:
        with pytest.raises(RuntimeError):
            await cluster_authority.execute_reauthorization("project", "cluster", {"generation": 2})
        activate.assert_not_awaited()
        error.assert_awaited_once()
        assert error.await_args.args[:2] == ("cluster", 2)
        if failure == "authentication":
            rollout.assert_not_awaited()
    else:
        await cluster_authority.execute_reauthorization("project", "cluster", {"generation": 2})
        rollout.assert_awaited_once_with("cluster", 2, {"id": "credential-guest", "secret": "safe-secret-value-123"},
                                        kms_required=False, kms_detect=False)
        activate.assert_awaited_once_with("project", "cluster", 2)
        error.assert_not_awaited()


@pytest.mark.asyncio
async def test_deleted_cluster_retires_departed_creators_credentials_without_authentication(monkeypatch):
    rows = [credential_row(1, "control", "active"), credential_row(1, "guest", "active"),
            credential_row(2, "control", "staged", "operator")]
    session = _Session(batches=[rows])
    monkeypatch.setattr(db, "get_session_factory", lambda: lambda: session)
    revalidate = MagicMock(side_effect=AssertionError("Departed creator must not be authenticated"))
    monkeypatch.setattr(delegation, "revalidate_principal_sync", revalidate)
    authenticate = MagicMock(side_effect=AssertionError("No creator credential may be used"))
    monkeypatch.setattr(cluster_authority, "_authenticate_sync", authenticate)
    assert await cluster_authority.retire_remaining_for_deleted_cluster("cluster", "project", "legacy-guest") == 4
    assert all(row.state == "retiring" and row.secret_encrypted is None and row.retired_at for row in rows)
    assert session.added[0].owner_user_id is None and session.added[0].secret_encrypted is None
    revalidate.assert_not_called()
    authenticate.assert_not_called()


@pytest.mark.asyncio
async def test_retire_owned_does_not_claim_failed_owner_revocations(monkeypatch):
    first = credential_row(1, "control", "retiring", "operator")
    second = credential_row(1, "guest", "retiring", "operator")
    session = _Session(batches=[[first, second], [first]])
    monkeypatch.setattr(db, "get_session_factory", lambda: lambda: session)
    conn = MagicMock()
    conn.identity.delete_application_credential.side_effect = [None, RuntimeError("Keystone unavailable")]
    deleted = await cluster_authority.retire_owned(conn, cluster_id="cluster", owner_user_id="operator", states=("retiring",))
    assert deleted == [first.app_credential_id]
    assert first.state == "deleted" and first.secret_encrypted is None and first.deleted_at
    assert second.state == "retiring" and second.secret_encrypted == "ciphertext"
    assert conn.identity.delete_application_credential.call_args_list[0].args == ("operator", first.app_credential_id)


@pytest.mark.asyncio
async def test_delete_job_uses_new_actor_trust_after_creator_departure(monkeypatch):
    from drover.services import deletion

    conn = MagicMock()
    cluster = {"id": "cluster", "project_id": "project", "name": "cluster",
               "created_by_user_id": "departed-creator", "app_credential_id": "credential-1-guest"}
    rows = [credential_row(1, "control", "active", "departed-creator"),
            credential_row(1, "guest", "active", "departed-creator")]
    session = _Session(batches=[rows])
    monkeypatch.setattr(db, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(deletion.k3s_cluster, "get_cluster", AsyncMock(return_value=cluster))
    monkeypatch.setattr(deletion.k3s_cluster, "update_cluster_status", AsyncMock())
    delete_record = AsyncMock()
    monkeypatch.setattr(deletion.k3s_cluster, "delete_cluster_record", delete_record)
    monkeypatch.setattr(deletion.inventory, "list_managed_resources", AsyncMock(return_value=[]))
    monkeypatch.setattr(deletion.octavia, "list_occm_service_load_balancers", MagicMock(return_value=[]))
    monkeypatch.setattr(deletion.octavia, "delete_occm_service_load_balancers", MagicMock(return_value=[]))
    monkeypatch.setattr(deletion, "rec", AsyncMock())
    close = AsyncMock()
    monkeypatch.setattr(deletion.keystone, "close_connection", close)
    trust = AsyncMock(return_value=conn)
    control = AsyncMock(side_effect=AssertionError("The departed creator's control credential cannot authorize deletion"))
    retire_owned = AsyncMock(side_effect=AssertionError("Trust-scoped tokens cannot revoke application credentials"))
    monkeypatch.setattr(delegation, "open_trust_connection", trust)
    monkeypatch.setattr(cluster_authority, "open_control_connection", control)
    monkeypatch.setattr(cluster_authority, "retire_owned", retire_owned)
    await jobs._execute_job_direct("delete", {"_delegation_id": "new-actor-delegation", "user_id": "new-actor"},
                                   "cluster", "project")
    trust.assert_awaited_once_with(execution.OperationAuthority("new-actor-delegation", "project", "cluster", "delete"))
    control.assert_not_awaited()
    retire_owned.assert_not_awaited()
    assert all(row.state == "retiring" and row.secret_encrypted is None for row in rows)
    delete_record.assert_awaited_once_with("project", "cluster", user_id="new-actor", reason="사용자 삭제 요청")
    close.assert_awaited_once_with(conn)


@pytest.mark.asyncio
async def test_partial_rollout_records_staged_error_without_retiring_active_generation(monkeypatch):
    active = credential_row(1, "control", "active")
    staged = credential_row(2, "control", "staged", "operator")
    session = _Session(batches=[[staged]])
    monkeypatch.setattr(db, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(cluster_authority, "_now", lambda: NOW)
    await cluster_authority._record_staged_error("cluster", 2, "partial guest rollout")
    assert staged.state == "staged" and staged.last_error == "partial guest rollout" and staged.updated_at == NOW
    assert staged.secret_encrypted == "ciphertext"
    assert active.state == "active" and active.secret_encrypted == "ciphertext"
    query = session.statements[0].compile()
    assert "staged" in query.params.values() and 2 in query.params.values()


