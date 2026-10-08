"""Native HTTP capability checks, independent of Afterglow/BFF gates."""
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from drover.auth import get_os_conn
from drover.main import app
from drover.policy import authorize_workload_namespace, reset_enforcer
from drover.services.credentials import workload_namespace


@pytest.fixture
def native_principal(monkeypatch, mock_conn):
    principal = {"token": "current-token", "project_id": "test-project-123", "user_id": "test-user-123",
                 "roles": ["member", "reader", "drover-inventory_reader", "drover-access_user", "drover-clusters_editor", "drover-workloads_editor"], "expires_at": "2099-01-01T00:00:00Z",
                 "is_system_admin": False}
    monkeypatch.setattr("drover.auth.validate_token", lambda *_: dict(principal))

    async def connection():
        yield mock_conn

    app.dependency_overrides[get_os_conn] = connection
    reset_enforcer()
    yield principal
    app.dependency_overrides.clear()
    reset_enforcer()


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [["member"], ["reader"], ["member", "reader", "drover-access_user"],
                                   ["member", "reader", "drover-inventory_reader"], ["drover-access_admin"]])
@pytest.mark.parametrize(("method", "path", "body"), [
    ("PATCH", "/v1/clusters/c/scale", {"agent_count": 2}),
    ("DELETE", "/v1/clusters/c", None),
    ("POST", "/v1/clusters/c/delete-async", None),
    ("POST", "/v1/clusters/c/rotate-certs", None),
    ("PATCH", "/v1/clusters/c/namespaces/default/deployments/app/scale", {"replicas": 2}),
])
async def test_native_mutations_reject_unentitled_principal(native_principal, monkeypatch, roles, method, path, body):
    native_principal["roles"] = roles
    lookup = AsyncMock(side_effect=AssertionError("Denied request reached storage"))
    monkeypatch.setattr("drover.services.store.get_cluster", lookup)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://native") as client:
        response = await client.request(method, path, json=body, headers={"X-Auth-Token": "current-token"})
    assert response.status_code == 403
    lookup.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/clusters/c", "/v1/clusters/c/delete-async", "/v1/clusters/c/rotate-certs"])
async def test_editor_cannot_delete_or_rotate(native_principal, path):
    method = "DELETE" if path.endswith("/c") else "POST"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://native") as client:
        response = await client.request(method, path, headers={"X-Auth-Token": "current-token"})
    assert response.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [["member", "reader", "drover-clusters_admin", "drover-access_admin"], ["admin"], ["manager"],
                                   ["member", "admin", "drover-access_admin"]])
async def test_tenant_roles_never_get_system_api(native_principal, roles):
    native_principal["roles"] = roles
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://native") as client:
        response = await client.get("/v1/admin/clusters", headers={"X-Auth-Token": "current-token"})
    assert response.status_code == 403


def test_namespace_authorization_is_principal_bound(native_principal):
    from fastapi import HTTPException

    own = workload_namespace(native_principal["project_id"], native_principal["user_id"])
    authorize_workload_namespace(own, native_principal)
    for namespace in ("kube-system", "afterglow-shell", "default", workload_namespace("test-project-123", "another")):
        with pytest.raises(HTTPException) as denied:
            authorize_workload_namespace(namespace, native_principal)
        assert denied.value.status_code == 403


@pytest.mark.asyncio
async def test_editor_cannot_install_host_ssh_key(native_principal, monkeypatch):
    lookup = AsyncMock(side_effect=AssertionError("Host-key request reached provisioning"))
    monkeypatch.setattr("drover.services.store.get_cluster", lookup)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://native") as client:
        response = await client.post("/v1/clusters/async", json={"name": "editor-cluster", "key_name": "my-root-key"},
                                     headers={"X-Auth-Token": "current-token"})
    assert response.status_code == 403
    lookup.assert_not_called()


@pytest.mark.asyncio
async def test_editor_cannot_delete_nodegroup(native_principal, monkeypatch):
    mutation = AsyncMock(side_effect=AssertionError("Editor reached nodegroup deletion"))
    monkeypatch.setattr("drover.services.nodegroup.enqueue_nodegroup_delete", mutation)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://native") as client:
        response = await client.delete("/v1/clusters/c/nodegroups/g", headers={"X-Auth-Token": "current-token"})
    assert response.status_code == 403
    mutation.assert_not_called()


@pytest.mark.asyncio
async def test_editor_namespace_picker_gets_prepared_private_namespace(native_principal, monkeypatch):
    namespace = workload_namespace(native_principal["project_id"], native_principal["user_id"])
    monkeypatch.setattr("drover.services.store.get_cluster", AsyncMock(return_value={"id": "c", "project_id": native_principal["project_id"]}))
    prepare = AsyncMock(return_value=namespace)
    monkeypatch.setattr("drover.services.credentials.ensure_workload_namespace", prepare)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://native") as client:
        response = await client.get("/v1/clusters/c/namespaces", headers={"X-Auth-Token": "current-token"})
    assert response.status_code == 200
    assert response.json() == [namespace]
    prepare.assert_awaited_once_with("c", native_principal)
