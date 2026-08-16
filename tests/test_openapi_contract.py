"""Regression tests for Drover OpenAPI contract, security schemes, public discovery, and SDK parity."""

import warnings
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app


def test_openapi_security_scheme_and_unique_operation_ids():
    """Verify OpenAPI security scheme, unique operation IDs, and absence of warnings."""
    with warnings.catch_warnings(record=True) as recorded_warnings:
        warnings.simplefilter("always")
        schema = app.openapi()

    # Assert no warnings were generated during OpenAPI compilation
    dup_warnings = [w for w in recorded_warnings if "Duplicate Operation ID" in str(w.message)]
    assert not dup_warnings, f"Unexpected duplicate operation ID warnings: {dup_warnings}"

    # Verify KeystoneToken security scheme
    sec_schemes = schema.get("components", {}).get("securitySchemes", {})
    assert "KeystoneToken" in sec_schemes
    keystone_scheme = sec_schemes["KeystoneToken"]
    assert keystone_scheme["type"] == "apiKey"
    assert keystone_scheme["name"] == "X-Auth-Token"
    assert keystone_scheme["in"] == "header"

    # Verify unique operation IDs across all operations
    op_ids = set()
    for path, methods in schema.get("paths", {}).items():
        for method, op in methods.items():
            if isinstance(op, dict) and "operationId" in op:
                op_id = op["operationId"]
                assert op_id not in op_ids, f"Duplicate operationId '{op_id}' at {method.upper()} {path}"
                op_ids.add(op_id)


def test_public_discovery_and_health_schema():
    """Verify public version discovery and service health endpoints in OpenAPI schema."""
    schema = app.openapi()
    paths = schema.get("paths", {})

    # Root discovery /
    assert "/" in paths
    root_op = paths["/"]["get"]
    assert root_op.get("security") is None
    root_resp = root_op["responses"]["200"]["content"]["application/json"]["schema"]
    assert root_resp.get("$ref", "").endswith("RootDiscoveryResponse")

    # Version discovery /v1/
    assert "/v1/" in paths
    v1_op = paths["/v1/"]["get"]
    assert v1_op.get("security") is None
    v1_resp = v1_op["responses"]["200"]["content"]["application/json"]["schema"]
    assert v1_resp.get("$ref", "").endswith("VersionDiscoveryResponse")

    # Health check /v1/health
    assert "/v1/health" in paths
    health_op = paths["/v1/health"]["get"]
    assert health_op.get("security") is None
    health_resp = health_op["responses"]["200"]["content"]["application/json"]["schema"]
    assert health_resp.get("$ref", "").endswith("HealthResponse")


def test_protected_endpoints_require_keystone_token_in_openapi():
    """Verify protected endpoints mandate KeystoneToken security scheme."""
    schema = app.openapi()
    paths = schema.get("paths", {})

    protected_paths = ["/v1/clusters", "/v1/clusters/health", "/v1/clusters/{cluster_id}"]
    for path in protected_paths:
        assert path in paths, f"Path {path} missing from OpenAPI schema"
        get_op = paths[path]["get"]
        assert get_op.get("security") == [{"KeystoneToken": []}]


@pytest.mark.asyncio
async def test_public_discovery_endpoints_runtime():
    """Verify runtime GET /, GET /v1/, and GET /v1/health return 200 without auth."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res_root = await client.get("/")
        assert res_root.status_code == 200
        root_data = res_root.json()
        assert "versions" in root_data
        assert root_data["versions"][0]["id"] == "v1.0"

        res_v1 = await client.get("/v1/")
        assert res_v1.status_code == 200
        v1_data = res_v1.json()
        assert "version" in v1_data
        assert v1_data["version"]["id"] == "v1.0"

        res_health = await client.get("/v1/health")
        assert res_health.status_code == 200
        assert res_health.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_repaired_cluster_health_list_route():
    """Verify GET /v1/clusters/health is authenticated and routed to list_cluster_health."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Unauthenticated access returns 401 (not 404 from get_k3s_cluster catch-all)
        res_unauth = await client.get("/v1/clusters/health")
        assert res_unauth.status_code == 401

    # Authenticated access returns cluster health list
    valid_token = {
        "project_id": "proj-123",
        "user_id": "user-1",
        "is_system_admin": False,
    }
    with (
        patch("drover.auth.validate_token", return_value=valid_token),
        patch("drover.api.health.k3s_cluster.list_clusters", new=AsyncMock(return_value=[])),
        patch("drover.api.health.k3s_health.get_health_results_for_clusters", new=AsyncMock(return_value=[])),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            headers = {"X-Auth-Token": "valid-token"}
            res_auth = await client.get("/v1/clusters/health", headers=headers)
            assert res_auth.status_code == 200
            assert res_auth.json() == []


@pytest.mark.asyncio
async def test_kubeconfig_head_operation():
    """Verify HEAD /v1/clusters/{cluster_id}/kubeconfig returns 200 with empty body when ready."""
    valid_token = {
        "project_id": "proj-123",
        "user_id": "user-1",
        "is_system_admin": False,
    }
    cluster_rec = {"id": "c1", "name": "mycluster", "status": "ACTIVE"}
    with (
        patch("drover.auth.validate_token", return_value=valid_token),
        patch("drover.api.clusters.k3s_cluster.get_cluster", new=AsyncMock(return_value=cluster_rec)),
        patch("drover.api.clusters.k3s_cluster.get_kubeconfig", new=AsyncMock(return_value=b"apiVersion: v1")),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            headers = {"X-Auth-Token": "valid-token"}
            res = await client.request("HEAD", "/v1/clusters/c1/kubeconfig", headers=headers)
            assert res.status_code == 200
            assert res.content == b""
