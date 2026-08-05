"""Standalone Drover policy and runtime-setting API contracts."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drover.main import app
from drover.api.resource_policies import get_admin_os_conn
from drover.services.resource_policies import ResourcePolicyValidationError

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def policy_admin_client(admin_client):
    conn = MagicMock()

    async def override_admin_conn():
        yield conn

    app.dependency_overrides[get_admin_os_conn] = override_admin_conn
    yield admin_client, conn


async def test_non_admin_cannot_list_resource_policies(non_admin_client):
    response = await non_admin_client.get("/v1/admin/resource-policies")
    assert response.status_code == 403


async def test_admin_lists_drover_owned_policies(policy_admin_client):
    client, conn = policy_admin_client
    policies = [{"key": "k3s.server_image", "state": "configured", "resource_id": "image-1"}]
    with patch(
        "drover.api.resource_policies.store.inspect_policies",
        new=AsyncMock(return_value=policies),
    ) as inspect:
        response = await client.get("/v1/admin/resource-policies")

    assert response.status_code == 200
    assert response.json() == policies
    inspect.assert_awaited_once_with(conn)


async def test_admin_updates_policy_with_authenticated_user(policy_admin_client):
    client, conn = policy_admin_client
    result = {"key": "k3s.server_image", "resource_id": "image-1", "state": "configured"}
    update = AsyncMock(return_value=result)
    with patch("drover.api.resource_policies.store.set_policy", new=update):
        response = await client.put(
            "/v1/admin/resource-policies/k3s.server_image",
            json={"resource_id": "image-1"},
        )

    assert response.status_code == 200
    assert response.json() == result
    update.assert_awaited_once_with(
        conn=conn,
        key="k3s.server_image",
        resource_id="image-1",
        updated_by_user_id="test-user-123",
    )


async def test_unknown_policy_catalog_returns_not_found(policy_admin_client):
    client, _conn = policy_admin_client
    with patch(
        "drover.api.resource_policies.resource_policies.get_spec",
        side_effect=ResourcePolicyValidationError("unknown resource policy"),
    ):
        response = await client.get("/v1/admin/resource-policies/catalog/unknown")

    assert response.status_code == 404


async def test_admin_lists_runtime_settings(admin_client):
    settings = [{"key": "k3s.version", "value": "v1.31.4+k3s1", "state": "configured"}]
    with patch(
        "drover.api.resource_policies.store.list_runtime_settings",
        new=AsyncMock(return_value=settings),
    ):
        response = await admin_client.get("/v1/admin/runtime-settings")

    assert response.status_code == 200
    assert response.json() == settings


async def test_admin_updates_k3s_version(admin_client):
    result = {"key": "k3s.version", "value": "v1.31.5+k3s1", "state": "configured"}
    update = AsyncMock(return_value=result)
    with patch("drover.api.resource_policies.store.set_runtime_setting", new=update):
        response = await admin_client.put(
            "/v1/admin/runtime-settings/k3s.version",
            json={"value": "v1.31.5+k3s1"},
        )

    assert response.status_code == 200
    assert response.json() == result
    update.assert_awaited_once_with(
        key="k3s.version",
        value="v1.31.5+k3s1",
        updated_by_user_id="test-user-123",
    )
