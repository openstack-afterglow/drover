"""k3s Cloud Shell 엔드포인트 단위 테스트."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app


def _make_cluster_record(status: str = "ACTIVE") -> dict:
    return {
        "id": "k3s-1",
        "name": "mycluster",
        "status": status,
        "server_vm_id": "vm-1",
        "agent_vm_ids": [],
        "agent_count": 0,
    }


@pytest.mark.asyncio
async def test_create_shell_ticket_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/v1/clusters/k3s-1/shell-ticket")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_create_shell_ticket_cluster_not_found(client):
    with patch("drover.api.shell.k3s_cluster") as mock_cluster:
        mock_cluster.get_cluster = AsyncMock(return_value=None)
        resp = await client.post("/v1/clusters/k3s-1/shell-ticket")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_create_shell_ticket_cluster_not_active(client):
    with patch("drover.api.shell.k3s_cluster") as mock_cluster:
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record(status="CREATING"))
        resp = await client.post("/v1/clusters/k3s-1/shell-ticket")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_create_shell_ticket_success(client):
    mock_redis = AsyncMock()
    mock_redis.setex = AsyncMock()
    with (
        patch("drover.api.shell.k3s_cluster") as mock_cluster,
        patch("drover.api.shell._get_redis", new_callable=AsyncMock, return_value=mock_redis),
        patch("drover.api.shell.rec", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        resp = await client.post("/v1/clusters/k3s-1/shell-ticket")
    assert resp.status_code == 201
    data = resp.json()
    assert "ticket" in data
    assert len(data["ticket"]) >= 32
    assert data["expires_in"] == 30
    mock_redis.setex.assert_called_once()
    call_args = mock_redis.setex.call_args[0]
    assert call_args[0].startswith("afterglow:k3s-shell-ticket:")
    assert call_args[1] == 30
    payload = json.loads(call_args[2])
    assert payload["cluster_id"] == "k3s-1"
    assert payload["project_id"] == "test-project-123"
