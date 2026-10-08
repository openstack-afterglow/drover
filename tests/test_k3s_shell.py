"""Shell tickets and WebSocket exec preserve current Keystone authority."""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.api import shell
from drover.auth import get_token_info, require_token
from drover.main import app
from drover.services.credentials import workload_namespace


def _token(roles=None, **changes):
    return {
        "token": "current-token", "user_id": "user-1", "project_id": "test-project-123",
        "roles": roles if roles is not None else [
            "member", "reader", "drover-inventory_reader", "drover-access_user",
            "drover-clusters_editor", "drover-workloads_editor",
        ],
        "is_system_admin": False, "expires_at": "2099-01-01T00:00:00Z",
    } | changes


@pytest.fixture
async def shell_client(client):
    app.dependency_overrides[get_token_info] = lambda: _token()
    app.dependency_overrides[require_token] = lambda: _token()
    yield client


def _cluster(status="ACTIVE"):
    return {"id": "k3s-1", "project_id": "test-project-123", "status": status}


@pytest.mark.asyncio
async def test_create_shell_ticket_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/v1/clusters/k3s-1/shell-ticket")
    assert response.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [
    ["member"], ["reader"], ["member", "reader", "drover-inventory_reader"],
    ["reader", "drover-workloads_editor"],
    ["member", "reader", "drover-inventory_reader", "drover-access_user"],
])
async def test_reader_and_user_cannot_create_shell_ticket(client, roles):
    app.dependency_overrides[get_token_info] = lambda: _token(roles)
    app.dependency_overrides[require_token] = lambda: _token(roles)
    with patch.object(shell.k3s_cluster, "get_cluster", new_callable=AsyncMock) as lookup:
        response = await client.post("/v1/clusters/k3s-1/shell-ticket")
    assert response.status_code == 403
    lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("record", "status"), [(None, 404), (_cluster("CREATING"), 409)])
async def test_create_shell_ticket_checks_cluster(shell_client, record, status):
    with patch.object(shell.k3s_cluster, "get_cluster", AsyncMock(return_value=record)):
        response = await shell_client.post("/v1/clusters/k3s-1/shell-ticket")
    assert response.status_code == status


@pytest.mark.asyncio
async def test_create_shell_ticket_success(shell_client):
    redis = AsyncMock()
    with (
        patch.object(shell.k3s_cluster, "get_cluster", AsyncMock(return_value=_cluster())),
        patch.object(shell, "_get_redis", AsyncMock(return_value=redis)),
        patch.object(shell, "rec", AsyncMock()),
    ):
        response = await shell_client.post("/v1/clusters/k3s-1/shell-ticket")
    assert response.status_code == 201
    assert len(response.json()["ticket"]) >= 32
    assert response.json()["expires_in"] == 30
    key, ttl, raw = redis.setex.call_args.args
    assert key.startswith("afterglow:k3s-shell-ticket:") and ttl == 30
    assert json.loads(raw) == {
        "cluster_id": "k3s-1", "project_id": "test-project-123", "user_id": "user-1", "token": "current-token",
    }


def _ticket():
    return json.dumps({"cluster_id": "k3s-1", "project_id": "test-project-123", "user_id": "user-1", "token": "original-token"})


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [None, json.dumps({"cluster_id": "other-cluster"})])
async def test_shell_ticket_consumed_and_cluster_bound(raw):
    ws, redis = AsyncMock(), AsyncMock()
    redis.getdel.return_value = raw
    with patch.object(shell, "_get_redis", AsyncMock(return_value=redis)), patch.object(shell, "validate_token") as validate:
        await shell.shell_ws("k3s-1", ws, "ticket")
    redis.getdel.assert_awaited_once_with("afterglow:k3s-shell-ticket:ticket")
    assert ws.close.call_args.kwargs["code"] in (4401, 4403)
    validate.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("current", [
    _token(["member", "reader", "drover-inventory_reader", "drover-access_user"]), _token(["reader"]),
    _token(user_id="other-user"), _token(project_id="other-project"), None,
])
async def test_shell_revalidates_token_and_denies_changed_authority(current):
    ws, redis = AsyncMock(), AsyncMock()
    redis.getdel.return_value = _ticket()
    with (
        patch.object(shell, "_get_redis", AsyncMock(return_value=redis)),
        patch.object(shell, "validate_token", return_value=current, side_effect=ValueError() if current is None else None) as validate,
        patch.object(shell.k3s_cloud_shell, "ensure_session", AsyncMock()) as ensure,
    ):
        await shell.shell_ws("k3s-1", ws, "ticket")
    validate.assert_called_once_with("original-token")
    ensure.assert_not_awaited()
    assert ws.close.call_args.kwargs["code"] in (4401, 4403)


@pytest.mark.asyncio
async def test_shell_preserves_project_cluster_ownership():
    ws, redis = AsyncMock(), AsyncMock()
    redis.getdel.return_value = _ticket()
    with (
        patch.object(shell, "_get_redis", AsyncMock(return_value=redis)),
        patch.object(shell, "validate_token", return_value=_token()),
        patch.object(shell.k3s_cluster, "get_cluster", AsyncMock(return_value=None)) as lookup,
        patch.object(shell.k3s_cloud_shell, "ensure_session", AsyncMock()) as ensure,
    ):
        await shell.shell_ws("k3s-1", ws, "ticket")
    lookup.assert_awaited_once_with("test-project-123", "k3s-1")
    ensure.assert_not_awaited()
    assert ws.close.call_args.kwargs["code"] == 4404


@asynccontextmanager
async def _ws_params(*args, **kwargs):
    yield None, "wss://kube.invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize("violation", [None, "another-user", "system-namespace"])
async def test_exec_is_bound_to_current_principal_and_workload_namespace(violation):
    ws, redis = AsyncMock(), AsyncMock()
    redis.getdel.return_value = _ticket()
    token = _token()
    expiry = time.time() + 1000
    annotations = {
        "drover.io/user-id": "another-user" if violation == "another-user" else "user-1",
        "drover.io/project-id": "test-project-123",
        "drover.io/workload-namespace": "kube-system" if violation == "system-namespace" else workload_namespace("test-project-123", "user-1"),
        "drover.io/credential-expiry": str(expiry),
    }
    backend = MagicMock()
    backend.__aenter__ = AsyncMock(return_value=MagicMock())
    backend.__aexit__ = AsyncMock(return_value=False)
    with (
        patch.object(shell, "_get_redis", AsyncMock(return_value=redis)),
        patch.object(shell, "validate_token", return_value=token),
        patch.object(shell.k3s_cluster, "get_cluster", AsyncMock(return_value=_cluster())),
        patch.object(shell.k3s_cloud_shell, "ensure_session", AsyncMock(return_value="own-unique-pod")) as ensure,
        patch.object(shell.k3s_kube, "get_pod", AsyncMock(return_value={"metadata": {"annotations": annotations}})),
        patch.object(shell.k3s_kube, "_kube_ws_params", _ws_params),
        patch.object(shell.websockets, "connect", return_value=backend) as connect,
        patch.object(shell, "_proxy", AsyncMock()) as proxy,
        patch.object(shell, "_gc_pod", AsyncMock()) as gc,
    ):
        await shell.shell_ws("k3s-1", ws, "ticket")
        await asyncio.sleep(0)  # Let the connection-specific cleanup task run.
    ensure.assert_awaited_once_with("k3s-1", token, project_id="test-project-123")
    if violation == "another-user":
        gc.assert_not_awaited()
    else:
        gc.assert_awaited_once_with("k3s-1", "own-unique-pod", "test-project-123")
    if violation:
        connect.assert_not_called()
        assert ws.close.call_args.kwargs["code"] == 4403
    else:
        assert "/namespaces/afterglow-shell/pods/own-unique-pod/exec" in connect.call_args.args[0]
        proxy.assert_awaited_once_with(ws, backend.__aenter__.return_value, expires_at=expiry)


@pytest.mark.asyncio
async def test_expired_session_stops_active_proxy():
    class Backend:
        async def send(self, data):
            pass

        def __aiter__(self):
            return self.messages()

        async def messages(self):
            await asyncio.Event().wait()
            yield b"never"

    ws = AsyncMock()
    # Keep client receive blocked; expiration must terminate both proxy tasks.
    async def receive():
        await asyncio.Event().wait()
    ws.receive_bytes.side_effect = receive
    await shell._proxy(ws, Backend(), expires_at=time.time() - 1)
    ws.close.assert_awaited_once_with(code=4401, reason="session credentials expired")
