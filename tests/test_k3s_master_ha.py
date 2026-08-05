"""PR 2B — k3s Master HA 테스트."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drover.models.schemas import CreateK3sClusterRequest

# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

_VALID_TOKEN_INFO = {
    "project_id": "test-project",
    "user_id": "uid1",
    "username": "tester",
    "is_admin": False,
}


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


# ---------------------------------------------------------------------------
# Pydantic 모델 유효성
# ---------------------------------------------------------------------------


def test_master_count_default_is_1():
    req = CreateK3sClusterRequest(name="my-cluster")
    assert req.master_count == 1


def test_master_count_3_allowed():
    req = CreateK3sClusterRequest(name="ha-cluster", master_count=3)
    assert req.master_count == 3


def test_master_count_2_raises():
    with pytest.raises(Exception):
        CreateK3sClusterRequest(name="bad", master_count=2)


def test_master_count_5_raises():
    with pytest.raises(Exception):
        CreateK3sClusterRequest(name="bad", master_count=5)


# ---------------------------------------------------------------------------
# k3s_cloudinit — cluster_init 분기
# ---------------------------------------------------------------------------


def test_generate_server_userdata_cluster_init():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="ha-test",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="token123",
        cluster_init=True,
    )
    assert result.data  # base64 인코딩됨
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "--cluster-init" in raw
    assert "--server" not in raw


def test_generate_server_userdata_ha_join():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="ha-test",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="token456",
        cluster_init=False,
        join_url="https://10.0.0.5:6443",
        ha_node_token="K10abc::server:xyz",
    )
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "--server" in raw
    assert "10.0.0.5" in raw
    assert "--cluster-init" not in raw


def test_generate_server_userdata_single_master():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="single",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="tok789",
    )
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "--cluster-init" not in raw
    assert "--server" not in raw


def test_generate_server_userdata_uses_direct_drover_callback():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="single",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="tok789",
    )
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "http://cb.test/v1/callback" in raw
    assert "/api/k3s/callback" not in raw


# ---------------------------------------------------------------------------
# HA 콜백 토큰
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_and_consume_ha_callback_token():
    from unittest.mock import AsyncMock, patch

    mock_client = AsyncMock()
    mock_client.setex = AsyncMock()
    mock_client.getdel = AsyncMock(
        return_value=json.dumps({"project_id": "proj1", "cluster_id": "clus1", "server_index": 2}).encode()
    )

    with patch("drover.services.redis_store._get_client", return_value=mock_client):
        from drover.services.redis_store import consume_ha_callback_token, create_ha_callback_token

        token = await create_ha_callback_token("proj1", "clus1", 2)
        assert isinstance(token, str) and len(token) > 20

        data = await consume_ha_callback_token(token)
        assert data is not None
        assert data["server_index"] == 2
        assert data["project_id"] == "proj1"


@pytest.mark.asyncio
async def test_incr_ha_join_count():
    from unittest.mock import AsyncMock, patch

    mock_client = AsyncMock()
    mock_client.incr = AsyncMock(return_value=2)
    mock_client.expire = AsyncMock()

    with patch("drover.services.redis_store._get_client", return_value=mock_client):
        from drover.services.redis_store import incr_ha_join_count

        count = await incr_ha_join_count("clus1")
        assert count == 2
        mock_client.expire.assert_called_once()


# ---------------------------------------------------------------------------
# callback.py — HA 조인 콜백 처리
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_ha_joiner_adds_lb_member_and_triggers_agents():
    """조인 카운터가 master_count-1에 도달하면 provision_agents가 스폰된다."""
    cluster_info = {
        "project_id": "proj1",
        "name": "ha-cluster",
        "master_count": 3,
        "api_lb_pool_id": "pool-xyz",
        "network_id": "net-abc",
        "server_ip": "10.0.0.5",
        "node_token": "K10abc::server:xyz",
    }

    with (
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=cluster_info)),
        patch("drover.services.store.incr_ha_join_count", new=AsyncMock(return_value=2)),
        patch("drover.services.keystone.get_admin_connection_for_project") as mock_conn,
        patch("drover.services.octavia.add_member") as mock_add,
        patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()) as enqueue_job,
    ):
        mock_conn.return_value = MagicMock()
        mock_conn.return_value.network.subnets = MagicMock(return_value=[MagicMock(id="sub-1")])
        mock_add.return_value = {}

        from drover.api.callback import _handle_ha_joiner
        from drover.models.schemas import K3sCallbackRequest

        req = K3sCallbackRequest(
            token="dummytoken123",
            success=True,
            server_ip="10.0.0.6",
            node_token="K10abc::server:xyz",
        )
        result = await _handle_ha_joiner("proj1", "clus1", 2, req)
        assert result == {"ok": True}
        enqueue_job.assert_awaited_once_with(
            cluster_id="clus1",
            project_id="proj1",
            kind="provision_agents",
            payload={"server_ip": "10.0.0.5", "node_token": "K10abc::server:xyz"},
        )


@pytest.mark.asyncio
async def test_handle_ha_joiner_no_agents_if_not_all_joined():
    """조인 카운터가 master_count-1 미만이면 provision_agents가 스폰되지 않는다."""
    cluster_info = {
        "name": "ha-cluster",
        "master_count": 3,
        "api_lb_pool_id": "pool-xyz",
        "network_id": "net-abc",
        "server_ip": "10.0.0.5",
        "node_token": "K10abc::server:xyz",
    }

    with (
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=cluster_info)),
        patch("drover.services.store.incr_ha_join_count", new=AsyncMock(return_value=1)),
        patch("drover.services.keystone.get_admin_connection_for_project") as mock_conn,
        patch("drover.services.octavia.add_member"),
        patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()) as enqueue_job,
    ):
        mock_conn.return_value = MagicMock()
        mock_conn.return_value.network.subnets = MagicMock(return_value=[])

        from drover.api.callback import _handle_ha_joiner
        from drover.models.schemas import K3sCallbackRequest

        req = K3sCallbackRequest(token="dummytoken456", success=True, server_ip="10.0.0.7")
        await _handle_ha_joiner("proj1", "clus1", 3, req)
        enqueue_job.assert_not_awaited()
