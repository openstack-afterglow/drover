"""K3s 헬스체크 API + 서비스 단위 테스트."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app  # noqa: F401
from drover.models.schemas import K3sClusterHealth, K3sNodeHealth

# ---------------------------------------------------------------------------
# 픽스처
# ---------------------------------------------------------------------------


def _make_cluster_record(cluster_id="k3s-1", status="ACTIVE", server_ip="10.0.0.1"):
    return {
        "id": cluster_id,
        "name": "mycluster",
        "status": status,
        "status_reason": None,
        "server_vm_id": "vm-1",
        "agent_vm_ids": [],
        "agent_count": 1,
        "api_address": f"https://{server_ip}:6443",
        "server_ip": server_ip,
        "network_id": "net-1",
        "key_name": None,
        "k3s_version": "v1.31.4+k3s1",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
        "project_id": "proj-1",
    }


def _make_health_result(cluster_id="k3s-1", status="HEALTHY"):
    return K3sClusterHealth(
        cluster_id=cluster_id,
        cluster_name="mycluster",
        status=status,
        api_server_reachable=True,
        healthz_ok=True,
        nodes=[
            K3sNodeHealth(name="k3s-server", role="server", ready=True, conditions=["Ready"]),
        ],
        checked_at="2024-01-01T00:00:00+00:00",
        reachability="direct",
    )


# ---------------------------------------------------------------------------
# API 엔드포인트 테스트
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_cluster_health_unauthenticated():
    """인증 없이 헬스 목록 접근 시 401."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/health")  # 주의: 라우터 등록 경로 확인
    # k3s 서비스 비활성화 상태면 404, 활성화 상태면 401
    assert resp.status_code in (401, 404)


@pytest.mark.asyncio
async def test_get_cluster_health_unauthenticated():
    """인증 없이 단일 클러스터 헬스 접근 시 401."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/k3s-1/health")
    assert resp.status_code in (401, 404)


@pytest.mark.asyncio
async def test_get_cluster_health_not_found(client):
    """존재하지 않는 클러스터 헬스 조회 시 404."""
    with (
        patch("drover.api.health.k3s_cluster") as mock_db,
        patch("drover.api.health.k3s_health") as mock_health,
    ):
        mock_db.get_cluster = AsyncMock(return_value=None)
        mock_health.get_health_result = AsyncMock(return_value=None)
        resp = await client.get("/v1/clusters/nonexistent/health")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_cluster_health_success(client):
    """HEALTHY 결과 반환 시 200 + 올바른 응답."""
    health = _make_health_result("k3s-1", "HEALTHY")
    with (
        patch("drover.api.health.k3s_cluster") as mock_db,
        patch("drover.api.health.k3s_health") as mock_health,
    ):
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_health.get_health_result = AsyncMock(return_value=health)
        resp = await client.get("/v1/clusters/k3s-1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cluster_id"] == "k3s-1"
    assert data["status"] == "HEALTHY"
    assert data["healthz_ok"] is True
    assert len(data["nodes"]) == 1
    assert data["nodes"][0]["ready"] is True


@pytest.mark.asyncio
async def test_get_cluster_health_no_cache(client):
    """Redis에 데이터 없을 때 404."""
    with (
        patch("drover.api.health.k3s_cluster") as mock_db,
        patch("drover.api.health.k3s_health") as mock_health,
    ):
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_health.get_health_result = AsyncMock(return_value=None)
        resp = await client.get("/v1/clusters/k3s-1/health")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_health_check_success(client):
    """즉시 헬스체크 트리거 성공."""
    health = _make_health_result("k3s-1", "UNREACHABLE")
    health.status = "UNREACHABLE"
    health.reachability = "unreachable"
    with (
        patch("drover.api.health.k3s_cluster") as mock_db,
        patch("drover.api.health.k3s_health") as mock_health,
    ):
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_health.check_cluster_health = AsyncMock(return_value=health)
        mock_health.store_health_result = AsyncMock()
        resp = await client.post("/v1/clusters/k3s-1/health/check")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cluster_id"] == "k3s-1"


# ---------------------------------------------------------------------------
# 서비스 레이어 단위 테스트
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_cluster_health_no_server_ip():
    """server_ip 미설정 시 UNKNOWN 반환."""
    from drover.services.health import check_cluster_health

    cluster = _make_cluster_record(server_ip="")
    cluster["server_ip"] = ""

    result = await check_cluster_health(cluster)
    assert result.status == "UNKNOWN"
    assert result.api_server_reachable is False


@pytest.mark.asyncio
async def test_check_cluster_health_unreachable():
    """K3s API 연결 실패(ConnectError) 시 UNREACHABLE 반환."""
    from drover.services.health import check_cluster_health

    cluster = _make_cluster_record()

    with (
        patch("drover.services.health._get_probe_ip", AsyncMock(return_value="10.0.0.1")),
        patch("httpx.AsyncClient") as mock_client_cls,
    ):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(side_effect=httpx.ConnectError("연결 거부"))
        mock_client_cls.return_value = mock_client

        result = await check_cluster_health(cluster)

    assert result.status == "UNREACHABLE"
    assert result.reachability == "unreachable"
    assert result.api_server_reachable is False


@pytest.mark.asyncio
async def test_check_cluster_health_unreachable_on_timeout():
    """K3s API 연결 타임아웃(ConnectTimeout) 시 UNREACHABLE 반환."""
    from drover.services.health import check_cluster_health

    cluster = _make_cluster_record()

    with (
        patch("drover.services.health._get_probe_ip", AsyncMock(return_value="10.0.0.1")),
        patch("httpx.AsyncClient") as mock_client_cls,
    ):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(side_effect=httpx.ConnectTimeout("연결 타임아웃"))
        mock_client_cls.return_value = mock_client

        result = await check_cluster_health(cluster)

    assert result.status == "UNREACHABLE"
    assert result.reachability == "unreachable"
    assert result.api_server_reachable is False


@pytest.mark.asyncio
async def test_check_cluster_health_healthy_no_nodes():
    """healthz OK + 노드 조회 실패 시 HEALTHY (healthz 기준)."""
    from drover.services.health import check_cluster_health

    cluster = _make_cluster_record()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "ok"

    with (
        patch("drover.services.health._get_probe_ip", AsyncMock(return_value="10.0.0.1")),
        patch("httpx.AsyncClient") as mock_client_cls,
        patch("drover.services.store.get_kubeconfig_admin", AsyncMock(return_value=None)),
    ):
        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        result = await check_cluster_health(cluster)

    assert result.status == "HEALTHY"
    assert result.healthz_ok is True
    assert result.nodes == []


@pytest.mark.asyncio
async def test_check_all_active_clusters_empty():
    """ACTIVE 클러스터 없을 때 오류 없이 반환."""
    from drover.services.health import check_all_active_clusters

    with patch(
        "drover.services.store.list_all_clusters",
        AsyncMock(
            return_value=[
                _make_cluster_record(status="CREATING"),
                _make_cluster_record(cluster_id="k3s-2", status="ERROR"),
            ]
        ),
    ):
        # 예외 없이 완료되어야 함
        await check_all_active_clusters()


@pytest.mark.asyncio
async def test_store_and_get_health_result():
    """Redis에 저장하고 조회하는 경로 검증."""
    from drover.services.health import get_health_result, store_health_result

    health = _make_health_result("k3s-test", "DEGRADED")

    mock_redis = AsyncMock()
    mock_redis.setex = AsyncMock()
    mock_redis.get = AsyncMock(return_value=health.model_dump_json())

    with patch("drover.services.health._redis", AsyncMock(return_value=mock_redis)):
        await store_health_result("k3s-test", health)
        result = await get_health_result("k3s-test")

    assert result is not None
    assert result.cluster_id == "k3s-test"
    assert result.status == "DEGRADED"
