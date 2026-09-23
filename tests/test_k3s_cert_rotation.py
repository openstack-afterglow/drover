"""PR 3-B — k3s 인증서 회전 테스트."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------


def _make_cluster(master_count: int = 3, status: str = "ACTIVE") -> dict:
    return {
        "id": "c1",
        "name": "mycluster",
        "project_id": "test-project-123",
        "status": status,
        "master_count": master_count,
    }


# ---------------------------------------------------------------------------
# 서비스 단위 테스트
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rotate_certificates_emits_completed():
    """정상 경로: ROTATE_DISCOVER → ROTATE_SERVER → COMPLETED."""
    from drover.services.cert_rotation import rotate_certificates

    with (
        patch("drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=["node-1"])),
        patch("drover.services.cert_rotation.k3s_kube.create_job", new=AsyncMock(return_value={})),
        patch("drover.services.cert_rotation.k3s_kube.wait_job_completed", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_kube.wait_node_ready", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_db.record_rotation", new=AsyncMock()),
        patch("drover.services.cert_rotation._invalidate_expiry_cache", new=AsyncMock()),
    ):
        steps = [msg.step.value async for msg in rotate_certificates("c1", "proj1", "testuser")]

    assert "rotate_discover" in steps
    assert "rotate_server" in steps
    assert "completed" in steps


@pytest.mark.asyncio
async def test_rotate_certificates_no_nodes_emits_failed():
    """control-plane 노드 없으면 FAILED."""
    from drover.services.cert_rotation import rotate_certificates

    with patch("drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=[])):
        steps = [msg.step.value async for msg in rotate_certificates("c1", "proj1", "testuser")]

    assert steps[-1] == "failed"


@pytest.mark.asyncio
async def test_rotate_certificates_job_failure_emits_failed():
    """Job 실패 시 FAILED."""
    from drover.services.cert_rotation import rotate_certificates

    with (
        patch("drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=["node-1"])),
        patch("drover.services.cert_rotation.k3s_kube.create_job", new=AsyncMock(return_value={})),
        patch("drover.services.cert_rotation.k3s_kube.wait_job_completed", new=AsyncMock(return_value=False)),
        patch("drover.services.cert_rotation._invalidate_expiry_cache", new=AsyncMock()),
    ):
        steps = [msg.step.value async for msg in rotate_certificates("c1", "proj1", "testuser")]

    assert steps[-1] == "failed"


@pytest.mark.asyncio
async def test_rotate_certificates_node_not_ready_emits_failed():
    """노드 Ready 타임아웃 시 FAILED."""
    from drover.services.cert_rotation import rotate_certificates

    with (
        patch("drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=["node-1"])),
        patch("drover.services.cert_rotation.k3s_kube.create_job", new=AsyncMock(return_value={})),
        patch("drover.services.cert_rotation.k3s_kube.wait_job_completed", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_kube.wait_node_ready", new=AsyncMock(return_value=False)),
        patch("drover.services.cert_rotation._invalidate_expiry_cache", new=AsyncMock()),
    ):
        steps = [msg.step.value async for msg in rotate_certificates("c1", "proj1", "testuser")]

    assert steps[-1] == "failed"


@pytest.mark.asyncio
async def test_rotate_certificates_multi_node_calls_job_per_node():
    """서버 3개 → Job 3번 생성."""
    from drover.services.cert_rotation import rotate_certificates

    mock_create_job = AsyncMock(return_value={})
    with (
        patch(
            "drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=["n1", "n2", "n3"])
        ),
        patch("drover.services.cert_rotation.k3s_kube.create_job", new=mock_create_job),
        patch("drover.services.cert_rotation.k3s_kube.wait_job_completed", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_kube.wait_node_ready", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_db.record_rotation", new=AsyncMock()),
        patch("drover.services.cert_rotation._invalidate_expiry_cache", new=AsyncMock()),
    ):
        msgs = [msg async for msg in rotate_certificates("c1", "proj1", "testuser")]

    assert mock_create_job.call_count == 3
    assert msgs[-1].step.value == "completed"


@pytest.mark.asyncio
async def test_record_rotation_called_on_success():
    """성공 시 record_rotation 호출됨."""
    from drover.services.cert_rotation import rotate_certificates

    mock_record = AsyncMock()
    with (
        patch("drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=["node-1"])),
        patch("drover.services.cert_rotation.k3s_kube.create_job", new=AsyncMock(return_value={})),
        patch("drover.services.cert_rotation.k3s_kube.wait_job_completed", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_kube.wait_node_ready", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_db.record_rotation", new=mock_record),
        patch("drover.services.cert_rotation._invalidate_expiry_cache", new=AsyncMock()),
    ):
        async for _ in rotate_certificates("c1", "proj1", "alice"):
            pass

    mock_record.assert_called_once_with("c1", "alice")


def _node_ready_wait_messages(msgs: list) -> list:
    return [m for m in msgs if m.step.value == "rotate_server" and "노드 Ready 대기 중" in m.message]


@pytest.mark.asyncio
async def test_rotate_certificates_proceeds_as_soon_as_node_ready():
    """노드가 즉시 Ready면 keepalive 간격(기본 10초)을 기다리지 않고 완료한다."""
    import asyncio

    from drover.services.cert_rotation import _NODE_READY_KEEPALIVE_SECONDS, rotate_certificates

    assert _NODE_READY_KEEPALIVE_SECONDS >= 5

    async def _collect() -> list:
        return [msg async for msg in rotate_certificates("c1", "proj1", "testuser")]

    with (
        patch("drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=["node-1"])),
        patch("drover.services.cert_rotation.k3s_kube.create_job", new=AsyncMock(return_value={})),
        patch("drover.services.cert_rotation.k3s_kube.wait_job_completed", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_kube.wait_node_ready", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_db.record_rotation", new=AsyncMock()),
        patch("drover.services.cert_rotation._invalidate_expiry_cache", new=AsyncMock()),
    ):
        # 이전 구현은 Ready 여부와 무관하게 10초를 sleep했으므로 이 timeout을 넘겼다.
        msgs = await asyncio.wait_for(_collect(), timeout=2.0)

    assert msgs[-1].step.value == "completed"
    assert len(_node_ready_wait_messages(msgs)) == 1, "즉시 Ready인 노드에는 keepalive를 보내지 않는다"


@pytest.mark.asyncio
async def test_rotate_certificates_emits_keepalive_while_waiting_for_node_ready():
    """노드 Ready 대기가 keepalive 간격보다 길면 대기 중 keepalive 메시지를 계속 보낸다."""
    import asyncio

    from drover.services.cert_rotation import rotate_certificates

    async def _slow_ready(*args, **kwargs) -> bool:
        await asyncio.sleep(0.2)
        return True

    with (
        patch("drover.services.cert_rotation._NODE_READY_KEEPALIVE_SECONDS", 0.01),
        patch("drover.services.cert_rotation.k3s_kube.list_server_nodes", new=AsyncMock(return_value=["node-1"])),
        patch("drover.services.cert_rotation.k3s_kube.create_job", new=AsyncMock(return_value={})),
        patch("drover.services.cert_rotation.k3s_kube.wait_job_completed", new=AsyncMock(return_value=True)),
        patch("drover.services.cert_rotation.k3s_kube.wait_node_ready", new=AsyncMock(side_effect=_slow_ready)),
        patch("drover.services.cert_rotation.k3s_db.record_rotation", new=AsyncMock()),
        patch("drover.services.cert_rotation._invalidate_expiry_cache", new=AsyncMock()),
    ):
        msgs = [msg async for msg in rotate_certificates("c1", "proj1", "testuser")]

    wait_msgs = _node_ready_wait_messages(msgs)
    # 최초 대기 안내 1건 + keepalive 최소 1건
    assert len(wait_msgs) >= 2
    assert len({m.progress for m in wait_msgs}) == 1, "keepalive는 진행률을 바꾸지 않는다"
    assert msgs[-1].step.value == "completed"


# ---------------------------------------------------------------------------
# API 엔드포인트 테스트
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rotate_certs_single_master_returns_422(client):
    """단일 마스터 클러스터 → 422."""
    with patch(
        "drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=_make_cluster(master_count=1))
    ):
        resp = await client.post("/v1/clusters/c1/rotate-certs")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_rotate_certs_cluster_not_found_returns_404(client):
    """클러스터 없음 → 404."""
    with patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=None)):
        resp = await client.post("/v1/clusters/nonexistent/rotate-certs")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_rotate_certs_not_active_returns_409(client):
    """CREATING 상태 클러스터 → 409."""
    with patch(
        "drover.api.certificates.k3s_db.get_cluster",
        new=AsyncMock(return_value=_make_cluster(master_count=3, status="CREATING")),
    ):
        resp = await client.post("/v1/clusters/c1/rotate-certs")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_rotate_certs_concurrent_lock_returns_409(client):
    """동시 회전 락 → 409."""
    with (
        patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=_make_cluster(master_count=3))),
        patch("drover.api.certificates.acquire_rotation_lock", new=AsyncMock(return_value=False)),
    ):
        resp = await client.post("/v1/clusters/c1/rotate-certs")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_rotate_certs_sse_stream_returns_200(client):
    """HA 클러스터 정상 요청 → 200 + SSE 스트림."""

    async def _fake_rotate(*args, **kwargs):
        from drover.models.schemas import K3sProgressMessage, K3sProgressStep

        yield K3sProgressMessage(step=K3sProgressStep.ROTATE_DISCOVER, progress=5, message="검색 중", cluster_id="c1")
        yield K3sProgressMessage(step=K3sProgressStep.COMPLETED, progress=100, message="완료", cluster_id="c1")

    with (
        patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=_make_cluster(master_count=3))),
        patch("drover.api.certificates.acquire_rotation_lock", new=AsyncMock(return_value=True)),
        patch("drover.api.certificates.release_rotation_lock", new=AsyncMock()),
        patch("drover.api.certificates.rotate_certificates", new=_fake_rotate),
    ):
        resp = await client.post("/v1/clusters/c1/rotate-certs")

    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_rotate_certs_unauthenticated():
    """인증 없음 → 401."""
    from httpx import ASGITransport, AsyncClient

    from drover.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/v1/clusters/c1/rotate-certs")
    assert resp.status_code == 401
