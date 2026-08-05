"""kubeconfig 다운로드 audit log + callback IP 로깅 검증.

토큰 탈취 시 forensic 추적 가능하도록 매 GET 다운로드와 callback 모두 기록.
"""

from unittest.mock import AsyncMock, patch

import pytest


def _make_cluster_record():
    return {
        "id": "k3s-1",
        "name": "test-cluster",
        "project_id": "test-project-123",
        "status": "ACTIVE",
        "agent_count": 0,
    }


@pytest.mark.asyncio
async def test_kubeconfig_download_records_audit(client):
    """GET /kubeconfig 시 rec(action="kubeconfig_download") 호출."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.rec", new=AsyncMock()) as mock_rec,
    ):
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_db.get_kubeconfig = AsyncMock(return_value=b"apiVersion: v1\n...")
        resp = await client.get("/v1/clusters/k3s-1/kubeconfig")
    assert resp.status_code == 200
    mock_rec.assert_called_once()
    kwargs = mock_rec.call_args.kwargs
    assert kwargs["action"] == "kubeconfig_download"
    assert kwargs["resource_type"] == "k3s_cluster"
    assert kwargs["resource_id"] == "k3s-1"
    assert "source_ip" in kwargs["extra"]


@pytest.mark.asyncio
async def test_kubeconfig_head_does_not_record(client):
    """HEAD 는 보통 사전 요청 — audit 기록 안 함 (GET 시에만)."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.rec", new=AsyncMock()) as mock_rec,
    ):
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_db.get_kubeconfig = AsyncMock(return_value=b"apiVersion: v1\n...")
        resp = await client.request("HEAD", "/v1/clusters/k3s-1/kubeconfig")
    assert resp.status_code == 200
    mock_rec.assert_not_called()


@pytest.mark.asyncio
async def test_kubeconfig_404_does_not_record(client):
    """존재하지 않는 클러스터 — audit 기록 안 함."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.rec", new=AsyncMock()) as mock_rec,
    ):
        mock_db.get_cluster = AsyncMock(return_value=None)
        resp = await client.get("/v1/clusters/missing/kubeconfig")
    assert resp.status_code == 404
    mock_rec.assert_not_called()
