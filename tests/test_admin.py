"""Drover system-admin cluster control contracts."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

pytestmark = pytest.mark.asyncio


def _cluster(cluster_id: str, status: str = "ACTIVE") -> dict:
    return {
        "id": cluster_id,
        "project_id": "project-1",
        "name": cluster_id,
        "status": status,
        "agent_vm_ids": [],
        "agent_count": 0,
    }


async def test_admin_clusters_require_system_admin(non_admin_client):
    response = await non_admin_client.get("/v1/admin/clusters")
    assert response.status_code == 403


async def test_admin_cluster_status_filter_accepts_dashboard_status_set(admin_client):
    clusters = [
        _cluster("creating", "CREATE_IN_PROGRESS"),
        _cluster("error", "ERROR"),
        _cluster("active", "ACTIVE"),
    ]
    with patch(
        "drover.api.admin.k3s_cluster.list_all_clusters",
        new=AsyncMock(return_value=clusters),
    ):
        response = await admin_client.get(
            "/v1/admin/clusters",
            params={"status": "CREATE_IN_PROGRESS,UPDATE_IN_PROGRESS,ERROR"},
        )

    assert response.status_code == 200
    assert [item["id"] for item in response.json()] == ["creating", "error"]
    assert {item["project_id"] for item in response.json()} == {"project-1"}


async def test_admin_scale_persists_durable_job_before_success(admin_client):
    enqueue = AsyncMock(return_value="job-1")
    update = AsyncMock()
    with (
        patch(
            "drover.api.admin.k3s_cluster.get_cluster_admin",
            new=AsyncMock(return_value=_cluster("cluster-1")),
        ),
        patch("drover.api.admin.k3s_cluster.update_cluster_status", new=update),
        patch("drover.api.admin._jobs_svc.enqueue_job", new=enqueue),
    ):
        response = await admin_client.patch(
            "/v1/admin/clusters/cluster-1/scale",
            json={"agent_count": 3},
        )

    assert response.status_code == 200
    update.assert_awaited_once_with(
        "project-1",
        "cluster-1",
        "SCALING",
        "에이전트 노드 스케일 변경 중",
    )
    enqueue.assert_awaited_once_with(
        cluster_id="cluster-1",
        project_id="project-1",
        kind="scale",
        payload={"desired_count": 3},
        user_id="admin",
        username="system-admin",
    )


async def test_admin_delete_persists_durable_job(admin_client):
    enqueue = AsyncMock(return_value="job-1")
    update = AsyncMock()
    with (
        patch(
            "drover.api.admin.k3s_cluster.get_cluster_admin",
            new=AsyncMock(return_value=_cluster("cluster-1")),
        ),
        patch("drover.api.admin.k3s_cluster.update_cluster_status", new=update),
        patch("drover.api.admin._jobs_svc.enqueue_job", new=enqueue),
    ):
        response = await admin_client.delete("/v1/admin/clusters/cluster-1")

    assert response.status_code == 204
    enqueue.assert_awaited_once_with(
        cluster_id="cluster-1",
        project_id="project-1",
        kind="delete",
        payload={"user_id": "admin", "username": "system-admin"},
        user_id="admin",
        username="system-admin",
    )
    update.assert_awaited_once_with(
        "project-1",
        "cluster-1",
        "DELETING",
        "관리자 삭제 요청",
    )
