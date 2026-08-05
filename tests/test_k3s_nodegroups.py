"""k3s 노드그룹 API 단위 테스트."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_CLUSTER_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

_NG_SERVER = {
    "id": "ng-server-0000-1111-2222-333333333333",
    "cluster_id": _CLUSTER_ID,
    "name": "default-server",
    "role": "server",
    "node_count": 1,
    "flavor_id": "flavor-001",
    "image_id": None,
    "labels": {},
    "taints": [],
    "is_default": True,
    "vms": [],
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}

_NG_AGENT = {
    "id": "ng-agent-0000-1111-2222-333333333333",
    "cluster_id": _CLUSTER_ID,
    "name": "default-agent",
    "role": "agent",
    "node_count": 2,
    "flavor_id": "flavor-002",
    "image_id": None,
    "labels": {},
    "taints": [],
    "is_default": True,
    "vms": [{"vm_id": "vm-001", "name": "agent-1", "status": "RUNNING"}],
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}

_NG_CUSTOM = {
    "id": "ng-custom-0000-1111-2222-333333333333",
    "cluster_id": _CLUSTER_ID,
    "name": "gpu-workers",
    "role": "agent",
    "node_count": 3,
    "flavor_id": "flavor-gpu",
    "image_id": None,
    "labels": {"accelerator": "gpu"},
    "taints": [],
    "is_default": False,
    "vms": [],
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}

_CLUSTER = {
    "id": _CLUSTER_ID,
    "name": "test-cluster",
    "status": "RUNNING",
    "project_id": "test-project-123",
    "agent_vm_ids": ["vm-001"],
    "agent_count": 2,
}


def _cluster_access_ok():
    return patch("drover.api.nodegroups.k3s_db.get_cluster", new=AsyncMock(return_value=_CLUSTER))




# ---------------------------------------------------------------------------
# 목록 조회
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_nodegroups(client):
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.list_nodegroups", new=AsyncMock(return_value=[_NG_SERVER, _NG_AGENT])),
    ):
        resp = await client.get(f"/v1/clusters/{_CLUSTER_ID}/nodegroups")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "default-server"
    assert data[1]["name"] == "default-agent"


@pytest.mark.asyncio
async def test_list_nodegroups_cluster_not_found(client):
    with patch("drover.api.nodegroups.k3s_db.get_cluster", new=AsyncMock(return_value=None)):
        resp = await client.get(f"/v1/clusters/{_CLUSTER_ID}/nodegroups")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 단건 조회
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_nodegroup(client):
    with _cluster_access_ok(), patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_AGENT)):
        resp = await client.get(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_AGENT['id']}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "default-agent"
    assert resp.json()["node_count"] == 2


@pytest.mark.asyncio
async def test_get_nodegroup_not_found(client):
    with _cluster_access_ok(), patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=None)):
        resp = await client.get(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/nonexistent")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 생성
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_nodegroup_success(client):
    enqueue = AsyncMock(return_value="job-1")
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.create_nodegroup", new=AsyncMock(return_value=_NG_CUSTOM)),
        patch("drover.api.nodegroups._jobs.enqueue_job", new=enqueue),
    ):
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "gpu-workers", "role": "agent", "node_count": 3, "flavor_id": "flavor-gpu"},
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "gpu-workers"
    assert data["node_count"] == 3
    assert data["is_default"] is False
    enqueue.assert_awaited_once_with(
        cluster_id=_CLUSTER_ID,
        project_id="test-project-123",
        kind="nodegroup_reconcile",
        payload={"action": "provision", "nodegroup": _NG_CUSTOM, "add_count": 3},
        user_id="test-user-123",
        username="testuser",
    )


@pytest.mark.asyncio
async def test_create_nodegroup_invalid_name(client):
    with _cluster_access_ok():
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "bad name!", "role": "agent", "node_count": 1},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_nodegroup_invalid_role(client):
    with _cluster_access_ok():
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "workers", "role": "master", "node_count": 1},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_nodegroup_duplicate_name(client):
    with (
        _cluster_access_ok(),
        patch(
            "drover.services.nodegroup.create_nodegroup",
            new=AsyncMock(side_effect=ValueError("이미 같은 이름의 노드그룹이 존재합니다: default-agent")),
        ),
    ):
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "default-agent", "role": "agent", "node_count": 1},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_nodegroup_db_unavailable(client):
    with (
        _cluster_access_ok(),
        patch(
            "drover.services.nodegroup.create_nodegroup",
            new=AsyncMock(side_effect=RuntimeError("DB가 설정되지 않아 노드그룹 기능을 사용할 수 없습니다.")),
        ),
    ):
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "workers", "role": "agent", "node_count": 1},
        )
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# 수정
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_nodegroup_node_count(client):
    updated = {**_NG_AGENT, "node_count": 5}
    enqueue = AsyncMock(return_value="job-1")
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_AGENT)),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock(return_value=updated)),
        patch("drover.api.nodegroups._jobs.enqueue_job", new=enqueue),
    ):
        resp = await client.patch(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_AGENT['id']}",
            json={"node_count": 5},
        )
    assert resp.status_code == 200
    assert resp.json()["node_count"] == 5
    enqueue.assert_awaited_once_with(
        cluster_id=_CLUSTER_ID,
        project_id="test-project-123",
        kind="nodegroup_reconcile",
        payload={"action": "provision", "nodegroup": updated, "add_count": 4},
        user_id="test-user-123",
        username="testuser",
    )


@pytest.mark.asyncio
async def test_update_nodegroup_not_found(client):
    with _cluster_access_ok(), patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock(return_value=None)):
        resp = await client.patch(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups/nonexistent",
            json={"node_count": 3},
        )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_create_stampede_nodegroup_requires_flavor(client):
    with _cluster_access_ok():
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "auto-workers", "role": "agent", "node_count": 0, "stampede_enabled": True},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_default_nodegroups_adds_server_and_agent_rows():
    from drover.services.nodegroup import create_default_nodegroups

    class _Scalars:
        def all(self):
            return []

    result = MagicMock()
    result.scalars.return_value = _Scalars()
    session = AsyncMock()
    session.execute.return_value = result
    session.add = MagicMock()

    await create_default_nodegroups(
        session,
        cluster_id=_CLUSTER_ID,
        server_flavor_id="server-flavor",
        server_image_id="server-image",
        agent_flavor_id="agent-flavor",
        agent_image_id="agent-image",
        agent_count=2,
    )

    added = [call.args[0] for call in session.add.call_args_list]
    assert [row.name for row in added] == ["default-server", "default-agent"]
    assert added[0].role == "server"
    assert added[0].node_count == 1
    assert added[1].role == "agent"
    assert added[1].node_count == 2
    assert added[1].flavor_id == "agent-flavor"
    assert added[0].image_id == "server-image"
    assert added[1].image_id == "agent-image"


@pytest.mark.asyncio
async def test_create_server_nodegroup_rejected(client):
    with _cluster_access_ok():
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "servers", "role": "server", "node_count": 1, "flavor_id": "flavor-001"},
        )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 삭제
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_custom_nodegroup(client):
    enqueue = AsyncMock(return_value="job-1")
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_CUSTOM)),
        patch("drover.api.nodegroups._jobs.enqueue_job", new=enqueue),
    ):
        resp = await client.delete(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_CUSTOM['id']}")
    assert resp.status_code == 204
    enqueue.assert_awaited_once_with(
        cluster_id=_CLUSTER_ID,
        project_id="test-project-123",
        kind="nodegroup_reconcile",
        payload={"action": "delete_group", "nodegroup": _NG_CUSTOM, "remove_entries": []},
        user_id="test-user-123",
        username="testuser",
    )


@pytest.mark.asyncio
async def test_delete_default_nodegroup_rejected(client):
    enqueue = AsyncMock()
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_AGENT)),
        patch("drover.api.nodegroups._jobs.enqueue_job", new=enqueue),
    ):
        resp = await client.delete(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_AGENT['id']}")
    assert resp.status_code == 422
    enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_nodegroup_not_found(client):
    with _cluster_access_ok(), patch(
        "drover.services.nodegroup.get_nodegroup",
        new=AsyncMock(return_value=None),
    ):
        resp = await client.delete(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/nonexistent")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# node_count 범위 검증
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_nodegroup_node_count_too_large(client):
    with _cluster_access_ok():
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "big-group", "role": "agent", "node_count": 99},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_update_nodegroup_node_count_too_large(client):
    with _cluster_access_ok():
        resp = await client.patch(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_AGENT['id']}",
            json={"node_count": 99},
        )
    assert resp.status_code == 422
