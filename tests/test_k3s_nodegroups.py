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
    "status": "ACTIVE",
    "project_id": "test-project-123",
    "agent_vm_ids": ["vm-001"],
    "agent_count": 2,
}


def _cluster_access_ok():
    return patch("drover.api.nodegroups.k3s_db.get_cluster", new=AsyncMock(return_value=_CLUSTER))


@pytest.fixture(autouse=True)
def nodegroup_api_boundaries(monkeypatch):
    monkeypatch.setattr("drover.api.nodegroups.is_db_available", lambda: True)

    async def validate_id(conn, key, identifier):
        return {"id": identifier, "name": "test resource"}

    monkeypatch.setattr("drover.api.nodegroups.resource_policies.validate_existing_selection", validate_id)




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
    create = AsyncMock(return_value=_NG_CUSTOM)
    with _cluster_access_ok(), patch("drover.services.nodegroup.create_nodegroup", new=create):
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "gpu-workers", "role": "agent", "node_count": 3, "flavor_id": "flavor-gpu"},
        )
    assert resp.status_code == 201
    assert resp.json()["name"] == "gpu-workers"
    assert resp.json()["node_count"] == 3
    assert resp.json()["is_default"] is False
    assert create.await_args.kwargs["project_id"] == "test-project-123"


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
    update = AsyncMock(return_value=updated)
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_AGENT)),
        patch("drover.services.nodegroup.update_nodegroup", new=update),
    ):
        resp = await client.patch(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_AGENT['id']}", json={"node_count": 5},
        )
    assert resp.status_code == 200
    assert resp.json()["node_count"] == 5
    assert update.await_args.kwargs["project_id"] == "test-project-123"


@pytest.mark.asyncio
async def test_update_nodegroup_not_found(client):
    with _cluster_access_ok(), patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=None)):
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
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.enqueue_nodegroup_delete", new=AsyncMock(return_value=True)),
    ):
        resp = await client.delete(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_CUSTOM['id']}")
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_delete_default_nodegroup_rejected(client):
    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.enqueue_nodegroup_delete", new=AsyncMock(side_effect=ValueError("default group"))),
    ):
        resp = await client.delete(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_AGENT['id']}")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_delete_nodegroup_not_found(client):
    with _cluster_access_ok(), patch(
        "drover.services.nodegroup.enqueue_nodegroup_delete",
        new=AsyncMock(return_value=False),
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


def _service_session(monkeypatch, values):
    session = MagicMock()
    results = []
    for value in values:
        result = MagicMock()
        result.scalar_one_or_none.return_value = value
        results.append(result)
    session.execute = AsyncMock(side_effect=results)
    session.refresh = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.begin.return_value = session
    monkeypatch.setattr("drover.services.nodegroup.is_db_available", lambda: True)
    monkeypatch.setattr("drover.services.nodegroup.get_session_factory", lambda: lambda: session)
    return session


def _service_group():
    from drover.models.orm import K3sNodegroup, K3sNodegroupVM

    return K3sNodegroup(
        id="group-1", cluster_id=_CLUSTER_ID, name="workers", role="agent",
        node_count=2, min_size=1, max_size=5, flavor_id="flavor-cpu", image_id=None,
        stampede_enabled=True, stampede_state={}, is_default=False,
        vms=[K3sNodegroupVM(vm_id="vm-1", name="worker-1", status="ACTIVE")],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("updates", [{"min_size": 3}, {"max_size": 1}, {"node_count": 6}])
async def test_service_rejects_invalid_merged_sizes(monkeypatch, updates):
    from types import SimpleNamespace

    from drover.services import nodegroup

    group = _service_group()
    session = _service_session(monkeypatch, [SimpleNamespace(status="ACTIVE"), None, group])
    with pytest.raises(ValueError, match="범위 밖"):
        await nodegroup.update_nodegroup(_CLUSTER_ID, group.id, updates)
    assert group.node_count == 2
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_service_rejects_live_flavor_change(monkeypatch):
    from types import SimpleNamespace

    from drover.services import nodegroup

    group = _service_group()
    session = _service_session(monkeypatch, [SimpleNamespace(status="ACTIVE"), None, group])
    with pytest.raises(nodegroup.NodegroupConflict, match="flavor_id"):
        await nodegroup.update_nodegroup(_CLUSTER_ID, group.id, {"flavor_id": "flavor-gpu"})
    assert group.flavor_id == "flavor-cpu"
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_service_rejects_mutation_during_active_job(monkeypatch):
    from types import SimpleNamespace

    from drover.services import nodegroup

    session = _service_session(monkeypatch, [SimpleNamespace(status="ACTIVE"), "job-1"])
    with pytest.raises(nodegroup.NodegroupConflict, match="진행 중"):
        await nodegroup.update_nodegroup(_CLUSTER_ID, "group-1", {"max_size": 8})
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_service_rejects_state_only_inflight_mutation(monkeypatch):
    from types import SimpleNamespace

    from drover.services import nodegroup

    group = _service_group()
    group.stampede_state = {"in_flight_count": 1}
    _service_session(monkeypatch, [SimpleNamespace(status="ACTIVE"), None, group])
    with pytest.raises(nodegroup.NodegroupConflict, match="Stampede"):
        await nodegroup.update_nodegroup(_CLUSTER_ID, group.id, {"stampede_enabled": False})


@pytest.mark.asyncio
async def test_service_rolls_back_sizing_when_enqueue_unavailable(monkeypatch):
    from types import SimpleNamespace

    from drover.services import nodegroup

    group = _service_group()
    session = _service_session(monkeypatch, [SimpleNamespace(status="ACTIVE", project_id="project-1"), None, group])
    with patch("drover.services.jobs.enqueue_job", new=AsyncMock(side_effect=RuntimeError("DB unavailable"))):
        with pytest.raises(RuntimeError):
            await nodegroup.update_nodegroup(_CLUSTER_ID, group.id, {"node_count": 3}, project_id="project-1")
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_service_manual_scale_persists_observable_job_state(monkeypatch):
    from types import SimpleNamespace

    from drover.services import nodegroup

    group = _service_group()
    session = _service_session(monkeypatch, [SimpleNamespace(status="ACTIVE", project_id="project-1"), None,
                                             group, "operation-1"])
    with patch("drover.services.jobs.enqueue_job", new=AsyncMock(return_value="job-1")):
        result = await nodegroup.update_nodegroup(_CLUSTER_ID, group.id, {"node_count": 3}, project_id="project-1")
    assert result["node_count"] == 3
    assert result["stampede_state"]["in_flight_count"] == 2
    assert result["stampede_state"]["last_job_id"] == "job-1"
    assert result["stampede_state"]["last_operation_id"] == "operation-1"
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_nodegroup_api_storage_unavailable_never_calls_mutation(client, monkeypatch):
    monkeypatch.setattr("drover.api.nodegroups.is_db_available", lambda: False)
    mutate = AsyncMock()
    with _cluster_access_ok(), patch("drover.services.nodegroup.create_nodegroup", new=mutate):
        response = await client.post(f"/v1/clusters/{_CLUSTER_ID}/nodegroups", json={"name": "workers"})
    assert response.status_code == 503
    mutate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", ["", "name with spaces", "a" * 65])
async def test_nodegroup_rejects_invalid_identifiers(client, identifier):
    with _cluster_access_ok():
        response = await client.post(f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
                                     json={"name": "workers", "flavor_id": identifier})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_nodegroup_rejects_nonexistent_image(client):
    from drover.services.resource_policies import ResourcePolicyValidationError

    mutate = AsyncMock()
    with (
        _cluster_access_ok(), patch("drover.services.nodegroup.create_nodegroup", new=mutate),
        patch("drover.api.nodegroups.resource_policies.validate_existing_selection",
              new=AsyncMock(side_effect=ResourcePolicyValidationError("missing image"))),
    ):
        response = await client.post(f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
                                     json={"name": "workers", "image_id": "missing-image"})
    assert response.status_code == 422
    mutate.assert_not_awaited()


@pytest.mark.asyncio
async def test_nodegroup_api_conflict_is_not_success(client):
    from drover.services.nodegroup import NodegroupConflict

    with (
        _cluster_access_ok(),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_AGENT)),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock(side_effect=NodegroupConflict("busy"))),
    ):
        response = await client.patch(f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NG_AGENT['id']}", json={"max_size": 8})
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_service_rejects_default_server_flavor_change_with_untracked_primary(monkeypatch):
    from types import SimpleNamespace

    from drover.services import nodegroup

    group = _service_group()
    group.role = "server"
    group.name = "default-server"
    group.is_default = True
    group.node_count = 1
    group.min_size = 1
    group.max_size = 1
    group.stampede_enabled = False
    group.vms = []
    cluster = SimpleNamespace(status="ACTIVE", server_vm_id="server-vm-1")
    session = _service_session(monkeypatch, [cluster, None, group])
    with pytest.raises(nodegroup.NodegroupConflict, match="flavor_id"):
        await nodegroup.update_nodegroup(_CLUSTER_ID, group.id, {"flavor_id": "different-flavor"})
    assert group.flavor_id == "flavor-cpu"
    session.commit.assert_not_awaited()
