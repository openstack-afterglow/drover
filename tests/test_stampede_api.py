"""Behavioral Stampede configuration/status contracts; no live services."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drover.services import nodegroup

CLUSTER_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
PROJECT_ID = "test-project-123"


def _group(**updates):
    values = dict(
        id="group-1", cluster_id=CLUSTER_ID, name="cpu-workers", role="agent",
        flavor_id="flavor-cpu", image_id=None, node_count=2, min_size=1, max_size=5,
        stampede_enabled=True, stampede_state={}, vms=[],
    )
    values.update(updates)
    return SimpleNamespace(**values)


@pytest.fixture(autouse=True)
def stampede_api_boundaries(monkeypatch, mock_conn):
    monkeypatch.setattr("drover.db.is_db_available", lambda: True)
    monkeypatch.setattr("drover.api.clusters.get_settings", lambda: SimpleNamespace(
        drover_stampede_enabled=True, drover_stampede_interval=30,
        drover_stampede_scale_down_window=600, drover_stampede_scale_up_cooldown=120,
        drover_stampede_scale_down_cooldown=300, drover_stampede_scale_down_threshold=0.5,
        drover_stampede_resource_headroom_factor=0.3,
    ))

    async def validate_id(conn, key, identifier):
        return {"id": identifier, "name": "resource"}

    monkeypatch.setattr("drover.api.clusters.resource_policies.validate_existing_selection", validate_id)
    # Project-scoped Nova resolves flavors the project may use, including shared private ones.
    mock_conn.compute.get_flavor.side_effect = lambda flavor_id: SimpleNamespace(id=flavor_id, name="flavor", is_public=False)
    monkeypatch.setattr("drover.api.clusters.k3s_cluster.get_cluster", AsyncMock(return_value={
        "id": CLUSTER_ID, "project_id": PROJECT_ID, "status": "ACTIVE", "stampede_enabled": True,
    }))
    monkeypatch.setattr("drover.api.clusters.invalidate", AsyncMock())
    monkeypatch.setattr("drover.api.clusters.rec", AsyncMock())


def _database(monkeypatch, cluster, groups):
    session = MagicMock()
    cluster_result = MagicMock()
    cluster_result.scalar_one_or_none.return_value = cluster
    groups_result = MagicMock()
    groups_result.scalars.return_value.all.return_value = groups
    session.execute = AsyncMock(side_effect=[cluster_result, groups_result])
    session.scalar = AsyncMock(return_value="credential-control")
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.begin.return_value = session
    monkeypatch.setattr("drover.db.get_session_factory", lambda: lambda: session)
    return session


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["enable", "disable"])
async def test_stampede_unavailable_db_never_succeeds(client, monkeypatch, route):
    monkeypatch.setattr("drover.db.is_db_available", lambda: False)
    mutate = AsyncMock()
    with patch("drover.api.clusters._set_stampede_enabled", new=mutate):
        response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/{route}")
    assert response.status_code == 503
    mutate.assert_not_awaited()


@pytest.mark.asyncio
async def test_stampede_cannot_access_another_project(client):
    mutate = AsyncMock()
    with (
        patch("drover.api.clusters.k3s_cluster.get_cluster", new=AsyncMock(return_value=None)),
        patch("drover.api.clusters._set_stampede_enabled", new=mutate),
    ):
        response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 404
    mutate.assert_not_awaited()


@pytest.mark.asyncio
async def test_stampede_enable_requires_global_policy(client, monkeypatch):
    monkeypatch.setattr("drover.api.clusters.get_settings", lambda: SimpleNamespace(drover_stampede_enabled=False))
    mutate = AsyncMock()
    with patch("drover.api.clusters._set_stampede_enabled", new=mutate):
        response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 400
    mutate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["CREATING", "SCALING", "ERROR", "DELETING"])
async def test_stampede_enable_requires_active_cluster(client, monkeypatch, status):
    cluster = SimpleNamespace(status=status, stampede_enabled=False)
    _database(monkeypatch, cluster, [_group()])
    response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 409
    assert cluster.stampede_enabled is False


@pytest.mark.asyncio
@pytest.mark.parametrize("groups", [[], [_group(flavor_id=None)], [_group(role="server")],
                                   [_group(min_size=3)], [_group(max_size=1)]])
async def test_stampede_enable_requires_valid_configured_groups(client, monkeypatch, groups):
    cluster = SimpleNamespace(status="ACTIVE", stampede_enabled=False)
    _database(monkeypatch, cluster, groups)
    response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 422
    assert cluster.stampede_enabled is False


@pytest.mark.asyncio
async def test_stampede_enable_persists_even_when_redis_auxiliary_fails(client, monkeypatch):
    cluster = SimpleNamespace(status="ACTIVE", stampede_enabled=False)
    _database(monkeypatch, cluster, [_group()])
    monkeypatch.setattr("drover.api.clusters.invalidate", AsyncMock(side_effect=ConnectionError("Redis unavailable")))
    response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 200
    assert response.json()["stampede_enabled"] is True
    assert cluster.stampede_enabled is True


@pytest.mark.asyncio
async def test_stampede_enable_requires_reauthorized_control_credential(client, monkeypatch):
    cluster = SimpleNamespace(status="ACTIVE", stampede_enabled=False)
    session = _database(monkeypatch, cluster, [_group()])
    session.scalar.return_value = None
    response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 409
    assert "reauthorize" in response.json()["detail"]
    assert cluster.stampede_enabled is False



@pytest.mark.asyncio
async def test_stampede_disable_allowed_during_scaling(client, monkeypatch):
    cluster = SimpleNamespace(status="SCALING", stampede_enabled=True)
    _database(monkeypatch, cluster, [])
    response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/disable")
    assert response.status_code == 200
    assert cluster.stampede_enabled is False


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["stampede", "stampede/status"])
async def test_stampede_status_exposes_policy_counts_and_decision(client, suffix):
    state = {
        "observed_at": 1791000000.0, "ready_count": 1, "tracked_count": 99,
        "in_flight_count": 1, "last_decision": "scale_up", "last_operation_id": "operation-1",
        "last_job_id": "job-1", "last_blocked_reason": "", "idle_since": {"worker-1": 1790999700.0},
        "capacity": {"allocatable": {"cpu_m": 4000}, "requested": {"cpu_m": 3000}, "free": {"cpu_m": 1000}},
        "pending_assignments": [{"pod": "app/worker", "nodegroup_id": "group-1"}],
        "blocked_reasons": ["max_size_reached"], "flavor_summary": {"gpu": 0}, "quota_state": {"allowed": True},
    }
    group = vars(_group(stampede_state=state, vms=[{"vm_id": "vm-1"}, {"vm_id": "vm-2"}]))
    with (
        patch("drover.services.nodegroup.list_nodegroups", new=AsyncMock(return_value=[group])),
        patch("drover.services.jobs.list_active_mutation_jobs", new=AsyncMock(return_value=[
            {"id": "job-1", "operation_id": "operation-1", "nodegroup_id": "group-1"},
        ])),
    ):
        response = await client.get(f"/v1/clusters/{CLUSTER_ID}/{suffix}")
    assert response.status_code == 200
    data = response.json()
    assert data["policy"]["interval"] == 30
    assert data["policy"]["scale_down_window"] == 600
    assert data["policy"]["scale_down_threshold"] == 0.5
    observed = data["nodegroups"][0]
    assert observed["desired_count"] == 2
    assert observed["tracked_count"] == 2
    assert observed["ready_count"] == 1
    assert observed["in_flight"] == 1
    assert observed["observed_at"] == 1791000000.0
    assert observed["last_operation_id"] == "operation-1"
    assert observed["active_operation_ids"] == ["operation-1"]
    assert observed["capacity"] == state["capacity"]
    assert observed["blocked_reasons"] == ["max_size_reached"]
    assert observed["stampede_state"]["idle_since"] == state["idle_since"]


@pytest.mark.asyncio
async def test_stampede_status_does_not_infer_ready_from_vm_status(client):
    group = vars(_group(vms=[{"vm_id": "vm-1", "status": "ACTIVE"}]))
    with (
        patch("drover.services.nodegroup.list_nodegroups", new=AsyncMock(return_value=[group])),
        patch("drover.services.jobs.list_active_mutation_jobs", new=AsyncMock(return_value=[])),
    ):
        response = await client.get(f"/v1/clusters/{CLUSTER_ID}/stampede/status")
    assert response.status_code == 200
    assert response.json()["nodegroups"][0]["ready_count"] is None
    assert response.json()["nodegroups"][0]["observed_at"] is None


@pytest.mark.asyncio
async def test_stampede_events_are_best_effort(client):
    with patch("drover.services.activity.list_stampede_events", new=AsyncMock(side_effect=ConnectionError("Redis"))):
        response = await client.get(f"/v1/clusters/{CLUSTER_ID}/stampede/events")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_stampede_state_merge_preserves_concurrent_job_and_planner_updates(monkeypatch):
    shared = {"last_scale_up": 100.0, "last_job_id": "previous-job"}
    row_lock = asyncio.Lock()

    class Session:
        def __init__(self):
            self.row = None
            self.locked = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            if self.row is not None and exc_type is None:
                shared.clear()
                shared.update(self.row.stampede_state)
            if self.locked:
                self.locked = False
                row_lock.release()
            self.row = None

        def begin(self):
            return self

        async def execute(self, statement):
            if statement._for_update_arg is not None:
                await row_lock.acquire()
                self.locked = True
            self.row = SimpleNamespace(stampede_state=dict(shared), updated_at=None)
            await asyncio.sleep(0)
            result = MagicMock()
            result.scalar_one_or_none.return_value = self.row
            return result

    monkeypatch.setattr("drover.services.nodegroup.is_db_available", lambda: True)
    monkeypatch.setattr("drover.services.nodegroup.get_session_factory", lambda: Session)
    await asyncio.gather(
        nodegroup.merge_stampede_state(CLUSTER_ID, "group-1", {"capacity": {"free": {"cpu_m": 1000}}}),
        nodegroup.merge_stampede_state(CLUSTER_ID, "group-1", {"last_job_id": "new-job", "in_flight_count": 1}),
    )
    assert shared["last_scale_up"] == 100.0
    assert shared["last_job_id"] == "new-job"
    assert shared["in_flight_count"] == 1
    assert shared["capacity"]["free"]["cpu_m"] == 1000


@pytest.mark.asyncio
async def test_stampede_state_merge_unavailable_db_raises(monkeypatch):
    monkeypatch.setattr("drover.services.nodegroup.is_db_available", lambda: False)
    with pytest.raises(RuntimeError, match="MariaDB"):
        await nodegroup.merge_stampede_state(CLUSTER_ID, "group-1", {"in_flight_count": 0})


@pytest.mark.asyncio
async def test_stampede_enable_failed_transaction_does_not_report_success(client, monkeypatch):
    cluster = SimpleNamespace(status="ACTIVE", stampede_enabled=False)
    session = _database(monkeypatch, cluster, [_group()])
    session.execute = AsyncMock(side_effect=RuntimeError("database connection lost"))
    response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 503
    assert cluster.stampede_enabled is False


@pytest.mark.asyncio
async def test_stampede_policy_denial_precedes_mutation(client):
    from fastapi import HTTPException

    mutate = AsyncMock()
    with (
        patch("drover.api.clusters.authorize", side_effect=HTTPException(status_code=403, detail="denied")),
        patch("drover.api.clusters._set_stampede_enabled", new=mutate),
    ):
        response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 403
    mutate.assert_not_awaited()


@pytest.mark.asyncio
async def test_stampede_enable_rejects_unavailable_flavor(client, monkeypatch, mock_conn):
    from openstack.exceptions import NotFoundException

    cluster = SimpleNamespace(status="ACTIVE", stampede_enabled=False)
    _database(monkeypatch, cluster, [_group()])
    mock_conn.compute.get_flavor.side_effect = NotFoundException("flavor not visible to project")
    response = await client.post(f"/v1/clusters/{CLUSTER_ID}/stampede/enable")
    assert response.status_code == 422
    assert cluster.stampede_enabled is False
