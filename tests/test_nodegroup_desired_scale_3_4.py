"""Focused unit tests for roadmap step 3.4 (Nodegroup desired-state scaling & operation cutover).

Covers:
1. Bound rejection (min_size, max_size, desired count bounds)
2. Enqueue not inline completion (API enqueues operation job, returns nodegroup immediately)
3. Desired-state convergence to Nova tags (reconcile_nodegroup_vms against Nova server tags)
4. Cooldown decision event (Stampede scale-up/scale-down cooldown decision recorded in events)
5. Result VM IDs (Operation & Stampede events record resulting Nova vm_ids)
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drover.models.schemas import CreateK3sNodegroupRequest, UpdateK3sNodegroupRequest
from drover.services import autoscale
from drover.services import nodegroup as nodegroup_svc

_CLUSTER_ID = "11111111-2222-3333-4444-555555555555"
_NODEGROUP_ID = "ng-9999-8888-7777-6666"

_CLUSTER = {
    "id": _CLUSTER_ID,
    "project_id": "proj-123",
    "name": "cluster-test",
    "status": "ACTIVE",
    "agent_vm_ids": ["vm-001"],
}

_NG_AGENT = {
    "id": _NODEGROUP_ID,
    "cluster_id": _CLUSTER_ID,
    "name": "default-agent",
    "role": "agent",
    "node_count": 1,
    "min_size": 1,
    "max_size": 5,
    "flavor_id": "flavor-cpu",
    "image_id": "image-ubuntu",
    "labels": {},
    "taints": [],
    "is_default": True,
    "vms": [{"vm_id": "vm-001", "name": "agent-1", "status": "ACTIVE"}],
    "stampede_state": {},
}


# ---------------------------------------------------------------------------
# 1. Bound Rejection
# ---------------------------------------------------------------------------


def test_schema_bound_rejection_node_count_outside_range():
    """CreateK3sNodegroupRequest rejects node_count < min_size or node_count > max_size."""
    with pytest.raises(ValueError, match="node_count .* 범위 밖입니다"):
        CreateK3sNodegroupRequest(name="agent-pool", role="agent", node_count=10, min_size=1, max_size=5)

    with pytest.raises(ValueError, match="node_count .* 범위 밖입니다"):
        CreateK3sNodegroupRequest(name="agent-pool", role="agent", node_count=0, min_size=2, max_size=5)


def test_schema_bound_rejection_min_greater_than_max():
    """CreateK3sNodegroupRequest rejects min_size > max_size."""
    with pytest.raises(ValueError, match="min_size는 max_size보다 클 수 없습니다"):
        CreateK3sNodegroupRequest(name="agent-pool", role="agent", node_count=3, min_size=5, max_size=2)


def test_update_schema_bound_rejection():
    """UpdateK3sNodegroupRequest rejects node_count outside updated bounds."""
    with pytest.raises(ValueError, match="node_count .* min_size .*보다 작을 수 없습니다"):
        UpdateK3sNodegroupRequest(node_count=1, min_size=2)

    with pytest.raises(ValueError, match="node_count .* max_size .*보다 클 수 없습니다"):
        UpdateK3sNodegroupRequest(node_count=8, max_size=5)


@pytest.mark.asyncio
async def test_service_bound_rejection_on_update():
    """update_nodegroup rejects node_count outside resulting bounds."""
    with patch("drover.services.nodegroup.is_db_available", return_value=True):
        mock_ng = MagicMock()
        mock_ng.role = "agent"
        mock_ng.node_count = 1
        mock_ng.flavor_id = "flavor-cpu"
        mock_ng.stampede_enabled = False
        mock_ng.min_size = 1
        mock_ng.max_size = 5

        exec_res = MagicMock()
        exec_res.scalar_one_or_none.return_value = mock_ng

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=exec_res)

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=None)

        with patch("drover.services.nodegroup.get_session_factory", return_value=MagicMock(return_value=mock_ctx)):
            # Updating node_count to 10 when max_size is 5 raises ValueError
            with pytest.raises(ValueError, match="범위 밖입니다"):
                await nodegroup_svc.update_nodegroup(_CLUSTER_ID, _NODEGROUP_ID, {"node_count": 10})


@pytest.mark.asyncio
async def test_scale_agents_rejects_count_outside_nodegroup_bounds():
    """scale_agents rejects desired_count outside min_size/max_size bounds."""
    with (
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=_CLUSTER)),
        patch("drover.services.nodegroup.get_default_agent_nodegroup_id", new=AsyncMock(return_value=_NODEGROUP_ID)),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_AGENT)),
    ):
        # min_size=1, max_size=5 -> desired_count 10 raises ValueError
        with pytest.raises(ValueError, match="outside nodegroup bounds"):
            await autoscale.scale_agents("proj-123", _CLUSTER_ID, desired_count=10)


# ---------------------------------------------------------------------------
# 2. Enqueue Not Inline Completion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_nodegroup_enqueues_operation_not_inline(client):
    """POST /v1/clusters/{cluster_id}/nodegroups enqueues job and returns nodegroup immediately."""
    enqueue = AsyncMock(return_value="job-100")
    ng_data = {**_NG_AGENT, "id": "ng-new", "node_count": 3}
    with (
        patch("drover.api.nodegroups.k3s_db.get_cluster", new=AsyncMock(return_value=_CLUSTER)),
        patch("drover.services.nodegroup.create_nodegroup", new=AsyncMock(return_value=ng_data)),
        patch("drover.api.nodegroups._jobs.enqueue_job", new=enqueue),
    ):
        resp = await client.post(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups",
            json={"name": "new-workers", "role": "agent", "node_count": 3, "min_size": 0, "max_size": 5},
        )
    assert resp.status_code == 201
    assert resp.json()["node_count"] == 3
    # Enqueued job, did not run inline provisioning
    enqueue.assert_awaited_once()
    assert enqueue.call_args.kwargs["kind"] == "nodegroup_reconcile"
    assert enqueue.call_args.kwargs["payload"]["action"] == "provision"


@pytest.mark.asyncio
async def test_update_nodegroup_enqueues_operation_not_inline(client):
    """PATCH /v1/clusters/{cluster_id}/nodegroups/{id} enqueues job and returns immediately."""
    enqueue = AsyncMock(return_value="job-101")
    updated = {**_NG_AGENT, "node_count": 4}
    with (
        patch("drover.api.nodegroups.k3s_db.get_cluster", new=AsyncMock(return_value=_CLUSTER)),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=_NG_AGENT)),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock(return_value=updated)),
        patch("drover.api.nodegroups._jobs.enqueue_job", new=enqueue),
    ):
        resp = await client.patch(
            f"/v1/clusters/{_CLUSTER_ID}/nodegroups/{_NODEGROUP_ID}",
            json={"node_count": 4},
        )
    assert resp.status_code == 200
    assert resp.json()["node_count"] == 4
    enqueue.assert_awaited_once()
    assert enqueue.call_args.kwargs["payload"]["add_count"] == 3


# ---------------------------------------------------------------------------
# 3. Desired-state Convergence to Nova Tags
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_nodegroup_vms_nova_tags_convergence():
    """reconcile_nodegroup_vms checks Nova tags/metadata and updates DB nodegroup count."""
    ng = {
        **_NG_AGENT,
        "node_count": 2,  # DB claims 2, but only 1 active in Nova
        "vms": [
            {"vm_id": "vm-active", "name": "agent-1", "status": "CREATING"},
            {"vm_id": "vm-deleted", "name": "agent-2", "status": "CREATING"},
        ],
    }

    mock_server_active = MagicMock()
    mock_server_active.status = "ACTIVE"
    mock_server_active.metadata = {"drover.cluster_id": _CLUSTER_ID, "k3s_horse_generator_nodegroup_id": _NODEGROUP_ID}

    mock_conn = MagicMock()
    with (
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=ng)),
        patch("drover.services.keystone.get_admin_connection_for_project", return_value=mock_conn),
        patch("drover.services.nova.get_server", side_effect=[mock_server_active, None]),
        patch("drover.services.nodegroup.set_nodegroup_count", new=AsyncMock()) as set_count,
    ):
        verified = await autoscale.reconcile_nodegroup_vms("proj-123", _CLUSTER_ID, _NODEGROUP_ID)

    assert len(verified) == 1
    assert verified[0]["vm_id"] == "vm-active"
    set_count.assert_awaited_once_with(_CLUSTER_ID, _NODEGROUP_ID, 1)


# ---------------------------------------------------------------------------
# 4. Cooldown Decision Event
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stampede_cooldown_decision_event_scale_up():
    """Stampede records a cooldown decision event when scale-up is inside cooldown."""
    from drover.services.stampede import _scale_up_nodegroup

    ng = {**_NG_AGENT, "node_count": 1, "max_size": 5}
    s = MagicMock()
    s.drover_stampede_scale_up_cooldown = 300

    recent_time = time.time() - 50  # 50s ago (within 300s cooldown)
    state = {"last_scale_up": recent_time}

    with (
        patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value=state)),
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
    ):
        await _scale_up_nodegroup(
            cluster_id=_CLUSTER_ID,
            project_id="proj-123",
            nodegroup=ng,
            pending_pods=[{"name": "p1"}],
            node_pods=[],
            node_capacities=[],
            s=s,
        )

    record_event.assert_awaited_once()
    kwargs = record_event.call_args.kwargs
    assert kwargs["action"] == "cooldown"
    assert kwargs["status"] == "skipped"
    assert kwargs["extra"]["reason"] == "cooldown"
    assert kwargs["extra"]["cooldown_seconds"] == 300
    assert kwargs["extra"]["remaining_seconds"] > 0


@pytest.mark.asyncio
async def test_stampede_cooldown_decision_event_scale_down():
    """Stampede records a cooldown decision event when scale-down is inside cooldown."""
    from drover.services.stampede import _scale_down_nodegroup

    ng = {**_NG_AGENT, "node_count": 3, "min_size": 1}
    s = MagicMock()
    s.drover_stampede_scale_down_cooldown = 300

    recent_time = time.time() - 30
    state = {"last_scale_down": recent_time}

    with (
        patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value=state)),
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
    ):
        await _scale_down_nodegroup(
            cluster_id=_CLUSTER_ID,
            project_id="proj-123",
            nodegroup=ng,
            node_pods=[],
            node_capacities=[{"name": "node-1", "ready": True}],
            s=s,
        )

    record_event.assert_awaited_once()
    kwargs = record_event.call_args.kwargs
    assert kwargs["action"] == "cooldown"
    assert kwargs["status"] == "skipped"
    assert kwargs["extra"]["direction"] == "scale_down"


# ---------------------------------------------------------------------------
# 5. Result VM IDs Instrumenting Operation & Stampede Events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operation_and_stampede_events_record_result_vm_ids():
    """provision_nodegroup_and_reconcile appends operation events containing resulting vm_ids."""
    created_vms = [{"vm_id": "vm-101", "name": "agent-101"}]
    with (
        patch("drover.services.autoscale.provision_nodegroup_vms", new=AsyncMock(return_value=created_vms)),
        patch("drover.services.autoscale.reconcile_nodegroup_vms", new=AsyncMock(return_value=created_vms)),
        patch("drover.services.operations.append_operation_event", new=AsyncMock()) as append_op_event,
    ):
        await autoscale.provision_nodegroup_and_reconcile(
            project_id="proj-123",
            cluster_id=_CLUSTER_ID,
            nodegroup=_NG_AGENT,
            add_count=1,
            operation_id="op-555",
            triggering_metric="manual",
        )

    append_op_event.assert_awaited_once()
    call_args = append_op_event.call_args
    assert call_args.args[1] == "op-555"
    assert call_args.kwargs["phase"] == "nodegroup_reconciled"
    assert call_args.kwargs["payload_json"]["vm_ids"] == ["vm-101"]
    assert call_args.kwargs["payload_json"]["triggering_metric"] == "manual"
