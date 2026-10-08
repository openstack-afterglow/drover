"""Nodegroup sizing bounds and authoritative Nova inventory behavior."""

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
        mock_cluster = MagicMock()
        mock_cluster.status = "ACTIVE"
        mock_cluster.server_vm_id = None
        cluster_res = MagicMock()
        cluster_res.scalar_one_or_none.return_value = mock_cluster
        active_res = MagicMock()
        active_res.scalar_one_or_none.return_value = None

        mock_ng = MagicMock()
        mock_ng.role = "agent"
        mock_ng.node_count = 1
        mock_ng.flavor_id = "flavor-cpu"
        mock_ng.stampede_enabled = False
        mock_ng.min_size = 1
        mock_ng.max_size = 5
        mock_ng.vms = []
        mock_ng.stampede_state = {}

        ng_res = MagicMock()
        ng_res.scalar_one_or_none.return_value = mock_ng

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[cluster_res, active_res, ng_res])
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
        patch("drover.services.execution.open_connection", return_value=mock_conn),
        patch("drover.services.nova.observe_server", side_effect=[mock_server_active, None]),
        patch("drover.services.nodegroup.set_nodegroup_count", new=AsyncMock()) as set_count,
    ):
        verified = await autoscale.reconcile_nodegroup_vms("proj-123", _CLUSTER_ID, _NODEGROUP_ID)

    assert len(verified) == 1
    assert verified[0]["vm_id"] == "vm-active"
    set_count.assert_awaited_once_with(_CLUSTER_ID, _NODEGROUP_ID, 1)
    mock_conn.close.assert_called_once_with()



@pytest.mark.asyncio
async def test_nodegroup_reconcile_nova_failure_preserves_desired_count():
    ng = {**_NG_AGENT, "node_count": 2, "vms": [{"vm_id": "vm-1", "name": "worker-1"}]}
    conn = MagicMock()
    with (
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=ng)),
        patch("drover.services.execution.open_connection", new=AsyncMock(return_value=conn)),
        patch("drover.services.nova.observe_server", side_effect=RuntimeError("Nova unavailable")),
        patch("drover.services.nodegroup.set_nodegroup_count", new=AsyncMock()) as set_count,
    ):
        with pytest.raises(RuntimeError, match="Nova unavailable"):
            await autoscale.reconcile_nodegroup_vms("proj-123", _CLUSTER_ID, _NODEGROUP_ID)
    set_count.assert_not_awaited()
    conn.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_nodegroup_reconcile_error_worker_still_consumes_capacity():
    ng = {**_NG_AGENT, "node_count": 1, "vms": [{"vm_id": "vm-1", "name": "worker-1"}]}
    conn = MagicMock()
    server = MagicMock(status="ERROR")
    with (
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=ng)),
        patch("drover.services.execution.open_connection", new=AsyncMock(return_value=conn)),
        patch("drover.services.nova.observe_server", return_value=server),
        patch("drover.services.nodegroup.set_nodegroup_count", new=AsyncMock()) as set_count,
    ):
        verified = await autoscale.reconcile_nodegroup_vms("proj-123", _CLUSTER_ID, _NODEGROUP_ID)
    assert verified == ng["vms"]
    set_count.assert_not_awaited()


