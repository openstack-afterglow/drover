"""Stampede 오토스케일 단위 테스트."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app

# ---------------------------------------------------------------------------
# 공통 픽스처
# ---------------------------------------------------------------------------


def _make_cluster(stampede_enabled=True):
    return {
        "id": "cl-1",
        "name": "testcluster",
        "status": "ACTIVE",
        "status_reason": None,
        "server_vm_id": "vm-server",
        "agent_vm_ids": [],
        "agent_count": 0,
        "server_ip": "10.0.0.1",
        "network_id": "net-1",
        "security_group_id": "sg-1",
        "key_name": None,
        "ssh_public_key": None,
        "k3s_version": "v1.31.4+k3s1",
        "os_type": "ubuntu",
        "agent_flavor_id": "fl-small",
        "occm_enabled": False,
        "stampede_enabled": stampede_enabled,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }


def _make_nodegroup(stampede_enabled=True, node_count=0, min_size=1, max_size=3):
    return {
        "id": "ng-1",
        "cluster_id": "cl-1",
        "name": "auto-workers",
        "role": "agent",
        "node_count": node_count,
        "flavor_id": "fl-small",
        "image_id": None,
        "labels": {"env": "test"},
        "taints": [],
        "is_default": False,
        "stampede_enabled": stampede_enabled,
        "min_size": min_size,
        "max_size": max_size,
        "stampede_state": {},
        "vms": [],
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# k3s_stampede 로직 단위 테스트
# ---------------------------------------------------------------------------


def test_node_matches_nodegroup_no_selector():
    """nodeSelector 없는 pod는 어느 nodegroup에나 매칭."""
    from drover.services.stampede import _node_matches_nodegroup

    pod = {"node_selector": {}, "tolerations": []}
    ng = {"labels": {}, "taints": []}
    assert _node_matches_nodegroup(pod, ng) is True


def test_node_matches_nodegroup_selector_match():
    """nodeSelector가 nodegroup labels를 만족하면 매칭."""
    from drover.services.stampede import _node_matches_nodegroup

    pod = {"node_selector": {"env": "test"}, "tolerations": []}
    ng = {"labels": {"env": "test", "zone": "a"}, "taints": []}
    assert _node_matches_nodegroup(pod, ng) is True


def test_node_matches_nodegroup_selector_no_match():
    """nodeSelector가 nodegroup labels와 불일치면 매칭 실패."""
    from drover.services.stampede import _node_matches_nodegroup

    pod = {"node_selector": {"env": "prod"}, "tolerations": []}
    ng = {"labels": {"env": "test"}, "taints": []}
    assert _node_matches_nodegroup(pod, ng) is False


def test_node_matches_nodegroup_taint_no_toleration():
    """nodegroup에 NoSchedule taint 있고 pod에 toleration 없으면 매칭 실패."""
    from drover.services.stampede import _node_matches_nodegroup

    pod = {"node_selector": {}, "tolerations": []}
    ng = {"labels": {}, "taints": [{"key": "dedicated", "value": "gpu", "effect": "NoSchedule"}]}
    assert _node_matches_nodegroup(pod, ng) is False


def test_node_matches_nodegroup_taint_with_toleration():
    """pod에 적절한 toleration이 있으면 taint 있어도 매칭."""
    from drover.services.stampede import _node_matches_nodegroup

    pod = {
        "node_selector": {},
        "tolerations": [{"key": "dedicated", "operator": "Equal", "value": "gpu", "effect": "NoSchedule"}],
    }
    ng = {"labels": {}, "taints": [{"key": "dedicated", "value": "gpu", "effect": "NoSchedule"}]}
    assert _node_matches_nodegroup(pod, ng) is True


def test_is_pvc_issue_detected():
    """PVC 관련 메시지면 PVC 이슈로 판정."""
    from drover.services.stampede import _is_pvc_issue

    pod = {"message": "0/3 nodes are available: pod has unbound immediate PersistentVolumeClaims"}
    assert _is_pvc_issue(pod) is True


def test_is_pvc_issue_not_pvc():
    """일반 리소스 부족은 PVC 이슈 아님."""
    from drover.services.stampede import _is_pvc_issue

    pod = {"message": "Insufficient cpu"}
    assert _is_pvc_issue(pod) is False


def test_select_flavor_basic():
    """pending pod에 맞는 최소 flavor 선택."""
    from drover.services.stampede import _select_flavor

    pending = [{"resource_requests": {"cpu_m": 500, "memory_bytes": 512 * 1024**2, "gpu": 0}}]
    flavors = [
        {"id": "small", "name": "m1.small", "vcpus_m": 1000, "ram_bytes": 1024**3, "gpu": 0},
        {"id": "large", "name": "m1.large", "vcpus_m": 4000, "ram_bytes": 8 * 1024**3, "gpu": 0},
    ]
    ng = {"node_count": 0, "vms": []}
    result = _select_flavor(pending, [], flavors, ng, headroom_factor=0.3)
    assert result is not None
    assert result["id"] == "small"  # 최소 적합 flavor


def test_select_flavor_no_fit():
    """모든 flavor가 너무 작으면 None 반환."""
    from drover.services.stampede import _select_flavor

    pending = [{"resource_requests": {"cpu_m": 100_000, "memory_bytes": 1024**4, "gpu": 0}}]
    flavors = [{"id": "tiny", "name": "m1.tiny", "vcpus_m": 500, "ram_bytes": 512 * 1024**2, "gpu": 0}]
    ng = {"node_count": 0, "vms": []}
    result = _select_flavor(pending, [], flavors, ng, headroom_factor=0.3)
    assert result is None


def test_select_flavor_gpu_required():
    """GPU pod는 GPU flavor만 선택."""
    from drover.services.stampede import _select_flavor

    pending = [{"resource_requests": {"cpu_m": 500, "memory_bytes": 512 * 1024**2, "gpu": 1}}]
    flavors = [
        {"id": "cpu", "name": "m1.cpu", "vcpus_m": 2000, "ram_bytes": 4 * 1024**3, "gpu": 0},
        {"id": "gpu", "name": "g1.gpu", "vcpus_m": 2000, "ram_bytes": 4 * 1024**3, "gpu": 1},
    ]
    ng = {"node_count": 0, "vms": []}
    result = _select_flavor(pending, [], flavors, ng, headroom_factor=0.3)
    assert result is not None
    assert result["id"] == "gpu"


def test_assign_pending_pods_single_best_nodegroup_for_no_selector():
    from drover.services.stampede import _assign_pending_pods

    pod = {
        "name": "web",
        "namespace": "default",
        "node_selector": {},
        "tolerations": [],
        "affinity": {},
        "resource_requests": {"cpu_m": 500, "memory_bytes": 512 * 1024**2, "gpu": 0},
    }
    ngs = [
        {**_make_nodegroup(), "id": "small", "flavor_id": "small", "vms": []},
        {**_make_nodegroup(), "id": "large", "flavor_id": "large", "vms": []},
    ]
    flavors = {
        "small": {"id": "small", "vcpus_m": 1000, "ram_bytes": 1024**3, "gpu": 0},
        "large": {"id": "large", "vcpus_m": 4000, "ram_bytes": 8 * 1024**3, "gpu": 0},
    }
    assignments, blocked, _ = _assign_pending_pods([pod], ngs, flavors, [], [])
    assert blocked == []
    assert assignments["small"] == [pod]
    assert assignments["large"] == []


def test_assign_pending_pods_gpu_requires_gpu_nodegroup():
    from drover.services.stampede import _assign_pending_pods

    pod = {
        "name": "trainer",
        "namespace": "ml",
        "node_selector": {},
        "tolerations": [],
        "affinity": {},
        "resource_requests": {"cpu_m": 500, "memory_bytes": 512 * 1024**2, "gpu": 1},
    }
    ngs = [
        {**_make_nodegroup(), "id": "cpu-ng", "flavor_id": "cpu", "vms": []},
        {**_make_nodegroup(), "id": "gpu-ng", "flavor_id": "gpu", "labels": {"accelerator": "gpu"}, "vms": []},
    ]
    flavors = {
        "cpu": {"id": "cpu", "vcpus_m": 2000, "ram_bytes": 4 * 1024**3, "gpu": 0},
        "gpu": {"id": "gpu", "vcpus_m": 2000, "ram_bytes": 4 * 1024**3, "gpu": 1},
    }
    assignments, blocked, _ = _assign_pending_pods([pod], ngs, flavors, [], [])
    assert blocked == []
    assert assignments["cpu-ng"] == []
    assert assignments["gpu-ng"] == [pod]


def test_binpack_counts_gpu_memory_and_cpu():
    from drover.services.stampede import _binpack_count

    pods = [
        {"resource_requests": {"cpu_m": 1000, "memory_bytes": 1024**3, "gpu": 1}},
        {"resource_requests": {"cpu_m": 1000, "memory_bytes": 1024**3, "gpu": 1}},
        {"resource_requests": {"cpu_m": 1000, "memory_bytes": 1024**3, "gpu": 1}},
    ]
    assert _binpack_count(pods, {"vcpus_m": 4000, "ram_bytes": 8 * 1024**3, "gpu": 2}) == 2


def test_assign_pending_pods_required_node_affinity_matches_labels_only():
    from drover.services.stampede import _assign_pending_pods

    pod = {
        "name": "affinity",
        "namespace": "default",
        "node_selector": {},
        "tolerations": [],
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{"matchExpressions": [{"key": "disk", "operator": "In", "values": ["nvme"]}]}]
                }
            }
        },
        "resource_requests": {"cpu_m": 100, "memory_bytes": 128 * 1024**2, "gpu": 0},
    }
    ngs = [
        {**_make_nodegroup(), "id": "ssd", "flavor_id": "small", "labels": {"disk": "ssd"}, "vms": []},
        {**_make_nodegroup(), "id": "nvme", "flavor_id": "small", "labels": {"disk": "nvme"}, "vms": []},
    ]
    flavors = {"small": {"id": "small", "vcpus_m": 1000, "ram_bytes": 1024**3, "gpu": 0}}
    assignments, blocked, _ = _assign_pending_pods([pod], ngs, flavors, [], [])
    assert blocked == []
    assert assignments["ssd"] == []
    assert assignments["nvme"] == [pod]


def test_assign_pending_pods_reports_pinned_and_unmatched_blocked_reasons():
    from drover.services.stampede import _assign_pending_pods

    pods = [
        {
            "name": "pinned",
            "namespace": "default",
            "node_name": "missing-node",
            "node_selector": {},
            "tolerations": [],
            "affinity": {},
            "resource_requests": {"cpu_m": 100, "memory_bytes": 128 * 1024**2, "gpu": 0},
            "message": "Insufficient cpu",
        },
        {
            "name": "arm-only",
            "namespace": "default",
            "node_selector": {"arch": "arm64"},
            "tolerations": [],
            "affinity": {},
            "resource_requests": {"cpu_m": 100, "memory_bytes": 128 * 1024**2, "gpu": 0},
            "message": "Insufficient cpu",
        },
    ]
    ngs = [
        {
            **_make_nodegroup(),
            "id": "amd64",
            "flavor_id": "small",
            "labels": {"arch": "amd64"},
            "vms": [{"vm_id": "vm-1", "name": "node-1", "status": "ACTIVE"}],
        }
    ]
    flavors = {"small": {"id": "small", "vcpus_m": 1000, "ram_bytes": 1024**3, "gpu": 0}}

    assignments, blocked, _ = _assign_pending_pods(pods, ngs, flavors, [], [])

    assert assignments["amd64"] == []
    assert [(item["pod"]["name"], item["reason"]) for item in blocked] == [
        ("pinned", "pinned_missing_node"),
        ("arm-only", "no_matching_nodegroup"),
    ]


@pytest.mark.asyncio
async def test_scale_up_gpu_pending_pod_ignores_cpu_only_free_capacity():

    from drover.services.stampede import _scale_up_nodegroup

    ng = {
        **_make_nodegroup(node_count=1, max_size=3),
        "id": "gpu-ng",
        "name": "gpu-workers",
        "flavor_id": "gpu",
        "vms": [{"vm_id": "vm-1", "name": "gpu-1", "status": "ACTIVE"}],
    }
    pending = [
        {
            "name": "trainer",
            "namespace": "ml",
            "node_selector": {},
            "tolerations": [],
            "affinity": {},
            "resource_requests": {"cpu_m": 500, "memory_bytes": 512 * 1024**2, "gpu": 1},
            "message": "Insufficient nvidia.com/gpu",
        }
    ]
    node_capacities = [
        {
            "name": "gpu-1",
            "ready": True,
            "allocatable": {"cpu_m": 4000, "memory_bytes": 8 * 1024**3, "gpu": 0},
        }
    ]
    s = MagicMock()
    s.drover_stampede_scale_up_cooldown = 0


    with (
        patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value={})),
        patch(
            "drover.services.stampede._get_available_flavors",
            new=AsyncMock(
                return_value=[{"id": "gpu", "name": "gpu.large", "vcpus_m": 4000, "ram_bytes": 8 * 1024**3, "gpu": 1}]
            ),
        ),
        patch("drover.services.stampede._check_gpu_quota_for_nodes", new=AsyncMock(return_value=(True, ""))),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock()) as update_nodegroup,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
        patch("drover.services.jobs.enqueue_job", new=AsyncMock()) as enqueue_job,
    ):
        await _scale_up_nodegroup(
            cluster_id="cl-1",
            project_id="proj-1",
            nodegroup=ng,
            pending_pods=pending,
            node_pods=[],
            node_capacities=node_capacities,
            s=s,
        )

    update_nodegroup.assert_awaited_once_with("cl-1", "gpu-ng", {"node_count": 2})
    assert any(call.args[2]["in_flight_count"] == 1 for call in update_state.await_args_list)
    assert record_event.await_args.kwargs["action"] == "scale_up"
    assert record_event.await_args.kwargs["status"] == "started"
    enqueue_job.assert_awaited_once_with(
        cluster_id="cl-1",
        project_id="proj-1",
        kind="stampede_provision",
        payload={
            "nodegroup_id": "gpu-ng",
            "add_count": 1,
            "flavor_id": "gpu",
            "image_id": None,
            "labels": {"env": "test"},
            "taints": [],
            "gpu_required": True,
        },
        user_id="stampede-system",
        username="Stampede",
    )


@pytest.mark.asyncio
async def test_scale_up_caps_binpacked_nodes_by_in_flight_capacity():
    import time

    from drover.services.stampede import _scale_up_nodegroup

    ng = {**_make_nodegroup(node_count=1, max_size=3), "id": "cpu-ng", "flavor_id": "small"}
    pending = [
        {
            "name": "job-a",
            "namespace": "batch",
            "node_selector": {},
            "tolerations": [],
            "affinity": {},
            "resource_requests": {"cpu_m": 1500, "memory_bytes": 256 * 1024**2, "gpu": 0},
            "message": "Insufficient cpu",
        },
        {
            "name": "job-b",
            "namespace": "batch",
            "node_selector": {},
            "tolerations": [],
            "affinity": {},
            "resource_requests": {"cpu_m": 1500, "memory_bytes": 256 * 1024**2, "gpu": 0},
            "message": "Insufficient cpu",
        },
    ]
    s = MagicMock()
    s.drover_stampede_scale_up_cooldown = 0


    with (
        patch(
            "drover.services.stampede._get_stampede_state",
            new=AsyncMock(return_value={"in_flight_count": 1, "in_flight_since": time.time()}),
        ),
        patch(
            "drover.services.stampede._get_available_flavors",
            new=AsyncMock(
                return_value=[{"id": "small", "name": "cpu.small", "vcpus_m": 2000, "ram_bytes": 2 * 1024**3, "gpu": 0}]
            ),
        ),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock()) as update_nodegroup,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
        patch("drover.services.jobs.enqueue_job", new=AsyncMock()) as enqueue_job,
    ):
        await _scale_up_nodegroup(
            cluster_id="cl-1",
            project_id="proj-1",
            nodegroup=ng,
            pending_pods=pending,
            node_pods=[],
            node_capacities=[],
            s=s,
        )

    update_nodegroup.assert_awaited_once_with("cl-1", "cpu-ng", {"node_count": 2})
    blocked_event, started_event = [call.kwargs for call in record_event.await_args_list]
    assert blocked_event["action"] == "blocked"
    assert blocked_event["extra"]["reason"] == "max_size_cap"
    assert blocked_event["extra"]["requested_nodes"] == 2
    assert blocked_event["extra"]["add_count"] == 1
    assert started_event["action"] == "scale_up"
    assert started_event["status"] == "started"
    assert any(call.args[2]["in_flight_count"] == 2 for call in update_state.await_args_list)
    enqueue_job.assert_awaited_once_with(
        cluster_id="cl-1",
        project_id="proj-1",
        kind="stampede_provision",
        payload={
            "nodegroup_id": "cpu-ng",
            "add_count": 1,
            "flavor_id": "small",
            "image_id": None,
            "labels": {"env": "test"},
            "taints": [],
            "gpu_required": False,
        },
        user_id="stampede-system",
        username="Stampede",
    )


@pytest.mark.asyncio
async def test_scale_up_blocks_gpu_nodes_when_quota_check_fails():
    from drover.services.stampede import _scale_up_nodegroup

    ng = {**_make_nodegroup(node_count=0, max_size=3), "id": "gpu-ng", "flavor_id": "gpu"}
    pending = [
        {
            "name": "trainer",
            "namespace": "ml",
            "node_selector": {},
            "tolerations": [],
            "affinity": {},
            "resource_requests": {"cpu_m": 500, "memory_bytes": 512 * 1024**2, "gpu": 1},
            "message": "Insufficient nvidia.com/gpu",
        }
    ]
    s = MagicMock()
    s.drover_stampede_scale_up_cooldown = 0

    with (
        patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value={})),
        patch(
            "drover.services.stampede._get_available_flavors",
            new=AsyncMock(
                return_value=[{"id": "gpu", "name": "gpu.large", "vcpus_m": 4000, "ram_bytes": 8 * 1024**3, "gpu": 1}]
            ),
        ),
        patch(
            "drover.services.stampede._check_gpu_quota_for_nodes",
            new=AsyncMock(return_value=(False, "quota exceeded")),
        ),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock()) as update_nodegroup,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
        patch("drover.services.stampede.asyncio.create_task") as create_task,
    ):
        await _scale_up_nodegroup(
            cluster_id="cl-1",
            project_id="proj-1",
            nodegroup=ng,
            pending_pods=pending,
            node_pods=[],
            node_capacities=[],
            s=s,
        )

    update_nodegroup.assert_not_awaited()
    create_task.assert_not_called()
    event = record_event.await_args.kwargs
    assert event["action"] == "blocked"
    assert event["status"] == "skipped"
    assert event["extra"]["reason"] == "gpu_quota"
    assert event["extra"]["message"] == "quota exceeded"
    assert any(call.args[2]["last_blocked_reason"] == "gpu_quota" for call in update_state.await_args_list)


@pytest.mark.asyncio
async def test_scale_down_blocks_when_evicted_pods_do_not_fit_elsewhere():
    from drover.services.stampede import _scale_down_nodegroup

    ng = {
        **_make_nodegroup(node_count=2, min_size=1, max_size=3),
        "id": "mixed-ng",
        "vms": [
            {"vm_id": "vm-a", "name": "node-a", "status": "ACTIVE"},
            {"vm_id": "vm-b", "name": "node-b", "status": "ACTIVE"},
        ],
    }
    node_capacities = [
        {"name": "node-a", "ready": True, "allocatable": {"cpu_m": 4000, "memory_bytes": 8 * 1024**3, "gpu": 4}},
        {"name": "node-b", "ready": True, "allocatable": {"cpu_m": 8000, "memory_bytes": 16 * 1024**3, "gpu": 0}},
    ]
    node_pods = [
        {
            "node": "node-a",
            "cpu_m": 200,
            "memory_bytes": 3 * 1024**3,
            "gpu": 1,
            "is_daemonset": False,
            "is_mirror": False,
        },
        {
            "node": "node-b",
            "cpu_m": 200,
            "memory_bytes": 7 * 1024**3,
            "gpu": 0,
            "is_daemonset": False,
            "is_mirror": False,
        },
    ]
    s = MagicMock()
    s.drover_stampede_scale_down_cooldown = 0
    s.drover_stampede_scale_down_threshold = 0.5
    s.drover_stampede_scale_down_window = 0
    s.drover_stampede_interval = 60

    with (
        patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value={})),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock()) as update_nodegroup,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
        patch("drover.services.stampede.asyncio.create_task") as create_task,
    ):
        await _scale_down_nodegroup(
            cluster_id="cl-1",
            project_id="proj-1",
            nodegroup=ng,
            node_pods=node_pods,
            node_capacities=node_capacities,
            s=s,
        )

    update_nodegroup.assert_not_awaited()
    record_event.assert_not_awaited()
    create_task.assert_not_called()
    assert update_state.await_args_list[0].args[2] == {"consecutive_idle_checks": 1}
    assert update_state.await_args_list[-1].args[2] == {
        "consecutive_idle_checks": 0,
        "last_blocked_reason": "scale_down_no_fit",
    }


@pytest.mark.asyncio
async def test_scale_down_queues_durable_nodegroup_reconciliation():
    from drover.services.stampede import _scale_down_nodegroup

    ng = {
        **_make_nodegroup(node_count=2, min_size=1, max_size=3),
        "id": "gpu-ng",
        "vms": [
            {"vm_id": "vm-a", "name": "node-a", "status": "ACTIVE"},
            {"vm_id": "vm-b", "name": "node-b", "status": "ACTIVE"},
        ],
    }
    node_capacities = [
        {"name": "node-a", "ready": True, "allocatable": {"cpu_m": 4000, "memory_bytes": 8 * 1024**3, "gpu": 1}},
        {"name": "node-b", "ready": True, "allocatable": {"cpu_m": 4000, "memory_bytes": 8 * 1024**3, "gpu": 1}},
    ]
    node_pods = [
        {
            "node": "node-a",
            "cpu_m": 100,
            "memory_bytes": 128 * 1024**2,
            "gpu": 0,
            "is_daemonset": True,
            "is_mirror": False,
        },
        {
            "node": "node-b",
            "cpu_m": 3000,
            "memory_bytes": 2 * 1024**3,
            "gpu": 0,
            "is_daemonset": False,
            "is_mirror": False,
        },
    ]
    s = MagicMock()
    s.drover_stampede_scale_down_cooldown = 0
    s.drover_stampede_scale_down_threshold = 0.5
    s.drover_stampede_scale_down_window = 0
    s.drover_stampede_interval = 60

    with (
        patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value={})),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock()) as update_nodegroup,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
        patch("drover.services.jobs.enqueue_job", new=AsyncMock()) as enqueue_job,
    ):
        await _scale_down_nodegroup(
            cluster_id="cl-1",
            project_id="proj-1",
            nodegroup=ng,
            node_pods=node_pods,
            node_capacities=node_capacities,
            s=s,
        )

    update_nodegroup.assert_awaited_once_with("cl-1", "gpu-ng", {"node_count": 1})
    assert update_state.await_args_list[-1].args[2]["consecutive_idle_checks"] == 0
    assert record_event.await_args.kwargs["action"] == "scale_down"
    assert record_event.await_args.kwargs["status"] == "started"
    enqueue_job.assert_awaited_once_with(
        cluster_id="cl-1",
        project_id="proj-1",
        kind="nodegroup_reconcile",
        payload={
            "action": "delete_vms",
            "nodegroup": ng,
            "remove_entries": [{"vm_id": "vm-a", "name": "node-a", "status": "ACTIVE"}],
        },
        user_id="stampede-system",
        username="Stampede",
    )


@pytest.mark.asyncio
async def test_provision_and_track_marks_partial_when_a_node_never_becomes_ready():
    from drover.services.stampede import _provision_and_track

    ng = {
        **_make_nodegroup(node_count=2),
        "vms": [
            {"vm_id": "vm-1", "name": "node-1", "status": "ACTIVE"},
            {"vm_id": "vm-2", "name": "node-2", "status": "CREATING"},
        ],
        "stampede_state": {"in_flight_count": 2},
    }
    with (
        patch(
            "drover.services.autoscale.provision_nodegroup_vms",
            new=AsyncMock(
                return_value=[
                    {"vm_id": "vm-1", "name": "node-1"},
                    {"vm_id": "vm-2", "name": "node-2"},
                ]
            ),
        ),
        patch("drover.services.kube.wait_node_ready", new=AsyncMock(side_effect=[True, False])),
        patch("drover.services.nodegroup.update_nodegroup", new=AsyncMock()) as update_nodegroup,
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=ng)),
        patch("drover.services.nodegroup.set_nodegroup_count", new=AsyncMock()) as set_count,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
    ):
        await _provision_and_track(
            project_id="proj-1",
            cluster_id="cl-1",
            nodegroup_id="ng-1",
            add_count=2,
            flavor_id="cpu",
            image_id=None,
            labels=None,
            taints=None,
        )

    update_nodegroup.assert_awaited_once_with("cl-1", "ng-1", {})
    set_count.assert_awaited_once_with("cl-1", "ng-1", 1)
    assert update_state.await_args.args[2] == {"in_flight_count": 0, "last_blocked_reason": "node_not_ready"}
    event = record_event.await_args.kwargs
    assert event["status"] == "partial"
    assert event["extra"]["ready_nodes"] == ["node-1"]
    assert event["extra"]["failed_nodes"] == ["node-2"]
    assert event["extra"]["reason"] == "node_not_ready"


# ---------------------------------------------------------------------------
# k3s_kube 파서 단위 테스트
# ---------------------------------------------------------------------------


def test_parse_cpu_millicores():
    from drover.services.kube import _parse_cpu_millicores

    assert _parse_cpu_millicores("500m") == 500
    assert _parse_cpu_millicores("2") == 2000
    assert _parse_cpu_millicores("0.5") == 500
    assert _parse_cpu_millicores("") == 0


def test_parse_memory_bytes():
    from drover.services.kube import _parse_memory_bytes

    assert _parse_memory_bytes("512Mi") == 512 * 1024**2
    assert _parse_memory_bytes("1Gi") == 1024**3
    assert _parse_memory_bytes("1024Ki") == 1024**2
    assert _parse_memory_bytes("") == 0


# ---------------------------------------------------------------------------
# Stampede API 엔드포인트 테스트
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stampede_enable_unauthenticated():
    """미인증 요청은 401."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/v1/clusters/cl-1/stampede/enable")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_stampede_enable_cluster_not_found(client):
    """클러스터 없으면 404."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.get_settings") as mock_settings,
    ):
        mock_db.get_cluster = AsyncMock(return_value=None)
        s = MagicMock()
        s.drover_stampede_enabled = True
        mock_settings.return_value = s
        resp = await client.post("/v1/clusters/cl-1/stampede/enable")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_stampede_enable_global_disabled(client):
    """전역 stampede_enabled=false면 400."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.get_settings") as mock_settings,
    ):
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster())
        s = MagicMock()
        s.drover_stampede_enabled = False
        mock_settings.return_value = s
        resp = await client.post("/v1/clusters/cl-1/stampede/enable")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stampede_disable_unauthenticated():
    """미인증 요청은 401."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/v1/clusters/cl-1/stampede/disable")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_stampede_status_unauthenticated():
    """미인증 요청은 401."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/cl-1/stampede")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_stampede_status_success(client):
    """Stampede 상태 조회 성공."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.get_settings") as mock_settings,
        patch("drover.services.nodegroup.list_nodegroups", new=AsyncMock(return_value=[_make_nodegroup()])),
    ):
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster())
        s = MagicMock()
        s.drover_stampede_enabled = True
        mock_settings.return_value = s
        resp = await client.get("/v1/clusters/cl-1/stampede")
    assert resp.status_code == 200
    data = resp.json()
    assert "stampede_enabled" in data
    assert "nodegroups" in data


# ---------------------------------------------------------------------------
# Stampede reconcile 루프 통합 테스트 (mock 기반)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provision_and_track_reconciles_missing_created_vms():
    from drover.services.stampede import _provision_and_track

    ng = {
        **_make_nodegroup(node_count=2),
        "vms": [{"vm_id": "vm-1", "name": "node-1", "status": "CREATING"}],
        "stampede_state": {"in_flight_count": 2},
    }
    with (
        patch("drover.services.autoscale.provision_nodegroup_vms", new=AsyncMock(return_value=[])),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=ng)),
        patch("drover.services.nodegroup.set_nodegroup_count", new=AsyncMock()) as set_count,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
    ):
        await _provision_and_track(
            project_id="proj-1",
            cluster_id="cl-1",
            nodegroup_id="ng-1",
            add_count=2,
            flavor_id="gpu",
            image_id=None,
            labels=None,
            taints=None,
        )

    set_count.assert_called_once_with("cl-1", "ng-1", 1)
    update_state.assert_called_once()
    event_extra = record_event.call_args.kwargs["extra"]
    assert record_event.call_args.kwargs["status"] == "failed"
    assert event_extra["missing_count"] == 2
    assert event_extra["reason"] == "provision_failed"


@pytest.mark.asyncio
async def test_provision_and_track_reconciles_provision_exception():
    from drover.services.stampede import _provision_and_track

    ng = {
        **_make_nodegroup(node_count=2),
        "vms": [{"vm_id": "vm-existing", "name": "node-existing", "status": "RUNNING"}],
        "stampede_state": {"in_flight_count": 2},
    }
    with (
        patch(
            "drover.services.autoscale.provision_nodegroup_vms",
            new=AsyncMock(side_effect=RuntimeError("db write failed")),
        ),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value=ng)),
        patch("drover.services.nodegroup.set_nodegroup_count", new=AsyncMock()) as set_count,
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()) as update_state,
        patch("drover.services.stampede._record_stampede_event", new=AsyncMock()) as record_event,
    ):
        await _provision_and_track(
            project_id="proj-1",
            cluster_id="cl-1",
            nodegroup_id="ng-1",
            add_count=2,
            flavor_id="gpu",
            image_id=None,
            labels=None,
            taints=None,
        )

    set_count.assert_called_once_with("cl-1", "ng-1", 1)
    update_state.assert_called_once()
    assert update_state.call_args.args[2]["in_flight_count"] == 0
    assert update_state.call_args.args[2]["last_blocked_reason"] == "provision_failed"
    event_extra = record_event.call_args.kwargs["extra"]
    assert record_event.call_args.kwargs["status"] == "failed"
    assert event_extra["missing_count"] == 2
    assert event_extra["provision_error"] == "provision_failed"
    assert event_extra["reason"] == "provision_failed"


@pytest.mark.asyncio
async def test_run_all_skips_when_disabled():
    """k3s_stampede_enabled=False면 아무것도 하지 않는다."""
    with patch("drover.services.stampede.get_settings") as mock_s:
        s = MagicMock()
        s.drover_stampede_enabled = False
        mock_s.return_value = s
        from drover.services.stampede import run_all

        await run_all()  # 예외 없이 종료되어야 함


@pytest.mark.asyncio
async def test_run_all_no_active_clusters():
    """stampede_enabled 클러스터가 없으면 reconcile 실행 안 함."""
    with (
        patch("drover.services.stampede.get_settings") as mock_s,
        patch("drover.services.store.list_all_clusters", new=AsyncMock(return_value=[])),
    ):
        s = MagicMock()
        s.drover_stampede_enabled = True
        mock_s.return_value = s
        from drover.services.stampede import run_all

        await run_all()  # 예외 없이 종료되어야 함


@pytest.mark.asyncio
async def test_scale_up_respects_max_size():
    """node_count >= max_size면 scale-up을 하지 않는다."""
    from drover.services.stampede import _scale_up_nodegroup

    ng = _make_nodegroup(node_count=3, max_size=3)
    pending = [
        {
            "resource_requests": {"cpu_m": 500, "memory_bytes": 256 * 1024**2, "gpu": 0},
            "node_selector": {},
            "tolerations": [],
            "affinity": {},
            "message": "Insufficient cpu",
        }
    ]

    with (
        patch("drover.services.stampede.get_settings") as mock_s,
        patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value={})),
        patch("drover.services.stampede._update_stampede_state", new=AsyncMock()),
        patch("drover.services.stampede._get_available_flavors", new=AsyncMock(return_value=[])),
    ):
        s = MagicMock()
        s.drover_stampede_scale_up_cooldown = 0
        s.drover_stampede_resource_headroom_factor = 0.3
        mock_s.return_value = s
        # max_size 도달 → scale-up 없어야 함 (예외 없이 리턴)
        await _scale_up_nodegroup(
            cluster_id="cl-1",
            project_id="proj-1",
            nodegroup=ng,
            pending_pods=pending,
            node_pods=[],
            node_capacities=[],
            s=s,
        )


@pytest.mark.asyncio
async def test_scale_down_respects_min_size():
    """node_count <= min_size면 scale-down을 하지 않는다."""
    from drover.services.stampede import _scale_down_nodegroup

    ng = _make_nodegroup(node_count=1, min_size=1)

    with patch("drover.services.stampede._get_stampede_state", new=AsyncMock(return_value={})):
        s = MagicMock()
        s.drover_stampede_scale_down_cooldown = 0
        s.drover_stampede_scale_down_threshold = 0.5
        s.drover_stampede_scale_down_window = 0
        s.drover_stampede_interval = 60
        # min_size == node_count → 즉시 리턴 (예외 없이)
        await _scale_down_nodegroup(
            cluster_id="cl-1",
            project_id="proj-1",
            nodegroup=ng,
            node_pods=[],
            node_capacities=[],
            s=s,
        )


@pytest.mark.asyncio
async def test_reconcile_cluster_no_stampede_nodegroups():
    """stampede_enabled nodegroup이 없으면 K8s API 조회를 하지 않는다."""
    with (
        patch(
            "drover.services.nodegroup.list_nodegroups",
            new=AsyncMock(return_value=[_make_nodegroup(stampede_enabled=False)]),
        ),
        patch("drover.services.kube.list_unschedulable_pods") as mock_pods,
    ):
        from drover.services.stampede import reconcile_cluster

        await reconcile_cluster(_make_cluster())
        mock_pods.assert_not_called()
