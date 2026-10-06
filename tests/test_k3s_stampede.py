"""Stampede scheduling, lifecycle, stabilization and failure-boundary contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from openstack import exceptions as os_exceptions

from drover.config import Settings
from drover.services import activity, autoscale, inventory, jobs, keystone, kube, nodegroup, stampede


def _pod(name="work", *, cpu=500, memory=512, gpu=0, **extra):
    return {
        "name": name, "namespace": "workloads", "message": "0/1 nodes: Insufficient cpu",
        "resource_requests": {"cpu_m": cpu, "memory_bytes": memory, "gpu": gpu, "pods": 1},
        "node_selector": {}, "tolerations": [], "affinity": {}, "has_controller": True,
        "safe_to_evict": True, **extra,
    }


def _group(name="cpu", *, count=0, maximum=3, minimum=0, **extra):
    return {
        "id": name, "cluster_id": "cluster", "name": name, "role": "agent", "stampede_enabled": True,
        "flavor_id": name, "min_size": minimum, "max_size": maximum, "node_count": count,
        "labels": {}, "taints": [], "vms": [], "stampede_state": {}, **extra,
    }


def _node(name="worker", *, cpu=2000, memory=4096, gpu=0, **extra):
    return {
        "name": name, "ready": True, "unschedulable": False, "labels": {}, "taints": [],
        "allocatable": {"cpu_m": cpu, "memory_bytes": memory, "gpu": gpu, "pods": 110}, **extra,
    }


def _flavor(name="cpu", *, cpu=2000, memory=4096, gpu=0):
    return {"id": name, "name": name, "vcpus_m": cpu, "ram_bytes": memory, "gpu": gpu}


def _settings(**extra):
    return Settings(_env_file=None, drover_stampede_enabled=True, **extra)


@pytest.mark.parametrize("effect,toleration,expected", [
    ("NoSchedule", {"key": "gpu", "operator": "Exists", "effect": "NoExecute"}, False),
    ("NoSchedule", {"key": "gpu", "operator": "Exists"}, True),
    ("NoExecute", {"key": "gpu", "value": "yes"}, True),
    ("NoExecute", {"key": "gpu"}, False),
    ("PreferNoSchedule", {}, True),
])
def test_tolerations_match_effect_and_value(effect, toleration, expected):
    group = _group(taints=[{"key": "gpu", "value": "yes", "effect": effect}])
    assert stampede._node_matches_nodegroup(_pod(tolerations=[toleration]), group) is expected


@pytest.mark.parametrize("operator,values,expected", [
    ("In", ["4"], True), ("NotIn", ["4"], False), ("Exists", [], True),
    ("DoesNotExist", [], False), ("Gt", ["2"], True), ("Lt", ["2"], False), ("Invalid", [], False),
])
def test_required_node_affinity(operator, values, expected):
    pod = _pod(affinity={"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [{"matchExpressions": [{"key": "rack", "operator": operator, "values": values}]}],
    }}})
    assert stampede._node_matches_nodegroup(pod, _group(labels={"rack": "4"})) is expected


def test_empty_required_affinity_term_does_not_match():
    pod = _pod(affinity={"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{}]}}})
    assert not stampede._node_matches_nodegroup(pod, _group())


def test_gpu_demand_uses_gpu_flavor_and_stable_group_labels():
    groups = [_group(), _group("gpu", labels={"accelerator": "nvidia"})]
    pod = _pod(gpu=1, node_selector={"accelerator": "nvidia"}, message="Insufficient nvidia.com/gpu")
    assignments, blocked, _ = stampede._assign_pending_pods(
        [pod], groups, {"cpu": _flavor(), "gpu": _flavor("gpu", gpu=1)}, [], [],
    )
    assert assignments == {"cpu": [], "gpu": [pod]}
    assert blocked == []


def test_gpu_selector_matches_bootstrap_label_without_user_label_duplication():
    pod = _pod(gpu=1, node_selector={"afterglow.io/gpu": "true"}, message="Insufficient nvidia.com/gpu")
    assignments, blocked, _ = stampede._assign_pending_pods(
        [pod], [_group(), _group("gpu")], {"cpu": _flavor(), "gpu": _flavor("gpu", gpu=1)}, [], [],
    )
    assert assignments == {"cpu": [], "gpu": [pod]}
    assert blocked == []


def test_pending_pods_consume_existing_capacity_once_cluster_wide():
    pods = [_pod("first", cpu=1500), _pod("second", cpu=1500)]
    group = _group()
    assignments, blocked, _ = stampede._assign_pending_pods(
        pods, [group], {"cpu": _flavor()}, [], [_node("external")],
    )
    assert assignments["cpu"] == [pods[1]]
    assert blocked == []


def test_cordoned_or_selector_incompatible_capacity_cannot_resolve_pending():
    pod = _pod(node_selector={"pool": "new"})
    group = _group(labels={"pool": "new"})
    capacities = [_node("old", labels={"pool": "old"}), _node("cordoned", labels={"pool": "new"}, unschedulable=True)]
    assignments, _, _ = stampede._assign_pending_pods([pod], [group], {"cpu": _flavor()}, [], capacities)
    assert assignments["cpu"] == [pod]


@pytest.mark.parametrize("extra,reason", [
    ({"message": "pod has unbound immediate PersistentVolumeClaims"}, "pvc_unbound"),
    ({"message": "untolerated taint"}, "not_resource_shortage"),
    ({"node_name": "fixed"}, "pinned_missing_node"),
    ({"scheduler_name": "custom"}, "unsupported_scheduler"),
    ({"affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{}]}}}, "unsupported_pod_affinity"),
    ({"topology_spread_constraints": [{"whenUnsatisfiable": "DoNotSchedule"}]}, "unsupported_topology_spread"),
    ({"host_ports": [{"port": 80}]}, "unsupported_host_ports"),
    ({"resource_requests": {"cpu_m": 1, "extended_resources": {"nvidia.com/mig-1g.5gb": 1}}}, "unsupported_resource_request"),
])
def test_non_capacity_and_unsupported_pending_causes_do_not_add_workers(extra, reason):
    assignments, blocked, _ = stampede._assign_pending_pods([_pod(**extra)], [_group()], {"cpu": _flavor()}, [], [])
    assert assignments["cpu"] == []
    assert blocked[0]["reason"] == reason


def test_binpacking_gpu_slots_and_cpu_memory_headroom():
    pods = [_pod(str(i), cpu=600, memory=1000, gpu=1) for i in range(3)]
    flavor = _flavor("gpu", gpu=2)
    flavor["estimated_allocatable"] = stampede._flavor_capacity(flavor, 0.3)
    assert stampede._binpack_count(pods, flavor) == 2
    assert stampede._binpack_count([_pod(str(i), cpu=1, memory=0) for i in range(111)], _flavor()) == 2


def test_full_preferred_group_does_not_hide_another_suitable_group():
    groups = [_group("small", count=1, maximum=1), _group("large")]
    assignments, _, _ = stampede._assign_pending_pods([_pod()], groups, {"small": _flavor("small"), "large": _flavor("large")}, [], [])
    assert assignments["small"] == []
    assert len(assignments["large"]) == 1


async def test_in_flight_desired_count_cannot_reserve_a_second_scale_up(monkeypatch):
    group = _group(count=1, stampede_state={"in_flight_count": 1})
    updates = []
    monkeypatch.setattr(stampede, "_update_stampede_state", AsyncMock(side_effect=lambda _, __, patch: updates.append(patch)))
    monkeypatch.setattr(stampede, "_record_stampede_event", AsyncMock())
    enqueue = AsyncMock()
    monkeypatch.setattr(jobs, "enqueue_stampede_job", enqueue)
    await stampede._scale_up_nodegroup("cluster", "project", group, [_pod()], [], [], _settings(), flavor=_flavor())
    assert not enqueue.await_count
    assert updates[-1]["last_blocked_reason"] == "provisioning_in_progress"


async def test_gpu_admission_outage_blocks_without_fallback(monkeypatch):
    from drover.services import afterglow

    updates = []
    monkeypatch.setattr(stampede, "_update_stampede_state", AsyncMock(side_effect=lambda _, __, patch: updates.append(patch)))
    monkeypatch.setattr(stampede, "_record_stampede_event", AsyncMock())
    monkeypatch.setattr(afterglow, "check_gpu_admission", AsyncMock(return_value=(False, "gpu_admission_unavailable")))
    enqueue = AsyncMock()
    monkeypatch.setattr(jobs, "enqueue_stampede_job", enqueue)
    await stampede._scale_up_nodegroup("cluster", "project", _group("gpu"), [_pod(gpu=1)], [], [], _settings(drover_afterglow_admission_url="https://admission.example"), flavor=_flavor("gpu", gpu=1))
    assert not enqueue.await_count
    assert updates[-1]["last_blocked_reason"] == "gpu_admission_unavailable"


@pytest.mark.parametrize("extra,reason", [
    ({"has_controller": False}, "unmanaged_pod"), ({"is_mirror": True}, "unmanaged_pod"),
    ({"safe_to_evict": False}, "protected_pod"), ({"has_local_storage": True}, "protected_pod"),
    ({"has_pvc": True}, "protected_pod"), ({"deleting": True}, "unsupported_relocation"),
])
def test_scale_down_refuses_protected_pods(extra, reason):
    pods = [_pod(node="a", **extra)]
    assert stampede._removal_block(_node("a"), pods, [_node("a"), _node("b")]) == reason


def test_relocation_obeys_destination_selector_and_cumulative_demand():
    nodes = [_node("a", labels={"pool": "x"}), _node("b", labels={"pool": "y"})]
    pods = [_pod(node="a", node_selector={"pool": "x"})]
    assert stampede._removal_block(nodes[0], pods, nodes) == "scale_down_no_fit"
    pods = [_pod("one", node="a", cpu=1500), _pod("two", node="a", cpu=1000)]
    assert stampede._removal_block(nodes[0], pods, nodes) == "scale_down_no_fit"


async def test_scale_down_requires_real_elapsed_window_for_same_node(monkeypatch):
    group = _group(count=2, vms=[{"vm_id": "a", "name": "a"}, {"vm_id": "b", "name": "b"}])
    now = [1000.0]
    monkeypatch.setattr(stampede.time, "time", lambda: now[0])
    async def merge(_, __, updates):
        group["stampede_state"].update(updates)
    monkeypatch.setattr(stampede, "_update_stampede_state", merge)
    monkeypatch.setattr(stampede, "_record_stampede_event", AsyncMock())
    reservations = []
    async def reserve(*_, **kwargs):
        reservations.append(kwargs)
        return {"count": 1, "job_id": "job", "operation_id": "operation"}
    monkeypatch.setattr(jobs, "enqueue_stampede_job", reserve)
    nodes = [_node("a"), _node("b")]
    for _ in range(12):
        await stampede._scale_down_nodegroup("cluster", "project", group, [], nodes, _settings())
    assert not reservations  # Check count cannot substitute for elapsed time.
    now[0] += 599
    await stampede._scale_down_nodegroup("cluster", "project", group, [], nodes, _settings())
    assert not reservations
    now[0] += 1
    await stampede._scale_down_nodegroup("cluster", "project", group, [], nodes, _settings())
    assert reservations[0]["payload"]["remove_entries"] == [{"vm_id": "a", "name": "a"}]


async def test_observation_failure_resets_stabilization_without_mutating_infrastructure(monkeypatch):
    group = _group(count=1, stampede_state={"idle_since": {"worker": 1}})
    monkeypatch.setattr(nodegroup, "list_nodegroups", AsyncMock(return_value=[group]))
    monkeypatch.setattr(stampede, "get_settings", _settings)
    monkeypatch.setattr(kube, "list_unschedulable_pods", AsyncMock(side_effect=RuntimeError("unavailable")))
    monkeypatch.setattr(kube, "get_node_capacity", AsyncMock(return_value=[]))
    monkeypatch.setattr(kube, "get_pod_resource_usage", AsyncMock(return_value=[]))
    updates = []
    monkeypatch.setattr(stampede, "_update_stampede_state", AsyncMock(side_effect=lambda _, __, patch: updates.append(patch)))
    enqueue = AsyncMock()
    monkeypatch.setattr(jobs, "enqueue_stampede_job", enqueue)
    await stampede.reconcile_cluster({"id": "cluster", "project_id": "project"})
    assert updates[-1]["idle_since"] == {}
    assert updates[-1]["last_decision"] == "observation_failed"
    assert not enqueue.await_count


@pytest.mark.parametrize("observation_gap,should_remove", [(60, True), (180, False)])
async def test_worker_observation_gap_cannot_count_as_continuous_spare_capacity(monkeypatch, observation_gap, should_remove):
    group = _group(
        count=2, vms=[{"vm_id": "a", "name": "a"}, {"vm_id": "b", "name": "b"}],
        stampede_state={"observed_at": 1000 - observation_gap, "idle_since": {"a": 200, "b": 200}},
    )
    monkeypatch.setattr(stampede.time, "time", lambda: 1000)
    monkeypatch.setattr(nodegroup, "list_nodegroups", AsyncMock(return_value=[group]))
    monkeypatch.setattr(stampede, "get_settings", _settings)
    monkeypatch.setattr(stampede, "_get_available_flavors", AsyncMock(return_value=[_flavor()]))
    monkeypatch.setattr(kube, "list_unschedulable_pods", AsyncMock(return_value=[]))
    monkeypatch.setattr(kube, "get_node_capacity", AsyncMock(return_value=[_node("a"), _node("b")]))
    monkeypatch.setattr(kube, "get_pod_resource_usage", AsyncMock(return_value=[]))
    monkeypatch.setattr(stampede, "_update_stampede_state", AsyncMock())
    monkeypatch.setattr(stampede, "_record_stampede_event", AsyncMock())
    enqueue = AsyncMock(return_value={"count": 1, "job_id": "job", "operation_id": "operation"})
    monkeypatch.setattr(jobs, "enqueue_stampede_job", enqueue)
    await stampede.reconcile_cluster({"id": "cluster", "project_id": "project"})
    if should_remove:
        assert enqueue.await_args.kwargs["payload"]["remove_entries"] == [{"vm_id": "a", "name": "a"}]
    else:
        enqueue.assert_not_awaited()


@pytest.fixture
def stampede_deletion(monkeypatch):
    entry = {"vm_id": "a", "name": "a"}
    group = _group(count=0, vms=[entry.copy()])  # Desired count already reserved down.
    server = SimpleNamespace(
        id="a", name="a", project_id="project",
        status="ACTIVE", task_state=None,
        metadata={"drover.cluster_id": "cluster", "drover.managed": "true"},
    )
    volume = SimpleNamespace(service="cinder", resource_type="volume", resource_id="boot", name="a-boot")
    state = SimpleNamespace(
        group=group, servers={"a": server}, volumes={"boot": volume}, nodes={"a": _node("a")},
        pending=[], pods=[], deleted_resources=set(), transitions=[], volume_failure=False, node_failure=False,
        delete_on_termination=True, volume_status="available", volume_attachments=[], volume_project="project",
    )
    connection = MagicMock()

    def get_server(vm_id):
        if vm_id not in state.servers:
            raise os_exceptions.NotFoundException(f"No Server found for {vm_id}")
        return state.servers[vm_id]

    connection.compute.get_server.side_effect = get_server

    def delete_server(vm_id, **_):
        state.transitions.append("nova_deleted")
        del state.servers[vm_id]
        if state.delete_on_termination and not state.volume_failure:
            state.volumes.pop("boot", None)

    def get_volume(volume_id):
        if state.volume_failure:
            raise TimeoutError("Cinder deletion observation timed out")
        if volume_id not in state.volumes:
            raise os_exceptions.NotFoundException(f"No Volume found for {volume_id}")
        return SimpleNamespace(
            id=volume_id, name="a-boot", status=state.volume_status, attachments=list(state.volume_attachments),
            project_id=state.volume_project, metadata={},
        )

    def delete_volume(volume_id, **_):
        state.transitions.append("volume_deleted")
        state.volumes.pop(volume_id, None)

    connection.compute.delete_server.side_effect = delete_server
    connection.block_storage.get_volume.side_effect = get_volume
    connection.block_storage.delete_volume.side_effect = delete_volume
    state.connection = connection
    monkeypatch.setattr(keystone, "get_project_manager_connection", AsyncMock(return_value=connection))
    state.close = AsyncMock()
    monkeypatch.setattr(keystone, "close_connection", state.close)
    monkeypatch.setattr(nodegroup, "get_nodegroup", AsyncMock(side_effect=lambda *_: group))
    state.pending_observation = AsyncMock(side_effect=lambda *_: list(state.pending))
    state.capacity_observation = AsyncMock(side_effect=lambda *_: list(state.nodes.values()))
    monkeypatch.setattr(kube, "list_unschedulable_pods", state.pending_observation)
    monkeypatch.setattr(kube, "get_node_capacity", state.capacity_observation)
    monkeypatch.setattr(kube, "get_pod_resource_usage", AsyncMock(side_effect=lambda *_: list(state.pods)))

    async def cordon(_, name, *, removal_vm_id):
        state.transitions.append("cordon")
        state.nodes[name].update(unschedulable=True, removal_vm_id=removal_vm_id)
        return True

    async def drain(_, name):
        state.transitions.append("drain")
        return True

    async def uncordon(_, name):
        state.transitions.append("uncordon")
        if name in state.nodes:
            state.nodes[name].update(unschedulable=False, removal_vm_id=None)
        return True

    async def delete_node(_, name):
        state.transitions.append("node_cleanup")
        if state.node_failure:
            return False
        state.nodes.pop(name, None)
        return True

    async def remove_vms(_, vm_ids):
        state.transitions.append("tracking_removed")
        group["vms"] = [vm for vm in group["vms"] if vm["vm_id"] not in vm_ids]

    async def set_count(_, __, count):
        group["node_count"] = count

    async def mark_deleted(*, service, resource_type, resource_id):
        state.deleted_resources.add((service, resource_type, resource_id))

    monkeypatch.setattr(kube, "cordon_node", cordon)
    monkeypatch.setattr(kube, "drain_node", drain)
    monkeypatch.setattr(kube, "uncordon_node", uncordon)
    monkeypatch.setattr(kube, "delete_k8s_node", delete_node)
    monkeypatch.setattr(nodegroup, "remove_nodegroup_vms", remove_vms)
    monkeypatch.setattr(nodegroup, "set_nodegroup_count", set_count)
    monkeypatch.setattr(inventory, "list_managed_resources", AsyncMock(return_value=[volume]))
    monkeypatch.setattr(inventory, "mark_resource_deleted", mark_deleted)
    monkeypatch.setattr(activity, "record", AsyncMock())
    monkeypatch.setattr(stampede, "_blocked", AsyncMock())
    state.payload = {
        "action": "delete_vms", "stampede": True, "nodegroup": {"id": "cpu"}, "remove_entries": [entry],
    }
    return state


async def test_worker_rechecks_pending_before_deleting_queued_candidate(stampede_deletion):
    state = stampede_deletion
    state.pending = [_pod()]
    with pytest.raises(RuntimeError, match="pending_pods_present"):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "a" in state.servers
    assert "boot" in state.volumes
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert state.nodes["a"]["unschedulable"] is False
    assert state.transitions == []
    assert not state.deleted_resources
    state.connection.compute.get_server.assert_called_once_with("a")
    state.close.assert_awaited_once_with(state.connection)


@pytest.mark.parametrize("failure,reason", [
    ("volume", "boot_volume_delete_unverified:boot"), ("node", "node_delete_failed:a"),
])
@pytest.mark.parametrize("retry_node", ["missing", "not_ready"])
async def test_stampede_job_retries_post_nova_cleanup_without_redrain(stampede_deletion, failure, reason, retry_node):
    state = stampede_deletion
    state.volume_failure = failure == "volume"
    state.node_failure = failure == "node"
    with pytest.raises(autoscale.NodegroupDeletionError, match=reason):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert not state.servers
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert "a" in state.nodes
    assert ("nova", "server", "a") in state.deleted_resources
    assert (("cinder", "volume", "boot") in state.deleted_resources) is (failure == "node")
    assert bool(state.volumes) is (failure == "volume")
    assert state.transitions[:3] == ["cordon", "drain", "nova_deleted"]
    assert "uncordon" not in state.transitions
    assert state.nodes["a"]["unschedulable"] is True
    assert "tracking_removed" not in state.transitions

    observed = (state.pending_observation.await_count, state.capacity_observation.await_count)
    # Nova's delayed delete-on-termination converges before the retry observes Cinder.
    state.volumes.clear()
    state.volume_failure = state.node_failure = False
    state.nodes = {} if retry_node == "missing" else {"a": _node("a", ready=False)}
    state.pending = [_pod("new-demand")]
    state.pods = [_pod("protected", node="a", safe_to_evict=False)]
    state.group.update(min_size=1, node_count=1)  # Cleanup cannot consume fresh sizing headroom.
    await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert not state.servers and not state.volumes and not state.nodes
    assert state.group["vms"] == []
    assert state.group["node_count"] == 0
    assert state.deleted_resources == {("nova", "server", "a"), ("cinder", "volume", "boot")}
    assert state.transitions[-2:] == ["node_cleanup", "tracking_removed"]
    assert state.transitions.count("cordon") == state.transitions.count("drain") == 1
    assert state.transitions.count("nova_deleted") == 1
    assert (state.pending_observation.await_count, state.capacity_observation.await_count) == observed


async def test_stampede_retry_waits_for_deleting_server_without_uncordon_or_redrain(stampede_deletion, monkeypatch):
    from drover.services import nova

    state = stampede_deletion
    wait_attempts = 0
    original_wait = nova.wait_server_deleted

    def request_delete(vm_id, **_):
        state.transitions.append("nova_delete_requested")
        state.servers[vm_id].task_state = "deleting"

    def observe_deletion(conn, vm_id):
        nonlocal wait_attempts
        wait_attempts += 1
        if wait_attempts == 1:
            raise TimeoutError("Nova deletion still in progress")
        state.servers.pop(vm_id)
        state.volumes.clear()
        original_wait(conn, vm_id)

    state.connection.compute.delete_server.side_effect = request_delete
    monkeypatch.setattr(nova, "wait_server_deleted", observe_deletion)
    with pytest.raises(autoscale.NodegroupDeletionError):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert state.servers["a"].task_state == "deleting"
    assert state.nodes["a"]["unschedulable"] is True
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert not state.deleted_resources
    assert "uncordon" not in state.transitions

    state.nodes["a"]["ready"] = False
    state.pending = [_pod("new-demand")]
    state.pods = [_pod("protected", node="a", safe_to_evict=False)]
    state.group.update(min_size=1, node_count=1)
    await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert not state.servers and not state.volumes and not state.nodes
    assert state.group["vms"] == [] and state.group["node_count"] == 0
    assert state.transitions.count("cordon") == state.transitions.count("drain") == 1
    assert state.transitions.count("nova_delete_requested") == 1
    assert "uncordon" not in state.transitions


@pytest.mark.parametrize("include_absent_in_batch", [False, True])
async def test_stampede_delete_cannot_use_absent_tracking_as_minimum_headroom(stampede_deletion, include_absent_in_batch):
    state = stampede_deletion
    absent = {"vm_id": "absent", "name": "absent"}
    state.group["vms"].append(absent)
    state.group.update(min_size=1, node_count=1)
    if include_absent_in_batch:
        state.payload["remove_entries"].insert(0, absent)
    with pytest.raises(RuntimeError, match="min_size_reached"):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "a" in state.servers and "boot" in state.volumes
    assert state.nodes["a"]["unschedulable"] is False
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}, absent]
    assert state.transitions == [] and not state.deleted_resources


async def test_stampede_batch_cannot_delete_below_minimum(stampede_deletion):
    state = stampede_deletion
    second = {"vm_id": "b", "name": "b"}
    state.group["vms"].append(second)
    state.group.update(min_size=1, node_count=1)
    state.servers["b"] = SimpleNamespace(
        id="b", name="b", project_id="project", status="ACTIVE", task_state=None,
        metadata={"drover.cluster_id": "cluster", "drover.managed": "true"},
    )
    state.nodes["b"] = _node("b")
    state.payload["remove_entries"].append(second)
    with pytest.raises(RuntimeError, match="min_size_reached"):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert set(state.servers) == {"a", "b"}
    assert "boot" in state.volumes
    assert all(not node["unschedulable"] for node in state.nodes.values())
    assert state.transitions == [] and not state.deleted_resources


@pytest.mark.parametrize("boundary", ["auth", "nova", "kubernetes"])
async def test_stampede_job_observation_errors_never_authorize_cleanup(stampede_deletion, monkeypatch, boundary):
    state = stampede_deletion
    if boundary == "auth":
        # A later autoscale lookup could succeed: the first failure must still stop the job.
        monkeypatch.setattr(keystone, "get_project_manager_connection", AsyncMock(side_effect=[PermissionError("manager auth failed"), state.connection]))
        expected = PermissionError
    elif boundary == "nova":
        lookup = state.connection.compute.get_server.side_effect

        def fail_lookup_once(*_, **__):
            state.connection.compute.get_server.side_effect = lookup
            raise RuntimeError("Nova observation failed")

        state.connection.compute.get_server.side_effect = fail_lookup_once
        expected = RuntimeError
    else:
        state.capacity_observation.side_effect = RuntimeError("Kubernetes observation failed")
        expected = RuntimeError
    with pytest.raises(expected):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "a" in state.servers and "boot" in state.volumes and "a" in state.nodes
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert state.transitions == []
    assert not state.deleted_resources
    if boundary in {"auth", "nova"}:
        state.pending_observation.assert_not_awaited()
        state.capacity_observation.assert_not_awaited()


@pytest.mark.parametrize("guard,reason", [
    ("missing", "scale_down_node_missing"), ("not_ready", "node_not_ready"),
    ("foreign_cordon", "node_not_ready"),
    ("minimum", "min_size_reached"), ("relocation", "scale_down_no_fit"),
])
async def test_stampede_job_live_server_keeps_scale_down_guards(stampede_deletion, guard, reason):
    state = stampede_deletion
    if guard == "missing":
        state.nodes.clear()
    elif guard == "not_ready":
        state.nodes["a"]["ready"] = False
    elif guard == "foreign_cordon":
        state.nodes["a"].update(unschedulable=True, removal_vm_id=None)
    elif guard == "minimum":
        state.group["min_size"] = 1
    else:
        state.pods = [_pod(node="a")]
    with pytest.raises(RuntimeError, match=reason):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "a" in state.servers and "boot" in state.volumes
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert not state.deleted_resources
    assert state.transitions == []


@pytest.mark.parametrize("failure", ["ownership", "drain"])
async def test_stampede_job_live_deletion_preserves_ownership_and_drain_safety(stampede_deletion, monkeypatch, failure):
    state = stampede_deletion
    if failure == "ownership":
        state.servers["a"].project_id = "another-project"
    else:
        monkeypatch.setattr(kube, "drain_node", AsyncMock(return_value=False))
    with pytest.raises(autoscale.NodegroupDeletionError):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "a" in state.servers and "boot" in state.volumes
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert state.nodes["a"]["unschedulable"] is False
    assert not state.deleted_resources
    assert "nova_deleted" not in state.transitions
    if failure == "drain":
        assert state.transitions[-1] == "uncordon"
    else:
        assert state.transitions == []


async def test_stampede_denied_server_lookup_never_looks_absent(stampede_deletion):
    state = stampede_deletion
    state.connection.compute.get_server.side_effect = os_exceptions.ForbiddenException("denied")
    # The SDK's find_server swallows that 403 and falls back to an empty name search.
    state.connection.compute.find_server.return_value = None
    with pytest.raises(os_exceptions.ForbiddenException):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "a" in state.servers and "boot" in state.volumes and "a" in state.nodes
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert state.transitions == [] and not state.deleted_resources


async def test_stampede_final_lookup_failure_uncordons_without_delete(stampede_deletion):
    state = stampede_deletion
    lookup = state.connection.compute.get_server.side_effect
    calls = 0

    def fail_final_recheck(vm_id):
        nonlocal calls
        calls += 1
        if calls == 3:  # Job guard, deletion preflight, then the recheck right before DELETE.
            raise RuntimeError("Nova observation failed")
        return lookup(vm_id)

    state.connection.compute.get_server.side_effect = fail_final_recheck
    with pytest.raises(autoscale.NodegroupDeletionError, match="server_delete_failed:a"):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert state.transitions == ["cordon", "drain", "uncordon"]
    assert state.nodes["a"]["unschedulable"] is False and state.nodes["a"]["removal_vm_id"] is None
    assert "a" in state.servers and state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert not state.deleted_resources


async def test_stampede_failed_delete_keeps_cordon_and_retry_resumes(stampede_deletion):
    state = stampede_deletion
    remove_server = state.connection.compute.delete_server.side_effect

    def reject_once(vm_id, **kwargs):
        state.connection.compute.delete_server.side_effect = remove_server
        raise os_exceptions.ConflictException("Cannot delete while task_state is set")

    state.connection.compute.delete_server.side_effect = reject_once
    with pytest.raises(autoscale.NodegroupDeletionError, match="server_delete_failed:a"):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert state.servers["a"].status == "ACTIVE"
    assert state.nodes["a"]["unschedulable"] is True and state.nodes["a"]["removal_vm_id"] == "a"
    assert "uncordon" not in state.transitions
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]

    await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert not state.servers and not state.nodes
    assert state.group["vms"] == [] and state.group["node_count"] == 0
    assert state.transitions.count("nova_deleted") == 1
    assert "uncordon" not in state.transitions


@pytest.mark.parametrize("retry_failure", ["drain", "final_lookup"])
async def test_stampede_retry_failure_never_uncordons_inherited_removal(stampede_deletion, monkeypatch, retry_failure):
    from drover.services import nova

    state = stampede_deletion

    def ambiguous_delete(vm_id, **_):
        state.transitions.append("nova_delete_sent")  # Nova may still accept it later.

    state.connection.compute.delete_server.side_effect = ambiguous_delete
    monkeypatch.setattr(nova, "wait_server_deleted", MagicMock(side_effect=TimeoutError("still ACTIVE")))
    with pytest.raises(autoscale.NodegroupDeletionError, match="server_delete_failed:a"):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert state.servers["a"].status == "ACTIVE" and state.servers["a"].task_state is None

    if retry_failure == "drain":
        monkeypatch.setattr(kube, "drain_node", AsyncMock(return_value=False))
        expected = "drain_failed:a"
    else:
        lookup = state.connection.compute.get_server.side_effect
        calls = 0

        def fail_final_recheck(vm_id):
            nonlocal calls
            calls += 1
            if calls == 3:  # Retry guard, deletion preflight, then the recheck right before DELETE.
                raise RuntimeError("Nova observation failed")
            return lookup(vm_id)

        state.connection.compute.get_server.side_effect = fail_final_recheck
        expected = "server_delete_failed:a"
    with pytest.raises(autoscale.NodegroupDeletionError, match=expected):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "uncordon" not in state.transitions
    assert state.nodes["a"]["unschedulable"] is True and state.nodes["a"]["removal_vm_id"] == "a"
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]
    assert not state.deleted_resources


async def test_stampede_permitted_delete_completes_with_absent_sibling(stampede_deletion):
    state = stampede_deletion
    live = {"vm_id": "b", "name": "b"}
    absent = {"vm_id": "absent", "name": "absent"}
    state.group["vms"] += [live, absent]
    state.group.update(min_size=1, node_count=2)
    state.servers["b"] = SimpleNamespace(
        id="b", name="b", project_id="project", status="ACTIVE", task_state=None,
        metadata={"drover.cluster_id": "cluster", "drover.managed": "true"},
    )
    state.nodes["b"] = _node("b")
    await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert set(state.servers) == {"b"}
    # The absent sibling keeps its row for its own cleanup and supplies no live capacity.
    assert state.group["vms"] == [live, absent]
    assert state.group["node_count"] == 1


async def test_stampede_deletes_boot_volume_kept_after_server_deletion(stampede_deletion):
    """Afterglow provisioning intents boot without delete-on-termination."""
    state = stampede_deletion
    state.delete_on_termination = False
    await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert not state.servers and not state.volumes
    assert state.transitions.index("nova_deleted") < state.transitions.index("volume_deleted")
    assert ("cinder", "volume", "boot") in state.deleted_resources
    assert state.group["vms"] == [] and state.group["node_count"] == 0


@pytest.mark.parametrize("volume_state", ["attached_elsewhere", "foreign_project"])
async def test_stampede_preserves_boot_volume_it_cannot_safely_delete(stampede_deletion, monkeypatch, volume_state):
    import functools

    from drover.services import cinder

    state = stampede_deletion
    state.delete_on_termination = False
    if volume_state == "attached_elsewhere":
        state.volume_status, state.volume_attachments = "in-use", [{"server_id": "someone-else"}]
    else:
        state.volume_project = "another-project"
    monkeypatch.setattr(cinder, "delete_detached_boot_volume", functools.partial(cinder.delete_detached_boot_volume, timeout=0))
    with pytest.raises(autoscale.NodegroupDeletionError, match="boot_volume_delete_unverified:boot"):
        await jobs._execute_job_direct("nodegroup_reconcile", state.payload, "cluster", "project")
    assert "boot" in state.volumes and "volume_deleted" not in state.transitions
    assert ("cinder", "volume", "boot") not in state.deleted_resources
    assert state.group["vms"] == [{"vm_id": "a", "name": "a"}]


async def test_gpu_worker_not_successful_until_devices_are_allocatable(monkeypatch):
    from drover.services import gpu

    monkeypatch.setattr(autoscale, "provision_nodegroup_vms", AsyncMock(return_value=[{"vm_id": "gpu-vm", "name": "gpu-node"}]))
    monkeypatch.setattr(gpu, "ensure_device_plugin", AsyncMock())
    monkeypatch.setattr(kube, "wait_node_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(kube, "wait_node_gpu_allocatable", AsyncMock(return_value=False))
    states = {}
    async def status(_, vm_id, value):
        states[vm_id] = value
    monkeypatch.setattr(nodegroup, "set_nodegroup_vm_status", status)
    updates = []
    monkeypatch.setattr(stampede, "_update_stampede_state", AsyncMock(side_effect=lambda _, __, patch: updates.append(patch)))
    monkeypatch.setattr(stampede, "_record_stampede_event", AsyncMock())
    with pytest.raises(RuntimeError, match="gpu_not_allocatable"):
        await jobs._execute_job_direct("stampede_provision", {
            "nodegroup_id": "gpu", "add_count": 1, "flavor_id": "gpu",
            "gpu_required": True, "gpu_count": 2,
        }, "cluster", "project")
    assert states == {"gpu-vm": "ERROR"}
    assert updates[-1]["last_blocked_reason"] == "gpu_not_allocatable"


async def test_gpu_job_finishes_only_after_all_requested_devices_register(monkeypatch):
    from contextlib import asynccontextmanager

    import httpx

    from drover.services import gpu

    physical = {"gpu": 1}
    client = SimpleNamespace(get=AsyncMock(side_effect=lambda *_args, **_kwargs: httpx.Response(
        200, json={"status": {"allocatable": {"nvidia.com/gpu": str(physical["gpu"])}}},
    )))

    @asynccontextmanager
    async def kube_client(_):
        yield client, "https://kube.test"

    async def device_registration(_):
        assert physical["gpu"] == 1
        physical["gpu"] = 2

    states = {}

    async def status(_, vm_id, value):
        states[vm_id] = value

    monkeypatch.setattr(kube, "_kube_client", kube_client)
    monkeypatch.setattr("asyncio.sleep", device_registration)
    monkeypatch.setattr(autoscale, "provision_nodegroup_vms", AsyncMock(return_value=[{"vm_id": "gpu-vm", "name": "gpu-node"}]))
    monkeypatch.setattr(gpu, "ensure_device_plugin", AsyncMock())
    monkeypatch.setattr(kube, "wait_node_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(nodegroup, "set_nodegroup_vm_status", status)
    monkeypatch.setattr(stampede, "_update_stampede_state", AsyncMock())
    monkeypatch.setattr(stampede, "_record_stampede_event", AsyncMock())
    await jobs._execute_job_direct("stampede_provision", {
        "nodegroup_id": "gpu", "add_count": 1, "flavor_id": "gpu",
        "gpu_required": True, "gpu_count": 2,
    }, "cluster", "project")
    assert physical["gpu"] == 2
    assert states == {"gpu-vm": "ACTIVE"}


@pytest.mark.parametrize("extra_specs,count", [
    ({"gpu_count": "2"}, 2), ({"pci_passthrough:alias": "gpu-3060:1,audio:1"}, 1),
    ({"pci_passthrough:alias": "sriov:2,rdma:1"}, 0),
])
def test_gpu_aliases_exclude_non_gpu_pci_functions(extra_specs, count):
    assert stampede._flavor_gpu_count(extra_specs) == count


@pytest.mark.parametrize("os_type", ["ubuntu", "fcos"])
@pytest.mark.parametrize("driver_ok", [True, False])
def test_gpu_bootstrap_passes_before_k3s_provides_a_low_level_runtime(tmp_path, os_type, driver_ok):
    """Fresh GPU workers have no runc/crun on PATH until K3s starts; the join must not need one."""
    import os
    import shutil
    import subprocess

    from drover.services import gpu

    stubs = {
        "nvidia-smi": "echo 'GPU 0: NVIDIA GeForce RTX 3060'",
        # Real 1.20.1 behaviour: --version prints, then fails to locate runc/crun and exits 1.
        "nvidia-container-runtime": "echo 'NVIDIA Container Runtime version 1.20.1'; "
                                    "echo 'no runtime binary found from candidate list: [runc crun]' >&2; exit 1",
        "nvidia-container-cli": "echo 'NVRM version: 580.0'" if driver_ok else "echo 'nvml error: driver not loaded' >&2; exit 1",
    }
    for name, body in stubs.items():
        path = tmp_path / name
        path.write_text(f"#!/bin/bash\n{body}\n")
        path.chmod(0o755)
    os.symlink(shutil.which("timeout"), tmp_path / "timeout")
    result = subprocess.run(
        [shutil.which("bash"), "-euo", "pipefail", "-c", gpu.bootstrap_script(os_type)],
        env={"PATH": str(tmp_path)}, capture_output=True, text=True, timeout=30,
    )
    assert (result.returncode == 0) is driver_ok, result.stderr
