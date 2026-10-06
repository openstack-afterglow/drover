"""Stampede Reconciler — k3s 노드 오토스케일 내부 루프.

drover worker.py에서 주기적으로 run_all()을 호출.
ACTIVE + stampede_enabled 클러스터의 각 노드그룹을 순회하며:
  - Unschedulable pod 감지 → fit-check → VM 추가 (scale-up)
  - 유휴 노드 감지 → cordon/drain/삭제 (scale-down)
"""

import asyncio
import logging
import re
import time
import uuid

from drover.config import get_settings

_logger = logging.getLogger("drover.stampede")


async def _record_stampede_event(
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
    action: str,
    status: str,
    extra: dict | None = None,
) -> None:
    """Record best-effort Redis activity; durable jobs retain operation events."""
    try:
        from drover.services.activity import record

        await record(
            project_id=project_id,
            user_id="stampede-system",
            username="Stampede",
            resource_type="k3s_stampede",
            resource_id=cluster_id,
            resource_name=nodegroup_id,
            action=action,
            status=status,  # type: ignore[arg-type]
            extra=extra or {},
        )
    except Exception:
        _logger.debug("Stampede activity recording unavailable")


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------


def _node_matches_nodegroup(pod: dict, nodegroup: dict) -> bool:
    """Match selectors, effect-specific tolerations and required node affinity."""
    labels = nodegroup.get("labels") or {}
    if any(labels.get(key) != value for key, value in (pod.get("node_selector") or {}).items()):
        return False
    for taint in nodegroup.get("taints") or []:
        if taint.get("effect") not in {"NoSchedule", "NoExecute"}:
            continue
        tolerated = False
        for tol in pod.get("tolerations") or []:
            if tol.get("effect") and tol["effect"] != taint["effect"]:
                continue
            operator = tol.get("operator", "Equal")
            if (operator == "Exists" and (not tol.get("key") or tol["key"] == taint.get("key"))) or (
                operator == "Equal"
                and tol.get("key") == taint.get("key")
                and tol.get("value", "") == taint.get("value", "")
            ):
                tolerated = True
        if not tolerated:
            return False
    affinity = ((pod.get("affinity") or {}).get("nodeAffinity") or {})
    if "requiredDuringSchedulingIgnoredDuringExecution" not in affinity:
        return True
    terms = (affinity["requiredDuringSchedulingIgnoredDuringExecution"] or {}).get("nodeSelectorTerms") or []
    for term in terms:
        expressions = term.get("matchExpressions") or []
        fields = term.get("matchFields") or []
        if not expressions and not fields:
            continue
        matched = True
        for expression in expressions + fields:
            key = expression.get("key", "")
            if expression in fields:
                actual = nodegroup.get("name") if key == "metadata.name" else None
                present = actual is not None
            else:
                actual = labels.get(key)
                present = key in labels
            values = expression.get("values") or []
            operator = expression.get("operator")
            if operator == "In":
                ok = present and actual in values
            elif operator == "NotIn":
                ok = actual not in values
            elif operator == "Exists":
                ok = present
            elif operator == "DoesNotExist":
                ok = not present
            elif operator in {"Gt", "Lt"}:
                try:
                    ok = len(values) == 1 and (int(actual) > int(values[0]) if operator == "Gt" else int(actual) < int(values[0]))
                except (TypeError, ValueError, IndexError):
                    ok = False
            else:
                ok = False
            if not ok:
                matched = False
                break
        if matched:
            return True
    return False


def _is_pvc_issue(pod: dict) -> bool:
    message = pod.get("message", "").lower()
    return any(text in message for text in ("unbound immediate persistentvolumeclaims", "persistentvolumeclaim", "volume node affinity", "volume binding"))






_NON_GPU_PCI_ALIAS_TOKENS = frozenset(
    {"audio", "crypto", "fpga", "infiniband", "network", "nic", "nvme", "qat", "rdma", "sriov"}
)


def _flavor_gpu_count(extra_specs: dict) -> int:
    raw = extra_specs.get("gpu_count")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    alias = extra_specs.get("pci_passthrough:alias", "")
    total = 0
    for entry in str(alias).split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            gpu_alias, _, count_text = entry.rpartition(":")
            try:
                count = int(count_text.strip())
                if count <= 0:
                    count = 1
            except ValueError:
                count = 1
        else:
            gpu_alias = entry
            count = 1
        alias_tokens = {token for token in re.split(r"[^a-z0-9]+", gpu_alias.lower()) if token}
        if alias_tokens & _NON_GPU_PCI_ALIAS_TOKENS:
            continue
        total += count
    return total


def _fits_capacity(req: dict, cap: dict) -> bool:
    if any(req.get(key, 0) > cap.get(key, 0) for key in ("cpu_m", "memory_bytes", "gpu")):
        return False
    if req.get("pods", 1) > cap.get("pods", 110):
        return False
    return all(value <= (cap.get("extended_resources") or {}).get(key, 0) for key, value in (req.get("extended_resources") or {}).items())


def _requests(pod: dict) -> dict:
    return pod.get("resource_requests") or {
        "cpu_m": pod.get("cpu_m", 0), "memory_bytes": pod.get("memory_bytes", 0),
        "gpu": pod.get("gpu", 0), "pods": 1,
    }


def _consume(free: dict, req: dict) -> None:
    for key in ("cpu_m", "memory_bytes", "gpu"):
        free[key] = free.get(key, 0) - req.get(key, 0)
    free["pods"] = free.get("pods", 110) - req.get("pods", 1)
    for key, value in (req.get("extended_resources") or {}).items():
        resources = free.setdefault("extended_resources", {})
        resources[key] = resources.get(key, 0) - value


def _flavor_capacity(flavor: dict, headroom: float = 0.0) -> dict:
    """Estimate schedulable CPU/RAM; GPU devices are indivisible."""
    return dict(flavor.get("estimated_allocatable") or {
        "cpu_m": int(flavor.get("vcpus_m", 0) * (1 - headroom)),
        "memory_bytes": int(flavor.get("ram_bytes", 0) * (1 - headroom)),
        "gpu": flavor.get("gpu", 0), "pods": 110,
    })


def _free_nodes(node_pods: list[dict], nodes: list[dict]) -> list[dict]:
    free_nodes = []
    for node in nodes:
        if not node.get("ready") or node.get("unschedulable"):
            continue
        free = dict(node.get("allocatable") or {})
        free["extended_resources"] = dict(free.get("extended_resources") or {})
        for pod in node_pods:
            if pod.get("node") == node.get("name"):
                _consume(free, _requests(pod))
        for key in ("cpu_m", "memory_bytes", "gpu", "pods"):
            free[key] = max(0, free.get(key, 110 if key == "pods" else 0))
        free_nodes.append({**node, **free})
    return free_nodes


def _scheduling_block(pod: dict) -> str:
    if pod.get("scheduler_name", "default-scheduler") not in {"", "default-scheduler"}:
        return "unsupported_scheduler"
    affinity = pod.get("affinity") or {}
    if affinity.get("podAffinity") or affinity.get("podAntiAffinity"):
        return "unsupported_pod_affinity"
    if any(item.get("whenUnsatisfiable") == "DoNotSchedule" for item in pod.get("topology_spread_constraints") or []):
        return "unsupported_topology_spread"
    if pod.get("host_ports"):
        return "unsupported_host_ports"
    return ""


def _binpack_count(pods: list[dict], flavor: dict) -> int:
    bins: list[dict] = []
    ordered = sorted(pods, key=lambda p: (_requests(p).get("gpu", 0), _requests(p).get("memory_bytes", 0), _requests(p).get("cpu_m", 0)), reverse=True)
    for pod in ordered:
        req = _requests(pod)
        if _pod_fits_existing_capacity(pod, bins):
            continue
        free = _flavor_capacity(flavor)
        if not _fits_capacity(req, free):
            raise ValueError("pod exceeds nodegroup capacity")
        _consume(free, req)
        bins.append(free)
    return len(bins)


def _nodegroup_resource_summary(nodegroup: dict, node_pods: list[dict], node_capacities: list[dict]) -> dict:
    names = {vm.get("name") for vm in nodegroup.get("vms") or []}
    nodes = [node for node in node_capacities if node.get("name") in names and node.get("ready")]
    free = _free_nodes(node_pods, nodes)
    summary = {kind: {key: 0 for key in ("cpu_m", "memory_bytes", "gpu", "pods")} for kind in ("allocatable", "requested", "free")}
    for node in nodes:
        for key in summary["allocatable"]:
            summary["allocatable"][key] += (node.get("allocatable") or {}).get(key, 0)
    for pod in node_pods:
        if pod.get("node") in {node["name"] for node in nodes}:
            for key in summary["requested"]:
                summary["requested"][key] += _requests(pod).get(key, 1 if key == "pods" else 0)
    for node in free:
        for key in summary["free"]:
            summary["free"][key] += node.get(key, 0)
    summary["nodes"] = free
    return summary


def _pod_fits_existing_capacity(pod: dict, free_nodes: list[dict]) -> bool:
    for free in free_nodes:
        if "labels" in free and not _node_matches_nodegroup(pod, free):
            continue
        if _fits_capacity(_requests(pod), free):
            _consume(free, _requests(pod))
            return True
    return False


def _assign_pending_pods(
    pending_pods: list[dict], nodegroups: list[dict], flavors_by_id: dict[str, dict],
    node_pods: list[dict], node_capacities: list[dict], headroom_factor: float = 0.0,
) -> tuple[dict[str, list[dict]], list[dict], dict[str, dict]]:
    summaries = {ng["id"]: _nodegroup_resource_summary(ng, node_pods, node_capacities) for ng in nodegroups}
    assignments = {ng["id"]: [] for ng in nodegroups}
    blocked = []
    free_nodes = _free_nodes(node_pods, node_capacities)
    for pod in sorted(pending_pods, key=lambda p: (_requests(p).get("gpu", 0), _requests(p).get("memory_bytes", 0)), reverse=True):
        reason = ""
        if _is_pvc_issue(pod):
            reason = "pvc_unbound"
        elif pod.get("node_name"):
            reason = "pinned_missing_node"
        elif _scheduling_block(pod):
            reason = _scheduling_block(pod)
        elif _requests(pod).get("extended_resources"):
            reason = "unsupported_resource_request"
        elif not re.search(r"insufficient (cpu|memory|nvidia\.com/gpu|pods)|no nodes available", pod.get("message", ""), re.IGNORECASE):
            reason = "not_resource_shortage"
        if reason:
            blocked.append({"pod": pod, "reason": reason})
            continue
        if _pod_fits_existing_capacity(pod, free_nodes):
            continue
        candidates = []
        for ng in nodegroups:
            flavor = flavors_by_id.get(ng.get("flavor_id"))
            template = {"labels": {"afterglow.io/nodegroup": ng["id"], "afterglow.io/stampede": "true", **(ng.get("labels") or {})}, "taints": ng.get("taints") or []}
            if flavor and flavor.get("gpu", 0) > 0:
                template["labels"]["afterglow.io/gpu"] = "true"
            if flavor and _node_matches_nodegroup(pod, template) and _fits_capacity(_requests(pod), _flavor_capacity(flavor, headroom_factor)):
                candidates.append((ng, flavor))
        candidates.sort(key=lambda item: (item[0].get("node_count", 0) >= item[0].get("max_size", 5), item[1].get("gpu", 0), item[1].get("vcpus_m", 0), item[1].get("ram_bytes", 0)))
        if candidates:
            assignments[candidates[0][0]["id"]].append(pod)
        else:
            blocked.append({"pod": pod, "reason": "no_matching_nodegroup"})
    return assignments, blocked, summaries


async def _get_available_flavors(project_id: str) -> list[dict]:
    from drover.services import keystone, nova

    async with keystone.project_manager_connection(project_id) as conn:
        raw = await asyncio.to_thread(nova.list_flavors, conn)
    return [{"id": flavor.id, "name": flavor.name, "vcpus_m": int(flavor.vcpus or 0) * 1000,
             "ram_bytes": int(flavor.ram or 0) * 1024 * 1024, "gpu": _flavor_gpu_count(flavor.extra_specs or {}),
             "extra_specs": flavor.extra_specs or {}} for flavor in raw]


async def _update_stampede_state(nodegroup_id: str, cluster_id: str, updates: dict) -> None:
    from drover.services import nodegroup

    await nodegroup.merge_stampede_state(cluster_id, nodegroup_id, updates)




# ---------------------------------------------------------------------------
# scale-up
# ---------------------------------------------------------------------------


async def _blocked(cluster_id: str, project_id: str, ng_id: str, reason: str, **extra) -> None:
    await _update_stampede_state(ng_id, cluster_id, {"last_decision": "blocked", "last_blocked_reason": reason})
    await _record_stampede_event(project_id, cluster_id, ng_id, "blocked", "skipped", {"reason": reason, **extra})


async def _scale_up_nodegroup(
    cluster_id: str, project_id: str, nodegroup: dict, pending_pods: list[dict],
    node_pods: list[dict], node_capacities: list[dict], s, flavor: dict | None = None,
) -> None:
    """Provision the pending demand assigned after cluster-wide free-capacity packing."""
    from drover.services import afterglow, jobs

    ng_id = nodegroup["id"]
    state = nodegroup.get("stampede_state") or {}
    now = time.time()
    if state.get("in_flight_count", 0):
        await _blocked(cluster_id, project_id, ng_id, "provisioning_in_progress")
        return
    if now - state.get("last_scale_up", 0) < s.drover_stampede_scale_up_cooldown:
        await _blocked(cluster_id, project_id, ng_id, "scale_up_cooldown")
        return
    if flavor is None:
        flavor = next((item for item in await _get_available_flavors(project_id) if item["id"] == nodegroup.get("flavor_id")), None)
    if not flavor:
        await _blocked(cluster_id, project_id, ng_id, "missing_explicit_flavor")
        return
    estimated = _flavor_capacity(flavor, s.drover_stampede_resource_headroom_factor)
    if any(not _fits_capacity(_requests(pod), estimated) for pod in pending_pods):
        await _blocked(cluster_id, project_id, ng_id, "flavor_too_small")
        return
    needed = max(
        _binpack_count(pending_pods, {**flavor, "estimated_allocatable": estimated}),
        max(0, nodegroup.get("min_size", 0) - nodegroup.get("node_count", 0)),
    )
    room = max(0, nodegroup.get("max_size", 5) - nodegroup.get("node_count", 0))
    if needed <= 0:
        return
    if room <= 0:
        await _blocked(cluster_id, project_id, ng_id, "max_size_reached")
        return
    gpu_required = flavor.get("gpu", 0) > 0
    if s.drover_afterglow_admission_url:
        admitted_gpu, reason = await afterglow.check_gpu_admission(project_id, flavor["id"], settings=s)
        if reason:
            await _update_stampede_state(ng_id, cluster_id, {"quota_state": {"allowed": False, "reason": reason}})
            await _blocked(cluster_id, project_id, ng_id, reason)
            return
        gpu_required = gpu_required or admitted_gpu
    await _update_stampede_state(ng_id, cluster_id, {"quota_state": {"allowed": True}, "flavor_summary": {**flavor, "estimated_allocatable": estimated}})
    reservation = await jobs.enqueue_stampede_job(
        cluster_id, project_id, ng_id, direction="up", requested_count=min(needed, room),
        expected_node_count=nodegroup.get("node_count", 0),
        payload={
            "gpu_required": gpu_required,
            "gpu_count": max(1, flavor.get("gpu", 0)) if gpu_required else 0,
            "provisioning_key_prefix": f"stampede-{cluster_id}-{ng_id}-{uuid.uuid4().hex}",
            "triggering_metric": "pending_pods" if pending_pods else "min_size",
        },
    )
    if reservation is None:
        await _blocked(cluster_id, project_id, ng_id, "operation_in_progress")
        return
    await _record_stampede_event(project_id, cluster_id, ng_id, "scale_up", "started", {
        "add_count": reservation["count"], "flavor_id": flavor["id"], "gpu_required": gpu_required,
        "pending_pod_count": len(pending_pods), "requested_nodes": needed, **reservation,
    })


async def _provision_and_track(
    project_id: str, cluster_id: str, nodegroup_id: str, add_count: int, flavor_id: str,
    image_id: str | None, labels: dict | None, taints: list | None,
    gpu_required: bool = False, provisioning_key_prefix: str | None = None,
    operation_id: str | None = None, triggering_metric: str | dict | None = None,
    gpu_count: int = 0,
) -> None:
    """A durable job succeeds only once its workers and GPU devices are schedulable."""
    from drover.services import autoscale, kube, nodegroup, operations

    new_vms = await autoscale.provision_nodegroup_vms(
        project_id=project_id, cluster_id=cluster_id, nodegroup_id=nodegroup_id, add_count=add_count,
        flavor_id=flavor_id, image_id=image_id, labels=labels, taints=taints,
        provisioning_key_prefix=provisioning_key_prefix,
        gpu_required=gpu_required,
    )
    if gpu_required:
        from drover.services.gpu import ensure_device_plugin

        await ensure_device_plugin(cluster_id)
    deadline = time.monotonic() + 2400

    async def observe(vm: dict) -> tuple[str, str]:
        name = vm.get("name", "")
        ready = bool(name) and await kube.wait_node_ready(cluster_id, name, timeout=max(0, deadline - time.monotonic()))
        failure = "" if ready else "node_not_ready"
        if ready and gpu_required and not await kube.wait_node_gpu_allocatable(cluster_id, name, min_gpu=max(1, gpu_count), timeout=600.0):
            failure = "gpu_not_allocatable"
        await nodegroup.set_nodegroup_vm_status(nodegroup_id, vm["vm_id"], "ERROR" if failure else "ACTIVE")
        return name, failure

    observed = await asyncio.gather(*(observe(vm) for vm in new_vms))
    ready_nodes = [name for name, failure in observed if not failure]
    failed_nodes = [name for name, failure in observed if failure]
    if len(new_vms) != add_count:
        reason = "provision_failed"
    elif any(failure == "node_not_ready" for _, failure in observed):
        reason = "node_not_ready"
    else:
        reason = "gpu_not_allocatable" if failed_nodes else ""
    await _update_stampede_state(nodegroup_id, cluster_id, {"last_blocked_reason": reason, "ready_nodes": ready_nodes, "failed_nodes": failed_nodes})
    result = {"nodegroup_id": nodegroup_id, "add_count": add_count, "vm_ids": [vm["vm_id"] for vm in new_vms],
              "ready_nodes": ready_nodes, "failed_nodes": failed_nodes, "reason": reason,
              "triggering_metric": triggering_metric or "pending_pods"}
    await _record_stampede_event(project_id, cluster_id, nodegroup_id, "scale_up", "failed" if reason else "success", result)
    if operation_id:
        await operations.append_operation_event(None, operation_id, phase="stampede_workers_observed", payload_json=result)
    if reason:
        raise RuntimeError(reason)


# ---------------------------------------------------------------------------
# scale-down
# ---------------------------------------------------------------------------


def _removal_block(candidate: dict, node_pods: list[dict], node_capacities: list[dict]) -> str:
    labels = candidate.get("labels") or {}
    if any(key in labels for key in ("node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master", "node-role.kubernetes.io/etcd")):
        return "control_plane_node"
    pods = [pod for pod in node_pods if pod.get("node") == candidate["name"] and not pod.get("is_daemonset")]
    for pod in pods:
        if pod.get("is_mirror") or not pod.get("has_controller", False):
            return "unmanaged_pod"
        if pod.get("safe_to_evict") is False or pod.get("has_local_storage") or pod.get("has_pvc"):
            return "protected_pod"
        if pod.get("deleting") or _scheduling_block(pod):
            return "unsupported_relocation"
    free = _free_nodes(node_pods, [node for node in node_capacities if node["name"] != candidate["name"]])
    for pod in sorted(pods, key=lambda p: (_requests(p).get("gpu", 0), _requests(p).get("memory_bytes", 0)), reverse=True):
        if not _pod_fits_existing_capacity(pod, free):
            return "scale_down_no_fit"
    return ""


async def _scale_down_nodegroup(
    cluster_id: str, project_id: str, nodegroup: dict,
    node_pods: list[dict], node_capacities: list[dict], s,
) -> None:
    """Remove one managed worker only after continuous low demand and a relocation fit."""
    from drover.services.jobs import enqueue_stampede_job

    ng_id = nodegroup["id"]
    state = nodegroup.get("stampede_state") or {}
    now = time.time()
    if nodegroup.get("node_count", 0) <= nodegroup.get("min_size", 0) or state.get("in_flight_count", 0):
        await _update_stampede_state(ng_id, cluster_id, {"idle_since": {}})
        return
    if now - max(state.get("last_scale_down", 0), state.get("last_scale_up", 0)) < s.drover_stampede_scale_down_cooldown:
        await _blocked(cluster_id, project_id, ng_id, "scale_down_cooldown")
        return
    names = {vm.get("name") for vm in nodegroup.get("vms") or []}
    nodes = [node for node in node_capacities if node.get("name") in names]
    if len(nodes) != len(names) or any(not node.get("ready") or node.get("unschedulable") for node in nodes):
        await _update_stampede_state(ng_id, cluster_id, {"idle_since": {}, "last_blocked_reason": "node_not_ready"})
        return
    previous = state.get("idle_since") or {}
    idle_since = {}
    candidates = []
    blocked_reason = ""
    for node in nodes:
        alloc = node.get("allocatable") or {}
        used = {key: sum(_requests(pod).get(key, 0) for pod in node_pods if pod.get("node") == node["name"]) for key in ("cpu_m", "memory_bytes", "gpu")}
        utilization = max(used["cpu_m"] / alloc["cpu_m"] if alloc.get("cpu_m") else 1,
                          used["memory_bytes"] / alloc["memory_bytes"] if alloc.get("memory_bytes") else 1,
                          used["gpu"] / alloc["gpu"] if alloc.get("gpu") else 0)
        if utilization >= s.drover_stampede_scale_down_threshold:
            continue
        reason = _removal_block(node, node_pods, node_capacities)
        if reason:
            blocked_reason = reason
            continue
        since = previous.get(node["name"], now)
        idle_since[node["name"]] = since
        if now - since >= s.drover_stampede_scale_down_window:
            candidates.append((utilization, node))
    await _update_stampede_state(ng_id, cluster_id, {
        "idle_since": idle_since, "last_decision": "stabilizing" if idle_since else "within_capacity",
        "last_blocked_reason": blocked_reason,
    })
    if not candidates:
        return
    remove_node = min(candidates, key=lambda item: (item[0], item[1]["name"]))[1]
    entry = next(vm for vm in nodegroup["vms"] if vm.get("name") == remove_node["name"])
    reservation = await enqueue_stampede_job(
        cluster_id, project_id, ng_id, direction="down", requested_count=1,
        expected_node_count=nodegroup.get("node_count", 0),
        payload={"remove_entries": [entry], "triggering_metric": "excess_requested_capacity"},
    )
    if reservation:
        await _record_stampede_event(project_id, cluster_id, ng_id, "scale_down", "started", {"node_name": remove_node["name"], **reservation})


async def _delete_and_track(project_id: str, cluster_id: str, payload: dict, operation_id: str | None) -> None:
    """Guard live VM deletion without blocking cleanup of confirmed-absent servers."""
    from drover.services import autoscale, keystone, kube, nodegroup, nova

    ng_id = payload["nodegroup"]["id"]
    ng = await nodegroup.get_nodegroup(cluster_id, ng_id)
    if not ng:
        raise RuntimeError("nodegroup_missing")
    entries = payload.get("remove_entries") or []
    tracked_ids = {vm["vm_id"] for vm in ng.get("vms") or [] if vm.get("vm_id")}
    requested_ids = set()
    for entry in entries:
        if not entry.get("vm_id"):
            raise ValueError("Deletion entry is missing vm_id")
        requested_ids.add(entry["vm_id"])
    live_ids = set()
    if entries:
        async with keystone.project_manager_connection(project_id) as conn:
            for vm_id in tracked_ids | requested_ids:
                server = await asyncio.to_thread(nova.observe_server, conn, vm_id)
                if server is not None and not nova.is_server_deleting(server):
                    live_ids.add(vm_id)
    # Lookup/auth failures propagate; absent/deleting servers only resume cleanup.
    live_entries = [entry for entry in entries if entry["vm_id"] in live_ids]
    remaining_live = len(tracked_ids & live_ids)
    if live_entries:
        pending, capacities, pods = await asyncio.gather(kube.list_unschedulable_pods(cluster_id), kube.get_node_capacity(cluster_id), kube.get_pod_resource_usage(cluster_id))
        for entry in live_entries:
            candidate = next((node for node in capacities if node.get("name") == entry.get("name")), None)
            if candidate is None:
                raise RuntimeError("scale_down_node_missing")
            reason = "pending_pods_present" if pending else _removal_block(candidate, pods, capacities)
            # Only this removal's own retained cordon may resume; any other cordon blocks.
            foreign_cordon = candidate.get("unschedulable") and candidate.get("removal_vm_id") != entry["vm_id"]
            if not candidate.get("ready") or foreign_cordon:
                reason = "node_not_ready"
            if remaining_live <= ng.get("min_size", 0):
                reason = "min_size_reached"
            if reason:
                await _blocked(cluster_id, project_id, ng_id, reason)
                raise RuntimeError(reason)
            remaining_live -= 1
    await autoscale.delete_nodegroup_and_reconcile(
        project_id, cluster_id, ng, entries, operation_id=operation_id,
        triggering_metric=payload.get("triggering_metric"),
    )


# ---------------------------------------------------------------------------
# 메인 reconcile 루프
# ---------------------------------------------------------------------------


async def reconcile_cluster(cluster: dict) -> None:
    from drover.services import kube, nodegroup

    cluster_id = cluster.get("id") or cluster.get("cluster_id", "")
    project_id = cluster.get("project_id", "")
    if not cluster_id or not project_id:
        return
    settings = get_settings()
    groups = [ng for ng in await nodegroup.list_nodegroups(cluster_id) if ng.get("stampede_enabled") and ng.get("role") == "agent" and ng.get("flavor_id")]
    if not groups:
        return
    try:
        pending, capacities, pods = await asyncio.gather(kube.list_unschedulable_pods(cluster_id), kube.get_node_capacity(cluster_id), kube.get_pod_resource_usage(cluster_id))
        flavors = {item["id"]: item for item in await _get_available_flavors(project_id)}
    except Exception:
        _logger.warning("Stampede observation failed cluster_id=%s", cluster_id)
        for ng in groups:
            await _update_stampede_state(ng["id"], cluster_id, {"idle_since": {}, "last_decision": "observation_failed", "last_blocked_reason": "observation_unavailable"})
        return
    assignments, blocked, summaries = _assign_pending_pods(pending, groups, flavors, pods, capacities, settings.drover_stampede_resource_headroom_factor)
    blocked_summary = [{"namespace": item["pod"].get("namespace"), "name": item["pod"].get("name"), "reason": item["reason"], "message": item["pod"].get("message", "")} for item in blocked]
    observed_at = time.time()
    for ng in groups:
        names = {vm.get("name") for vm in ng.get("vms") or []}
        updates = {
            "observed_at": observed_at, "capacity": summaries[ng["id"]], "blocked_reasons": blocked_summary,
            "pending_assignments": [{"namespace": pod.get("namespace"), "name": pod.get("name"), "resources": _requests(pod)} for pod in assignments[ng["id"]]],
            "tracked_count": len(ng.get("vms") or []), "ready_count": sum(node.get("ready", False) for node in capacities if node.get("name") in names),
        }
        previous_observation = (ng.get("stampede_state") or {}).get("observed_at")
        observation_continuous = isinstance(previous_observation, (int, float)) and 0 <= observed_at - previous_observation <= 2 * settings.drover_stampede_interval
        if pending or not observation_continuous:
            updates["idle_since"] = {}
        await _update_stampede_state(ng["id"], cluster_id, updates)
        ng["stampede_state"] = {**(ng.get("stampede_state") or {}), **updates}
        try:
            if assignments[ng["id"]] or ng.get("node_count", 0) < ng.get("min_size", 0):
                await _scale_up_nodegroup(cluster_id, project_id, ng, assignments[ng["id"]], pods, capacities, settings, flavor=flavors.get(ng["flavor_id"]))
            elif not pending:
                await _scale_down_nodegroup(cluster_id, project_id, ng, pods, capacities, settings)
        except Exception:
            _logger.warning("Stampede decision failed nodegroup_id=%s", ng["id"])
            await _update_stampede_state(ng["id"], cluster_id, {"idle_since": {}, "last_decision": "decision_failed", "last_blocked_reason": "decision_unavailable"})


async def run_all() -> None:
    from drover.services import store

    if not get_settings().drover_stampede_enabled:
        return
    clusters = [cluster for cluster in await store.list_all_clusters(include_deleted=False) if cluster.get("status") == "ACTIVE" and cluster.get("stampede_enabled")]
    results = await asyncio.gather(*(reconcile_cluster(cluster) for cluster in clusters), return_exceptions=True)
    for cluster, result in zip(clusters, results, strict=True):
        if isinstance(result, Exception):
            _logger.warning("Stampede reconcile failed cluster_id=%s", cluster["id"])
