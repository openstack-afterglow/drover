"""Stampede Reconciler — k3s 노드 오토스케일 내부 루프.

drover worker.py에서 주기적으로 run_all()을 호출.
ACTIVE + stampede_enabled 클러스터의 각 노드그룹을 순회하며:
  - Unschedulable pod 감지 → fit-check → VM 추가 (scale-up)
  - 유휴 노드 감지 → cordon/drain/삭제 (scale-down)
"""

import asyncio
import logging
import time

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
    """Stampede 스케일 이벤트를 activity_logs에 영속화 (best-effort)."""
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
    except Exception as e:
        _logger.debug("stampede: 이벤트 기록 실패 (무시): %s", e)


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------


def _node_matches_nodegroup(pod: dict, nodegroup: dict) -> bool:
    """pod가 이 nodegroup에 스케줄 가능한지 기초 매칭.

    확인 항목:
      1. pod.nodeSelector 키가 nodegroup.labels에 모두 포함되는지
      2. pod.tolerations가 nodegroup.taints를 커버하는지 (NoSchedule/NoExecute)
    """
    ng_labels: dict = nodegroup.get("labels") or {}
    ng_taints: list = nodegroup.get("taints") or []

    # nodeSelector 체크
    node_selector: dict = pod.get("node_selector") or {}
    for k, v in node_selector.items():
        if ng_labels.get(k) != v:
            return False

    # taints 체크 (NoSchedule / NoExecute — pod가 tolerate해야 배포 가능)
    pod_tolerations: list = pod.get("tolerations") or []

    def _tolerates(taint: dict) -> bool:
        effect = taint.get("effect", "")
        if effect not in ("NoSchedule", "NoExecute", ""):
            return True  # PreferNoSchedule은 스케줄 차단 안 함
        t_key = taint.get("key", "")
        t_val = taint.get("value")
        for tol in pod_tolerations:
            if tol.get("operator") == "Exists":
                if not tol.get("key") or tol.get("key") == t_key:
                    return True
            elif tol.get("key") == t_key and (tol.get("value") is None or tol.get("value") == t_val):
                return True
        return False

    if not all(not (isinstance(taint, dict) and not _tolerates(taint)) for taint in ng_taints):
        return False

    # required node affinity: at least one term must be satisfied; all expressions in a term are AND.
    required = ((pod.get("affinity") or {}).get("nodeAffinity") or {}).get(
        "requiredDuringSchedulingIgnoredDuringExecution", {}
    ).get("nodeSelectorTerms") or []
    if required:
        term_ok = False
        for term in required:
            exprs = term.get("matchExpressions") or []
            fields = term.get("matchFields") or []
            if fields:
                continue
            expr_ok = True
            for expr in exprs:
                key = expr.get("key", "")
                op = expr.get("operator", "In")
                values = [str(v) for v in (expr.get("values") or [])]
                actual = ng_labels.get(key)
                if (
                    (op == "In" and actual not in values)
                    or (op == "NotIn" and actual in values)
                    or (op == "Exists" and key not in ng_labels)
                    or (op == "DoesNotExist" and key in ng_labels)
                ):
                    expr_ok = False
                elif op == "Gt":
                    try:
                        expr_ok = actual is not None and int(actual) > int(values[0])
                    except (TypeError, ValueError, IndexError):
                        expr_ok = False
                elif op == "Lt":
                    try:
                        expr_ok = actual is not None and int(actual) < int(values[0])
                    except (TypeError, ValueError, IndexError):
                        expr_ok = False
                if not expr_ok:
                    break
            if expr_ok:
                term_ok = True
                break
        if not term_ok:
            return False

    return True


def _is_pvc_issue(pod: dict) -> bool:
    """pod가 PVC 미바운드로 Unschedulable인지 추정."""
    msg: str = pod.get("message", "").lower()
    return "persistentvolumeclaim" in msg or "pvc" in msg or "volume" in msg


def _pod_fits_flavor(pod: dict, flavor: dict) -> bool:
    """pod의 resource requests가 flavor capacity에 맞는지."""
    req = pod.get("resource_requests", {})
    cpu_m = req.get("cpu_m", 0)
    mem = req.get("memory_bytes", 0)
    gpu = req.get("gpu", 0)
    if cpu_m > flavor.get("vcpus_m", 0):
        return False
    if mem > flavor.get("ram_bytes", 0):
        return False
    if gpu > flavor.get("gpu", 0):
        return False
    return True


def _select_flavor(
    pending_pods: list[dict],
    node_pods: list[dict],
    available_flavors: list[dict],
    nodegroup: dict,
    headroom_factor: float,
) -> dict | None:
    """가중치 기반 flavor 자동 선택.

    target = Σ(pending pod requests) × (1 + α × 기존_부하_비율)
    → target 이상 capacity를 가진 최소 flavor 반환.
    """
    if not available_flavors:
        return None

    # pending pod requests 합산 (이 nodegroup에 할당될 pod만)
    total_cpu_m = sum(p["resource_requests"]["cpu_m"] for p in pending_pods)
    total_mem = sum(p["resource_requests"]["memory_bytes"] for p in pending_pods)
    needs_gpu = any(p["resource_requests"].get("gpu", 0) > 0 for p in pending_pods)

    # 기존 부하 비율 계산 (nodegroup 노드들의 평균 사용률)
    ng_node_names = {v["name"] for v in nodegroup.get("vms", []) if v.get("name")}
    ng_pods = [p for p in node_pods if p.get("node") in ng_node_names]

    existing_cpu_m = sum(p["cpu_m"] for p in ng_pods)
    existing_mem = sum(p["memory_bytes"] for p in ng_pods)

    # 노드그룹 총 allocatable (간략 추정: node_count × 첫 flavor capacity)
    node_count = nodegroup.get("node_count", 0)
    if node_count > 0 and available_flavors:
        ref = available_flavors[0]
        ng_total_cpu_m = node_count * ref.get("vcpus_m", 1)
        ng_total_mem = node_count * ref.get("ram_bytes", 1)
        existing_cpu_frac = min(existing_cpu_m / ng_total_cpu_m, 1.0) if ng_total_cpu_m else 0.0
        existing_mem_frac = min(existing_mem / ng_total_mem, 1.0) if ng_total_mem else 0.0
    else:
        existing_cpu_frac = 0.0
        existing_mem_frac = 0.0

    # 헤드룸 적용
    target_cpu_m = int(total_cpu_m * (1 + headroom_factor * existing_cpu_frac))
    target_mem = int(total_mem * (1 + headroom_factor * existing_mem_frac))

    # 최소 1 pod를 수용할 수 있는 후보 필터
    max_pod_cpu_m = max((p["resource_requests"]["cpu_m"] for p in pending_pods), default=0)
    max_pod_mem = max((p["resource_requests"]["memory_bytes"] for p in pending_pods), default=0)
    max_pod_gpu = max((p["resource_requests"].get("gpu", 0) for p in pending_pods), default=0)

    candidates = [
        f
        for f in available_flavors
        if f.get("vcpus_m", 0) >= max(max_pod_cpu_m, target_cpu_m)
        and f.get("ram_bytes", 0) >= max(max_pod_mem, target_mem)
        and (not needs_gpu or f.get("gpu", 0) >= max_pod_gpu)
    ]

    if not candidates:
        # target을 맞출 수 없으면 최소한 한 pod라도 담을 수 있는 것 선택
        candidates = [
            f
            for f in available_flavors
            if f.get("vcpus_m", 0) >= max_pod_cpu_m
            and f.get("ram_bytes", 0) >= max_pod_mem
            and (not needs_gpu or f.get("gpu", 0) >= max_pod_gpu)
        ]

    if not candidates:
        return None

    # 최소 flavor (비용 최소화)
    return min(candidates, key=lambda f: (f.get("vcpus_m", 0), f.get("ram_bytes", 0)))


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
        if ":" not in entry:
            continue
        gpu_alias, _, count = entry.strip().rpartition(":")
        if "audio" in gpu_alias.lower():
            continue
        try:
            total += int(count)
        except ValueError:
            total += 1
    return total


def _fits_capacity(req: dict, cap: dict) -> bool:
    return (
        req.get("cpu_m", 0) <= cap.get("cpu_m", cap.get("vcpus_m", 0))
        and req.get("memory_bytes", 0) <= cap.get("memory_bytes", cap.get("ram_bytes", 0))
        and req.get("gpu", 0) <= cap.get("gpu", 0)
    )


def _binpack_count(pods: list[dict], flavor: dict) -> int:
    bins: list[dict] = []
    ordered = sorted(
        pods,
        key=lambda p: (
            p.get("resource_requests", {}).get("gpu", 0),
            p.get("resource_requests", {}).get("memory_bytes", 0),
            p.get("resource_requests", {}).get("cpu_m", 0),
        ),
        reverse=True,
    )
    for pod in ordered:
        req = pod.get("resource_requests", {})
        placed = False
        for free in bins:
            if _fits_capacity(req, free):
                free["cpu_m"] -= req.get("cpu_m", 0)
                free["memory_bytes"] -= req.get("memory_bytes", 0)
                free["gpu"] -= req.get("gpu", 0)
                placed = True
                break
        if not placed:
            bins.append(
                {
                    "cpu_m": flavor.get("vcpus_m", 0) - req.get("cpu_m", 0),
                    "memory_bytes": flavor.get("ram_bytes", 0) - req.get("memory_bytes", 0),
                    "gpu": flavor.get("gpu", 0) - req.get("gpu", 0),
                }
            )
    return len(bins)


def _nodegroup_resource_summary(nodegroup: dict, node_pods: list[dict], node_capacities: list[dict]) -> dict:
    ng_node_names = {v["name"] for v in nodegroup.get("vms", []) if v.get("name")}
    nodes = [n for n in node_capacities if n.get("name") in ng_node_names and n.get("ready")]
    used_by_node: dict[str, dict] = {}
    for pod in node_pods:
        node = pod.get("node", "")
        if node not in ng_node_names:
            continue
        used = used_by_node.setdefault(node, {"cpu_m": 0, "memory_bytes": 0, "gpu": 0})
        used["cpu_m"] += pod.get("cpu_m", 0)
        used["memory_bytes"] += pod.get("memory_bytes", 0)
        used["gpu"] += pod.get("gpu", 0)
    totals = {
        "allocatable": {"cpu_m": 0, "memory_bytes": 0, "gpu": 0},
        "requested": {"cpu_m": 0, "memory_bytes": 0, "gpu": 0},
        "free": {"cpu_m": 0, "memory_bytes": 0, "gpu": 0},
    }
    free_nodes: list[dict] = []
    for node in nodes:
        alloc = node.get("allocatable", {})
        used = used_by_node.get(node["name"], {"cpu_m": 0, "memory_bytes": 0, "gpu": 0})
        free = {
            "name": node["name"],
            "cpu_m": max(0, alloc.get("cpu_m", 0) - used.get("cpu_m", 0)),
            "memory_bytes": max(0, alloc.get("memory_bytes", 0) - used.get("memory_bytes", 0)),
            "gpu": max(0, alloc.get("gpu", 0) - used.get("gpu", 0)),
        }
        free_nodes.append(free)
        for key in ("cpu_m", "memory_bytes", "gpu"):
            totals["allocatable"][key] += alloc.get(key, 0)
            totals["requested"][key] += used.get(key, 0)
            totals["free"][key] += free[key]
    totals["nodes"] = free_nodes
    return totals


def _pod_fits_existing_capacity(pod: dict, free_nodes: list[dict]) -> bool:
    req = pod.get("resource_requests", {})
    for free in free_nodes:
        if _fits_capacity(req, free):
            free["cpu_m"] -= req.get("cpu_m", 0)
            free["memory_bytes"] -= req.get("memory_bytes", 0)
            free["gpu"] -= req.get("gpu", 0)
            return True
    return False


def _assign_pending_pods(
    pending_pods: list[dict],
    nodegroups: list[dict],
    flavors_by_id: dict[str, dict],
    node_pods: list[dict],
    node_capacities: list[dict],
) -> tuple[dict[str, list[dict]], list[dict], dict[str, dict]]:
    summaries = {ng["id"]: _nodegroup_resource_summary(ng, node_pods, node_capacities) for ng in nodegroups}
    assignments: dict[str, list[dict]] = {ng["id"]: [] for ng in nodegroups}
    blocked: list[dict] = []
    for pod in pending_pods:
        if _is_pvc_issue(pod):
            blocked.append({"pod": pod, "reason": "pvc_unbound"})
            continue
        pinned = pod.get("node_name") or ""
        if pinned:
            tracked = {v.get("name") for ng in nodegroups for v in ng.get("vms", [])}
            if pinned not in tracked:
                blocked.append({"pod": pod, "reason": "pinned_missing_node"})
                continue
        candidates = []
        for ng in nodegroups:
            flavor = flavors_by_id.get(ng.get("flavor_id"))
            if not flavor:
                continue
            if not _node_matches_nodegroup(pod, ng):
                continue
            if not _pod_fits_flavor(pod, flavor):
                continue
            candidates.append((ng, flavor))
        if not candidates:
            blocked.append({"pod": pod, "reason": "no_matching_nodegroup"})
            continue
        candidates.sort(
            key=lambda pair: (
                pair[1].get("gpu", 0),
                pair[1].get("vcpus_m", 0),
                pair[1].get("ram_bytes", 0),
                -summaries[pair[0]["id"]]["free"].get("cpu_m", 0),
            )
        )
        assignments[candidates[0][0]["id"]].append(pod)
    return assignments, blocked, summaries


async def _get_available_flavors(project_id: str) -> list[dict]:
    """OpenStack flavor 목록 → Stampede용 구조로 변환."""
    from drover.services import keystone, nova

    try:
        conn = keystone.get_admin_connection_for_project(project_id)
        flavors_raw = await asyncio.to_thread(nova.list_flavors, conn)
        result = []
        for f in flavors_raw:
            extra_specs = getattr(f, "extra_specs", {}) or {}
            gpu = _flavor_gpu_count(extra_specs)
            result.append(
                {
                    "id": f.id,
                    "name": f.name,
                    "vcpus_m": int(f.vcpus or 0) * 1000,
                    "ram_bytes": int(f.ram or 0) * 1024 * 1024,
                    "gpu": gpu,
                    "extra_specs": extra_specs,
                }
            )
        return result
    except Exception as e:
        _logger.warning("_get_available_flavors 오류 (project=%s): %s", project_id, e)
        return []


async def _check_gpu_quota_for_nodes(project_id: str, flavor: dict, add_count: int) -> tuple[bool, str]:
    if flavor.get("gpu", 0) <= 0:
        return True, ""
    from drover.services import gpu_quota, keystone

    try:
        conn = keystone.get_admin_connection_for_project(project_id)
        base_specs = dict(flavor.get("extra_specs") or {})
        alias_counts = gpu_quota._parse_alias_counts(base_specs)
        if not alias_counts:
            return False, "gpu flavor is missing pci_passthrough:alias for quota accounting"
        requested: dict[str, int] = {}
        for alias, count in alias_counts.items():
            canonical = gpu_quota.normalize_gpu_alias(alias)
            requested[canonical] = requested.get(canonical, 0) + count * add_count
        effective = await gpu_quota.get_effective_gpu_quotas(project_id)
        usage = await gpu_quota.get_project_gpu_usage(conn, project_id)
        for alias, count in requested.items():
            limit = effective.get(alias, 0)
            if limit == -1:
                continue
            current = usage.get(alias, 0)
            if current + count > limit:
                return False, f"{alias}: current={current}, requested={count}, quota={limit}"
        return True, ""
    except Exception as e:
        return False, f"gpu quota check failed: {e}"


async def _update_stampede_state(nodegroup_id: str, cluster_id: str, updates: dict) -> None:
    """nodegroup.stampede_state 를 원자적으로 갱신 (merge patch)."""
    from drover.services import nodegroup as k3s_nodegroup

    current = await k3s_nodegroup.get_nodegroup(cluster_id, nodegroup_id)
    if not current:
        return
    state = dict(current.get("stampede_state") or {})
    state.update(updates)
    await k3s_nodegroup.update_nodegroup(cluster_id, nodegroup_id, {"stampede_state": state})


async def _get_stampede_state(nodegroup: dict) -> dict:
    return dict(nodegroup.get("stampede_state") or {})


# ---------------------------------------------------------------------------
# scale-up
# ---------------------------------------------------------------------------


async def _scale_up_nodegroup(
    cluster_id: str,
    project_id: str,
    nodegroup: dict,
    pending_pods: list[dict],
    node_pods: list[dict],
    node_capacities: list[dict],
    s,
) -> None:
    """nodegroup에 노드를 추가한다."""
    from drover.services import nodegroup as k3s_nodegroup

    ng_id = nodegroup["id"]
    max_size = nodegroup.get("max_size", 5)
    node_count = nodegroup.get("node_count", 0)
    state = await _get_stampede_state(nodegroup)

    # 쿨다운 체크
    last_up = state.get("last_scale_up", 0)
    cooldown = s.drover_stampede_scale_up_cooldown
    if time.time() - last_up < cooldown:
        rem = cooldown - (time.time() - last_up)
        _logger.debug(
            "stampede: nodegroup %s scale-up 쿨다운 중 (%.0fs 남음)", ng_id, rem
        )
        await _record_stampede_event(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=ng_id,
            action="cooldown",
            status="skipped",
            extra={
                "reason": "cooldown",
                "direction": "scale_up",
                "cooldown_seconds": cooldown,
                "remaining_seconds": rem,
                "triggering_metric": "pending_pods",
                "pending_pod_count": len(pending_pods),
            },
        )
        return

    # in-flight 노드 수 (아직 Ready 안 된 것)
    in_flight = state.get("in_flight_count", 0)
    # in-flight TTL: 40분 초과 시 stale로 간주해 0으로 리셋
    in_flight_since = state.get("in_flight_since", 0)
    if in_flight > 0 and time.time() - in_flight_since > 2400:  # 40분
        _logger.warning("stampede: nodegroup %s in-flight stale, 리셋", ng_id)
        in_flight = 0
        await _update_stampede_state(ng_id, cluster_id, {"in_flight_count": 0, "in_flight_since": 0})

    summary = _nodegroup_resource_summary(nodegroup, node_pods, node_capacities)
    free_nodes = [dict(node) for node in summary["nodes"]]
    unresolvable_pods = []
    for pod in pending_pods:
        if not _pod_fits_existing_capacity(pod, free_nodes):
            unresolvable_pods.append(pod)

    if not unresolvable_pods:
        _logger.debug("stampede: nodegroup %s — pending pod 없음 또는 기존 노드로 해결 가능", ng_id)
        await _update_stampede_state(ng_id, cluster_id, {"capacity": summary, "last_decision": "existing_capacity"})
        return

    available_flavors = await _get_available_flavors(project_id)
    flavors_by_id = {f["id"]: f for f in available_flavors}
    flavor = flavors_by_id.get(nodegroup.get("flavor_id") or "")
    if not flavor:
        await _record_stampede_event(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=ng_id,
            action="blocked",
            status="skipped",
            extra={"reason": "missing_explicit_flavor", "pod_count": len(unresolvable_pods)},
        )
        await _update_stampede_state(ng_id, cluster_id, {"last_blocked_reason": "missing_explicit_flavor"})
        return
    too_large = [p for p in unresolvable_pods if not _pod_fits_flavor(p, flavor)]
    if too_large:
        await _record_stampede_event(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=ng_id,
            action="blocked",
            status="skipped",
            extra={"reason": "flavor_too_small", "pod_count": len(too_large), "flavor_id": flavor["id"]},
        )
        await _update_stampede_state(ng_id, cluster_id, {"last_blocked_reason": "flavor_too_small"})
        return

    requested_count = _binpack_count(unresolvable_pods, flavor)
    capacity_left = max_size - node_count - in_flight
    if capacity_left <= 0:
        await _record_stampede_event(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=ng_id,
            action="blocked",
            status="skipped",
            extra={"reason": "max_size_reached", "pod_count": len(unresolvable_pods), "in_flight": in_flight},
        )
        await _update_stampede_state(ng_id, cluster_id, {"last_blocked_reason": "max_size_reached"})
        return
    add_count = min(requested_count, capacity_left)
    if add_count < requested_count:
        await _record_stampede_event(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=ng_id,
            action="blocked",
            status="skipped",
            extra={
                "reason": "max_size_cap",
                "requested_nodes": requested_count,
                "add_count": add_count,
                "unresolved_pods": len(unresolvable_pods),
            },
        )

    if flavor.get("gpu", 0) > 0:
        quota_ok, quota_reason = await _check_gpu_quota_for_nodes(project_id, flavor, add_count)
        if not quota_ok:
            await _record_stampede_event(
                project_id=project_id,
                cluster_id=cluster_id,
                nodegroup_id=ng_id,
                action="blocked",
                status="skipped",
                extra={
                    "reason": "gpu_quota",
                    "message": quota_reason,
                    "add_count": add_count,
                    "flavor_id": flavor["id"],
                },
            )
            await _update_stampede_state(ng_id, cluster_id, {"last_blocked_reason": "gpu_quota"})
            return

    _logger.info(
        "stampede: nodegroup %s scale-up %d개 (flavor=%s, unresolvable_pods=%d, in_flight=%d)",
        ng_id,
        add_count,
        flavor["name"],
        len(unresolvable_pods),
        in_flight,
    )

    # in-flight 증가 (즉시 DB 갱신 → 다음 루프에서 중복 scale-up 방지)
    await _update_stampede_state(
        ng_id,
        cluster_id,
        {
            "in_flight_count": in_flight + add_count,
            "in_flight_since": time.time(),
            "last_scale_up": time.time(),
        },
    )
    # node_count도 즉시 증가
    await k3s_nodegroup.update_nodegroup(
        cluster_id,
        ng_id,
        {
            "node_count": node_count + add_count,
        },
    )

    # scale-up 이벤트 기록
    await _record_stampede_event(
        project_id=project_id,
        cluster_id=cluster_id,
        nodegroup_id=ng_id,
        action="scale_up",
        status="started",
        extra={
            "add_count": add_count,
            "flavor_id": flavor["id"],
            "flavor_name": flavor.get("name", ""),
            "triggering_metric": "pending_pods",
            "pending_pod_count": len(unresolvable_pods),
        },
    )

    # Persist before returning: the worker, not this API/reconcile process,
    # owns VM provisioning and can reclaim a crashed lease.
    from drover.services.jobs import enqueue_job

    await enqueue_job(
        cluster_id=cluster_id,
        project_id=project_id,
        kind="stampede_provision",
        payload={
            "nodegroup_id": ng_id,
            "add_count": add_count,
            "flavor_id": flavor["id"],
            "image_id": nodegroup.get("image_id"),
            "labels": nodegroup.get("labels"),
            "taints": nodegroup.get("taints"),
            "gpu_required": flavor.get("gpu", 0) > 0,
        },
        user_id="stampede-system",
        username="Stampede",
    )


async def _provision_and_track(
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
    add_count: int,
    flavor_id: str,
    image_id: str | None,
    labels: dict | None,
    taints: list | None,
    gpu_required: bool = False,
    operation_id: str | None = None,
    triggering_metric: str | dict | None = None,
) -> None:
    """VM 프로비저닝 후 Ready/GPU 대기, in-flight 카운터 감소."""
    from drover.services import autoscale as k3s_autoscale
    from drover.services import kube as k3s_kube
    from drover.services import nodegroup as k3s_nodegroup

    provision_error = ""
    try:
        new_vms = await k3s_autoscale.provision_nodegroup_vms(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=nodegroup_id,
            add_count=add_count,
            flavor_id=flavor_id,
            image_id=image_id,
            labels=labels,
            taints=taints,
        )
    except Exception:
        _logger.exception("stampede: nodegroup %s provisioning failed", nodegroup_id)
        new_vms = []
        provision_error = "provision_failed"
    new_vm_ids = [v["vm_id"] for v in new_vms if v.get("vm_id")]
    await k3s_autoscale.reconcile_nodegroup_vms(project_id, cluster_id, nodegroup_id)
    missing_count = max(0, add_count - len(new_vms))
    if missing_count:
        _logger.warning(
            "stampede: nodegroup %s provisioning returned %d/%d VMs",
            nodegroup_id,
            len(new_vms),
            add_count,
        )
    ready_nodes = []
    failed_nodes = []
    for vm in new_vms:
        node_name = vm.get("name", "")
        if not node_name:
            continue
        ready = await k3s_kube.wait_node_ready(cluster_id, node_name, timeout=2400.0)
        if not ready:
            _logger.warning("stampede: node %s Ready 대기 timeout (40분)", node_name)
            failed_nodes.append(node_name)
            continue
        if gpu_required:
            gpu_ok = await k3s_kube.wait_node_gpu_allocatable(cluster_id, node_name, min_gpu=1, timeout=600.0)
            if not gpu_ok:
                _logger.warning("stampede: node %s Ready but GPU allocatable timeout", node_name)
                failed_nodes.append(node_name)
                continue
        await k3s_nodegroup.update_nodegroup(cluster_id, nodegroup_id, {})
        _logger.info("stampede: node %s Ready 확인됨", node_name)
        ready_nodes.append(node_name)

    ng = await k3s_nodegroup.get_nodegroup(cluster_id, nodegroup_id)
    if ng:
        state = dict(ng.get("stampede_state") or {})
        current_in_flight = max(0, state.get("in_flight_count", 0) - add_count)
        updates = {"in_flight_count": current_in_flight}
        failure_count = missing_count + len(failed_nodes)
        if failure_count:
            reason = "provision_failed"
            if gpu_required and failed_nodes:
                reason = "gpu_not_allocatable"
            elif failed_nodes:
                reason = "node_not_ready"
            updates["last_blocked_reason"] = reason
            if failed_nodes:
                tracked_count = len(ng.get("vms") or [])
                actual_count = max(0, tracked_count - len(failed_nodes))
                await k3s_nodegroup.set_nodegroup_count(cluster_id, nodegroup_id, actual_count)
        await _update_stampede_state(nodegroup_id, cluster_id, updates)

    if missing_count:
        reason = "provision_failed"
    elif gpu_required and failed_nodes:
        reason = "gpu_not_allocatable"
    elif failed_nodes:
        reason = "node_not_ready"
    else:
        reason = ""
    final_status = "success" if not reason else ("failed" if not ready_nodes else "partial")
    await _record_stampede_event(
        project_id=project_id,
        cluster_id=cluster_id,
        nodegroup_id=nodegroup_id,
        action="scale_up",
        status=final_status,
        extra={
            "add_count": add_count,
            "flavor_id": flavor_id,
            "ready_nodes": ready_nodes,
            "failed_nodes": failed_nodes,
            "missing_count": missing_count,
            "provision_error": provision_error,
            "reason": reason,
            "vm_ids": new_vm_ids,
            "triggering_metric": triggering_metric or "pending_pods",
        },
    )
    if operation_id:
        from drover.services import operations

        await operations.append_operation_event(
            None,
            operation_id,
            phase="stampede_provision_complete",
            message=f"Stampede provisioned {len(new_vms)} VMs for nodegroup {nodegroup_id}",
            payload_json={
                "nodegroup_id": nodegroup_id,
                "add_count": add_count,
                "vm_ids": new_vm_ids,
                "ready_nodes": ready_nodes,
                "failed_nodes": failed_nodes,
                "triggering_metric": triggering_metric or "pending_pods",
            },
        )


# ---------------------------------------------------------------------------
# scale-down
# ---------------------------------------------------------------------------


async def _scale_down_nodegroup(
    cluster_id: str,
    project_id: str,
    nodegroup: dict,
    node_pods: list[dict],
    node_capacities: list[dict],
    s,
) -> None:
    """nodegroup의 유휴 노드를 cordon→drain→삭제한다."""
    from drover.services import nodegroup as k3s_nodegroup

    ng_id = nodegroup["id"]
    min_size = nodegroup.get("min_size", 0)
    node_count = nodegroup.get("node_count", 0)
    state = await _get_stampede_state(nodegroup)

    if node_count <= min_size:
        return

    # 쿨다운 체크
    last_down = state.get("last_scale_down", 0)
    cooldown = s.drover_stampede_scale_down_cooldown
    if time.time() - last_down < cooldown:
        rem = cooldown - (time.time() - last_down)
        await _record_stampede_event(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=ng_id,
            action="cooldown",
            status="skipped",
            extra={
                "reason": "cooldown",
                "direction": "scale_down",
                "cooldown_seconds": cooldown,
                "remaining_seconds": rem,
                "triggering_metric": "idle_nodes",
            },
        )
        return

    threshold = s.drover_stampede_scale_down_threshold
    window = s.drover_stampede_scale_down_window
    interval = s.drover_stampede_interval

    ng_node_names = {v["name"] for v in nodegroup.get("vms", []) if v.get("name")}
    ng_nodes = [n for n in node_capacities if n["name"] in ng_node_names and n["ready"]]

    evictable_by_node: dict[str, list[dict]] = {}
    node_used: dict[str, dict] = {}
    ng_node_set = {n["name"] for n in ng_nodes}
    for p in node_pods:
        nn = p.get("node", "")
        if nn not in ng_node_set:
            continue
        used = node_used.setdefault(nn, {"cpu_m": 0, "memory_bytes": 0, "gpu": 0})
        used["cpu_m"] += p.get("cpu_m", 0)
        used["memory_bytes"] += p.get("memory_bytes", 0)
        used["gpu"] += p.get("gpu", 0)
        if not p.get("is_daemonset") and not p.get("is_mirror"):
            evictable_by_node.setdefault(nn, []).append(p)

    idle_nodes = []
    for node in ng_nodes:
        alloc = node["allocatable"]
        used = node_used.get(node["name"], {"cpu_m": 0, "memory_bytes": 0, "gpu": 0})
        cpu_util = used["cpu_m"] / alloc["cpu_m"] if alloc.get("cpu_m") else 1.0
        mem_util = used["memory_bytes"] / alloc["memory_bytes"] if alloc.get("memory_bytes") else 1.0
        gpu_util = used["gpu"] / alloc["gpu"] if alloc.get("gpu") else 0.0
        if max(cpu_util, mem_util, gpu_util) < threshold:
            idle_nodes.append((node, len(evictable_by_node.get(node["name"], [])), max(cpu_util, mem_util, gpu_util)))

    if not idle_nodes:
        await _update_stampede_state(ng_id, cluster_id, {"consecutive_idle_checks": 0})
        return

    consecutive = state.get("consecutive_idle_checks", 0) + 1
    await _update_stampede_state(ng_id, cluster_id, {"consecutive_idle_checks": consecutive})

    if consecutive * interval < window:
        _logger.debug(
            "stampede: nodegroup %s — 유휴 노드 %d개, stabilization window 대기 (%d/%ds)",
            ng_id,
            len(idle_nodes),
            consecutive * interval,
            window,
        )
        return

    idle_nodes.sort(key=lambda item: (item[1], item[2]))
    remove_node = None
    for candidate, _pod_count, _util in idle_nodes:
        candidate_name = candidate["name"]
        evictable = evictable_by_node.get(candidate_name, [])
        remaining_free = []
        for node in ng_nodes:
            if node["name"] == candidate_name:
                continue
            alloc = node["allocatable"]
            used = node_used.get(node["name"], {"cpu_m": 0, "memory_bytes": 0, "gpu": 0})
            remaining_free.append(
                {
                    "cpu_m": max(0, alloc.get("cpu_m", 0) - used.get("cpu_m", 0)),
                    "memory_bytes": max(0, alloc.get("memory_bytes", 0) - used.get("memory_bytes", 0)),
                    "gpu": max(0, alloc.get("gpu", 0) - used.get("gpu", 0)),
                }
            )
        if all(
            _pod_fits_existing_capacity(
                {
                    "resource_requests": {
                        "cpu_m": p.get("cpu_m", 0),
                        "memory_bytes": p.get("memory_bytes", 0),
                        "gpu": p.get("gpu", 0),
                    }
                },
                remaining_free,
            )
            for p in evictable
        ):
            remove_node = candidate
            break

    if remove_node is None:
        await _update_stampede_state(
            ng_id, cluster_id, {"consecutive_idle_checks": 0, "last_blocked_reason": "scale_down_no_fit"}
        )
        return

    remove_name = remove_node["name"]

    # 해당 노드 VM 찾기
    vm_entry = next(
        (v for v in nodegroup.get("vms", []) if v.get("name") == remove_name),
        None,
    )
    if not vm_entry:
        _logger.warning("stampede: scale-down — %s VM 레코드 없음", remove_name)
        return

    _logger.info("stampede: nodegroup %s scale-down — node %s 제거 시작", ng_id, remove_name)

    # node_count 감소 + 쿨다운 갱신
    await k3s_nodegroup.update_nodegroup(
        cluster_id,
        ng_id,
        {
            "node_count": max(min_size, node_count - 1),
        },
    )
    await _update_stampede_state(
        ng_id,
        cluster_id,
        {
            "last_scale_down": time.time(),
            "consecutive_idle_checks": 0,
        },
    )

    # scale-down 이벤트 기록
    await _record_stampede_event(
        project_id=project_id,
        cluster_id=cluster_id,
        nodegroup_id=ng_id,
        action="scale_down",
        status="started",
        extra={"node_name": remove_name, "vm_id": vm_entry.get("vm_id", "")},
    )

    # Persist deletion so an API/worker restart cannot strand the VM.
    from drover.services.jobs import enqueue_job

    await enqueue_job(
        cluster_id=cluster_id,
        project_id=project_id,
        kind="nodegroup_reconcile",
        payload={
            "action": "delete_vms",
            "nodegroup": nodegroup,
            "remove_entries": [vm_entry],
        },
        user_id="stampede-system",
        username="Stampede",
    )


# ---------------------------------------------------------------------------
# 메인 reconcile 루프
# ---------------------------------------------------------------------------


async def reconcile_cluster(cluster: dict) -> None:
    """클러스터 1개의 Stampede nodegroup을 순회해 scale-up/down 판단."""
    from drover.services import kube as k3s_kube
    from drover.services import nodegroup as k3s_nodegroup

    cluster_id = cluster.get("id") or cluster.get("cluster_id", "")
    project_id = cluster.get("project_id", "")

    if not cluster_id or not project_id:
        return

    s = get_settings()

    # 클러스터의 stampede agent nodegroup 목록. v1은 agent+explicit flavor만 autoscale한다.
    all_nodegroups = await k3s_nodegroup.list_nodegroups(cluster_id)
    stampede_ngs = [
        ng for ng in all_nodegroups if ng.get("stampede_enabled") and ng.get("role") == "agent" and ng.get("flavor_id")
    ]

    if not stampede_ngs:
        return

    # K8s 상태 조회 (1회 조회 후 재사용)
    try:
        pending_pods = await k3s_kube.list_unschedulable_pods(cluster_id)
        node_capacities = await k3s_kube.get_node_capacity(cluster_id)
        node_pods = await k3s_kube.get_pod_resource_usage(cluster_id)
    except Exception as e:
        _logger.warning("stampede: cluster %s K8s API 조회 실패: %s", cluster_id, e)
        return

    available_flavors = await _get_available_flavors(project_id)
    flavors_by_id = {f["id"]: f for f in available_flavors}
    assignments, blocked_pods, summaries = _assign_pending_pods(
        pending_pods,
        stampede_ngs,
        flavors_by_id,
        node_pods,
        node_capacities,
    )
    for blocked in blocked_pods:
        pod = blocked["pod"]
        await _record_stampede_event(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id="",
            action="blocked",
            status="skipped",
            extra={
                "reason": blocked["reason"],
                "pod": {"namespace": pod.get("namespace"), "name": pod.get("name")},
                "message": pod.get("message", ""),
            },
        )
    blocked_summary = [
        {
            "namespace": item["pod"].get("namespace"),
            "name": item["pod"].get("name"),
            "reason": item["reason"],
            "message": item["pod"].get("message", ""),
        }
        for item in blocked_pods
    ]

    # in_flight 재조정 (worker 재시작 대비 — 실제 CREATING VM 수와 비교)
    for ng in stampede_ngs:
        state = ng.get("stampede_state") or {}
        recorded_in_flight = state.get("in_flight_count", 0)
        if recorded_in_flight > 0:
            try:
                actual_creating = await k3s_nodegroup.count_creating_vms(ng["id"])
                if actual_creating != recorded_in_flight:
                    _logger.info(
                        "stampede: nodegroup %s in_flight 재조정 %d→%d (worker 재시작 보정)",
                        ng["id"],
                        recorded_in_flight,
                        actual_creating,
                    )
                    await _update_stampede_state(
                        ng["id"],
                        cluster_id,
                        {
                            "in_flight_count": actual_creating,
                        },
                    )
                    # ng dict 갱신 (이후 로직에서 사용)
                    ng = dict(ng)
                    state = dict(state)
                    state["in_flight_count"] = actual_creating
                    ng["stampede_state"] = state
            except Exception as e:
                _logger.warning("stampede: in_flight 재조정 실패 (%s): %s", ng["id"], e)

    for ng in stampede_ngs:
        ng_id = ng["id"]
        try:
            pending_summary = [
                {"namespace": p.get("namespace"), "name": p.get("name"), "resources": p.get("resource_requests", {})}
                for p in assignments.get(ng_id, [])
            ]
            await _update_stampede_state(
                ng_id,
                cluster_id,
                {
                    "capacity": summaries.get(ng_id, {}),
                    "pending_assignments": pending_summary,
                    "blocked_reasons": blocked_summary,
                },
            )
            ng_pending = assignments.get(ng_id, [])
            if ng_pending:
                await _scale_up_nodegroup(
                    cluster_id=cluster_id,
                    project_id=project_id,
                    nodegroup=ng,
                    pending_pods=ng_pending,
                    node_pods=node_pods,
                    node_capacities=node_capacities,
                    s=s,
                )
            else:
                await _scale_down_nodegroup(
                    cluster_id=cluster_id,
                    project_id=project_id,
                    nodegroup=ng,
                    node_pods=node_pods,
                    node_capacities=node_capacities,
                    s=s,
                )
        except Exception:
            _logger.exception("stampede: nodegroup %s reconcile 오류", ng_id)


async def run_all() -> None:
    """모든 ACTIVE + stampede_enabled 클러스터를 순회해 reconcile 실행."""
    from drover.services import store as k3s_db

    s = get_settings()
    if not s.drover_stampede_enabled:
        return

    try:
        all_clusters = await k3s_db.list_all_clusters(include_deleted=False)
    except Exception as e:
        _logger.warning("stampede: 클러스터 목록 조회 실패: %s", e)
        return

    active_stampede = [c for c in all_clusters if c.get("status") == "ACTIVE" and c.get("stampede_enabled")]

    if not active_stampede:
        return

    _logger.info("stampede: reconcile 시작 — %d개 클러스터", len(active_stampede))

    tasks = [reconcile_cluster(c) for c in active_stampede]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for c, r in zip(active_stampede, results, strict=False):
        if isinstance(r, Exception):
            _logger.error("stampede: cluster %s reconcile 예외: %s", c.get("id"), r)
