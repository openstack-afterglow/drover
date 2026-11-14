"""Cluster reconciliation service for OpenStack resource drift detection."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import Any

import openstack.connection
from openstack.exceptions import NotFoundException, ResourceNotFound

from drover.services import cluster_authority, execution, inventory, keystone, operations, store
from drover.services.worker_ownership import checkpoint, run_sync

_logger = logging.getLogger("drover.reconciliation")


def _now() -> datetime:
    return datetime.now(UTC)


AGENT_JOIN_WINDOW_SECONDS = 900
AGENT_JOIN_MIN_MISSES = 2
AGENT_READY_STATUS = "READY"
_AGENT_NODE_MARK = "Kubernetes agent node"
_HOSTNAME_FILTER = re.compile(r"[^a-z0-9.-]+")


def _node_name(name: str) -> str:
    """Normalise a VM name the way Nova derives a guest hostname (agents get no ``--node-name``).

    Nova lowercases, turns spaces and underscores into hyphens, drops other characters, trims leading/trailing
    ``.``/``-`` and caps the result at 63 characters; cluster names may contain ``_``.
    """
    value = re.sub(r"[ _]", "-", (name or "").casefold())
    return _HOSTNAME_FILTER.sub("", value).strip(".-")[:63]


async def _observe_agent_join(
    cluster: dict[str, Any], *, now: datetime | None = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    """Compare the recorded agent VMs with the live Kubernetes node list during and after their join.

    Returns ``(required_missing, pending, misses)``. Only an agent that has never been observed Ready can escalate:
    after its join window and ``AGENT_JOIN_MIN_MISSES`` consecutive missing observations it becomes a required
    missing resource, which reconciliation clears itself once a later pass sees the node Ready. An agent that was
    Ready before (a reboot or kubelet restart) is only reported as pending. Agents are matched to nodes by
    Nova-normalised name, and an unmatched agent is explained by an unmatched Ready agent node (a naming difference
    is not a join failure). A node list that cannot be observed reports nothing new but carries an earlier
    agent-node ``ERROR`` forward. The create operation is never failed for a slow join.
    """
    from drover.services import health

    agents = await store.list_agent_vms(cluster["id"])
    if not agents:
        return [], [], {}
    now = now or _now()
    try:
        result = await health.check_cluster_health(cluster)
    except Exception:
        _logger.warning("Agent join observation failed for cluster %s", cluster.get("id"), exc_info=True)
        result = None
    observed = bool(result and result.api_server_reachable and result.healthz_ok and result.nodes)
    previous_drift = cluster.get("drift_status") if isinstance(cluster.get("drift_status"), dict) else {}
    previous_misses = previous_drift.get("agent_join_misses") if isinstance(previous_drift, dict) else {}
    previous_misses = previous_misses if isinstance(previous_misses, dict) else {}
    if not observed:
        previous = cluster.get("status_reason") or ""
        if cluster.get("status") == "ERROR" and previous.startswith("Reconciliation failed:") and _AGENT_NODE_MARK in previous:
            return [{
                "service": "kubernetes", "resource_type": "node", "resource_id": None,
                "name": "agent nodes", "is_required": True,
                "reason": f"Required {_AGENT_NODE_MARK} state could not be re-observed (API unreachable)",
            }], [], {vm_id: int(count) for vm_id, count in previous_misses.items() if isinstance(count, int)}
        return [], [], {}
    ready_nodes = [node for node in result.nodes if node.ready]
    ready_names = {_node_name(node.name) for node in ready_nodes}
    unmatched: list[dict[str, Any]] = []
    for agent in agents:
        if _node_name(agent.get("name") or agent["vm_id"]) in ready_names:
            if agent.get("status") != AGENT_READY_STATUS:
                try:
                    await store.update_agent_vm_status(cluster["id"], agent["vm_id"], AGENT_READY_STATUS)
                except Exception:
                    _logger.warning("Could not record agent %s as Ready", agent["vm_id"], exc_info=True)
            continue
        unmatched.append(agent)
    matched_agents = len(agents) - len(unmatched)
    unexplained_ready = max(0, sum(1 for node in ready_nodes if node.role == "agent") - matched_agents)
    still_missing = sorted(unmatched, key=lambda a: a.get("created_at") or now)[: max(0, len(unmatched) - unexplained_ready)]
    missing: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    misses: dict[str, int] = {}
    for agent in still_missing:
        vm_id = agent["vm_id"]
        name = agent.get("name") or vm_id
        created = agent.get("created_at")
        if created is not None and created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        age = (now - created).total_seconds() if created is not None else float(AGENT_JOIN_WINDOW_SECONDS)
        if agent.get("status") == AGENT_READY_STATUS:
            pending.append({"vm_id": vm_id, "name": name, "age_seconds": int(age), "seen_ready": True})
            continue
        count = int(previous_misses.get(vm_id, 0) or 0) + 1
        misses[vm_id] = count
        if age < AGENT_JOIN_WINDOW_SECONDS or count < AGENT_JOIN_MIN_MISSES:
            pending.append({"vm_id": vm_id, "name": name, "age_seconds": int(age), "misses": count})
        else:
            missing.append({
                "service": "kubernetes", "resource_type": "node", "resource_id": vm_id,
                "name": name, "is_required": True,
                "reason": f"Required {_AGENT_NODE_MARK} '{name}' is not Ready",
            })
    return missing, pending, misses


def _is_not_found(exc: Exception) -> bool:
    if isinstance(exc, (ResourceNotFound, NotFoundException)):
        return True
    err_msg = str(exc).lower()
    return "404" in err_msg or "not found" in err_msg


def _fetch_nova_server(conn: openstack.connection.Connection, server_id: str) -> Any | None:
    try:
        if hasattr(conn, "compute") and hasattr(conn.compute, "find_server"):
            return conn.compute.find_server(server_id, ignore_missing=True)
        if hasattr(conn, "compute") and hasattr(conn.compute, "get_server"):
            return conn.compute.get_server(server_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_cinder_volume(conn: openstack.connection.Connection, volume_id: str) -> Any | None:
    try:
        if hasattr(conn, "block_storage") and hasattr(conn.block_storage, "find_volume"):
            return conn.block_storage.find_volume(volume_id, ignore_missing=True)
        if hasattr(conn, "block_storage") and hasattr(conn.block_storage, "get_volume"):
            return conn.block_storage.get_volume(volume_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_neutron_security_group(conn: openstack.connection.Connection, sg_id: str) -> Any | None:
    try:
        if hasattr(conn, "network") and hasattr(conn.network, "find_security_group"):
            return conn.network.find_security_group(sg_id, ignore_missing=True)
        if hasattr(conn, "network") and hasattr(conn.network, "get_security_group"):
            return conn.network.get_security_group(sg_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_neutron_floating_ip(conn: openstack.connection.Connection, fip_id: str) -> Any | None:
    try:
        if hasattr(conn, "network") and hasattr(conn.network, "find_ip"):
            return conn.network.find_ip(fip_id, ignore_missing=True)
        if hasattr(conn, "network") and hasattr(conn.network, "get_ip"):
            return conn.network.get_ip(fip_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_neutron_port(conn: openstack.connection.Connection, port_id: str) -> Any | None:
    try:
        if hasattr(conn, "network") and hasattr(conn.network, "find_port"):
            return conn.network.find_port(port_id, ignore_missing=True)
        if hasattr(conn, "network") and hasattr(conn.network, "get_port"):
            return conn.network.get_port(port_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_neutron_security_group_rule(conn: openstack.connection.Connection, rule_id: str) -> Any | None:
    try:
        if hasattr(conn, "network") and hasattr(conn.network, "find_security_group_rule"):
            return conn.network.find_security_group_rule(rule_id, ignore_missing=True)
        if hasattr(conn, "network") and hasattr(conn.network, "get_security_group_rule"):
            return conn.network.get_security_group_rule(rule_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_octavia_lb(conn: openstack.connection.Connection, lb_id: str) -> Any | None:
    try:
        if hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "find_load_balancer"):
            return conn.load_balancer.find_load_balancer(lb_id, ignore_missing=True)
        if hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "get_load_balancer"):
            return conn.load_balancer.get_load_balancer(lb_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_octavia_listener(conn: openstack.connection.Connection, listener_id: str) -> Any | None:
    try:
        if hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "find_listener"):
            return conn.load_balancer.find_listener(listener_id, ignore_missing=True)
        if hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "get_listener"):
            return conn.load_balancer.get_listener(listener_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_octavia_pool(conn: openstack.connection.Connection, pool_id: str) -> Any | None:
    try:
        if hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "find_pool"):
            return conn.load_balancer.find_pool(pool_id, ignore_missing=True)
        if hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "get_pool"):
            return conn.load_balancer.get_pool(pool_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_octavia_member(
    conn: openstack.connection.Connection, member_id: str, metadata: dict | list | None = None
) -> Any | None:
    try:
        pool_id = None
        if isinstance(metadata, dict):
            pool_id = metadata.get("pool_id")
        if pool_id and hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "find_member"):
            return conn.load_balancer.find_member(member_id, pool_id, ignore_missing=True)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


def _fetch_keystone_app_cred(conn: openstack.connection.Connection, app_cred_id: str) -> Any | None:
    try:
        return conn.identity.get_application_credential(conn.current_user_id, app_cred_id)
    except (ResourceNotFound, NotFoundException):
        return None


def fetch_recorded_resource(
    conn: openstack.connection.Connection,
    service: str,
    resource_type: str,
    resource_id: str,
    metadata: dict | list | None = None,
) -> Any | None:
    """Fetch OpenStack resource by recorded ID.

    Returns resource object if present, None if missing (404/ResourceNotFound).
    Raises exception on transient OpenStack errors (500, network failure, etc.).
    """
    if service == "nova" and resource_type == "server":
        return _fetch_nova_server(conn, resource_id)
    elif service == "cinder" and resource_type == "volume":
        return _fetch_cinder_volume(conn, resource_id)
    elif service == "neutron":
        if resource_type == "security_group":
            return _fetch_neutron_security_group(conn, resource_id)
        elif resource_type == "floating_ip":
            return _fetch_neutron_floating_ip(conn, resource_id)
        elif resource_type == "port":
            return _fetch_neutron_port(conn, resource_id)
        elif resource_type == "security_group_rule":
            return _fetch_neutron_security_group_rule(conn, resource_id)
    elif service == "octavia":
        if resource_type == "load_balancer":
            return _fetch_octavia_lb(conn, resource_id)
        elif resource_type == "listener":
            return _fetch_octavia_listener(conn, resource_id)
        elif resource_type == "pool":
            return _fetch_octavia_pool(conn, resource_id)
        elif resource_type == "member":
            return _fetch_octavia_member(conn, resource_id, metadata=metadata)
    elif service == "keystone" and resource_type == "app_credential":
        return _fetch_keystone_app_cred(conn, resource_id)

    return None


def extract_resource_state(resource: Any, service: str, resource_type: str) -> tuple[str, bool]:
    """Extract (state_str, is_mismatch) for a present OpenStack resource."""
    if resource is None:
        return ("MISSING", False)

    status = None
    if isinstance(resource, dict):
        status = (
            resource.get("status")
            or resource.get("operating_status")
            or resource.get("provisioning_status")
        )
    else:
        status = (
            getattr(resource, "status", None)
            or getattr(resource, "operating_status", None)
            or getattr(resource, "provisioning_status", None)
        )

    status_str = str(status).upper() if status else "UNKNOWN"

    is_mismatch = False
    if service == "nova" and resource_type == "server":
        if status_str in ("ERROR", "SHUTOFF", "SHELVED", "CRASHED"):
            is_mismatch = True
    elif service == "cinder" and resource_type == "volume":
        if status_str in ("ERROR", "ERROR_DELETING", "ERROR_BACKING_UP"):
            is_mismatch = True
    elif service == "octavia" and resource_type == "load_balancer" and status_str in (
        "ERROR",
        "DEGRADED",
        "OFFLINE",
    ):
        is_mismatch = True

    return (status_str, is_mismatch)


def scan_orphan_resources(
    conn: openstack.connection.Connection,
    project_id: str,
    cluster_id: str,
    known_resource_ids: set[str],
) -> list[dict[str, Any]]:
    """Scan OpenStack for unknown tagged resources matching cluster_id.

    Report-only: returns list of orphan resource descriptions, never deletes.
    First-observation safe.
    """
    orphans: list[dict[str, Any]] = []
    cluster_tag = f"drover.cluster_id={cluster_id}"

    # 1. Nova servers
    try:
        if hasattr(conn, "compute") and hasattr(conn.compute, "servers"):
            servers = list(conn.compute.servers(details=True))
            for s in servers:
                s_id = str(getattr(s, "id", None) or (s.get("id") if isinstance(s, dict) else ""))
                if not s_id or s_id in known_resource_ids:
                    continue
                meta = getattr(s, "metadata", None) or (s.get("metadata") if isinstance(s, dict) else {})
                tags = getattr(s, "tags", None) or (s.get("tags") if isinstance(s, dict) else [])
                has_tag = (
                    isinstance(meta, dict) and meta.get("drover.cluster_id") == cluster_id
                ) or (
                    isinstance(tags, (list, tuple, set)) and (cluster_tag in tags or "drover.managed=true" in tags)
                )
                if has_tag:
                    name = str(getattr(s, "name", None) or (s.get("name") if isinstance(s, dict) else s_id))
                    orphans.append({
                        "service": "nova",
                        "resource_type": "server",
                        "resource_id": s_id,
                        "name": name,
                        "reason": f"Unknown tagged Nova server '{s_id}' ({name}) found in project '{project_id}' for cluster '{cluster_id}'",
                    })
    except Exception as e:
        if not _is_not_found(e):
            _logger.warning("Orphan scan failed for Nova servers: %s", e)

    # 2. Cinder volumes
    try:
        if hasattr(conn, "block_storage") and hasattr(conn.block_storage, "volumes"):
            volumes = list(conn.block_storage.volumes(details=True))
            for v in volumes:
                v_id = str(getattr(v, "id", None) or (v.get("id") if isinstance(v, dict) else ""))
                if not v_id or v_id in known_resource_ids:
                    continue
                meta = getattr(v, "metadata", None) or (v.get("metadata") if isinstance(v, dict) else {})
                if isinstance(meta, dict) and meta.get("drover.cluster_id") == cluster_id:
                    name = str(getattr(v, "name", None) or (v.get("name") if isinstance(v, dict) else v_id))
                    orphans.append({
                        "service": "cinder",
                        "resource_type": "volume",
                        "resource_id": v_id,
                        "name": name,
                        "reason": f"Unknown tagged Cinder volume '{v_id}' ({name}) found in project '{project_id}' for cluster '{cluster_id}'",
                    })
    except Exception as e:
        if not _is_not_found(e):
            _logger.warning("Orphan scan failed for Cinder volumes: %s", e)

    # 3. Neutron security groups
    try:
        if hasattr(conn, "network") and hasattr(conn.network, "security_groups"):
            sgs = list(conn.network.security_groups(project_id=project_id))
            for sg in sgs:
                sg_id = str(getattr(sg, "id", None) or (sg.get("id") if isinstance(sg, dict) else ""))
                if not sg_id or sg_id in known_resource_ids:
                    continue
                tags = getattr(sg, "tags", None) or (sg.get("tags") if isinstance(sg, dict) else [])
                if isinstance(tags, (list, tuple, set)) and cluster_tag in tags:
                    name = str(getattr(sg, "name", None) or (sg.get("name") if isinstance(sg, dict) else sg_id))
                    orphans.append({
                        "service": "neutron",
                        "resource_type": "security_group",
                        "resource_id": sg_id,
                        "name": name,
                        "reason": f"Unknown tagged Neutron security group '{sg_id}' ({name}) found in project '{project_id}' for cluster '{cluster_id}'",
                    })
    except Exception as e:
        if not _is_not_found(e):
            _logger.warning("Orphan scan failed for Neutron security groups: %s", e)

    # 4. Octavia load balancers
    try:
        if hasattr(conn, "load_balancer") and hasattr(conn.load_balancer, "load_balancers"):
            lbs = list(conn.load_balancer.load_balancers(project_id=project_id))
            for lb in lbs:
                lb_id = str(getattr(lb, "id", None) or (lb.get("id") if isinstance(lb, dict) else ""))
                if not lb_id or lb_id in known_resource_ids:
                    continue
                tags = getattr(lb, "tags", None) or (lb.get("tags") if isinstance(lb, dict) else [])
                if isinstance(tags, (list, tuple, set)) and cluster_tag in tags:
                    name = str(getattr(lb, "name", None) or (lb.get("name") if isinstance(lb, dict) else lb_id))
                    orphans.append({
                        "service": "octavia",
                        "resource_type": "load_balancer",
                        "resource_id": lb_id,
                        "name": name,
                        "reason": f"Unknown tagged Octavia load balancer '{lb_id}' ({name}) found in project '{project_id}' for cluster '{cluster_id}'",
                    })
    except Exception as e:
        if not _is_not_found(e):
            _logger.warning("Orphan scan failed for Octavia load balancers: %s", e)

    return orphans


async def reconcile_cluster(
    project_id: str,
    cluster_id: str,
    conn: openstack.connection.Connection | None = None,
    operation_id: str | None = None,
) -> dict[str, Any]:
    """Reconcile OpenStack state for a non-deleted cluster against recorded inventory.

    Queries service adapters by recorded IDs.
    Persists last_seen_at for present resources, drift_status for cluster, and appends operation events.
    Transitions established clusters to ERROR with actionable status_reason if required resources
    or provisioning evidence are missing. Only reconciler-owned missing-resource ERROR can recover;
    a failed create is not evidence of health even when its inventory is empty.
    Reports unknown tagged resources as orphans (report-only, no deletion).
    Bubbles up transient service errors for job retry behavior.
    """
    if operation_id:
        await operations.append_operation_event(
            None,
            operation_id,
            phase="reconcile_start",
            message=f"Reconciliation started for cluster {cluster_id}",
        )

    cluster_dict = await store.get_cluster(project_id, cluster_id)
    if not cluster_dict or cluster_dict.get("deleted_at") is not None:
        _logger.info("Cluster %s is missing or deleted, skipping reconciliation", cluster_id)
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase="reconcile_complete",
                message="Cluster is deleted, reconciliation skipped",
            )
        return {
            "has_drift": False,
            "missing_count": 0,
            "orphan_count": 0,
            "mismatch_count": 0,
            "missing": [],
            "orphans": [],
            "mismatches": [],
            "reconciled_at": _now().isoformat(),
        }

    # Provisioning and deletion own these transitions; reconciliation must not race their stages.
    if cluster_dict.get("status") not in {"ACTIVE", "ERROR"}:
        if operation_id:
            await operations.append_operation_event(
                None, operation_id, phase="reconcile_complete",
                message=f"Reconciliation skipped while cluster is {cluster_dict.get('status')}",
            )
        return {
            "has_drift": False, "missing_count": 0, "orphan_count": 0, "mismatch_count": 0,
            "missing": [], "orphans": [], "mismatches": [], "reconciled_at": _now().isoformat(),
        }

    # These are required provisioning outputs, not optional inventory entries. A create can fail
    # before recording any OpenStack IDs, or after booting Nova but before receiving kubeconfig.
    missing_resources: list[dict[str, Any]] = []
    for service, resource_type, present, name in (
        ("nova", "server", bool(cluster_dict.get("server_vm_id")), "Primary server"),
        ("drover", "kubeconfig", bool(await store.get_kubeconfig(project_id, cluster_id)), "Kubeconfig"),
    ):
        if not present:
            missing_resources.append({
                "service": service, "resource_type": resource_type, "resource_id": None,
                "name": name, "is_required": True,
                "reason": f"Required {name} was not provisioned or recorded",
            })

    state_mismatches: list[dict[str, Any]] = []
    create_op = await operations.get_latest_create_operation(project_id, cluster_id)
    if create_op is not None and create_op.status in {"FAILED", "CANCELLED"}:
        state_mismatches.append({
            "service": "drover", "resource_type": "provisioning", "resource_id": create_op.id,
            "name": "Create operation", "state": create_op.status,
            "reason": f"Create operation {create_op.status.lower()}: {create_op.error or 'provisioning did not complete'}",
        })
    # Load active recorded resources from DB
    managed_resources = await inventory.list_managed_resources(None, cluster_id=cluster_id, active_only=True)

    # Build target resources list to check
    targets_map: dict[tuple[str, str, str], dict[str, Any]] = {}

    for r in managed_resources:
        if r.service == "keystone" and r.resource_type == "app_credential":
            # Credential authority lives in drover_cluster_credentials; historic manager-owned rows are not
            # readable by the cluster owner's credential and are tracked there as retiring.
            continue
        key = (r.service, r.resource_type, r.resource_id)
        targets_map[key] = {
            "service": r.service,
            "resource_type": r.resource_type,
            "resource_id": r.resource_id,
            "name": r.name or r.resource_id,
            "is_required": True,
            "metadata": r.metadata_json,
        }

    # Add primary cluster recorded IDs if not already present
    primary_items = [
        ("nova", "server", cluster_dict.get("server_vm_id"), "Server VM", True),
        ("neutron", "security_group", cluster_dict.get("security_group_id"), "Security Group", True),
        ("octavia", "load_balancer", cluster_dict.get("api_lb_id"), "API Load Balancer", True),
        ("neutron", "floating_ip", cluster_dict.get("api_fip_id"), "API Floating IP", True),
    ]
    for svc, rtype, rid, label, req in primary_items:
        if rid:
            key = (svc, rtype, rid)
            if key not in targets_map:
                targets_map[key] = {
                    "service": svc,
                    "resource_type": rtype,
                    "resource_id": rid,
                    "name": f"{label} ({rid[:8]})",
                    "is_required": req,
                    "metadata": None,
                }

    # Add agent VMs from cluster
    agent_ids = cluster_dict.get("agent_vm_ids") or []
    for aid in agent_ids:
        if aid:
            key = ("nova", "server", aid)
            if key not in targets_map:
                targets_map[key] = {
                    "service": "nova",
                    "resource_type": "server",
                    "resource_id": aid,
                    "name": f"Agent VM ({aid[:8]})",
                    "is_required": True,
                    "metadata": None,
                }

    for credential in await cluster_authority.active_credentials(cluster_id):
        key = ("keystone", "app_credential", credential["app_credential_id"])
        targets_map[key] = {
            "service": "keystone",
            "resource_type": "app_credential",
            "resource_id": credential["app_credential_id"],
            "name": f"{credential['purpose'].title()} credential g{credential['generation']}",
            # A deleted credential means lost authority, repaired by reauthorization (ACTIVE-only), not cluster ERROR.
            "is_required": False,
            "metadata": None,
        }

    known_resource_ids = {t["resource_id"] for t in targets_map.values()}

    created_conn = False
    if conn is None:
        try:
            conn = await execution.open_connection(project_id)
        except execution.ReauthorizationRequired:
            _logger.info("Cluster %s has no resource authority; reconciliation requires reauthorization", cluster_id)
            drift_summary = {
                "has_drift": bool(missing_resources or state_mismatches),
                "missing_count": len(missing_resources), "orphan_count": 0,
                "mismatch_count": len(state_mismatches),
                "missing": missing_resources, "orphans": [], "mismatches": state_mismatches,
                "reconciled_at": _now().isoformat(), "reauthorization_required": True,
            }
            if drift_summary["has_drift"]:
                reasons = [r["reason"] for r in missing_resources + state_mismatches]
                await store.update_cluster_reconciliation(
                    cluster_id=cluster_id, last_reconciled_at=_now(), drift_status=drift_summary,
                    status="ERROR", status_reason=f"Reconciliation failed: {'; '.join(reasons)}",
                )
            if operation_id:
                await operations.append_operation_event(
                    None, operation_id, phase="reconcile_complete",
                    message="Cloud inventory check skipped: cluster requires reauthorization",
                    payload_json=drift_summary,
                )
            return drift_summary
        created_conn = True

    try:
        # Local provisioning gaps remain drift even when there are no cloud targets to check.
        checked_count = 0

        for target in targets_map.values():
            svc = target["service"]
            rtype = target["resource_type"]
            rid = target["resource_id"]
            name = target["name"]
            req = target["is_required"]
            meta = target["metadata"]

            # Query OpenStack by recorded ID (transient errors bubble up)
            resource_obj = await run_sync(fetch_recorded_resource, conn, svc, rtype, rid, metadata=meta)
            checked_count += 1

            if resource_obj is not None:
                # Ownership validation
                valid_owner = inventory.validate_resource_ownership(resource_obj, project_id, cluster_id, rtype)
                if not valid_owner:
                    _logger.warning(
                        "Resource %s/%s failed ownership validation for project %s cluster %s",
                        svc,
                        rid,
                        project_id,
                        cluster_id,
                    )
                    resource_obj = None

            if resource_obj is not None:
                # Resource is present in OpenStack
                state_str, is_mismatch = extract_resource_state(resource_obj, svc, rtype)

                # Update last_seen_at and state in ManagedOpenStackResource
                await inventory.record_resource(
                    None,
                    cluster_id=cluster_id,
                    service=svc,
                    resource_type=rtype,
                    resource_id=rid,
                    operation_id=operation_id,
                    name=name,
                    state=state_str,
                    metadata=meta,
                )

                if is_mismatch:
                    state_mismatches.append({
                        "service": svc,
                        "resource_type": rtype,
                        "resource_id": rid,
                        "name": name,
                        "state": state_str,
                        "reason": f"{svc} {rtype} '{rid}' ({name}) is in abnormal state '{state_str}' in OpenStack",
                    })
            else:
                # Resource is missing
                reason = f"Required {svc} {rtype} '{rid}' ({name}) is missing or deleted in OpenStack"
                missing_resources.append({
                    "service": svc,
                    "resource_type": rtype,
                    "resource_id": rid,
                    "name": name,
                    "is_required": req,
                    "reason": reason,
                })

        agent_missing, agent_pending, agent_misses = await _observe_agent_join(cluster_dict)
        missing_resources.extend(agent_missing)

        # Scan for unknown tagged orphan resources (report-only, no deletion)
        orphaned_resources = await run_sync(scan_orphan_resources, conn, project_id, cluster_id, known_resource_ids)

        has_missing_required = any(m.get("is_required", True) for m in missing_resources)
        has_drift = bool(missing_resources or orphaned_resources or state_mismatches or agent_pending)

        drift_summary = {
            "has_drift": has_drift,
            "missing_count": len(missing_resources),
            "orphan_count": len(orphaned_resources),
            "mismatch_count": len(state_mismatches),
            "missing": missing_resources,
            "orphans": orphaned_resources,
            "mismatches": state_mismatches,
            "reconciled_at": _now().isoformat(),
            **({"agent_join_pending": agent_pending} if agent_pending else {}),
            **({"agent_join_misses": agent_misses} if agent_misses else {}),
        }

        # Update cluster record status and drift_status
        new_status = None
        status_reason = None
        if has_missing_required or state_mismatches:
            new_status = "ERROR"
            reasons = [m["reason"] for m in missing_resources if m.get("is_required", True)]
            reasons.extend(m["reason"] for m in state_mismatches)
            status_reason = f"Reconciliation failed: {'; '.join(reasons)}"
        elif (
            cluster_dict.get("status") == "ERROR"
            and (cluster_dict.get("status_reason") or "").startswith("Reconciliation failed:")
        ):
            new_status = "ACTIVE"
            # The store only writes non-None reasons; explicitly clear the resolved error.
            status_reason = ""

        await store.update_cluster_reconciliation(
            cluster_id=cluster_id,
            last_reconciled_at=_now(),
            drift_status=drift_summary,
            status=new_status,
            status_reason=status_reason,
        )

        # Append ordered operation events if operation_id present
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase="reconcile_inventory",
                message=f"Inventory check completed: {checked_count} resources verified",
                payload_json={"checked_count": checked_count},
            )
            if missing_resources:
                await operations.append_operation_event(
                    None,
                    operation_id,
                    phase="reconcile_drift_missing",
                    message=f"Detected {len(missing_resources)} missing resource(s)",
                    payload_json={"missing": missing_resources},
                )
            if orphaned_resources:
                await operations.append_operation_event(
                    None,
                    operation_id,
                    phase="reconcile_drift_orphan",
                    message=f"Reported {len(orphaned_resources)} unknown tagged orphan resource(s)",
                    payload_json={"orphans": orphaned_resources},
                )
            if state_mismatches:
                await operations.append_operation_event(
                    None,
                    operation_id,
                    phase="reconcile_drift_mismatch",
                    message=f"Detected {len(state_mismatches)} state mismatch(es)",
                    payload_json={"mismatches": state_mismatches},
                )
            await operations.append_operation_event(
                None,
                operation_id,
                phase="reconcile_complete",
                message=f"Reconciliation completed for cluster {cluster_id}. Drift: {has_drift}",
                payload_json=drift_summary,
            )

        return drift_summary
    finally:
        if created_conn and hasattr(conn, "close"):
            await keystone.close_connection(conn)


async def schedule_worker_reconciliations(cluster_ids: list[str], max_per_project: int = 2) -> list[str]:
    """Schedule discovery-published targets, bounded per-project and serialized per-cluster."""
    from collections import defaultdict

    from sqlalchemy import select

    from drover.db import get_session_factory
    from drover.models.orm import DroverJob, DroverOperation, K3sCluster
    from drover.services import jobs

    factory = get_session_factory()
    if factory is None or not cluster_ids:
        return []

    enqueued_job_ids: list[str] = []

    async with factory() as session:
        await checkpoint()
        stmt = select(K3sCluster).where(
            K3sCluster.id.in_(cluster_ids),
            K3sCluster.deleted_at.is_(None),
            K3sCluster.status.in_(["ACTIVE", "ERROR"]),
        )
        res = await session.execute(stmt)
        clusters = list(res.scalars().all())

        if not clusters:
            return []

        job_stmt = select(DroverJob).where(
            DroverJob.status.in_(["queued", "running"])
        )
        job_res = await session.execute(job_stmt)
        active_jobs = list(job_res.scalars().all())

        project_active_reconcile_count: dict[str, int] = defaultdict(int)
        clusters_with_active_jobs: set[str] = set()

        for j in active_jobs:
            clusters_with_active_jobs.add(j.cluster_id)
            if j.kind == "reconcile":
                project_active_reconcile_count[j.project_id] += 1

        op_stmt = select(DroverOperation).where(
            DroverOperation.kind == "reconcile",
            DroverOperation.status.in_(["QUEUED", "RUNNING", "WAITING_CALLBACK"]),
        )
        op_res = await session.execute(op_stmt)
        active_ops = list(op_res.scalars().all())
        for op in active_ops:
            clusters_with_active_jobs.add(op.cluster_id)

        # Continuous reconciliation runs only under a reauthorized cluster's control credential.
        authorized = await cluster_authority.authorized_cluster_ids(session)
        clusters_by_project: dict[str, list[K3sCluster]] = defaultdict(list)
        for c in clusters:
            if c.id in authorized:
                clusters_by_project[c.project_id].append(c)

        for project_id, proj_clusters in clusters_by_project.items():
            current_active = project_active_reconcile_count[project_id]
            if current_active >= max_per_project:
                continue

            proj_clusters.sort(
                key=lambda c: (
                    c.last_reconciled_at is not None,
                    c.last_reconciled_at or datetime.min.replace(tzinfo=UTC),
                )
            )

            for c in proj_clusters:
                if current_active >= max_per_project:
                    break
                if c.id in clusters_with_active_jobs:
                    continue

                await checkpoint()
                job_id = await jobs.enqueue_job(
                    cluster_id=c.id,
                    project_id=project_id,
                    kind="reconcile",
                    payload={},
                    op_kind="reconcile",
                )
                enqueued_job_ids.append(job_id)
                clusters_with_active_jobs.add(c.id)
                current_active += 1
                project_active_reconcile_count[project_id] = current_active

    return enqueued_job_ids
