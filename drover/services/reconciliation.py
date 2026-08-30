"""Cluster reconciliation service for OpenStack resource drift detection."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import openstack.connection
from openstack.exceptions import NotFoundException, ResourceNotFound

from drover.services import inventory, keystone, operations, store

_logger = logging.getLogger("drover.reconciliation")


def _now() -> datetime:
    return datetime.now(UTC)


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
        if hasattr(conn, "identity") and hasattr(conn.identity, "find_application_credential"):
            return conn.identity.find_application_credential(app_cred_id, ignore_missing=True)
        if hasattr(conn, "identity") and hasattr(conn.identity, "get_application_credential"):
            return conn.identity.get_application_credential(app_cred_id)
        return None
    except Exception as e:
        if _is_not_found(e):
            return None
        raise


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
    Transitions cluster to ERROR with actionable status_reason if missing required resources.
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

    # Load active recorded resources from DB
    managed_resources = await inventory.list_managed_resources(None, cluster_id=cluster_id, active_only=True)

    # Build target resources list to check
    targets_map: dict[tuple[str, str, str], dict[str, Any]] = {}

    for r in managed_resources:
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
        ("keystone", "app_credential", cluster_dict.get("app_credential_id"), "App Credential", True),
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

    known_resource_ids = {t["resource_id"] for t in targets_map.values()}

    created_conn = False
    if conn is None:
        conn = await asyncio.to_thread(keystone.get_admin_connection_for_project, project_id)
        created_conn = True

    try:
        missing_resources: list[dict[str, Any]] = []
        state_mismatches: list[dict[str, Any]] = []
        checked_count = 0

        for target in targets_map.values():
            svc = target["service"]
            rtype = target["resource_type"]
            rid = target["resource_id"]
            name = target["name"]
            req = target["is_required"]
            meta = target["metadata"]

            # Query OpenStack by recorded ID (transient errors bubble up)
            resource_obj = fetch_recorded_resource(conn, svc, rtype, rid, metadata=meta)
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

        # Scan for unknown tagged orphan resources (report-only, no deletion)
        orphaned_resources = scan_orphan_resources(conn, project_id, cluster_id, known_resource_ids)

        has_missing_required = any(m.get("is_required", True) for m in missing_resources)
        has_drift = bool(missing_resources or orphaned_resources or state_mismatches)

        drift_summary = {
            "has_drift": has_drift,
            "missing_count": len(missing_resources),
            "orphan_count": len(orphaned_resources),
            "mismatch_count": len(state_mismatches),
            "missing": missing_resources,
            "orphans": orphaned_resources,
            "mismatches": state_mismatches,
            "reconciled_at": _now().isoformat(),
        }

        # Update cluster record status and drift_status
        new_status = None
        status_reason = None
        if has_missing_required:
            new_status = "ERROR"
            reasons = [m["reason"] for m in missing_resources if m.get("is_required", True)]
            status_reason = f"Reconciliation failed: {'; '.join(reasons)}"
        elif cluster_dict.get("status") == "ERROR" and not has_missing_required:
            new_status = "ACTIVE"
            status_reason = None

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
            await asyncio.to_thread(conn.close)


async def schedule_worker_reconciliations(max_per_project: int = 2) -> list[str]:
    """Schedule cluster reconciliations for active clusters bounded per-project and serialized per-cluster."""
    from collections import defaultdict

    from sqlalchemy import select

    from drover.db import get_session_factory
    from drover.models.orm import DroverJob, DroverOperation, K3sCluster
    from drover.services import jobs

    factory = get_session_factory()
    if factory is None:
        return []

    enqueued_job_ids: list[str] = []

    async with factory() as session:
        stmt = select(K3sCluster).where(
            K3sCluster.deleted_at.is_(None),
            K3sCluster.status.not_in(["DELETED"]),
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

        clusters_by_project: dict[str, list[K3sCluster]] = defaultdict(list)
        for c in clusters:
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
