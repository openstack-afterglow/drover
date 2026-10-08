"""Durable Drover job queue with leased, attempt-fenced execution."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from drover.db import get_session_factory
from drover.logging import safe_metadata
from drover.models.orm import DroverJob, DroverOperation, K3sCluster, K3sNodegroup, K3sNodegroupVM
from drover.services import delegation as _delegation
from drover.services import execution, operations
from drover.services.delegation import AdmittedDelegation

_logger = logging.getLogger("drover.jobs")
_LEASE_SECONDS = 900
_MAX_ATTEMPTS = 3
_BATCH_SIZE = 5
_SUPPORTED_KINDS = frozenset(
    {
        "create",
        "bootstrap_ha",
        "provision_agents",
        "scale",
        "nodegroup_reconcile",
        "stampede_provision",
        "delete",
        "rotate_certificates",
        "reconcile",
        "reauthorize",
    }
)

JOB_TO_OP_KIND = {
    "create": "create",
    "bootstrap_ha": "create",
    "provision_agents": "create",
    "scale": "scale",
    "nodegroup_reconcile": "nodegroup_reconcile",
    "stampede_provision": "nodegroup_reconcile",
    "delete": "delete",
    "rotate_certificates": "rotate_certificates",
    "reconcile": "reconcile",
    "reauthorize": "reauthorize",
}

# Job kinds that must run under an admitted operation delegation versus cluster resource authority.
_OPERATION_KINDS = frozenset({"create", "bootstrap_ha", "provision_agents", "scale", "delete"})
_TERMINAL_CLUSTER_SAFE_KINDS = frozenset({"reauthorize"})


def _now() -> datetime:
    return datetime.now(UTC)


async def enqueue_job(
    cluster_id: str,
    project_id: str,
    kind: str,
    payload: dict,
    user_id: str | None = None,
    username: str | None = None,
    operation_id: str | None = None,
    request_id: str | None = None,
    idempotency_key: str | None = None,
    request_hash: str | None = None,
    op_kind: str | None = None,
    *,
    session: AsyncSession | None = None,
    delegation: AdmittedDelegation | None = None,
    delegation_id: str | None = None,
) -> str:
    """Persist a job/operation, optionally inside the caller's state transaction.

    ``delegation`` persists a freshly admitted requester trust with the job; ``delegation_id`` lets a callback
    continuation reuse its operation's already admitted delegation. Neither may be combined.
    """
    if delegation is not None and delegation_id is not None:
        raise ValueError("A job references exactly one delegation")
    if kind not in _SUPPORTED_KINDS:
        raise ValueError(f"unsupported Drover job kind: {kind}")

    async def persist() -> str:
        target_op_id = operation_id
        if not target_op_id:
            mapped_op_kind = op_kind or JOB_TO_OP_KIND.get(kind, "create")
            active_op = await operations._get_active_op_impl(session, cluster_id, kind=mapped_op_kind)
            if active_op is not None:
                target_op_id = active_op.id
            else:
                new_op = await operations.create_or_get_operation(
                    session,
                    project_id=project_id,
                    cluster_id=cluster_id,
                    kind=mapped_op_kind,
                    request_id=request_id,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    status="QUEUED",
                )
                target_op_id = new_op.id
        job_delegation_id = delegation_id
        if delegation is not None:
            if delegation.project_id != project_id or delegation.cluster_id != cluster_id:
                raise ValueError("Delegation scope does not match the job")
            job_delegation_id = await _delegation.persist(session, delegation, target_op_id)
        job = DroverJob(
            id=str(uuid.uuid4()),
            cluster_id=cluster_id,
            project_id=project_id,
            kind=kind,
            status="queued",
            payload_json=payload,
            user_id=user_id or None,
            username=username or None,
            operation_id=target_op_id,
            delegation_id=job_delegation_id,
            created_at=_now(),
            updated_at=_now(),
        )
        session.add(job)
        if target_op_id:
            await operations._append_event_impl(
                session,
                target_op_id,
                phase="job_enqueued",
                message=f"Job {kind} enqueued",
                payload_json={"job_id": job.id, "kind": kind, "request_id": request_id},
            )
        return job.id

    if session is not None:
        return await persist()
    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database unavailable for durable job enqueue")
    async with factory() as session, session.begin():
        return await persist()


async def enqueue_stampede_job(
    cluster_id: str,
    project_id: str,
    nodegroup_id: str,
    *,
    direction: str,
    requested_count: int,
    payload: dict,
    expected_node_count: int,
) -> dict | None:
    """Fence a sizing decision and persist count, reservation, operation and job together."""
    if direction not in {"up", "down"} or requested_count <= 0:
        raise ValueError("invalid Stampede sizing decision")
    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database unavailable for Stampede reservation")
    async with factory() as session, session.begin():
        cluster = await session.get(K3sCluster, cluster_id, with_for_update=True)
        if (
            cluster is None
            or cluster.project_id != project_id
            or cluster.deleted_at is not None
            or cluster.status != "ACTIVE"
            or not cluster.stampede_enabled
        ):
            return None
        ng = await session.get(K3sNodegroup, nodegroup_id, with_for_update=True)
        if (
            ng is None
            or ng.cluster_id != cluster_id
            or ng.deleted_at is not None
            or ng.role != "agent"
            or not ng.stampede_enabled
            or not ng.flavor_id
            or ng.node_count != expected_node_count
        ):
            return None
        busy = await session.scalar(
            select(DroverJob.id).where(
                DroverJob.cluster_id == cluster_id,
                DroverJob.status.in_(["queued", "running"]),
                DroverJob.kind != "reconcile",
            ).limit(1)
        )
        if busy:
            return None
        state = dict(ng.stampede_state or {})
        if state.get("in_flight_count", 0):
            return None
        now = _now()
        job_payload = dict(payload)
        if direction == "up":
            count = min(requested_count, max(0, ng.max_size - ng.node_count))
            if count <= 0:
                return None
            ng.node_count += count
            kind = "stampede_provision"
            job_payload.update(
                nodegroup_id=nodegroup_id,
                add_count=count,
                flavor_id=ng.flavor_id,
                image_id=ng.image_id,
                labels=ng.labels,
                taints=ng.taints,
            )
            state.update(in_flight_count=count, in_flight_since=now.timestamp(), last_scale_up=now.timestamp())
        else:
            entries = job_payload.get("remove_entries") or []
            if len(entries) != 1 or requested_count != 1 or ng.node_count <= ng.min_size:
                return None
            vm = await session.scalar(
                select(K3sNodegroupVM).where(
                    K3sNodegroupVM.nodegroup_id == nodegroup_id,
                    K3sNodegroupVM.vm_id == entries[0].get("vm_id"),
                    K3sNodegroupVM.name == entries[0].get("name"),
                )
            )
            if vm is None:
                return None
            count = 1
            ng.node_count -= count
            kind = "nodegroup_reconcile"
            job_payload.update(action="delete_vms", nodegroup={"id": nodegroup_id})
            state.update(last_scale_down=now.timestamp(), deleting_nodes=[vm.name])
        op = await operations.create_or_get_operation(
            session, project_id=project_id, cluster_id=cluster_id, kind="nodegroup_reconcile"
        )
        job_payload["stampede"] = True
        job_id = await enqueue_job(
            cluster_id, project_id, kind, job_payload,
            user_id="stampede-system", username="Stampede", operation_id=op.id, session=session,
        )
        state.update(
            idle_since={},
            last_decision=f"scale_{direction}_queued",
            last_blocked_reason="",
            last_job_id=job_id,
            last_operation_id=op.id,
        )
        ng.stampede_state = state
        ng.updated_at = now
        return {"job_id": job_id, "operation_id": op.id, "count": count}


def _authority_for(kind: str, payload: dict, cluster_id: str, project_id: str) -> execution.Authority | None:
    """Resolve the only authority a job kind may use; mutation jobs without a delegation never run."""
    from drover.services.cluster_authority import CAPABILITY_READ, CAPABILITY_SCALE

    delegation_id = payload.pop("_delegation_id", None)
    stampede = bool(payload.get("stampede"))
    if kind in _OPERATION_KINDS or (kind == "nodegroup_reconcile" and not stampede):
        if not delegation_id:
            raise execution.AuthorityRevoked("This mutation job has no admitted requester delegation; resubmit it")
        rollback = payload.get("expired_operation_id") if kind == "delete" else None
        return execution.OperationAuthority(delegation_id, project_id, cluster_id, kind, rollback)
    if kind == "stampede_provision" or (kind == "nodegroup_reconcile" and stampede):
        return execution.ResourceAuthority(project_id, cluster_id, CAPABILITY_SCALE)
    if kind == "reconcile":
        return execution.ResourceAuthority(project_id, cluster_id, CAPABILITY_READ)
    return None


async def _execute_job_direct(
    kind: str,
    payload: dict,
    cluster_id: str,
    project_id: str,
    operation_id: str | None = None,
) -> None:
    authority = _authority_for(kind, payload, cluster_id, project_id)
    if authority is None:
        await _dispatch_job(kind, payload, cluster_id, project_id, operation_id)
        return
    with execution.bound(authority):
        await _dispatch_job(kind, payload, cluster_id, project_id, operation_id)


async def _dispatch_job(
    kind: str,
    payload: dict,
    cluster_id: str,
    project_id: str,
    operation_id: str | None = None,
) -> None:
    from drover.services import autoscale, deletion, provisioner

    if kind == "create":
        await provisioner.create_cluster_job(project_id, cluster_id, payload, operation_id=operation_id)
    elif kind == "bootstrap_ha":
        await provisioner.bootstrap_ha_servers(
            project_id,
            cluster_id,
            payload.get("server_ip", ""),
            payload.get("node_token", ""),
            int(payload.get("master_count", 3)),
            payload.get("lb_pool_id", ""),
            payload.get("lb_fip_address", ""),
            operation_id=operation_id,
        )
    elif kind == "provision_agents":
        await provisioner.provision_agents(
            project_id,
            cluster_id,
            payload.get("server_ip", ""),
            payload.get("node_token", ""),
        )
    elif kind == "scale":
        op_id = operation_id or payload.pop("_operation_id", None)
        metric = payload.get("triggering_metric")
        if op_id or metric:
            await autoscale.scale_agents(
                project_id,
                cluster_id,
                int(payload["desired_count"]),
                operation_id=op_id,
                triggering_metric=metric,
            )
        else:
            await autoscale.scale_agents(project_id, cluster_id, int(payload["desired_count"]))
    elif kind == "nodegroup_reconcile":
        nodegroup = payload.get("nodegroup") or {}
        action = payload.get("action")
        op_id = operation_id or payload.pop("_operation_id", None)
        if payload.get("stampede") and action == "delete_vms":
            from drover.services.stampede import _delete_and_track

            await _delete_and_track(project_id, cluster_id, payload, op_id)
            return
        metric = payload.get("triggering_metric", "manual")
        if action == "provision":
            await autoscale.provision_nodegroup_and_reconcile(
                project_id,
                cluster_id,
                nodegroup,
                int(payload.get("add_count", 0)),
                operation_id=op_id,
                triggering_metric=metric,
            )
        elif action in {"delete_vms", "delete_group"}:
            await autoscale.delete_nodegroup_and_reconcile(
                project_id,
                cluster_id,
                nodegroup,
                payload.get("remove_entries") or [],
                delete_group=action == "delete_group",
                operation_id=op_id,
                triggering_metric=metric,
            )
        else:
            raise ValueError(f"unknown nodegroup reconciliation action: {action!r}")
    elif kind == "stampede_provision":
        from drover.services.stampede import _provision_and_track

        op_id = operation_id or payload.pop("_operation_id", None)
        metric = payload.get("triggering_metric")
        kwargs = {}
        if op_id is not None:
            kwargs["operation_id"] = op_id
        if metric is not None:
            kwargs["triggering_metric"] = metric

        await _provision_and_track(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=str(payload["nodegroup_id"]),
            add_count=int(payload["add_count"]),
            flavor_id=str(payload["flavor_id"]),
            image_id=payload.get("image_id"),
            labels=payload.get("labels"),
            taints=payload.get("taints"),
            gpu_required=bool(payload.get("gpu_required")),
            gpu_count=int(payload.get("gpu_count", 0)),
            provisioning_key_prefix=payload.get("provisioning_key_prefix"),
            **kwargs,
        )
    elif kind == "delete":
        await deletion.execute_delete_cluster(project_id, cluster_id, payload, operation_id=operation_id)
    elif kind == "rotate_certificates":
        from drover.services import cert_rotation

        await cert_rotation.rotate_cluster_certificates(project_id, cluster_id)
    elif kind == "reconcile":
        from drover.services import reconciliation

        await reconciliation.reconcile_cluster(
            project_id=project_id,
            cluster_id=cluster_id,
            operation_id=operation_id,
        )
    elif kind == "reauthorize":
        from drover.services import cluster_authority

        op_id = operation_id or payload.pop("_operation_id", None)
        await cluster_authority.execute_reauthorization(project_id, cluster_id, payload, operation_id=op_id)
    else:
        raise ValueError(f"unsupported Drover job kind: {kind}")


async def _settle_stampede_job(session: AsyncSession, job: DroverJob, error: str = "") -> None:
    payload = job.payload_json or {}
    if job.kind not in {"stampede_provision", "nodegroup_reconcile"}:
        return
    ng_id = payload.get("nodegroup_id") or (payload.get("nodegroup") or {}).get("id")
    if not ng_id:
        return
    ng = await session.get(K3sNodegroup, ng_id, with_for_update=True)
    if ng is None or ng.cluster_id != job.cluster_id:
        return
    state = dict(ng.stampede_state or {})
    if state.get("last_job_id") != job.id:
        return
    vms = (await session.scalars(select(K3sNodegroupVM).where(K3sNodegroupVM.nodegroup_id == ng_id))).all()
    ng.node_count = len(vms)
    direction = "up" if job.kind == "stampede_provision" or payload.get("action") == "provision" else "down"
    state.update(
        in_flight_count=0, in_flight_since=0, deleting_nodes=[], idle_since={},
        last_decision=f"scale_{direction}_{'failed' if error else 'complete'}",
        last_blocked_reason=error or "",
        tracked_count=len(vms),
    )
    state[f"last_scale_{direction}"] = _now().timestamp()
    if error:
        for vm in vms:
            if vm.status == "CREATING":
                vm.status = "ERROR"
    ng.stampede_state = state
    ng.updated_at = _now()


async def _mark_cluster_failed(session, job: DroverJob, error: str, *, authority_failure: bool = False) -> None:
    if job.kind in {"stampede_provision", "nodegroup_reconcile"}:
        await _settle_stampede_job(session, job, error)
        return
    if job.kind in _TERMINAL_CLUSTER_SAFE_KINDS:
        # A failed reauthorization leaves the previously active generation serving the cluster.
        return
    if authority_failure and job.kind == "reconcile":
        # Lost continuous authority must not make the cluster non-ACTIVE: reauthorization requires ACTIVE.
        return
    cluster = await session.get(K3sCluster, job.cluster_id, with_for_update=True)
    if cluster is not None and cluster.project_id == job.project_id and cluster.deleted_at is None:
        cluster.status = "ERROR"
        cluster.status_reason = error
        cluster.updated_at = _now()


async def _claim_one() -> tuple[str, int, str, str, str, dict] | None:
    factory = get_session_factory()
    if factory is None:
        return None
    now = _now()
    stale_before = now - timedelta(seconds=_LEASE_SECONDS)
    if _logger.isEnabledFor(logging.DEBUG):
        _logger.debug(
            "Drover job query=%s", safe_metadata({"status": ("queued", "running"), "cluster_id": None})
        )
    active_job = aliased(DroverJob)
    async with factory() as session, session.begin():
        while True:
            job = (
                await session.execute(
                    select(DroverJob)
                    .where(
                        or_(
                            DroverJob.status == "queued",
                            (DroverJob.status == "running")
                            & DroverJob.claimed_at.is_not(None)
                            & (DroverJob.claimed_at < stale_before),
                        ),
                        ~exists(
                            select(active_job.id).where(
                                active_job.cluster_id == DroverJob.cluster_id,
                                active_job.id != DroverJob.id,
                                active_job.status == "running",
                                active_job.claimed_at.is_not(None),
                                active_job.claimed_at >= stale_before,
                            )
                        ),
                    )
                    .order_by(DroverJob.created_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            ).scalar_one_or_none()
            if job is None:
                return None
            cluster = await session.get(K3sCluster, job.cluster_id, with_for_update=True)
            if cluster is None or cluster.project_id != job.project_id:
                job.status = "failed"
                job.last_error = "Drover cluster not found"
                job.claimed_at = None
                job.updated_at = now
                continue
            active_other = (
                await session.execute(
                    select(DroverJob.id)
                    .where(
                        DroverJob.cluster_id == job.cluster_id,
                        DroverJob.id != job.id,
                        DroverJob.status == "running",
                        DroverJob.claimed_at.is_not(None),
                        DroverJob.claimed_at >= stale_before,
                    )
                    .limit(1)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if active_other is not None:
                return None
            if job.attempts >= _MAX_ATTEMPTS:
                error = job.last_error or "Drover job retry limit exceeded"
                job.status = "failed"
                job.last_error = error
                job.claimed_at = None
                job.updated_at = now
                await _mark_cluster_failed(session, job, error)
                op_id = getattr(job, "operation_id", None)
                if op_id:
                    op = await session.get(DroverOperation, op_id, with_for_update=True)
                    if op:
                        op.status = "FAILED"
                        op.error = error
                        op.finished_at = now
                        await operations._append_event_impl(
                            session,
                            op.id,
                            phase="job_failed",
                            message=f"Job {job.kind} retry limit exceeded: {error}",
                            payload_json={"job_id": job.id, "error": error},
                        )
                await _delegation.release_if_idle(session, getattr(job, "delegation_id", None), reason="job failed")
                continue

            is_lease_recovery = job.status == "running" and job.claimed_at is not None
            prior_attempts = job.attempts
            job.status = "running"
            job.attempts += 1
            job.claimed_at = now
            job.updated_at = now

            op_id = getattr(job, "operation_id", None)
            if op_id:
                op = await session.get(DroverOperation, op_id, with_for_update=True)
                if op:
                    if op.status == "QUEUED":
                        op.status = "RUNNING"
                        if not op.started_at:
                            op.started_at = now
                    if is_lease_recovery or prior_attempts > 0:
                        pass
            payload = dict(job.payload_json or {})
            if getattr(job, "operation_id", None):
                payload["_operation_id"] = job.operation_id
            if getattr(job, "delegation_id", None):
                payload["_delegation_id"] = job.delegation_id
            return (
                job.id,
                job.attempts,
                job.kind,
                job.cluster_id,
                job.project_id,
                payload,
            )


async def _complete(job_id: str, *, attempt: int) -> bool:
    """Complete only the lease attempt owned by this worker."""
    factory = get_session_factory()
    if factory is None:
        return False
    async with factory() as session, session.begin():
        job = await session.get(DroverJob, job_id, with_for_update=True)
        if job is None or job.status != "running" or job.attempts != attempt:
            return False
        job.status = "completed"
        job.claimed_at = None
        job.last_error = None
        job.updated_at = _now()
        await _settle_stampede_job(session, job)

        op_id = getattr(job, "operation_id", None)
        if op_id:
            op = await session.get(DroverOperation, op_id, with_for_update=True)
            if op:
                cluster = await session.get(K3sCluster, job.cluster_id)
                create_is_active = (
                    op.kind == "create"
                    and cluster is not None
                    and cluster.status == "ACTIVE"
                    and op.status not in {"FAILED", "CANCELLED", "SUCCEEDED"}
                )
                if create_is_active or (op.kind != "create" and op.status not in {"FAILED", "CANCELLED", "SUCCEEDED"}):
                    op.status = "SUCCEEDED"
                    op.finished_at = _now()
                    phase = "job_completed"
                    msg = f"Job {job.kind} completed"
                elif op.kind == "create" and op.status not in {"FAILED", "CANCELLED", "SUCCEEDED"}:
                    phase = "server_boot_ready"
                    msg = "Create stage completed; waiting for the cluster to become ACTIVE"
                else:
                    phase = "job_completed"
                    msg = f"Job {job.kind} completed after operation terminalized"
                await operations._append_event_impl(
                    session,
                    op.id,
                    phase=phase,
                    message=msg,
                    payload_json={"job_id": job.id, "op_status": op.status},
                )
        await _delegation.release_if_idle(session, getattr(job, "delegation_id", None))
        return True


async def _retry_or_fail(job_id: str, *, attempt: int, error: str, terminal: bool = False) -> bool:
    """Requeue a failed attempt, terminalizing on the third failure or on an authorization failure."""
    factory = get_session_factory()
    if factory is None:
        return False
    clean_error = (error.strip() or "Drover job failed")[:4096]
    async with factory() as session, session.begin():
        job = await session.get(DroverJob, job_id, with_for_update=True)
        if job is None or job.status != "running" or job.attempts != attempt:
            return False
        job.last_error = clean_error
        job.claimed_at = None
        job.updated_at = _now()

        op = None
        op_id = getattr(job, "operation_id", None)
        if op_id:
            op = await session.get(DroverOperation, op_id, with_for_update=True)

        if terminal or job.attempts >= _MAX_ATTEMPTS:
            job.status = "failed"
            await _mark_cluster_failed(session, job, clean_error, authority_failure=terminal)
            if op:
                op.status = "FAILED"
                op.error = clean_error
                op.finished_at = _now()
                await operations._append_event_impl(
                    session,
                    op.id,
                    phase="job_failed",
                    message=f"Job {job.kind} failed after {job.attempts} attempts: {clean_error}",
                    payload_json={"job_id": job.id, "error": clean_error},
                )
            await _delegation.release_if_idle(session, getattr(job, "delegation_id", None), reason="job failed")
        else:
            job.status = "queued"
            if op:
                await operations._append_event_impl(
                    session,
                    op.id,
                    phase="job_attempt_failed",
                    message=f"Job {job.kind} attempt {attempt} failed: {clean_error}. Retrying...",
                    payload_json={"job_id": job.id, "attempt": attempt, "error": clean_error},
                )
        return True


async def _renew_lease(job_id: str, *, attempt: int) -> bool:
    """Extend only the active lease attempt currently owned by this worker."""
    factory = get_session_factory()
    if factory is None:
        return False
    async with factory() as session, session.begin():
        job = await session.get(DroverJob, job_id, with_for_update=True)
        if job is None or job.status != "running" or job.attempts != attempt:
            return False
        job.claimed_at = _now()
        job.updated_at = _now()
        return True


async def _heartbeat_lease(job_id: str, *, attempt: int, stop: asyncio.Event) -> None:
    """Renew long jobs before their reclaim deadline; stop when ownership changes."""
    interval = max(1, _LEASE_SECONDS // 3)
    while True:
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        except TimeoutError:
            if not await _renew_lease(job_id, attempt=attempt):
                return


def _log_job_completion(job_id: str, attempt: int, kind: str, outcome: str) -> None:
    """Report only committed outcomes, with a validated durable ID and kind."""
    try:
        safe_id = str(uuid.UUID(job_id))
        if safe_id != job_id:
            safe_id = "untrusted"
    except (ValueError, TypeError, AttributeError):
        safe_id = "untrusted"
    safe_kind = kind if kind in _SUPPORTED_KINDS else "unknown"
    _logger.info(
        "Drover job completion kind=%s job_id=%s attempt=%d outcome=%s", safe_kind, safe_id, attempt, outcome
    )
    if _logger.isEnabledFor(logging.DEBUG):
        _logger.debug("Drover job result=%s", safe_metadata({"status": outcome, "attempt": attempt}))


async def process_one_job() -> bool:
    """Claim and execute at most one durable Drover job."""
    claimed = await _claim_one()
    if claimed is None:
        return False
    job_id, attempt, kind, cluster_id, project_id, payload = claimed
    if _logger.isEnabledFor(logging.DEBUG):
        _logger.debug(
            "Drover job claimed state=%s",
            safe_metadata({"status": "running", "kind": kind, "attempt": attempt, "cluster_id": cluster_id}),
        )
    stop = asyncio.Event()
    heartbeat = asyncio.create_task(_heartbeat_lease(job_id, attempt=attempt, stop=stop))
    delegation_id = payload.get("_delegation_id")
    try:
        await _execute_job_direct(kind, payload, cluster_id, project_id)
        if await _complete(job_id, attempt=attempt):
            _log_job_completion(job_id, attempt, kind, "success")
    except execution.ExecutionAuthorityError as exc:
        # Revoked, mismatched or missing authority is never retried and never replaced by another identity.
        if await _retry_or_fail(job_id, attempt=attempt, error=str(exc), terminal=True):
            _log_job_completion(job_id, attempt, kind, "error")
    except Exception as exc:
        if await _retry_or_fail(job_id, attempt=attempt, error=str(exc)):
            _log_job_completion(job_id, attempt, kind, "error" if attempt >= _MAX_ATTEMPTS else "retry")
    finally:
        stop.set()
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
    # A released or revoked trust is deleted right away through its own token; the sweep retries failures.
    await _delegation.delete_released(delegation_id)
    return True


async def claim_and_run_jobs() -> int:
    """Process a bounded batch; worker loops call this repeatedly."""
    processed = 0
    while processed < _BATCH_SIZE and await process_one_job():
        processed += 1
    return processed


async def list_active_mutation_jobs(cluster_id: str) -> list[dict]:
    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database unavailable for active job query")
    async with factory() as session:
        jobs = (await session.scalars(select(DroverJob).where(
            DroverJob.cluster_id == cluster_id,
            DroverJob.status.in_(["queued", "running"]), DroverJob.kind != "reconcile",
        ))).all()
        return [{"id": job.id, "kind": job.kind, "status": job.status,
                 "operation_id": job.operation_id,
                 "nodegroup_id": (job.payload_json or {}).get("nodegroup_id") or ((job.payload_json or {}).get("nodegroup") or {}).get("id")} for job in jobs]


async def get_job(job_id: str) -> dict | None:
    """Return the durable status needed by streaming API adapters."""
    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session:
        job = await session.get(DroverJob, job_id)
        if job is None:
            return None
        return {
            "id": job.id,
            "status": job.status,
            "attempts": job.attempts,
            "last_error": job.last_error,
            "operation_id": getattr(job, "operation_id", None),
        }
