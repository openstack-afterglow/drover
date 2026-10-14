"""Durable Drover job queue with leased, attempt-fenced execution."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from drover.db import get_session_factory
from drover.logging import safe_metadata
from drover.models.orm import DroverJob, DroverOperation, K3sCluster, K3sNodegroup, K3sNodegroupVM
from drover.services import delegation as _delegation
from drover.services import execution, execution_safety, operations, worker_ownership
from drover.services.delegation import AdmittedDelegation

_logger = logging.getLogger("drover.jobs")
_LEASE_SECONDS = 900
_MAX_ATTEMPTS = 3
_EXECUTION_START = "_drover_execution_started"
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
    if any(key in payload for key in (_EXECUTION_START, "_delegation_id", "_operation_id")):
        raise ValueError("Execution checkpoint metadata is owned by the worker")

    async def persist() -> str:
        await worker_ownership.checkpoint()
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
        await worker_ownership.checkpoint()
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
        await worker_ownership.checkpoint()
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
            or (direction == "up" and payload.get("flavor_id", ng.flavor_id) != ng.flavor_id)
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
        await worker_ownership.checkpoint()
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
            await worker_ownership.checkpoint()
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
    await execution_safety.ownership_checkpoint()
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
            flavor_extra_specs=payload.get("flavor_extra_specs"),
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


def _mutation_started(job: DroverJob) -> bool:
    return _EXECUTION_START in (getattr(job, "payload_json", None) or {})


def _lease_is_live(job: DroverJob, attempt: int) -> bool:
    if job is None or job.status != "running" or job.attempts != attempt or job.claimed_at is None:
        return False
    claimed_at = job.claimed_at
    if claimed_at.tzinfo is None:
        claimed_at = claimed_at.replace(tzinfo=UTC)
    return claimed_at >= _now() - timedelta(seconds=_LEASE_SECONDS)


async def _execution_checkpoint(
    job_id: str, *, attempt: int, kind: str, cluster_id: str, project_id: str, mutation: bool = True,
    session: AsyncSession | None = None,
) -> float:
    """Fence attempts and keep SQL domain validation in the write transaction.

    External mutation checkpoints commit the non-replay marker before sending.
    Domain writes hold the job row until their transaction commits, so reclaim
    cannot pass validation and race a later cluster status mutation.
    """
    async def validate(active_session):
        locking = mutation or session is not None
        job = await active_session.get(DroverJob, job_id, with_for_update=locking, populate_existing=True)
        if (
            not _lease_is_live(job, attempt)
            or job.kind != kind
            or job.cluster_id != cluster_id
            or job.project_id != project_id
        ):
            raise execution_safety.OwnershipLost("SQL job attempt no longer owns dispatch")
        cluster = await active_session.get(K3sCluster, cluster_id, with_for_update=locking, populate_existing=True)
        if cluster is None or cluster.project_id != project_id or cluster.deleted_at is not None:
            raise execution_safety.OwnershipLost("SQL job cluster no longer authorizes dispatch")
        if mutation and kind == "reconcile":
            raise execution_safety.OwnershipLost("Read-only reconciliation cannot dispatch a mutation")
        op_id = getattr(job, "operation_id", None)
        if op_id:
            op = await active_session.get(DroverOperation, op_id, with_for_update=locking, populate_existing=True)
            if (
                op is None or op.status in {"FAILED", "CANCELLED", "SUCCEEDED"}
                or op.cluster_id != cluster_id or op.project_id != project_id
            ):
                raise execution_safety.OwnershipLost("Job operation no longer authorizes dispatch")
        claimed_at = job.claimed_at
        if claimed_at.tzinfo is None:
            claimed_at = claimed_at.replace(tzinfo=UTC)
        deadline = time.monotonic() + max(0, _LEASE_SECONDS - (_now() - claimed_at).total_seconds())
        if not mutation:
            return deadline
        payload = dict(job.payload_json or {})
        marker = payload.get(_EXECUTION_START)
        if _EXECUTION_START in payload and (not isinstance(marker, dict) or marker.get("attempt") != attempt):
            raise execution_safety.OwnershipLost("A previous attempt already started mutation dispatch")
        payload[_EXECUTION_START] = {"attempt": attempt}
        job.payload_json = payload
        job.updated_at = _now()
        return deadline

    if session is not None:
        return await validate(session)
    factory = get_session_factory()
    if factory is None:
        raise execution_safety.OwnershipLost("SQL job ownership is unavailable")
    async with factory() as owned_session, owned_session.begin():
        return await validate(owned_session)


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
            ambiguous = _mutation_started(job) or (job.status == "running" and job.kind != "reconcile")
            if ambiguous or job.attempts >= _MAX_ATTEMPTS:
                error = (
                    "Mutation execution outcome is uncertain; automatic replay is disabled"
                    if ambiguous else job.last_error or "Drover job retry limit exceeded"
                )
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
                            message=f"Job {job.kind} stopped without replay: {error}",
                            payload_json={"job_id": job.id, "error": error},
                        )
                await _delegation.release_if_idle(session, getattr(job, "delegation_id", None), reason="job failed")
                continue

            job.status = "running"
            job.attempts += 1
            job.claimed_at = now
            job.updated_at = now

            op_id = getattr(job, "operation_id", None)
            if op_id:
                op = await session.get(DroverOperation, op_id, with_for_update=True)
                if op and op.status == "QUEUED":
                    op.status = "RUNNING"
                    if not op.started_at:
                        op.started_at = now
            payload = dict(job.payload_json or {})
            payload.pop(_EXECUTION_START, None)
            payload.pop("_delegation_id", None)
            payload.pop("_operation_id", None)
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
    """Complete the owned lease attempt; create success also requires live provisioning outputs."""
    factory = get_session_factory()
    if factory is None:
        return False
    async with factory() as session, session.begin():
        job = await session.get(DroverJob, job_id, with_for_update=True)
        if not _lease_is_live(job, attempt):
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
                    and cluster.project_id == job.project_id
                    and cluster.deleted_at is None
                    and bool(cluster.server_vm_id)
                    and bool(cluster.kubeconfig_encrypted)
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


async def _retry_or_fail(
    job_id: str, *, attempt: int, error: str, terminal: bool = False, authority_failure: bool = False,
) -> bool:
    """Retry only attempts without a mutation boundary; preserve authority failure semantics."""
    factory = get_session_factory()
    if factory is None:
        return False
    clean_error = (error.strip() or "Drover job failed")[:4096]
    async with factory() as session, session.begin():
        job = await session.get(DroverJob, job_id, with_for_update=True)
        if job is None or job.status != "running" or job.attempts != attempt:
            return False
        mutation_started = _mutation_started(job)
        if mutation_started:
            clean_error = (
                "Mutation outcome may be uncertain; automatic replay disabled; inspect provider resources before "
                "operator recovery: " + clean_error
            )[:4096]
        job.last_error = clean_error
        job.claimed_at = None
        job.updated_at = _now()
        terminal = terminal or mutation_started

        op = None
        op_id = getattr(job, "operation_id", None)
        if op_id:
            op = await session.get(DroverOperation, op_id, with_for_update=True)

        if terminal or job.attempts >= _MAX_ATTEMPTS:
            job.status = "failed"
            await _mark_cluster_failed(session, job, clean_error, authority_failure=authority_failure)
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
        if not _lease_is_live(job, attempt):
            return False
        job.claimed_at = _now()
        job.updated_at = _now()
        return True


async def _heartbeat_lease(
    job_id: str, *, attempt: int, stop: asyncio.Event,
    guard: execution_safety.DispatchGuard, dispatch: asyncio.Task,
) -> None:
    """Revoke and cancel dispatch on any heartbeat loss, including SQL errors."""
    interval = max(1, _LEASE_SECONDS // 3)
    while True:
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        except TimeoutError:
            try:
                renewed = await _renew_lease(job_id, attempt=attempt)
            except Exception:
                renewed = False
            if not renewed:
                guard.revoke()
                dispatch.cancel()
                return
            guard.renewed(_LEASE_SECONDS)


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
    guard = execution_safety.DispatchGuard(
        lambda: _execution_checkpoint(job_id, attempt=attempt, kind=kind, cluster_id=cluster_id, project_id=project_id),
        ownership_checkpoint=lambda: _execution_checkpoint(
            job_id, attempt=attempt, kind=kind, cluster_id=cluster_id, project_id=project_id, mutation=False,
        ),
        transaction_checkpoint=lambda session: _execution_checkpoint(
            job_id, attempt=attempt, kind=kind, cluster_id=cluster_id, project_id=project_id,
            mutation=False, session=session,
        ),
    )
    operation_id = payload.pop("_operation_id", None)
    with execution_safety.bound(guard):
        dispatch = asyncio.create_task(_execute_job_direct(kind, payload, cluster_id, project_id, operation_id))
    heartbeat = asyncio.create_task(_heartbeat_lease(job_id, attempt=attempt, stop=stop, guard=guard, dispatch=dispatch))
    delegation_id = payload.get("_delegation_id")
    cancelled = False
    propagate_cancel = False
    try:
        await asyncio.shield(dispatch)
        guard.check()
        if await _complete(job_id, attempt=attempt):
            _log_job_completion(job_id, attempt, kind, "success")
    except asyncio.CancelledError:
        guard.revoke()
        dispatch.cancel()
        cancelled = True
        propagate_cancel = bool(asyncio.current_task().cancelling())
    except (execution.ExecutionAuthorityError, execution_safety.OwnershipLost) as exc:
        # Authority and ownership failures never replace the actor or start a new attempt.
        guard.revoke()
        if await _retry_or_fail(
            job_id, attempt=attempt, error=str(exc), terminal=True,
            authority_failure=isinstance(exc, execution.ExecutionAuthorityError),
        ):
            _log_job_completion(job_id, attempt, kind, "error")
    except Exception as exc:
        if await _retry_or_fail(job_id, attempt=attempt, error=str(exc)):
            _log_job_completion(job_id, attempt, kind, "error" if guard.started or attempt >= _MAX_ATTEMPTS else "retry")
    finally:
        guard.revoke()
        dispatch.cancel()

        async def drain() -> None:
            await asyncio.gather(dispatch, return_exceptions=True)
            await guard.drain()
            stop.set()
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

        draining = asyncio.create_task(drain())
        while not draining.done():
            try:
                await asyncio.shield(draining)
            except asyncio.CancelledError:
                # Further shutdown cancellation must not abandon an issued SDK send.
                continue
        try:
            await draining
            if cancelled:
                # Keep the SQL lease nonclaimable until real SDK work drains.
                # Only then may a pre-send cancellation be safely retried.
                await _retry_or_fail(job_id, attempt=attempt, error="Execution cancelled")
        finally:
            guard.close()
    if propagate_cancel:
        raise asyncio.CancelledError
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
