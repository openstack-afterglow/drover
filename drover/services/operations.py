"""Drover Operation & Event management service."""

from __future__ import annotations

import inspect
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import exists, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from drover.db import get_session_factory
from drover.models.orm import DroverJob, DroverOperation, DroverOperationEvent, K3sCluster

_logger = logging.getLogger("drover.operations")

VALID_OP_KINDS = frozenset(
    {"create", "scale", "nodegroup_reconcile", "delete", "rotate_certificates", "reconcile", "reauthorize"}
)

VALID_OP_STATUSES = frozenset(
    {"QUEUED", "RUNNING", "WAITING_CALLBACK", "SUCCEEDED", "FAILED", "CANCELLED"}
)


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with a different request hash."""

    def __init__(self, key: str):
        super().__init__(f"Idempotency key {key!r} reused with a different request hash")
        self.key = key


def _now() -> datetime:
    return datetime.now(UTC)


async def _safe_flush(session: Any) -> None:
    if hasattr(session, "flush") and callable(session.flush):
        res = session.flush()
        if inspect.isawaitable(res):
            await res

async def create_or_get_operation(
    session: AsyncSession,
    *,
    project_id: str,
    cluster_id: str,
    kind: str,
    request_id: str | None = None,
    idempotency_key: str | None = None,
    request_hash: str | None = None,
    status: str = "QUEUED",
) -> DroverOperation:
    """Create a new DroverOperation or return an existing one if idempotency_key matches."""
    if kind not in VALID_OP_KINDS:
        raise ValueError(f"Invalid operation kind: {kind!r}")
    if status not in VALID_OP_STATUSES:
        raise ValueError(f"Invalid operation status: {status!r}")

    if idempotency_key:
        stmt = select(DroverOperation).where(
            DroverOperation.project_id == project_id,
            DroverOperation.idempotency_key == idempotency_key,
        )
        res = await session.execute(stmt)
        existing = res.scalar_one_or_none()
        if existing is not None:
            if existing.request_hash != request_hash:
                raise IdempotencyConflictError(idempotency_key)
            return existing

    op = DroverOperation(
        id=str(uuid.uuid4()),
        project_id=project_id,
        cluster_id=cluster_id,
        kind=kind,
        status=status,
        request_id=request_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        created_at=_now(),
    )
    if status == "RUNNING":
        op.started_at = _now()
    session.add(op)
    await _safe_flush(session)
    return op


async def append_operation_event(
    session_or_factory: AsyncSession | Any,
    operation_id: str,
    *,
    phase: str,
    message: str | None = None,
    payload_json: dict | list | None = None,
) -> DroverOperationEvent | None:
    """Append a sequenced event to an operation."""
    if isinstance(session_or_factory, AsyncSession):
        return await _append_event_impl(
            session_or_factory, operation_id, phase, message, payload_json
        )

    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session, session.begin():
        return await _append_event_impl(
            session, operation_id, phase, message, payload_json
        )


async def _append_event_impl(
    session: AsyncSession,
    operation_id: str,
    phase: str,
    message: str | None = None,
    payload_json: dict | list | None = None,
) -> DroverOperationEvent:
    # Serialize max(sequence) + 1 with every other writer for this operation.
    await session.get(DroverOperation, operation_id, with_for_update=True)
    # Distinct first-event writers can share an InnoDB index gap; a deadlock must abort
    # the caller's transaction, never leave a partial event/status update.
    stmt = (
        select(DroverOperationEvent.sequence)
        .where(DroverOperationEvent.operation_id == operation_id)
        .order_by(DroverOperationEvent.sequence.desc())
        .limit(1)
        .with_for_update()
    )
    res = await session.execute(stmt)
    last_seq = res.scalar_one_or_none()
    next_seq = (last_seq or 0) + 1

    event = DroverOperationEvent(
        operation_id=operation_id,
        sequence=next_seq,
        phase=phase,
        message=message,
        payload_json=payload_json,
        created_at=_now(),
    )
    session.add(event)
    await _safe_flush(session)
    return event


async def update_operation_status(
    session_or_factory: AsyncSession | Any,
    operation_id: str,
    status: str,
    *,
    error: str | None = None,
) -> DroverOperation | None:
    """Update operation status, recording started_at, finished_at, and error as appropriate."""
    if status not in VALID_OP_STATUSES:
        raise ValueError(f"Invalid operation status: {status!r}")

    if isinstance(session_or_factory, AsyncSession):
        return await _update_status_impl(session_or_factory, operation_id, status, error=error)

    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session, session.begin():
        return await _update_status_impl(session, operation_id, status, error=error)


async def _update_status_impl(
    session: AsyncSession,
    operation_id: str,
    status: str,
    error: str | None = None,
) -> DroverOperation | None:
    op = await session.get(DroverOperation, operation_id, with_for_update=True)
    if op is None:
        return None

    op.status = status
    now = _now()
    if status == "RUNNING" and op.started_at is None:
        op.started_at = now
    elif status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
        op.finished_at = now

    if error is not None:
        op.error = error

    await _safe_flush(session)
    return op


async def get_active_operation(
    session_or_factory: AsyncSession | Any,
    cluster_id: str,
    kind: str | None = None,
) -> DroverOperation | None:
    """Find the active non-terminal operation for a cluster."""
    if isinstance(session_or_factory, AsyncSession):
        return await _get_active_op_impl(session_or_factory, cluster_id, kind=kind)

    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session:
        return await _get_active_op_impl(session, cluster_id, kind=kind)


async def _get_active_op_impl(
    session: AsyncSession,
    cluster_id: str,
    kind: str | None = None,
) -> DroverOperation | None:
    stmt = select(DroverOperation).where(
        DroverOperation.cluster_id == cluster_id,
        DroverOperation.status.not_in(["SUCCEEDED", "FAILED", "CANCELLED"]),
    )
    if kind:
        stmt = stmt.where(DroverOperation.kind == kind)
    stmt = stmt.order_by(DroverOperation.created_at.desc()).limit(1)
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def set_operation_waiting_callback(
    operation_id: str,
    message: str = "Boot server ready, waiting for cloud-init callback",
) -> DroverOperation | None:
    """Transition an operation to WAITING_CALLBACK and record an event."""
    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session, session.begin():
        op = await _update_status_impl(session, operation_id, "WAITING_CALLBACK")
        if op:
            await _append_event_impl(session, operation_id, phase="waiting_callback", message=message, payload_json=None)
        return op
async def get_operation(
    session_or_factory: AsyncSession | Any,
    operation_id: str,
) -> DroverOperation | None:
    if isinstance(session_or_factory, AsyncSession):
        return await session_or_factory.get(DroverOperation, operation_id)
    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session:
        return await session.get(DroverOperation, operation_id)


async def get_operation_by_idempotency_key(
    session_or_factory: AsyncSession | Any,
    project_id: str,
    idempotency_key: str,
) -> DroverOperation | None:
    stmt = select(DroverOperation).where(
        DroverOperation.project_id == project_id,
        DroverOperation.idempotency_key == idempotency_key,
    )
    if isinstance(session_or_factory, AsyncSession):
        res = await session_or_factory.execute(stmt)
        return res.scalar_one_or_none()
    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session:
        res = await session.execute(stmt)
        return res.scalar_one_or_none()


async def get_operation_events(
    session_or_factory: AsyncSession | Any,
    operation_id: str,
    since_sequence: int = 0,
) -> list[DroverOperationEvent]:
    stmt = (
        select(DroverOperationEvent)
        .where(
            DroverOperationEvent.operation_id == operation_id,
            DroverOperationEvent.sequence > since_sequence,
        )
        .order_by(DroverOperationEvent.sequence.asc())
    )
    if isinstance(session_or_factory, AsyncSession):
        res = await session_or_factory.execute(stmt)
        return list(res.scalars().all())
    factory = get_session_factory()
    if factory is None:
        return []
    async with factory() as session:
        res = await session.execute(stmt)
        return list(res.scalars().all())


async def get_latest_create_operation(project_id: str, cluster_id: str) -> DroverOperation | None:
    """Read the latest create attempt for this project/cluster, including terminal failures."""
    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session:
        stmt = (
            select(DroverOperation)
            .where(
                DroverOperation.project_id == project_id,
                DroverOperation.cluster_id == cluster_id,
                DroverOperation.kind == "create",
            )
            .order_by(DroverOperation.created_at.desc(), DroverOperation.id.desc())
            .limit(1)
        )
        return (await session.execute(stmt)).scalar_one_or_none()


async def recover_deleted_cluster_operations(
    *, project_id: str | None = None, operation_id: str | None = None, batch_size: int = 100,
) -> list[str]:
    """Cancel orphaned RUNNING operations on same-project soft-deleted clusters, without cloud work.

    Any queued/running job targeting the cluster or linked to the operation vetoes recovery,
    regardless of project, kind, or lease age. Serializable job-range locks fence new admissions;
    each cancellation and sequenced event commit together. Nonblocking cluster/operation locks
    avoid inversion with job retry locks.
    InnoDB deadlock aborts roll back the candidate; periodic maintenance revisits it.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    factory = get_session_factory()
    if factory is None:
        return []

    active_job = exists(select(DroverJob.id).where(
        DroverJob.status.in_(["queued", "running"]),
        or_(
            DroverJob.cluster_id == DroverOperation.cluster_id,
            DroverJob.operation_id == DroverOperation.id,
        ),
    ))
    stmt = (
        select(DroverOperation)
        .join(K3sCluster, K3sCluster.id == DroverOperation.cluster_id)
        .where(
            DroverOperation.status == "RUNNING",
            K3sCluster.project_id == DroverOperation.project_id,
            K3sCluster.deleted_at.is_not(None),
            ~active_job,
        )
        .order_by(DroverOperation.created_at, DroverOperation.id)
        .limit(batch_size)
    )
    if project_id is not None:
        stmt = stmt.where(DroverOperation.project_id == project_id)
    if operation_id is not None:
        stmt = stmt.where(DroverOperation.id == operation_id)
    async with factory() as session:
        candidates = [
            (op.id, op.project_id, op.cluster_id)
            for op in (await session.execute(stmt)).scalars().all()
        ]

    recovered: list[str] = []
    for op_id, op_project_id, cluster_id in candidates:
        async with factory() as session, session.begin():
            # Set isolation before the first connection/statement, independently of server defaults.
            await session.connection(execution_options={"isolation_level": "SERIALIZABLE"})
            # Locking reads see current job state, not the candidate scan's snapshot.
            # Do not skip locked jobs: a busy/stale lease is still active work.
            job_stmt = (
                select(DroverJob)
                .where(
                    DroverJob.status.in_(["queued", "running"]),
                    or_(DroverJob.cluster_id == cluster_id, DroverJob.operation_id == op_id),
                )
                .order_by(DroverJob.id)
                .with_for_update()
            )
            if (await session.execute(job_stmt)).scalars().all():
                continue

            # Claims use job -> cluster -> operation; retries use job -> operation -> cluster.
            # Never wait for either parent row while holding the other parent/job range locks.
            cluster_stmt = (
                select(K3sCluster)
                .where(
                    K3sCluster.id == cluster_id,
                    K3sCluster.project_id == op_project_id,
                    K3sCluster.deleted_at.is_not(None),
                )
                .with_for_update(skip_locked=True)
            )
            if (await session.execute(cluster_stmt)).scalar_one_or_none() is None:
                continue
            op_stmt = (
                select(DroverOperation)
                .where(
                    DroverOperation.id == op_id,
                    DroverOperation.project_id == op_project_id,
                    DroverOperation.cluster_id == cluster_id,
                    DroverOperation.status == "RUNNING",
                )
                .with_for_update(skip_locked=True)
            )
            op = (await session.execute(op_stmt)).scalar_one_or_none()
            if op is None:
                continue

            reason = "Target cluster was deleted before the operation completed"
            op.status = "CANCELLED"
            op.finished_at = _now()
            op.error = op.error or reason
            await _append_event_impl(
                session, op.id, phase="deleted_cluster_recovered", message=reason,
                payload_json={
                    "cluster_id": cluster_id, "project_id": op_project_id,
                    "previous_status": "RUNNING", "status": "CANCELLED",
                },
            )
            recovered.append(op.id)
    return recovered



async def recover_expired_callback_operations(timeout_seconds: int = 1800) -> list[str]:
    """Scan for WAITING_CALLBACK operations past TTL, set them FAILED, append event, mark cluster ERROR, and enqueue rollback."""
    factory = get_session_factory()
    if factory is None:
        return []

    cutoff = _now() - timedelta(seconds=timeout_seconds)
    recovered_op_ids: list[str] = []

    async with factory() as session, session.begin():
        stmt = (
            select(DroverOperation)
            .where(
                DroverOperation.status == "WAITING_CALLBACK",
                DroverOperation.created_at < cutoff,
            )
            .with_for_update()
        )
        res = await session.execute(stmt)
        expired_ops = list(res.scalars().all())

        for op in expired_ops:
            now = _now()
            op.status = "FAILED"
            op.error = "Cloud-init callback timed out"
            op.finished_at = now

            await _append_event_impl(
                session,
                op.id,
                phase="callback_timeout",
                message="Cloud-init callback timed out",
                payload_json={"timeout_seconds": timeout_seconds, "cluster_id": op.cluster_id},
            )

            cluster = await session.get(K3sCluster, op.cluster_id, with_for_update=True)
            if cluster is not None and cluster.project_id == op.project_id:
                cluster.status = "ERROR"
                cluster.status_reason = "Cloud-init callback timed out"
                cluster.updated_at = now

            recovered_op_ids.append(op.id)

    if recovered_op_ids:
        from drover.services import delegation, jobs

        for op_id in recovered_op_ids:
            op = await get_operation(None, op_id)
            if op:
                # Rollback continues the admitted create operation only; without its delegation the job fails closed.
                await jobs.enqueue_job(
                    cluster_id=op.cluster_id,
                    project_id=op.project_id,
                    kind="delete",
                    payload={"reason": "Cloud-init callback timed out", "expired_operation_id": op.id},
                    op_kind="delete",
                    delegation_id=await delegation.active_delegation_for_operation(None, op.id),
                )

    return recovered_op_ids
async def fail_waiting_callback_operations(
    reason: str = "Invalid, expired, or reused callback token",
    request_id: str = "",
    source_ip: str = "",
) -> list[str]:
    """Scan for any still-waiting (WAITING_CALLBACK) operations, set them FAILED, append callback_failed event, and mark cluster ERROR without queuing jobs."""
    factory = get_session_factory()
    if factory is None:
        return []

    failed_op_ids: list[str] = []
    async with factory() as session, session.begin():
        stmt = (
            select(DroverOperation)
            .where(DroverOperation.status == "WAITING_CALLBACK")
            .with_for_update()
        )
        res = await session.execute(stmt)
        waiting_ops = list(res.scalars().all())

        for op in waiting_ops:
            now = _now()
            op.status = "FAILED"
            op.error = reason
            op.finished_at = now

            await _append_event_impl(
                session,
                op.id,
                phase="callback_failed",
                message=reason,
                payload_json={"request_id": request_id, "source_ip": source_ip, "reason": reason},
            )

            cluster = await session.get(K3sCluster, op.cluster_id, with_for_update=True)
            if cluster is not None and cluster.project_id == op.project_id:
                cluster.status = "ERROR"
                cluster.status_reason = reason
                cluster.updated_at = now

            failed_op_ids.append(op.id)

    return failed_op_ids
