"""Durable Drover job queue with leased, attempt-fenced execution."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import exists, or_, select
from sqlalchemy.orm import aliased

from drover.db import get_session_factory
from drover.models.orm import DroverJob, K3sCluster

_logger = logging.getLogger("drover.jobs")
_LEASE_SECONDS = 900
_MAX_ATTEMPTS = 3
_BATCH_SIZE = 5
_SUPPORTED_KINDS = frozenset(
    {"bootstrap_ha", "provision_agents", "scale", "nodegroup_reconcile", "stampede_provision", "delete"}
)


def _now() -> datetime:
    return datetime.now(UTC)


async def enqueue_job(
    cluster_id: str,
    project_id: str,
    kind: str,
    payload: dict,
    user_id: str | None = None,
    username: str | None = None,
) -> str:
    """Persist a job before a request reports that background work started."""
    if kind not in _SUPPORTED_KINDS:
        raise ValueError(f"unsupported Drover job kind: {kind}")
    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database unavailable for durable job enqueue")

    job = DroverJob(
        id=str(uuid.uuid4()),
        cluster_id=cluster_id,
        project_id=project_id,
        kind=kind,
        status="queued",
        payload_json=payload,
        user_id=user_id or None,
        username=username or None,
        created_at=_now(),
        updated_at=_now(),
    )
    async with factory() as session, session.begin():
        session.add(job)
    return job.id


async def _execute_job_direct(kind: str, payload: dict, cluster_id: str, project_id: str) -> None:
    from drover.services import autoscale, provisioner

    if kind == "bootstrap_ha":
        await provisioner.bootstrap_ha_servers(
            project_id,
            cluster_id,
            payload.get("server_ip", ""),
            payload.get("node_token", ""),
            int(payload.get("master_count", 3)),
            payload.get("lb_pool_id", ""),
            payload.get("lb_fip_address", ""),
        )
    elif kind == "provision_agents":
        await provisioner.provision_agents(
            project_id,
            cluster_id,
            payload.get("server_ip", ""),
            payload.get("node_token", ""),
        )
    elif kind == "scale":
        await autoscale.scale_agents(project_id, cluster_id, int(payload["desired_count"]))
    elif kind == "nodegroup_reconcile":
        nodegroup = payload.get("nodegroup") or {}
        action = payload.get("action")
        if action == "provision":
            await autoscale.provision_nodegroup_and_reconcile(
                project_id,
                cluster_id,
                nodegroup,
                int(payload.get("add_count", 0)),
            )
        elif action in {"delete_vms", "delete_group"}:
            await autoscale.delete_nodegroup_and_reconcile(
                project_id,
                cluster_id,
                nodegroup,
                payload.get("remove_entries") or [],
                delete_group=action == "delete_group",
            )
        else:
            raise ValueError(f"unknown nodegroup reconciliation action: {action!r}")
    elif kind == "stampede_provision":
        from drover.services.stampede import _provision_and_track

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
        )
    elif kind == "delete":
        from drover.api.clusters import _delete_cluster_progress
        from drover.services import keystone, store

        cluster = await store.get_cluster(project_id, cluster_id)
        if not cluster or cluster.get("deleted_at"):
            return
        conn = await asyncio.to_thread(keystone.get_admin_connection_for_project, project_id)
        token_info = {
            "project_id": project_id,
            "user_id": payload.get("user_id", ""),
            "username": payload.get("username", ""),
        }
        try:
            async for _ in _delete_cluster_progress(conn, project_id, cluster, token_info):
                pass
        finally:
            await asyncio.to_thread(conn.close)
    else:
        raise ValueError(f"unsupported Drover job kind: {kind}")


async def _mark_cluster_failed(session, job: DroverJob, error: str) -> None:
    cluster = await session.get(K3sCluster, job.cluster_id, with_for_update=True)
    if cluster is not None and cluster.project_id == job.project_id and cluster.deleted_at is None:
        cluster.status = "ERROR"
        cluster.status_reason = error
        cluster.updated_at = _now()


async def _claim_one() -> tuple[str, int, str, str, str, dict] | None:
    """Claim one job without allowing concurrent work for the same cluster."""
    factory = get_session_factory()
    if factory is None:
        return None
    now = _now()
    stale_before = now - timedelta(seconds=_LEASE_SECONDS)
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
                continue
            job.status = "running"
            job.attempts += 1
            job.claimed_at = now
            job.updated_at = now
            return (
                job.id,
                job.attempts,
                job.kind,
                job.cluster_id,
                job.project_id,
                job.payload_json or {},
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
        return True


async def _retry_or_fail(job_id: str, *, attempt: int, error: str) -> bool:
    """Requeue a failed attempt, terminalizing the cluster on the third failure."""
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
        if job.attempts >= _MAX_ATTEMPTS:
            job.status = "failed"
            await _mark_cluster_failed(session, job, clean_error)
        else:
            job.status = "queued"
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


async def process_one_job() -> bool:
    """Claim and execute at most one durable Drover job."""
    claimed = await _claim_one()
    if claimed is None:
        return False
    job_id, attempt, kind, cluster_id, project_id, payload = claimed
    stop = asyncio.Event()
    heartbeat = asyncio.create_task(_heartbeat_lease(job_id, attempt=attempt, stop=stop))
    try:
        await _execute_job_direct(kind, payload, cluster_id, project_id)
        await _complete(job_id, attempt=attempt)
    except Exception as exc:
        _logger.exception("Drover job failed job_id=%s kind=%s attempt=%d", job_id, kind, attempt)
        await _retry_or_fail(job_id, attempt=attempt, error=str(exc))
    finally:
        stop.set()
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
    return True


async def claim_and_run_jobs() -> int:
    """Process a bounded batch; worker loops call this repeatedly."""
    processed = 0
    while processed < _BATCH_SIZE and await process_one_job():
        processed += 1
    return processed


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
        }
