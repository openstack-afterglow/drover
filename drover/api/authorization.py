"""Cluster resource authority: status, operator reauthorization and owner retirement of superseded credentials."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from drover.api.delegated import issue_credentials
from drover.auth import get_os_conn, get_token_info
from drover.db import get_session_factory
from drover.models.orm import DroverJob, K3sCluster
from drover.models.schemas import (
    ClusterAuthorizationStatus,
    ClusterCredentialRetireResponse,
    ClusterReauthorizationResponse,
)
from drover.policy import authorize
from drover.services import cluster_authority
from drover.services import jobs as _jobs
from drover.services import store as k3s_store

router = APIRouter()
_logger = logging.getLogger(__name__)


async def _project_cluster(project_id: str, cluster_id: str) -> dict:
    cluster = await k3s_store.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    return cluster


@router.get("/{cluster_id}/authorization", response_model=ClusterAuthorizationStatus)
async def get_cluster_authorization(cluster_id: str, token_info: dict = Depends(get_token_info)):
    """Credential references, generations and owner-revocation backlog without any secret."""
    project_id = token_info["project_id"]
    authorize("drover:clusters:get", {"project_id": project_id}, token_info)
    await _project_cluster(project_id, cluster_id)
    return await cluster_authority.authority_status(cluster_id)


@router.post("/{cluster_id}/authorization", status_code=202, response_model=ClusterReauthorizationResponse)
async def reauthorize_cluster(
    cluster_id: str,
    conn=Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """Stage the operator's own restricted credentials and roll them into the guest before activation."""
    project_id = conn._afterglow_project_id
    authorize("drover:clusters:reauthorize", {"project_id": project_id}, token_info)
    factory = get_session_factory()
    if factory is None:
        raise HTTPException(status_code=503, detail="Durable job storage is unavailable")
    async with factory() as session:
        cluster = await session.get(K3sCluster, cluster_id)
        if cluster is None or cluster.project_id != project_id or cluster.deleted_at is not None:
            raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
        if cluster.status != "ACTIVE":
            raise HTTPException(status_code=409, detail="Only ACTIVE clusters can be reauthorized")
        generation, purposes, guest_plugins = await cluster_authority.plan_reauthorization(session, cluster)

    issued = await issue_credentials(
        conn, token_info, project_id=project_id, cluster_id=cluster_id, generation=generation, purposes=purposes
    )
    try:
        async with factory() as session, session.begin():
            cluster = await session.get(K3sCluster, cluster_id, with_for_update=True)
            if cluster is None or cluster.deleted_at is not None or cluster.status != "ACTIVE":
                raise HTTPException(status_code=409, detail="Only ACTIVE clusters can be reauthorized")
            busy = await session.scalar(
                select(DroverJob.id).where(
                    DroverJob.cluster_id == cluster_id,
                    DroverJob.status.in_(["queued", "running"]),
                    DroverJob.kind != "reconcile",
                ).limit(1)
            )
            if busy:
                raise HTTPException(status_code=409, detail="Another cluster mutation is in progress")
            job_id = await _jobs.enqueue_job(
                cluster_id=cluster_id,
                project_id=project_id,
                kind="reauthorize",
                payload={"generation": generation},
                user_id=token_info.get("user_id"),
                username=token_info.get("username"),
                op_kind="reauthorize",
                session=session,
            )
            await session.flush()
            operation_id = (await session.get(DroverJob, job_id)).operation_id
            cluster_authority.add_generation(
                session, cluster_id=cluster_id, project_id=project_id, generation=generation, issued=issued,
                state="staged", operation_id=operation_id, guest_plugins=guest_plugins,
            )
    except IntegrityError as exc:
        await cluster_authority.discard_issued(conn, issued)
        raise HTTPException(status_code=409, detail="A concurrent reauthorization claimed this generation") from exc
    except BaseException:
        await cluster_authority.discard_issued(conn, issued)
        raise

    retired: list[str] = []
    try:
        retired = await cluster_authority.retire_owned(
            conn, cluster_id=cluster_id, owner_user_id=token_info.get("user_id") or "", states=("retiring",)
        )
    except Exception:
        _logger.warning("Superseded caller-owned credentials of cluster %s were not retired now", cluster_id)
    return {
        "cluster_id": cluster_id,
        "generation": generation,
        "operation_id": operation_id or "",
        "job_id": job_id,
        "credentials": [
            {"app_credential_id": item.app_credential_id, "purpose": item.purpose, "generation": generation,
             "owner_user_id": item.owner_user_id, "state": "staged", "role_names": item.role_names}
            for item in issued
        ],
        "retired_credential_ids": retired,
    }


@router.post("/{cluster_id}/authorization/retire", response_model=ClusterCredentialRetireResponse)
async def retire_cluster_credentials(
    cluster_id: str,
    conn=Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """Delete the caller's own superseded or orphaned cluster credentials with the caller's token."""
    project_id = conn._afterglow_project_id
    authorize("drover:clusters:retire_credentials", {"project_id": project_id}, token_info)
    await _project_cluster(project_id, cluster_id)
    try:
        deleted = await cluster_authority.retire_owned(
            conn, cluster_id=cluster_id, owner_user_id=token_info.get("user_id") or "", states=("retiring",)
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Credential retirement is temporarily unavailable") from exc
    status = await cluster_authority.authority_status(cluster_id)
    return {
        "cluster_id": cluster_id,
        "deleted_credential_ids": deleted,
        "owner_revocation_required": status["owner_revocation_required"],
    }
