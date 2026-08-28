"""Drover system-admin routes for cluster and template management."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncGenerator
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from drover.models.orm import ManagedOpenStackResource
from drover.models.schemas import (
    K3sClusterInfo,
    K3sClusterTemplateInfo,
    K3sProgressMessage,
    K3sProgressStep,
    ManagedOpenStackResourceInfo,
    ScaleK3sClusterRequest,
)
from drover.policy import require_policy
from drover.services import cert_rotation, certs
from drover.services import inventory as _inventory_svc
from drover.services import jobs as _jobs_svc
from drover.services import store as k3s_cluster
from drover.services import template as _tmpl_svc

router = APIRouter(dependencies=[Depends(require_policy("drover:admin"))])
_logger = logging.getLogger(__name__)

_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
    "Content-Encoding": "identity",
}


@router.get("/clusters", response_model=list[K3sClusterInfo])
async def list_admin_clusters(
    include_deleted: bool = Query(default=False),
    status: str | None = Query(default=None),
):
    clusters = await k3s_cluster.list_all_clusters(include_deleted=include_deleted)
    if status:
        statuses = {value.strip() for value in status.split(",") if value.strip()}
        clusters = [cluster for cluster in clusters if cluster.get("status") in statuses]
    from drover.api.clusters import _cluster_to_info

    return [_cluster_to_info(c) for c in clusters]


@router.get("/clusters/{cluster_id}", response_model=K3sClusterInfo)
async def get_admin_cluster(cluster_id: str):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    from drover.api.clusters import _cluster_to_info

    return _cluster_to_info(cluster)


@router.get("/clusters/{cluster_id}/kubeconfig")
async def download_admin_kubeconfig(cluster_id: str):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    kubeconfig = await k3s_cluster.get_kubeconfig_admin(cluster_id)
    if not kubeconfig:
        raise HTTPException(status_code=404, detail="kubeconfig가 아직 준비되지 않았습니다.")
    cluster_name = cluster.get("name", cluster_id)
    return Response(
        content=kubeconfig if isinstance(kubeconfig, bytes) else kubeconfig.encode(),
        media_type="application/yaml",
        headers={"Content-Disposition": f'attachment; filename="kubeconfig-{cluster_name}.yaml"'},
    )


@router.patch("/clusters/{cluster_id}/scale")
async def scale_admin_cluster(cluster_id: str, req: ScaleK3sClusterRequest):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    project_id = cluster["project_id"]
    await k3s_cluster.update_cluster_status(project_id, cluster_id, "SCALING", "에이전트 노드 스케일 변경 중")
    await _jobs_svc.enqueue_job(
        cluster_id=cluster_id,
        project_id=project_id,
        kind="scale",
        payload={"desired_count": req.agent_count},
        user_id="admin",
        username="system-admin",
    )
    return {"message": f"에이전트 노드가 {req.agent_count}개로 스케일 요청되었습니다", "target_count": req.agent_count}


@router.delete("/clusters/{cluster_id}", status_code=204)
async def delete_admin_cluster(cluster_id: str):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    project_id = cluster["project_id"]
    if cluster.get("deleted_at"):
        return
    await _jobs_svc.enqueue_job(
        cluster_id=cluster_id,
        project_id=project_id,
        kind="delete",
        payload={"user_id": "admin", "username": "system-admin"},
        user_id="admin",
        username="system-admin",
    )
    await k3s_cluster.update_cluster_status(project_id, cluster_id, "DELETING", "관리자 삭제 요청")


@router.post("/clusters/{cluster_id}/delete-async")
async def delete_admin_cluster_async(cluster_id: str):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    project_id = cluster["project_id"]

    job_id = await _jobs_svc.enqueue_job(
        cluster_id=cluster_id,
        project_id=project_id,
        kind="delete",
        payload={"user_id": "admin", "username": "system-admin"},
        user_id="admin",
        username="system-admin",
    )
    await k3s_cluster.update_cluster_status(project_id, cluster_id, "DELETING", "관리자 삭제 요청")

    async def gen() -> AsyncGenerator[str, None]:
        yield ": " + " " * 2048 + "\n\n"
        start = time.monotonic()
        while True:
            job = await _jobs_svc.get_job(job_id)
            if job is None:
                message = K3sProgressMessage(
                    step=K3sProgressStep.FAILED,
                    progress=0,
                    message="삭제 작업 상태를 찾을 수 없습니다",
                    error="job_not_found",
                    elapsed_seconds=round(time.monotonic() - start, 1),
                )
                yield f"data: {message.model_dump_json()}\n\n"
                return
            if job["status"] == "completed":
                message = K3sProgressMessage(
                    step=K3sProgressStep.COMPLETED,
                    progress=100,
                    message="클러스터 삭제 처리 완료",
                    elapsed_seconds=round(time.monotonic() - start, 1),
                )
                yield f"data: {message.model_dump_json()}\n\n"
                return
            if job["status"] == "failed":
                error = job.get("last_error") or "삭제 작업 실패"
                message = K3sProgressMessage(
                    step=K3sProgressStep.FAILED,
                    progress=0,
                    message=f"삭제 실패: {error}",
                    error=error,
                    elapsed_seconds=round(time.monotonic() - start, 1),
                )
                yield f"data: {message.model_dump_json()}\n\n"
                return
            message = K3sProgressMessage(
                step=K3sProgressStep.DELETE_INIT,
                progress=10,
                message="클러스터 삭제 작업 대기 중...",
                elapsed_seconds=round(time.monotonic() - start, 1),
            )
            yield f"data: {message.model_dump_json()}\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.get("/clusters/{cluster_id}/ca-certificate")
async def download_admin_ca_certificate(cluster_id: str):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    kubeconfig = await k3s_cluster.get_kubeconfig_admin(cluster_id)
    if not kubeconfig:
        raise HTTPException(status_code=404, detail="kubeconfig가 아직 준비되지 않았습니다.")
    ca_pem = certs.extract_ca_pem(kubeconfig if isinstance(kubeconfig, str) else kubeconfig.decode())
    if not ca_pem:
        raise HTTPException(status_code=404, detail="CA 인증서를 찾을 수 없습니다.")
    cluster_name = cluster.get("name", cluster_id)
    return Response(
        content=ca_pem,
        media_type="application/x-pem-file",
        headers={"Content-Disposition": f'attachment; filename="k3s-ca-{cluster_name}.crt"'},
    )


@router.get("/clusters/{cluster_id}/certificate-expiry")
async def get_admin_certificate_expiry(cluster_id: str):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    kubeconfig = await k3s_cluster.get_kubeconfig_admin(cluster_id)
    if not kubeconfig:
        raise HTTPException(status_code=404, detail="kubeconfig가 아직 준비되지 않았습니다.")
    raw_str = kubeconfig if isinstance(kubeconfig, str) else kubeconfig.decode()
    parsed = certs.parse_kubeconfig_certs(raw_str)
    api_addr = cluster.get("api_address") or cluster.get("server_ip")
    tls_certs = certs.probe_tls_server_cert(api_addr) if api_addr else []
    return {
        "ca": parsed.get("ca"),
        "client": parsed.get("client"),
        "server_via_tls": tls_certs,
    }


@router.post("/clusters/{cluster_id}/rotate-certs")
async def rotate_admin_certs(cluster_id: str):
    cluster = await k3s_cluster.get_cluster_admin(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    project_id = cluster["project_id"]

    async def gen() -> AsyncGenerator[str, None]:
        yield ": " + " " * 2048 + "\n\n"
        start = time.monotonic()
        try:
            async for step, pct, msg in cert_rotation.rotate_certificates(
                {"project_id": project_id, "user_id": "admin"}, None, cluster_id
            ):
                m = K3sProgressMessage(
                    step=K3sProgressStep(step),
                    progress=pct,
                    message=msg,
                    elapsed_seconds=round(time.monotonic() - start, 1),
                )
                yield f"data: {m.model_dump_json()}\n\n"
        except Exception as e:
            m = K3sProgressMessage(
                step=K3sProgressStep.FAILED,
                progress=0,
                message=f"회전 실패: {e}",
                error=str(e),
                elapsed_seconds=round(time.monotonic() - start, 1),
            )
            yield f"data: {m.model_dump_json()}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.get("/cluster-templates", response_model=list[K3sClusterTemplateInfo])
async def list_admin_cluster_templates():
    return await _tmpl_svc.list_templates(admin=True)
_SENSITIVE_PATTERNS = (
    "kubeconfig",
    "token",
    "password",
    "secret",
    "credential",
    "private_key",
    "auth",
    "payload",
)


def _is_sensitive(text: str) -> bool:
    low = text.lower()
    return any(p in low for p in _SENSITIVE_PATTERNS)


def _sanitize_dict(d: dict) -> dict:
    clean = {}
    for k, v in d.items():
        if not isinstance(k, str) or _is_sensitive(k):
            continue
        if isinstance(v, str):
            if _is_sensitive(v) or any(prefix in v.lower() for prefix in ("bearer ", "ssh-rsa")):
                continue
            clean[k] = v
        elif isinstance(v, dict):
            sub_clean = _sanitize_dict(v)
            if sub_clean:
                clean[k] = sub_clean
        elif isinstance(v, list):
            clean_list = []
            for item in v:
                if isinstance(item, str) and not _is_sensitive(item):
                    clean_list.append(item)
                elif isinstance(item, dict):
                    sub = _sanitize_dict(item)
                    if sub:
                        clean_list.append(sub)
                elif isinstance(item, (int, float, bool)):
                    clean_list.append(item)
            if clean_list:
                clean[k] = clean_list
        elif isinstance(v, (int, float, bool)):
            clean[k] = v
    return clean


def _extract_tags_and_metadata(metadata_raw: dict | list | None) -> tuple[list[str], dict | None]:
    if metadata_raw is None:
        return [], None

    if isinstance(metadata_raw, list):
        tags = [str(item) for item in metadata_raw if isinstance(item, str) and not _is_sensitive(str(item))]
        meta = {"tags": tags} if tags else None
        return tags, meta

    if isinstance(metadata_raw, dict):
        clean_meta = _sanitize_dict(metadata_raw)
        tags: list[str] = []
        if "tags" in clean_meta and isinstance(clean_meta["tags"], list):
            tags = [str(t) for t in clean_meta["tags"] if isinstance(t, str) and not _is_sensitive(str(t))]
        else:
            for k, v in clean_meta.items():
                if isinstance(v, (str, int, float, bool)):
                    tags.append(f"{k}={v}")
        return tags, clean_meta if clean_meta else None

    return [], None


def _format_dt(dt: object) -> str | None:
    if isinstance(dt, datetime):
        return dt.isoformat()
    if dt is not None:
        return str(dt)
    return None


def _resource_to_info(r: ManagedOpenStackResource) -> ManagedOpenStackResourceInfo:
    tags, clean_meta = _extract_tags_and_metadata(r.metadata_json)
    return ManagedOpenStackResourceInfo(
        id=r.id,
        cluster_id=r.cluster_id,
        operation_id=r.operation_id,
        service=r.service,
        resource_type=r.resource_type,
        resource_id=r.resource_id,
        name=r.name,
        state=r.state,
        tags=tags,
        metadata=clean_meta,
        created_at=_format_dt(r.created_at),
        last_seen_at=_format_dt(r.last_seen_at),
        deleted_at=_format_dt(r.deleted_at),
    )


@router.get("/managed-resources", response_model=list[ManagedOpenStackResourceInfo])
@router.get("/resources", response_model=list[ManagedOpenStackResourceInfo], include_in_schema=False)
@router.get("/inventory", response_model=list[ManagedOpenStackResourceInfo], include_in_schema=False)
async def list_admin_managed_resources(
    cluster_id: str | None = Query(default=None, alias="cluster_id"),
    cluster: str | None = Query(default=None),
    operation_id: str | None = Query(default=None, alias="operation_id"),
    operation: str | None = Query(default=None),
    include_deleted: bool = Query(default=False),
):
    target_cluster = cluster_id or cluster or ""
    target_operation = operation_id or operation or ""
    resources = await _inventory_svc.list_managed_resources(
        None,
        cluster_id=target_cluster,
        operation_id=target_operation,
        active_only=not include_deleted,
    )
    return [_resource_to_info(r) for r in resources]
