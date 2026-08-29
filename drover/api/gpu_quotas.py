"""Tenant and admin GPU quota API endpoints for Drover."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from drover.auth import get_os_conn, require_token
from drover.policy import require_policy
from drover.services.gpu_quota import (
    DEFAULT_PROJECT_ID,
    check_gpu_quota,
    delete_project_gpu_quota,
    get_effective_gpu_quotas,
    get_project_gpu_quotas,
    get_project_gpu_usage,
    set_project_gpu_quota,
)

if TYPE_CHECKING:
    import openstack

tenant_router = APIRouter()
admin_router = APIRouter(dependencies=[Depends(require_policy("drover:admin"))])


class GpuQuotaRequest(BaseModel):
    gpu_type: str
    limit: int


class GpuQuotaCheckRequest(BaseModel):
    extra_specs: dict = Field(default_factory=dict)


@tenant_router.get("/effective")
async def get_my_effective_gpu_quotas(token_info: dict = Depends(require_token)):
    project_id = token_info["project_id"]
    try:
        return await get_effective_gpu_quotas(project_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@tenant_router.get("/status")
async def get_my_gpu_quota_status(
    token_info: dict = Depends(require_token),
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = token_info["project_id"]
    try:
        effective, usage = await asyncio.gather(
            get_effective_gpu_quotas(project_id),
            get_project_gpu_usage(conn, project_id),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="GPU quota status 조회 실패") from exc

    all_types = sorted(set(effective.keys()) | set(usage.keys()))
    result = []
    for alias in all_types:
        limit = effective.get(alias, 0)
        in_use = usage.get(alias, 0)
        result.append(
            {
                "gpu_type": alias,
                "limit": limit,
                "in_use": in_use,
                "available": (limit - in_use) if limit >= 0 else -1,
            }
        )
    return result


@tenant_router.post("/check")
async def check_my_gpu_quota(
    req: GpuQuotaCheckRequest,
    token_info: dict = Depends(require_token),
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = token_info["project_id"]
    try:
        ok, msg = await check_gpu_quota(conn, project_id, req.extra_specs)
        return {"ok": ok, "detail": msg}
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="GPU quota 검증 실패") from exc


# -----------------------------------------------------------------------------
# Admin GPU Quota Endpoints
# -----------------------------------------------------------------------------


@admin_router.get("/defaults")
async def get_default_gpu_quotas():
    try:
        quotas = await get_project_gpu_quotas(DEFAULT_PROJECT_ID)
        return [{"gpu_type": q["gpu_type"], "limit": q["limit"]} for q in quotas]
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@admin_router.put("/defaults")
async def set_default_gpu_quota(req: GpuQuotaRequest):
    try:
        return await set_project_gpu_quota(DEFAULT_PROJECT_ID, req.gpu_type, req.limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@admin_router.delete("/defaults/{gpu_type}", status_code=204)
async def delete_default_gpu_quota(gpu_type: str):
    try:
        await delete_project_gpu_quota(DEFAULT_PROJECT_ID, gpu_type)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@admin_router.get("/{project_id}")
async def get_project_gpu_quotas_admin(
    project_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    try:
        effective, usage = await asyncio.gather(
            get_effective_gpu_quotas(project_id),
            get_project_gpu_usage(conn, project_id),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="GPU quota 조회 실패") from exc

    all_types = sorted(set(effective.keys()) | set(usage.keys()))
    result = []
    for alias in all_types:
        limit = effective.get(alias, 0)
        in_use = usage.get(alias, 0)
        result.append(
            {
                "gpu_type": alias,
                "limit": limit,
                "in_use": in_use,
                "available": (limit - in_use) if limit >= 0 else -1,
            }
        )
    return result


@admin_router.put("/{project_id}")
async def set_project_gpu_quota_admin(project_id: str, req: GpuQuotaRequest):
    try:
        return await set_project_gpu_quota(project_id, req.gpu_type, req.limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@admin_router.delete("/{project_id}/{gpu_type}", status_code=204)
async def delete_project_gpu_quota_admin(project_id: str, gpu_type: str):
    try:
        await delete_project_gpu_quota(project_id, gpu_type)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
