"""k3s 클러스터 템플릿 API."""

import logging

from fastapi import APIRouter, Depends, HTTPException

from drover.auth import get_token_info
from drover.models.schemas import (
    CreateK3sClusterTemplateRequest,
    K3sClusterTemplateInfo,
    UpdateK3sClusterTemplateRequest,
)
from drover.policy import require_policy
from drover.services import template as _svc

router = APIRouter()
_logger = logging.getLogger(__name__)


@router.get("", response_model=list[K3sClusterTemplateInfo])
async def list_k3s_cluster_templates(token_info: dict = Depends(get_token_info)):
    """사용자용 템플릿 목록 (public 또는 본인 생성)."""
    user_id = token_info.get("user_id") or token_info.get("sub")
    return await _svc.list_templates(user_id=user_id, admin=False)


@router.get("/{template_id}", response_model=K3sClusterTemplateInfo)
async def get_k3s_cluster_template(template_id: str, token_info: dict = Depends(get_token_info)):
    t = await _svc.get_template(template_id)
    if not t:
        raise HTTPException(status_code=404, detail="템플릿을 찾을 수 없습니다.")
    user_id = token_info.get("user_id") or token_info.get("sub")
    if not t["public_visible"] and t.get("created_by") != user_id:
        raise HTTPException(status_code=404, detail="템플릿을 찾을 수 없습니다.")
    return t


@router.post("", response_model=K3sClusterTemplateInfo, status_code=201, dependencies=[Depends(require_policy("drover:templates:manage"))])
async def create_k3s_cluster_template(req: CreateK3sClusterTemplateRequest, token_info: dict = Depends(get_token_info)):
    """admin 전용: 템플릿 생성."""
    user_id = token_info.get("user_id") or token_info.get("sub") or ""
    try:
        return await _svc.create_template(req.model_dump(), created_by=user_id)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.patch("/{template_id}", response_model=K3sClusterTemplateInfo, dependencies=[Depends(require_policy("drover:templates:manage"))])
async def update_k3s_cluster_template(template_id: str, req: UpdateK3sClusterTemplateRequest):
    """admin 전용: 템플릿 수정."""
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    t = await _svc.update_template(template_id, updates)
    if not t:
        raise HTTPException(status_code=404, detail="템플릿을 찾을 수 없습니다.")
    return t


@router.delete("/{template_id}", status_code=204, dependencies=[Depends(require_policy("drover:templates:manage"))])
async def delete_k3s_cluster_template(template_id: str):
    """admin 전용: 템플릿 삭제 (soft delete)."""
    deleted = await _svc.delete_template(template_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="템플릿을 찾을 수 없습니다.")
