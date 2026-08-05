"""k3s 노드그룹 API — /api/k3s/clusters/{cluster_id}/nodegroups"""

import logging

from fastapi import APIRouter, Depends, HTTPException

from drover.auth import get_token_info
from drover.models.schemas import CreateK3sNodegroupRequest, K3sNodegroupInfo, UpdateK3sNodegroupRequest
from drover.services import store as k3s_db
from drover.services import nodegroup as _svc
from drover.services import jobs as _jobs

router = APIRouter()
_logger = logging.getLogger(__name__)


async def _assert_cluster_access(cluster_id: str, token_info: dict) -> None:
    """클러스터가 현재 프로젝트에 속하는지 확인."""
    project_id = token_info.get("project_id") or ""
    cluster = await k3s_db.get_cluster(project_id, cluster_id)
    if cluster is None:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다.")




@router.get("/{cluster_id}/nodegroups", response_model=list[K3sNodegroupInfo])
async def list_nodegroups(cluster_id: str, token_info: dict = Depends(get_token_info)):
    """클러스터의 노드그룹 목록 조회."""
    await _assert_cluster_access(cluster_id, token_info)
    return await _svc.list_nodegroups(cluster_id)


@router.get("/{cluster_id}/nodegroups/{nodegroup_id}", response_model=K3sNodegroupInfo)
async def get_nodegroup(cluster_id: str, nodegroup_id: str, token_info: dict = Depends(get_token_info)):
    """노드그룹 단건 조회."""
    await _assert_cluster_access(cluster_id, token_info)
    ng = await _svc.get_nodegroup(cluster_id, nodegroup_id)
    if not ng:
        raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
    return ng


@router.post("/{cluster_id}/nodegroups", response_model=K3sNodegroupInfo, status_code=201)
async def create_nodegroup(
    cluster_id: str,
    req: CreateK3sNodegroupRequest,
    token_info: dict = Depends(get_token_info),
):
    """노드그룹 생성. agent 그룹은 node_count > 0이면 VM 프로비저닝을 시작한다."""
    await _assert_cluster_access(cluster_id, token_info)
    try:
        ng = await _svc.create_nodegroup(cluster_id, req.model_dump())
        if ng["role"] == "agent" and ng.get("node_count", 0) > 0:
            await _jobs.enqueue_job(
                cluster_id=cluster_id,
                project_id=token_info.get("project_id") or "",
                kind="nodegroup_reconcile",
                payload={
                    "action": "provision",
                    "nodegroup": ng,
                    "add_count": int(ng.get("node_count", 0)),
                },
                user_id=token_info.get("user_id"),
                username=token_info.get("username"),
            )
        return ng
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.patch("/{cluster_id}/nodegroups/{nodegroup_id}", response_model=K3sNodegroupInfo)
async def update_nodegroup(
    cluster_id: str,
    nodegroup_id: str,
    req: UpdateK3sNodegroupRequest,
    token_info: dict = Depends(get_token_info),
):
    """노드그룹 수정. agent node_count 변경은 VM 프로비저닝/삭제를 시작한다."""
    await _assert_cluster_access(cluster_id, token_info)
    before = await _svc.get_nodegroup(cluster_id, nodegroup_id)
    if not before:
        raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if before.get("role") == "server" and "node_count" in updates and updates["node_count"] != before.get("node_count"):
        raise HTTPException(status_code=422, detail="server 노드그룹 node_count 변경은 아직 지원되지 않습니다.")
    try:
        ng = await _svc.update_nodegroup(cluster_id, nodegroup_id, updates)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    if not ng:
        raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
    if ng["role"] == "agent" and "node_count" in updates:
        desired = int(ng.get("node_count", 0))
        current = len(before.get("vms") or [])
        project_id = token_info.get("project_id") or ""
        if desired > current:
            await _jobs.enqueue_job(
                cluster_id=cluster_id,
                project_id=project_id,
                kind="nodegroup_reconcile",
                payload={"action": "provision", "nodegroup": ng, "add_count": desired - current},
                user_id=token_info.get("user_id"),
                username=token_info.get("username"),
            )
        elif desired < current:
            remove_entries = list(reversed(before.get("vms") or []))[: current - desired]
            await _jobs.enqueue_job(
                cluster_id=cluster_id,
                project_id=project_id,
                kind="nodegroup_reconcile",
                payload={"action": "delete_vms", "nodegroup": ng, "remove_entries": remove_entries},
                user_id=token_info.get("user_id"),
                username=token_info.get("username"),
            )
    return ng


@router.delete("/{cluster_id}/nodegroups/{nodegroup_id}", status_code=204)
async def delete_nodegroup(
    cluster_id: str,
    nodegroup_id: str,
    token_info: dict = Depends(get_token_info),
):
    """Delete a non-default nodegroup and its VMs through the durable worker."""
    await _assert_cluster_access(cluster_id, token_info)
    nodegroup = await _svc.get_nodegroup(cluster_id, nodegroup_id)
    if not nodegroup:
        raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
    if nodegroup.get("is_default"):
        raise HTTPException(
            status_code=422,
            detail="기본 노드그룹(default-server / default-agent)은 삭제할 수 없습니다.",
        )
    await _jobs.enqueue_job(
        cluster_id=cluster_id,
        project_id=token_info.get("project_id") or "",
        kind="nodegroup_reconcile",
        payload={
            "action": "delete_group",
            "nodegroup": nodegroup,
            "remove_entries": nodegroup.get("vms") or [],
        },
        user_id=token_info.get("user_id"),
        username=token_info.get("username"),
    )
