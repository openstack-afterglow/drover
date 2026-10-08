"""k3s 노드그룹 API — /api/k3s/clusters/{cluster_id}/nodegroups"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from openstack.exceptions import ResourceNotFound
from sqlalchemy.exc import InterfaceError, OperationalError

from drover.api.delegated import admitted_operation
from drover.auth import get_os_conn, get_token_info
from drover.db import is_db_available
from drover.models.schemas import CreateK3sNodegroupRequest, K3sNodegroupInfo, UpdateK3sNodegroupRequest
from drover.policy import authorize
from drover.services import delegation as _delegation
from drover.services import nodegroup as _svc
from drover.services import resource_policies
from drover.services import store as k3s_db

router = APIRouter()
_logger = logging.getLogger(__name__)


async def _assert_cluster_access(cluster_id: str, token_info: dict, *, mutation: bool = False) -> dict:
    """Apply the cluster policy and project boundary before reading group state."""
    project_id = token_info.get("project_id") or ""
    authorize("drover:clusters:scale" if mutation else "drover:clusters:get", {"project_id": project_id}, token_info)
    if not is_db_available():
        raise HTTPException(status_code=503, detail="MariaDB unavailable")
    try:
        cluster = await k3s_db.get_cluster(project_id, cluster_id)
    except (RuntimeError, OperationalError, InterfaceError) as exc:
        raise HTTPException(status_code=503, detail="MariaDB unavailable") from exc
    if cluster is None:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다.")
    return cluster


async def _validate_resources(conn, updates: dict) -> None:
    for field in ("flavor_id", "image_id"):
        identifier = updates.get(field)
        if identifier is None:
            continue
        try:
            selection = await resource_policies.validate_nodegroup_resource(conn, field, identifier)
            if selection["id"] != identifier:
                raise resource_policies.ResourcePolicyValidationError("Use the resource ID, not its name")
        except (resource_policies.ResourcePolicyValidationError, ResourceNotFound) as exc:
            raise HTTPException(status_code=422, detail=f"Invalid {field}") from exc
        except Exception as exc:
            raise HTTPException(status_code=503, detail="OpenStack resource validation unavailable") from exc




@router.get("/{cluster_id}/nodegroups", response_model=list[K3sNodegroupInfo])
async def list_nodegroups(cluster_id: str, token_info: dict = Depends(get_token_info)):
    """클러스터의 노드그룹 목록 조회."""
    await _assert_cluster_access(cluster_id, token_info)
    try:
        return await _svc.list_nodegroups(cluster_id)
    except (RuntimeError, OperationalError, InterfaceError) as exc:
        raise HTTPException(status_code=503, detail="MariaDB unavailable") from exc


@router.get("/{cluster_id}/nodegroups/{nodegroup_id}", response_model=K3sNodegroupInfo)
async def get_nodegroup(cluster_id: str, nodegroup_id: str, token_info: dict = Depends(get_token_info)):
    """노드그룹 단건 조회."""
    await _assert_cluster_access(cluster_id, token_info)
    try:
        ng = await _svc.get_nodegroup(cluster_id, nodegroup_id)
    except (RuntimeError, OperationalError, InterfaceError) as exc:
        raise HTTPException(status_code=503, detail="MariaDB unavailable") from exc
    if not ng:
        raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
    return ng


@router.post("/{cluster_id}/nodegroups", response_model=K3sNodegroupInfo, status_code=201)
async def create_nodegroup(
    cluster_id: str,
    req: CreateK3sNodegroupRequest,
    token_info: dict = Depends(get_token_info),
    conn=Depends(get_os_conn),
):
    """Configure an agent group and enqueue its requested initial capacity."""
    await _assert_cluster_access(cluster_id, token_info, mutation=True)
    data = req.model_dump()
    await _validate_resources(conn, data)
    project_id = token_info.get("project_id") or ""
    try:
        if int(data.get("node_count") or 0) > 0:
            async with admitted_operation(
                token_info, project_id=project_id, cluster_id=cluster_id, action=_delegation.ACTION_SCALE
            ) as admitted:
                return await _svc.create_nodegroup(
                    cluster_id, data, project_id=project_id, user_id=token_info.get("user_id"),
                    username=token_info.get("username"), delegation=admitted,
                )
        return await _svc.create_nodegroup(
            cluster_id, data, project_id=project_id,
            user_id=token_info.get("user_id"), username=token_info.get("username"),
        )
    except _svc.NodegroupConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (RuntimeError, OperationalError, InterfaceError) as exc:
        raise HTTPException(status_code=503, detail="Nodegroup mutation storage unavailable") from exc


@router.patch("/{cluster_id}/nodegroups/{nodegroup_id}", response_model=K3sNodegroupInfo)
async def update_nodegroup(
    cluster_id: str,
    nodegroup_id: str,
    req: UpdateK3sNodegroupRequest,
    token_info: dict = Depends(get_token_info),
    conn=Depends(get_os_conn),
):
    """Merge config against locked DB state, then enqueue manual sizing work."""
    await _assert_cluster_access(cluster_id, token_info, mutation=True)
    updates = {k: v for k, v in req.model_dump(exclude_unset=True).items() if v is not None}
    project_id = token_info.get("project_id") or ""
    try:
        before = await _svc.get_nodegroup(cluster_id, nodegroup_id)
        if not before:
            raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
        await _validate_resources(conn, updates)
        if "node_count" in updates:
            async with admitted_operation(
                token_info, project_id=project_id, cluster_id=cluster_id, action=_delegation.ACTION_SCALE
            ) as admitted:
                ng = await _svc.update_nodegroup(
                    cluster_id, nodegroup_id, updates, project_id=project_id,
                    user_id=token_info.get("user_id"), username=token_info.get("username"), delegation=admitted,
                )
        else:
            ng = await _svc.update_nodegroup(
                cluster_id, nodegroup_id, updates, project_id=project_id,
                user_id=token_info.get("user_id"), username=token_info.get("username"),
            )
        if not ng:
            raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
        return ng
    except _svc.NodegroupConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (RuntimeError, OperationalError, InterfaceError) as exc:
        raise HTTPException(status_code=503, detail="Nodegroup mutation storage unavailable") from exc


@router.delete("/{cluster_id}/nodegroups/{nodegroup_id}", status_code=204)
async def delete_nodegroup(cluster_id: str, nodegroup_id: str, token_info: dict = Depends(get_token_info)):
    """Delete a non-default group through the durable worker, not while scaling."""
    authorize("drover:clusters:delete", {"project_id": token_info.get("project_id") or ""}, token_info)
    await _assert_cluster_access(cluster_id, token_info)
    project_id = token_info.get("project_id") or ""
    try:
        async with admitted_operation(
            token_info, project_id=project_id, cluster_id=cluster_id, action=_delegation.ACTION_DELETE
        ) as admitted:
            deleted = await _svc.enqueue_nodegroup_delete(
                cluster_id, nodegroup_id, project_id=project_id,
                user_id=token_info.get("user_id"), username=token_info.get("username"), delegation=admitted,
            )
        if not deleted:
            raise HTTPException(status_code=404, detail="노드그룹을 찾을 수 없습니다.")
    except _svc.NodegroupConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (RuntimeError, OperationalError, InterfaceError) as exc:
        raise HTTPException(status_code=503, detail="Nodegroup mutation storage unavailable") from exc
