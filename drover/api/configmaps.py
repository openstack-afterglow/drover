"""k3s 클러스터 ConfigMap CRUD 엔드포인트."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from fastapi import APIRouter, Depends, HTTPException, Query

from drover.auth import get_os_conn, get_token_info, require_token
from drover.models.schemas import ConfigMapCreateRequest, ConfigMapInfo, ConfigMapWriteRequest
from drover.policy import authorize, require_inventory, require_workload_access
from drover.services import kube as k3s_kube
from drover.services import store as k3s_cluster
from drover.services.activity import rec
from drover.services.cache import invalidate

router = APIRouter()


def _check_cluster(cluster):
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")


@router.get("/{cluster_id}/namespaces", dependencies=[Depends(require_inventory)])
async def list_namespaces(
    cluster_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(require_token),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    scope = {"project_id": project_id}
    if not authorize("drover:access:admin", scope, token_info, do_raise=False) and authorize(
        "drover:workloads:write", scope, token_info, do_raise=False
    ):
        from drover.services.credentials import ensure_workload_namespace

        return [await ensure_workload_namespace(cluster_id, token_info)]
    return await k3s_kube.list_namespaces(cluster_id, project_id=project_id)


@router.get("/{cluster_id}/configmaps", dependencies=[Depends(require_workload_access)])
async def list_configmaps(
    cluster_id: str,
    namespace: str = Query(default="default"),
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    items = await k3s_kube.list_configmaps(cluster_id, namespace, project_id=project_id)
    return [ConfigMapInfo(**item) for item in items]


@router.get("/{cluster_id}/namespaces/{namespace}/configmaps/{name}", dependencies=[Depends(require_workload_access)])
async def get_configmap(
    cluster_id: str,
    namespace: str,
    name: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    item = await k3s_kube.get_configmap(cluster_id, namespace, name, project_id=project_id)
    return ConfigMapInfo(**item)


@router.post("/{cluster_id}/namespaces/{namespace}/configmaps", status_code=201, dependencies=[Depends(require_workload_access)])
async def create_configmap(
    cluster_id: str,
    namespace: str,
    body: ConfigMapCreateRequest,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    item = await k3s_kube.create_configmap(
        cluster_id,
        namespace,
        body.name,
        body.data,
        labels=body.labels,
        annotations=body.annotations,
        project_id=project_id,
    )
    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}:cm:{namespace}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.configmap.create",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": body.name},
    )
    return ConfigMapInfo(**item)


@router.put("/{cluster_id}/namespaces/{namespace}/configmaps/{name}", dependencies=[Depends(require_workload_access)])
async def update_configmap(
    cluster_id: str,
    namespace: str,
    name: str,
    body: ConfigMapWriteRequest,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    item = await k3s_kube.update_configmap(
        cluster_id,
        namespace,
        name,
        body.data,
        labels=body.labels,
        annotations=body.annotations,
        project_id=project_id,
    )
    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}:cm:{namespace}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.configmap.update",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name},
    )
    return ConfigMapInfo(**item)


@router.delete("/{cluster_id}/namespaces/{namespace}/configmaps/{name}", status_code=204, dependencies=[Depends(require_workload_access)])
async def delete_configmap(
    cluster_id: str,
    namespace: str,
    name: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    await k3s_kube.delete_configmap(cluster_id, namespace, name, project_id=project_id)
    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}:cm:{namespace}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.configmap.delete",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name},
    )
