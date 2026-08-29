"""k3s 클러스터 Pod 조회·삭제·로그 엔드포인트."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from fastapi import APIRouter, Depends, Query

from drover.auth import get_os_conn, get_token_info
from drover.models.schemas import PodInfo, PodLogResponse
from drover.services import kube as k3s_kube
from drover.services import store as k3s_cluster
from drover.services.activity import rec

router = APIRouter()


def _check_cluster(cluster):
    from fastapi import HTTPException

    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")


@router.get("/{cluster_id}/namespaces/{namespace}/pods")
async def list_pods(
    cluster_id: str,
    namespace: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    items = await k3s_kube.list_pods(cluster_id, namespace, project_id=project_id)
    return [PodInfo(**item) for item in items]


@router.delete("/{cluster_id}/namespaces/{namespace}/pods/{name}", status_code=204)
async def delete_pod(
    cluster_id: str,
    namespace: str,
    name: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    await k3s_kube.delete_pod(cluster_id, namespace, name, project_id=project_id)
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.pod.delete",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name},
    )


@router.get("/{cluster_id}/namespaces/{namespace}/pods/{name}/log")
async def get_pod_log(
    cluster_id: str,
    namespace: str,
    name: str,
    container: str | None = Query(default=None),
    tail_lines: int = Query(default=200, ge=1, le=10000),
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    log = await k3s_kube.get_pod_log(
        cluster_id,
        namespace,
        name,
        container=container,
        tail_lines=tail_lines,
        project_id=project_id,
    )
    return PodLogResponse(name=name, namespace=namespace, container=container, log=log)
