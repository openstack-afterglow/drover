"""k3s 클러스터 Service 조회·삭제 엔드포인트."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from fastapi import APIRouter, Depends

from drover.auth import get_os_conn, get_token_info
from drover.models.schemas import ServiceInfo
from drover.services import kube as k3s_kube
from drover.services import store as k3s_cluster
from drover.services.activity import rec

router = APIRouter()


def _check_cluster(cluster):
    from fastapi import HTTPException

    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")


@router.get("/{cluster_id}/namespaces/{namespace}/services")
async def list_services(
    cluster_id: str,
    namespace: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    items = await k3s_kube.list_services(cluster_id, namespace, project_id=project_id)
    return [ServiceInfo(**item) for item in items]


@router.delete("/{cluster_id}/namespaces/{namespace}/services/{name}", status_code=204)
async def delete_service(
    cluster_id: str,
    namespace: str,
    name: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    await k3s_kube.delete_service(cluster_id, namespace, name, project_id=project_id)
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.service.delete",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name},
    )
