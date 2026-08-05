"""k3s 클러스터 Deployment·ReplicaSet 조회 및 액션 엔드포인트."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from fastapi import APIRouter, Depends

from drover.services.activity import rec
from drover.auth import get_os_conn, get_token_info
from drover.models.schemas import DeploymentInfo, ReplicaSetInfo, ScaleDeploymentRequest
from drover.services import store as k3s_cluster
from drover.services import kube as k3s_kube

router = APIRouter()


def _check_cluster(cluster):
    from fastapi import HTTPException

    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")


@router.get("/{cluster_id}/namespaces/{namespace}/deployments")
async def list_deployments(
    cluster_id: str,
    namespace: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    items = await k3s_kube.list_deployments(cluster_id, namespace, project_id=project_id)
    return [DeploymentInfo(**item) for item in items]


@router.get("/{cluster_id}/namespaces/{namespace}/replicasets")
async def list_replicasets(
    cluster_id: str,
    namespace: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    items = await k3s_kube.list_replicasets(cluster_id, namespace, project_id=project_id)
    return [ReplicaSetInfo(**item) for item in items]


@router.post("/{cluster_id}/namespaces/{namespace}/deployments/{name}/restart")
async def restart_deployment(
    cluster_id: str,
    namespace: str,
    name: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    result = await k3s_kube.restart_deployment(cluster_id, namespace, name, project_id=project_id)
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.deployment.restart",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name},
    )
    return DeploymentInfo(**result)


@router.patch("/{cluster_id}/namespaces/{namespace}/deployments/{name}/scale")
async def scale_deployment(
    cluster_id: str,
    namespace: str,
    name: str,
    body: ScaleDeploymentRequest,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    result = await k3s_kube.scale_deployment(cluster_id, namespace, name, body.replicas, project_id=project_id)
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.deployment.scale",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name, "replicas": body.replicas},
    )
    return DeploymentInfo(**result)
