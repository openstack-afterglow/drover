"""k3s 클러스터 Secret CRUD 엔드포인트.

주의: `rec` 의 extra 에는 Secret data 를 포함하지 않는다 (이름/namespace 만).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from fastapi import APIRouter, Depends, HTTPException, Query

from drover.auth import get_os_conn, get_token_info
from drover.models.schemas import SecretCreateRequest, SecretInfo, SecretWriteRequest
from drover.services import kube as k3s_kube
from drover.services import store as k3s_cluster
from drover.services.activity import rec
from drover.services.cache import invalidate

router = APIRouter()


def _check_cluster(cluster):
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")


@router.get("/{cluster_id}/secrets")
async def list_secrets(
    cluster_id: str,
    namespace: str = Query(default="default"),
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    items = await k3s_kube.list_secrets(cluster_id, namespace, project_id=project_id)
    return [SecretInfo(**item) for item in items]


@router.get("/{cluster_id}/namespaces/{namespace}/secrets/{name}")
async def get_secret(
    cluster_id: str,
    namespace: str,
    name: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    item = await k3s_kube.get_secret(cluster_id, namespace, name, project_id=project_id)
    return SecretInfo(**item)


@router.post("/{cluster_id}/namespaces/{namespace}/secrets", status_code=201)
async def create_secret(
    cluster_id: str,
    namespace: str,
    body: SecretCreateRequest,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    item = await k3s_kube.create_secret(
        cluster_id,
        namespace,
        body.name,
        body.data,
        secret_type=body.type,
        labels=body.labels,
        annotations=body.annotations,
        project_id=project_id,
    )
    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}:secret:{namespace}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.secret.create",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": body.name},
    )
    return SecretInfo(**item)


@router.put("/{cluster_id}/namespaces/{namespace}/secrets/{name}")
async def update_secret(
    cluster_id: str,
    namespace: str,
    name: str,
    body: SecretWriteRequest,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    item = await k3s_kube.update_secret(
        cluster_id,
        namespace,
        name,
        body.data,
        secret_type=body.type,
        labels=body.labels,
        annotations=body.annotations,
        project_id=project_id,
    )
    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}:secret:{namespace}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.secret.update",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name},
    )
    return SecretInfo(**item)


@router.delete("/{cluster_id}/namespaces/{namespace}/secrets/{name}", status_code=204)
async def delete_secret(
    cluster_id: str,
    namespace: str,
    name: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    _check_cluster(cluster)
    await k3s_kube.delete_secret(cluster_id, namespace, name, project_id=project_id)
    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}:secret:{namespace}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.secret.delete",
        status="success",
        resource_id=cluster_id,
        extra={"namespace": namespace, "name": name},
    )
