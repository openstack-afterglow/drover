"""k3s 워크로드(Pod·Service·Deployment·ReplicaSet) 엔드포인트 + 헬퍼 단위 테스트."""

from unittest.mock import ANY, AsyncMock, patch

import pytest

from drover.services.kube import (
    _deploy_from_k8s,
    _pod_from_k8s,
    _rs_from_k8s,
    _svc_from_k8s,
)

# ---------------------------------------------------------------------------
# 공통 fixture 헬퍼
# ---------------------------------------------------------------------------


def _cluster():
    return {
        "id": "k3s-1",
        "name": "mycluster",
        "status": "ACTIVE",
        "status_reason": None,
        "server_vm_id": "vm-1",
        "agent_vm_ids": [],
        "agent_count": 0,
        "api_address": None,
        "server_ip": "10.0.0.1",
        "network_id": "net-1",
        "key_name": None,
        "k3s_version": "v1.31.4+k3s1",
        "server_vm_name": "mycluster-server",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }


def _k8s_pod(name="mypod", phase="Running", ready=True, restarts=3):
    state = (
        {"running": {"startedAt": "2024-01-01T00:00:00Z"}} if ready else {"waiting": {"reason": "ContainerCreating"}}
    )
    return {
        "metadata": {
            "name": name,
            "namespace": "default",
            "creationTimestamp": "2024-01-01T00:00:00Z",
            "labels": {"app": "myapp"},
        },
        "spec": {
            "nodeName": "node-1",
            "containers": [{"name": "nginx", "image": "nginx:latest"}],
        },
        "status": {
            "phase": phase,
            "podIP": "10.244.0.5",
            "containerStatuses": [
                {
                    "name": "nginx",
                    "ready": ready,
                    "restartCount": restarts,
                    "state": state,
                }
            ],
        },
    }


def _k8s_service(name="mysvc", svc_type="ClusterIP"):
    return {
        "metadata": {"name": name, "namespace": "default", "creationTimestamp": "2024-01-01T00:00:00Z"},
        "spec": {
            "type": svc_type,
            "clusterIP": "10.96.0.10",
            "ports": [
                {"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"},
                {"name": "https", "port": 443, "targetPort": 8443, "nodePort": 30443, "protocol": "TCP"},
            ],
            "selector": {"app": "myapp"},
        },
    }


def _k8s_deployment(name="mydeploy", replicas=3, available=2, ready=2):
    return {
        "metadata": {"name": name, "namespace": "default", "creationTimestamp": "2024-01-01T00:00:00Z"},
        "spec": {
            "replicas": replicas,
            "strategy": {"type": "RollingUpdate"},
            "selector": {"matchLabels": {"app": "myapp"}},
            "template": {"spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]}},
        },
        "status": {
            "availableReplicas": available,
            "readyReplicas": ready,
            "updatedReplicas": replicas,
        },
    }


def _k8s_replicaset(name="mydeploy-abc", replicas=3, ready=2, owner="Deployment", owner_name="mydeploy"):
    return {
        "metadata": {
            "name": name,
            "namespace": "default",
            "creationTimestamp": "2024-01-01T00:00:00Z",
            "ownerReferences": [{"kind": owner, "name": owner_name, "apiVersion": "apps/v1"}],
        },
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": "myapp"}},
            "template": {"spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]}},
        },
        "status": {"readyReplicas": ready, "availableReplicas": ready},
    }


# ---------------------------------------------------------------------------
# 헬퍼 단위 테스트
# ---------------------------------------------------------------------------


def test_list_pods_normalizes_status():
    """Pending/Running 혼합 응답 → PodInfo 매핑 (ready, restarts 합산)."""
    running = _pod_from_k8s(_k8s_pod("pod-run", phase="Running", ready=True, restarts=2))
    pending = _pod_from_k8s(_k8s_pod("pod-pend", phase="Pending", ready=False, restarts=0))

    assert running["phase"] == "Running"
    assert running["ready"] == "1/1"
    assert running["restarts"] == 2
    assert running["containers"][0]["state"] == "running"

    assert pending["phase"] == "Pending"
    assert pending["ready"] == "0/1"
    assert pending["restarts"] == 0
    assert pending["containers"][0]["state"] == "waiting"


def test_list_services_extracts_ports():
    """multi-port service → ServicePort 매핑."""
    svc = _svc_from_k8s(_k8s_service("my-svc", svc_type="NodePort"))
    assert svc["type"] == "NodePort"
    assert svc["cluster_ip"] == "10.96.0.10"
    assert len(svc["ports"]) == 2
    assert svc["ports"][0]["port"] == 80
    assert svc["ports"][1]["node_port"] == 30443
    assert svc["selector"] == {"app": "myapp"}


def test_list_deployments_counts():
    """replicas/ready/available 추출."""
    dep = _deploy_from_k8s(_k8s_deployment("dep", replicas=3, available=2, ready=2))
    assert dep["replicas"] == 3
    assert dep["available"] == 2
    assert dep["ready"] == 2
    assert dep["images"] == ["nginx:latest"]
    assert dep["selector"] == {"app": "myapp"}


def test_list_replicasets_includes_owner():
    """ownerReferences → owner_kind/owner_name 추출."""
    rs = _rs_from_k8s(_k8s_replicaset("rs-abc", owner="Deployment", owner_name="mydeploy"))
    assert rs["owner_kind"] == "Deployment"
    assert rs["owner_name"] == "mydeploy"
    assert rs["ready"] == 2
    assert rs["images"] == ["nginx:latest"]


# ---------------------------------------------------------------------------
# 라우터 엔드포인트 테스트
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_pods_endpoint(client):
    """Pod 목록 엔드포인트 정상 응답."""
    pods = [_pod_from_k8s(_k8s_pod("pod-1")), _pod_from_k8s(_k8s_pod("pod-2"))]
    with (
        patch("drover.api.pods.k3s_cluster") as mc,
        patch("drover.api.pods.k3s_kube") as mk,
    ):
        mc.get_cluster = AsyncMock(return_value=_cluster())
        mk.list_pods = AsyncMock(return_value=pods)
        resp = await client.get("/v1/clusters/k3s-1/namespaces/default/pods")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "pod-1"


@pytest.mark.asyncio
async def test_delete_pod_idempotent_404(client):
    """Pod 삭제 — k3s_kube.delete_pod 404 무시(idempotent)."""
    with (
        patch("drover.api.pods.k3s_cluster") as mc,
        patch("drover.api.pods.k3s_kube") as mk,
        patch("drover.api.pods.rec", new_callable=AsyncMock),
    ):
        mc.get_cluster = AsyncMock(return_value=_cluster())
        mk.delete_pod = AsyncMock(return_value=None)
        resp = await client.delete("/v1/clusters/k3s-1/namespaces/default/pods/mypod")
    assert resp.status_code == 204
    mk.delete_pod.assert_called_once_with("k3s-1", "default", "mypod", project_id=ANY)


@pytest.mark.asyncio
async def test_get_pod_log_passes_tail_lines(client):
    """Pod 로그 — tail_lines query param 전달 확인."""
    with (
        patch("drover.api.pods.k3s_cluster") as mc,
        patch("drover.api.pods.k3s_kube") as mk,
    ):
        mc.get_cluster = AsyncMock(return_value=_cluster())
        mk.get_pod_log = AsyncMock(return_value="line1\nline2\n")
        resp = await client.get("/v1/clusters/k3s-1/namespaces/default/pods/mypod/log?tail_lines=50")
    assert resp.status_code == 200
    body = resp.json()
    assert "line1" in body["log"]
    mk.get_pod_log.assert_called_once_with(
        "k3s-1",
        "default",
        "mypod",
        container=None,
        tail_lines=50,
        project_id=ANY,
    )


@pytest.mark.asyncio
async def test_pods_endpoint_requires_project_match(client):
    """다른 project 의 cluster 접근 시 404 (_check_cluster 동작)."""
    with (
        patch("drover.api.pods.k3s_cluster") as mc,
        patch("drover.api.pods.k3s_kube"),
    ):
        mc.get_cluster = AsyncMock(return_value=None)
        resp = await client.get("/v1/clusters/other-cluster/namespaces/default/pods")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_restart_deployment_patches_annotation(client):
    """restart_deployment — k3s_kube.restart_deployment 호출 확인."""
    dep_result = _deploy_from_k8s(_k8s_deployment("mydeploy"))
    with (
        patch("drover.api.workloads.k3s_cluster") as mc,
        patch("drover.api.workloads.k3s_kube") as mk,
        patch("drover.api.workloads.rec", new_callable=AsyncMock),
    ):
        mc.get_cluster = AsyncMock(return_value=_cluster())
        mk.restart_deployment = AsyncMock(return_value=dep_result)
        resp = await client.post("/v1/clusters/k3s-1/namespaces/default/deployments/mydeploy/restart")
    assert resp.status_code == 200
    mk.restart_deployment.assert_called_once_with("k3s-1", "default", "mydeploy", project_id=ANY)
    assert resp.json()["name"] == "mydeploy"


@pytest.mark.asyncio
async def test_scale_deployment_zero_allowed(client):
    """replicas=0 허용."""
    dep_result = _deploy_from_k8s(_k8s_deployment("mydeploy", replicas=0, available=0, ready=0))
    with (
        patch("drover.api.workloads.k3s_cluster") as mc,
        patch("drover.api.workloads.k3s_kube") as mk,
        patch("drover.api.workloads.rec", new_callable=AsyncMock),
    ):
        mc.get_cluster = AsyncMock(return_value=_cluster())
        mk.scale_deployment = AsyncMock(return_value=dep_result)
        resp = await client.patch(
            "/v1/clusters/k3s-1/namespaces/default/deployments/mydeploy/scale",
            json={"replicas": 0},
        )
    assert resp.status_code == 200
    mk.scale_deployment.assert_called_once_with("k3s-1", "default", "mydeploy", 0, project_id=ANY)


@pytest.mark.asyncio
async def test_scale_deployment_negative_rejected(client):
    """replicas=-1 은 422 (Pydantic validation)."""
    with (
        patch("drover.api.workloads.k3s_cluster") as mc,
        patch("drover.api.workloads.k3s_kube"),
    ):
        mc.get_cluster = AsyncMock(return_value=_cluster())
        resp = await client.patch(
            "/v1/clusters/k3s-1/namespaces/default/deployments/mydeploy/scale",
            json={"replicas": -1},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_list_services_endpoint(client):
    """Service 목록 엔드포인트 정상 응답."""
    svcs = [_svc_from_k8s(_k8s_service("svc-1")), _svc_from_k8s(_k8s_service("svc-2"))]
    with (
        patch("drover.api.k3s_services.k3s_cluster") as mc,
        patch("drover.api.k3s_services.k3s_kube") as mk,
    ):
        mc.get_cluster = AsyncMock(return_value=_cluster())
        mk.list_services = AsyncMock(return_value=svcs)
        resp = await client.get("/v1/clusters/k3s-1/namespaces/default/services")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "svc-1"
