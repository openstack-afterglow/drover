"""K8s API 직접 호출 유틸리티.

kubeconfig의 클라이언트 인증서를 사용해 K8s API 서버에 직접 요청.
노드 삭제 등 클러스터 관리 작업에 사용.
"""

import base64
import contextlib
import logging
import re
import ssl
import tempfile
from decimal import ROUND_CEILING, Decimal, localcontext

import httpx
import yaml

from drover.services import store as k3s_db
from drover.services.errors import K3sApiError

_logger = logging.getLogger(__name__)


def _parse_kubeconfig(kubeconfig_yaml: str) -> tuple[bytes, bytes, str]:
    """kubeconfig에서 (client_cert_pem, client_key_pem, server_url) 반환."""
    kc = yaml.safe_load(kubeconfig_yaml)
    user = kc["users"][0]["user"]
    cert_data = base64.b64decode(user["client-certificate-data"])
    key_data = base64.b64decode(user["client-key-data"])
    server_url = kc["clusters"][0]["cluster"]["server"]
    return cert_data, key_data, server_url


def _make_ssl_context(cert_pem: bytes, key_pem: bytes) -> ssl.SSLContext:
    """클라이언트 인증서로 SSLContext 생성 (K3s 자체 서명 인증서 허용)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with (
        tempfile.NamedTemporaryFile(suffix=".crt") as cf,
        tempfile.NamedTemporaryFile(suffix=".key") as kf,
    ):
        cf.write(cert_pem)
        cf.flush()
        kf.write(key_pem)
        kf.flush()
        ctx.load_cert_chain(cf.name, kf.name)
    return ctx


async def delete_k8s_node(cluster_id: str, node_name: str) -> bool:
    """K8s API로 노드 삭제.

    Returns:
        True — 성공 또는 이미 없음(404)
        False — 오류 발생 (kubeconfig 없음, 연결 실패 등)
    """
    try:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
        if not kubeconfig_yaml:
            _logger.warning("k3s_kube: kubeconfig 없음 (cluster=%s), 노드 삭제 스킵: %s", cluster_id, node_name)
            return False

        cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
        ssl_ctx = _make_ssl_context(cert_pem, key_pem)

        async with httpx.AsyncClient(verify=ssl_ctx, timeout=10.0) as client:
            resp = await client.delete(
                f"{server_url}/api/v1/nodes/{node_name}",
                headers={"Accept": "application/json"},
            )
            if resp.status_code in (200, 404):
                _logger.info("k3s_kube: node %s 삭제 완료 (status=%d)", node_name, resp.status_code)
                return True
            _logger.warning(
                "k3s_kube: node %s 삭제 실패: HTTP %d %s",
                node_name,
                resp.status_code,
                resp.text[:200],
            )
            return False
    except Exception as e:
        _logger.warning("k3s_kube: node %s 삭제 중 오류: %s", node_name, e)
        return False


async def delete_k8s_nodes(cluster_id: str, node_names: list[str]) -> None:
    """여러 노드 순차 삭제 (best-effort — 실패해도 계속 진행)."""
    for name in node_names:
        await delete_k8s_node(cluster_id, name)


# ---------------------------------------------------------------------------
# K8s API 공통 클라이언트 + 헬퍼
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _kube_client(cluster_id: str, *, project_id: str | None = None):
    """K8s API 클라이언트 컨텍스트 매니저.

    project_id 가 주어지면 멀티테넌시 격리를 위해 `get_kubeconfig` 사용,
    없으면 관리자/내부 작업용 `get_kubeconfig_admin` 사용.
    """
    if project_id is not None:
        kubeconfig_yaml = await k3s_db.get_kubeconfig(project_id=project_id, cluster_id=cluster_id)
    else:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
    if not kubeconfig_yaml:
        raise K3sApiError(502, "kubeconfig 를 찾을 수 없습니다 (클러스터 미준비)")
    cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
    ssl_ctx = _make_ssl_context(cert_pem, key_pem)
    async with httpx.AsyncClient(verify=ssl_ctx, timeout=15.0) as client:
        yield client, server_url


def _raise_k8s_error(resp: httpx.Response, context: str) -> None:
    """K8s API 비정상 응답을 K3sApiError(502) 으로 정규화."""
    try:
        body = resp.json()
        detail = body.get("message") or body.get("reason") or resp.text
    except Exception:
        detail = resp.text[:500]
    _logger.warning("k3s_kube: %s 실패 (status=%d): %s", context, resp.status_code, detail)
    raise K3sApiError(502, f"K8s API {context} 실패: {detail}")


def _cm_from_k8s(item: dict) -> dict:
    meta = item.get("metadata", {})
    return {
        "name": meta.get("name", ""),
        "namespace": meta.get("namespace", ""),
        "data": item.get("data") or {},
        "binary_data": item.get("binaryData"),
        "labels": meta.get("labels") or {},
        "annotations": meta.get("annotations") or {},
        "created_at": meta.get("creationTimestamp", "") or "",
    }


def _secret_from_k8s(item: dict) -> dict:
    meta = item.get("metadata", {})
    return {
        "name": meta.get("name", ""),
        "namespace": meta.get("namespace", ""),
        "type": item.get("type", "Opaque"),
        "data": item.get("data") or {},
        "labels": meta.get("labels") or {},
        "annotations": meta.get("annotations") or {},
        "created_at": meta.get("creationTimestamp", "") or "",
    }


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


async def list_namespaces(cluster_id: str, *, project_id: str) -> list[str]:
    """클러스터의 네임스페이스 이름 목록."""
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/api/v1/namespaces", headers={"Accept": "application/json"})
        if resp.status_code != 200:
            _raise_k8s_error(resp, "namespace 목록 조회")
        items = resp.json().get("items", [])
        return [it.get("metadata", {}).get("name", "") for it in items if it.get("metadata", {}).get("name")]


# ---------------------------------------------------------------------------
# ConfigMap
# ---------------------------------------------------------------------------


async def list_configmaps(cluster_id: str, namespace: str, *, project_id: str) -> list[dict]:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(
            f"{server_url}/api/v1/namespaces/{namespace}/configmaps",
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            _raise_k8s_error(resp, "ConfigMap 목록 조회")
        return [_cm_from_k8s(it) for it in resp.json().get("items", [])]


async def get_configmap(cluster_id: str, namespace: str, name: str, *, project_id: str) -> dict:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(
            f"{server_url}/api/v1/namespaces/{namespace}/configmaps/{name}",
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 404:
            raise K3sApiError(404, f"ConfigMap {namespace}/{name} 을 찾을 수 없습니다")
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"ConfigMap {namespace}/{name} 조회")
        return _cm_from_k8s(resp.json())


async def create_configmap(
    cluster_id: str,
    namespace: str,
    name: str,
    data: dict[str, str],
    *,
    labels: dict | None = None,
    annotations: dict | None = None,
    project_id: str,
) -> dict:
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels or {},
            "annotations": annotations or {},
        },
        "data": data or {},
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.post(
            f"{server_url}/api/v1/namespaces/{namespace}/configmaps",
            json=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"ConfigMap {namespace}/{name} 생성")
        return _cm_from_k8s(resp.json())


async def update_configmap(
    cluster_id: str,
    namespace: str,
    name: str,
    data: dict[str, str],
    *,
    labels: dict | None = None,
    annotations: dict | None = None,
    project_id: str,
) -> dict:
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels or {},
            "annotations": annotations or {},
        },
        "data": data or {},
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.put(
            f"{server_url}/api/v1/namespaces/{namespace}/configmaps/{name}",
            json=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        if resp.status_code == 404:
            raise K3sApiError(404, f"ConfigMap {namespace}/{name} 을 찾을 수 없습니다")
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"ConfigMap {namespace}/{name} 업데이트")
        return _cm_from_k8s(resp.json())


async def delete_configmap(cluster_id: str, namespace: str, name: str, *, project_id: str) -> None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.delete(
            f"{server_url}/api/v1/namespaces/{namespace}/configmaps/{name}",
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 404:
            return  # 이미 없음 — idempotent 처리
        if resp.status_code not in (200, 202):
            _raise_k8s_error(resp, f"ConfigMap {namespace}/{name} 삭제")


# ---------------------------------------------------------------------------
# Secret
# ---------------------------------------------------------------------------


def _encode_secret_data(data: dict[str, str]) -> dict[str, str]:
    """Secret data 값을 base64 인코딩 (K8s API 가 요구하는 형식)."""
    return {k: base64.b64encode(v.encode()).decode() for k, v in (data or {}).items()}


async def list_secrets(cluster_id: str, namespace: str, *, project_id: str) -> list[dict]:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(
            f"{server_url}/api/v1/namespaces/{namespace}/secrets",
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            _raise_k8s_error(resp, "Secret 목록 조회")
        return [_secret_from_k8s(it) for it in resp.json().get("items", [])]


async def get_secret(cluster_id: str, namespace: str, name: str, *, project_id: str) -> dict:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(
            f"{server_url}/api/v1/namespaces/{namespace}/secrets/{name}",
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 404:
            raise K3sApiError(404, f"Secret {namespace}/{name} 을 찾을 수 없습니다")
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"Secret {namespace}/{name} 조회")
        return _secret_from_k8s(resp.json())


async def create_secret(
    cluster_id: str,
    namespace: str,
    name: str,
    data: dict[str, str],
    *,
    secret_type: str = "Opaque",
    labels: dict | None = None,
    annotations: dict | None = None,
    project_id: str,
) -> dict:
    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": secret_type,
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels or {},
            "annotations": annotations or {},
        },
        "data": _encode_secret_data(data),
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.post(
            f"{server_url}/api/v1/namespaces/{namespace}/secrets",
            json=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"Secret {namespace}/{name} 생성")
        return _secret_from_k8s(resp.json())


async def update_secret(
    cluster_id: str,
    namespace: str,
    name: str,
    data: dict[str, str],
    *,
    secret_type: str = "Opaque",
    labels: dict | None = None,
    annotations: dict | None = None,
    project_id: str,
) -> dict:
    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": secret_type,
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels or {},
            "annotations": annotations or {},
        },
        "data": _encode_secret_data(data),
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.put(
            f"{server_url}/api/v1/namespaces/{namespace}/secrets/{name}",
            json=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        if resp.status_code == 404:
            raise K3sApiError(404, f"Secret {namespace}/{name} 을 찾을 수 없습니다")
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"Secret {namespace}/{name} 업데이트")
        return _secret_from_k8s(resp.json())


async def delete_secret(cluster_id: str, namespace: str, name: str, *, project_id: str) -> None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.delete(
            f"{server_url}/api/v1/namespaces/{namespace}/secrets/{name}",
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 404:
            return
        if resp.status_code not in (200, 202):
            _raise_k8s_error(resp, f"Secret {namespace}/{name} 삭제")


# ---------------------------------------------------------------------------
# WebSocket 연결 파라미터
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _kube_ws_params(cluster_id: str, *, project_id: str | None = None):
    """K8s WS exec 용 (ssl_ctx, wss_server_url) yield.
    _kube_client 와 동일하게 kubeconfig 복호화하되 HTTP 클라이언트를 생성하지 않음.
    """
    if project_id is not None:
        kubeconfig_yaml = await k3s_db.get_kubeconfig(project_id=project_id, cluster_id=cluster_id)
    else:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
    if not kubeconfig_yaml:
        raise K3sApiError(502, "kubeconfig 를 찾을 수 없습니다 (클러스터 미준비)")
    cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
    ssl_ctx = _make_ssl_context(cert_pem, key_pem)
    wss_url = server_url.replace("https://", "wss://").replace("http://", "ws://")
    yield ssl_ctx, wss_url


# ---------------------------------------------------------------------------
# Pod / PVC / RBAC CRUD (Cloud Shell 용)
# ---------------------------------------------------------------------------


async def get_namespace(cluster_id: str, name: str, *, project_id: str | None = None) -> dict | None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/api/v1/namespaces/{name}")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"get namespace {name}")
        return resp.json()


async def create_namespace(cluster_id: str, name: str, *, project_id: str | None = None) -> dict:
    body = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}}
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.post(f"{server_url}/api/v1/namespaces", json=body)
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"create namespace {name}")
        return resp.json()


async def ensure_namespace(cluster_id: str, name: str, *, project_id: str | None = None) -> None:
    existing = await get_namespace(cluster_id, name, project_id=project_id)
    if not existing:
        try:
            await create_namespace(cluster_id, name, project_id=project_id)
        except K3sApiError as e:
            if e.status_code != 409:  # 409 Conflict = 이미 존재
                raise


async def get_pvc(cluster_id: str, namespace: str, name: str, *, project_id: str | None = None) -> dict | None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/api/v1/namespaces/{namespace}/persistentvolumeclaims/{name}")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"get pvc {name}")
        return resp.json()


async def create_pvc(
    cluster_id: str,
    namespace: str,
    name: str,
    size: str = "1Gi",
    *,
    storage_class: str | None = None,
    project_id: str | None = None,
) -> dict:
    spec: dict = {
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": size}},
    }
    if storage_class:
        spec["storageClassName"] = storage_class
    body = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": namespace},
        "spec": spec,
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.post(f"{server_url}/api/v1/namespaces/{namespace}/persistentvolumeclaims", json=body)
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"create pvc {name}")
        return resp.json()


async def ensure_pvc(
    cluster_id: str, namespace: str, name: str, size: str = "1Gi", *, project_id: str | None = None
) -> None:
    existing = await get_pvc(cluster_id, namespace, name, project_id=project_id)
    if not existing:
        try:
            await create_pvc(cluster_id, namespace, name, size, project_id=project_id)
        except K3sApiError as e:
            if e.status_code != 409:
                raise


async def get_pod(cluster_id: str, namespace: str, name: str, *, project_id: str | None = None) -> dict | None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/api/v1/namespaces/{namespace}/pods/{name}")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"get pod {name}")
        return resp.json()


async def create_pod(cluster_id: str, namespace: str, body: dict, *, project_id: str | None = None) -> dict:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.post(f"{server_url}/api/v1/namespaces/{namespace}/pods", json=body)
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"create pod {body.get('metadata', {}).get('name', '?')}")
        return resp.json()


async def delete_pod(cluster_id: str, namespace: str, name: str, *, project_id: str | None = None) -> None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.delete(f"{server_url}/api/v1/namespaces/{namespace}/pods/{name}")
        if resp.status_code not in (200, 202, 404):
            _raise_k8s_error(resp, f"delete pod {name}")


async def wait_pod_ready(
    cluster_id: str, namespace: str, name: str, *, timeout: float = 90.0, project_id: str | None = None
) -> bool:
    """pod 의 phase=Running + containerStatuses[*].ready=True 까지 대기."""
    import asyncio as _asyncio

    deadline = _asyncio.get_event_loop().time() + timeout
    while True:
        pod = await get_pod(cluster_id, namespace, name, project_id=project_id)
        if pod:
            phase = pod.get("status", {}).get("phase", "")
            statuses = pod.get("status", {}).get("containerStatuses", [])
            if phase == "Running" and statuses and all(s.get("ready") for s in statuses):
                return True
            if phase in ("Failed", "Succeeded", "Unknown"):
                return False
        remaining = deadline - _asyncio.get_event_loop().time()
        if remaining <= 0:
            return False
        await _asyncio.sleep(min(2.0, remaining))


async def get_cluster_role_binding(cluster_id: str, name: str, *, project_id: str | None = None) -> dict | None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{name}")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"get clusterrolebinding {name}")
        return resp.json()


async def create_cluster_role_binding(
    cluster_id: str, name: str, user_name: str, role_name: str = "cluster-admin", *, project_id: str | None = None
) -> dict:
    body = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": name},
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": role_name,
        },
        "subjects": [{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": user_name}],
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.post(f"{server_url}/apis/rbac.authorization.k8s.io/v1/clusterrolebindings", json=body)
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"create clusterrolebinding {name}")
        return resp.json()


async def ensure_cluster_role_binding_for_user(
    cluster_id: str, k8s_user: str, *, project_id: str | None = None
) -> None:
    crb_name = f"afterglow-shell-{k8s_user}"
    existing = await get_cluster_role_binding(cluster_id, crb_name, project_id=project_id)
    if not existing:
        try:
            await create_cluster_role_binding(cluster_id, crb_name, k8s_user, project_id=project_id)
        except K3sApiError as e:
            if e.status_code != 409:
                raise


async def create_k8s_secret(
    cluster_id: str, namespace: str, name: str, string_data: dict[str, str], *, project_id: str | None = None
) -> dict:
    """generic Opaque secret (stringData)."""
    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": "Opaque",
        "stringData": string_data,
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.post(f"{server_url}/api/v1/namespaces/{namespace}/secrets", json=body)
        if resp.status_code == 409:
            # 이미 존재 — PUT 으로 교체 (kubeconfig 가 갱신될 수 있음)
            resp = await client.put(
                f"{server_url}/api/v1/namespaces/{namespace}/secrets/{name}",
                json={**body, "metadata": {"name": name, "namespace": namespace}},
            )
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"upsert secret {name}")
        return resp.json()


# ---------------------------------------------------------------------------
# 인증서 회전 헬퍼
# ---------------------------------------------------------------------------


async def list_server_nodes(cluster_id: str) -> list[str]:
    """control-plane 역할 노드 이름 목록 반환 (관리자 kubeconfig 사용)."""
    async with _kube_client(cluster_id) as (client, server_url):
        resp = await client.get(
            f"{server_url}/api/v1/nodes",
            params={"labelSelector": "node-role.kubernetes.io/control-plane"},
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            _raise_k8s_error(resp, "control-plane 노드 목록 조회")
        items = resp.json().get("items", [])
        return [it["metadata"]["name"] for it in items if it.get("metadata", {}).get("name")]


async def create_job(cluster_id: str, namespace: str, job: dict) -> dict:
    """K8s Job 생성 (관리자 kubeconfig 사용)."""
    async with _kube_client(cluster_id) as (client, server_url):
        resp = await client.post(
            f"{server_url}/apis/batch/v1/namespaces/{namespace}/jobs",
            json=job,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"Job {job.get('metadata', {}).get('name', '?')} 생성")
        return resp.json()


async def wait_job_completed(cluster_id: str, namespace: str, job_name: str, *, timeout: float = 180.0) -> bool:
    """Job succeeded≥1 또는 failed≥1 을 대기한다. 성공 시 True, 실패/타임아웃 시 False."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with _kube_client(cluster_id) as (client, server_url):
                resp = await client.get(
                    f"{server_url}/apis/batch/v1/namespaces/{namespace}/jobs/{job_name}",
                    headers={"Accept": "application/json"},
                )
                if resp.status_code == 200:
                    status = resp.json().get("status", {})
                    if (status.get("succeeded") or 0) >= 1:
                        return True
                    if (status.get("failed") or 0) >= 1:
                        return False
        except Exception:
            pass
        import asyncio

        await asyncio.sleep(3)
    return False


async def wait_node_ready(cluster_id: str, node_name: str, *, timeout: float = 300.0) -> bool:
    """노드 Ready 상태를 대기한다. K8s API 재시작 중 연결 오류는 무시하고 재시도한다."""
    import asyncio
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with _kube_client(cluster_id) as (client, server_url):
                resp = await client.get(
                    f"{server_url}/api/v1/nodes/{node_name}",
                    headers={"Accept": "application/json"},
                )
                if resp.status_code == 200:
                    conditions = resp.json().get("status", {}).get("conditions", [])
                    for cond in conditions:
                        if cond.get("type") == "Ready" and cond.get("status") == "True":
                            return True
        except Exception:
            # k3s restart 중 API 서버 일시 불가 — 재시도
            pass
        await asyncio.sleep(5)
    return False


async def wait_node_gpu_allocatable(
    cluster_id: str, node_name: str, *, min_gpu: int = 1, timeout: float = 600.0
) -> bool:
    """노드가 Ready 이후 nvidia.com/gpu allocatable을 노출할 때까지 대기한다."""
    import asyncio
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with _kube_client(cluster_id) as (client, server_url):
                resp = await client.get(
                    f"{server_url}/api/v1/nodes/{node_name}",
                    headers={"Accept": "application/json"},
                )
                if resp.status_code == 200:
                    allocatable = resp.json().get("status", {}).get("allocatable", {})
                    if int(allocatable.get("nvidia.com/gpu", 0)) >= min_gpu:
                        return True
        except Exception:
            pass
        await asyncio.sleep(5)
    return False


# ---------------------------------------------------------------------------
# 워크로드 조회/액션 헬퍼 (Phase 53o)
# ---------------------------------------------------------------------------


def _pod_from_k8s(item: dict) -> dict:
    meta = item.get("metadata", {})
    spec = item.get("spec", {})
    status = item.get("status", {})
    container_statuses = status.get("containerStatuses", [])
    init_statuses = status.get("initContainerStatuses", [])
    all_statuses = container_statuses + init_statuses

    ready_count = sum(1 for s in container_statuses if s.get("ready"))
    total_count = len(spec.get("containers", []))
    restarts = sum(s.get("restartCount", 0) for s in all_statuses)

    containers = []
    for c in spec.get("containers", []):
        cs = next((s for s in all_statuses if s.get("name") == c.get("name")), {})
        state_obj = cs.get("state", {})
        if "running" in state_obj:
            state = "running"
        elif "waiting" in state_obj:
            state = "waiting"
        elif "terminated" in state_obj:
            state = "terminated"
        else:
            state = ""
        containers.append(
            {
                "name": c.get("name", ""),
                "image": c.get("image", ""),
                "ready": cs.get("ready", False),
                "restart_count": cs.get("restartCount", 0),
                "state": state,
            }
        )

    return {
        "name": meta.get("name", ""),
        "namespace": meta.get("namespace", ""),
        "phase": status.get("phase", ""),
        "ready": f"{ready_count}/{total_count}",
        "restarts": restarts,
        "node": spec.get("nodeName"),
        "pod_ip": status.get("podIP"),
        "containers": containers,
        "labels": meta.get("labels", {}),
        "created_at": meta.get("creationTimestamp", ""),
    }


def _svc_from_k8s(item: dict) -> dict:
    meta = item.get("metadata", {})
    spec = item.get("spec", {})
    ports = []
    for p in spec.get("ports", []):
        ports.append(
            {
                "name": p.get("name"),
                "port": p.get("port", 0),
                "target_port": p.get("targetPort"),
                "node_port": p.get("nodePort"),
                "protocol": p.get("protocol", "TCP"),
            }
        )
    return {
        "name": meta.get("name", ""),
        "namespace": meta.get("namespace", ""),
        "type": spec.get("type", "ClusterIP"),
        "cluster_ip": spec.get("clusterIP"),
        "external_ips": spec.get("externalIPs", []),
        "ports": ports,
        "selector": spec.get("selector", {}),
        "created_at": meta.get("creationTimestamp", ""),
    }


def _deploy_from_k8s(item: dict) -> dict:
    meta = item.get("metadata", {})
    spec = item.get("spec", {})
    status = item.get("status", {})
    images = [c.get("image", "") for c in spec.get("template", {}).get("spec", {}).get("containers", [])]
    return {
        "name": meta.get("name", ""),
        "namespace": meta.get("namespace", ""),
        "replicas": spec.get("replicas", 0),
        "available": status.get("availableReplicas", 0),
        "ready": status.get("readyReplicas", 0),
        "updated": status.get("updatedReplicas", 0),
        "strategy": spec.get("strategy", {}).get("type", ""),
        "selector": spec.get("selector", {}).get("matchLabels", {}),
        "images": images,
        "created_at": meta.get("creationTimestamp", ""),
    }


def _rs_from_k8s(item: dict) -> dict:
    meta = item.get("metadata", {})
    spec = item.get("spec", {})
    status = item.get("status", {})
    owners = meta.get("ownerReferences", [])
    owner = owners[0] if owners else {}
    images = [c.get("image", "") for c in spec.get("template", {}).get("spec", {}).get("containers", [])]
    return {
        "name": meta.get("name", ""),
        "namespace": meta.get("namespace", ""),
        "replicas": spec.get("replicas", 0),
        "ready": status.get("readyReplicas", 0),
        "available": status.get("availableReplicas", 0),
        "owner_kind": owner.get("kind"),
        "owner_name": owner.get("name"),
        "selector": spec.get("selector", {}).get("matchLabels", {}),
        "images": images,
        "created_at": meta.get("creationTimestamp", ""),
    }


async def list_pods(cluster_id: str, namespace: str, *, project_id: str) -> list[dict]:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/api/v1/namespaces/{namespace}/pods")
        if resp.status_code != 200:
            _raise_k8s_error(resp, "list pods")
        return [_pod_from_k8s(item) for item in resp.json().get("items", [])]


async def list_services(cluster_id: str, namespace: str, *, project_id: str) -> list[dict]:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/api/v1/namespaces/{namespace}/services")
        if resp.status_code != 200:
            _raise_k8s_error(resp, "list services")
        return [_svc_from_k8s(item) for item in resp.json().get("items", [])]


async def list_service_annotations(cluster_id: str) -> dict[str, dict[str, str]]:
    """Annotations of every Service in every namespace, keyed by ``namespace/name`` (admin kubeconfig).

    Raises unless the API returned the list, so an unreadable cluster is never mistaken for one without annotations.
    """
    async with _kube_client(cluster_id) as (client, server_url):
        resp = await client.get(f"{server_url}/api/v1/services", headers={"Accept": "application/json"})
        if resp.status_code != 200:
            _raise_k8s_error(resp, "list services")
        return {
            f"{meta.get('namespace', '')}/{meta.get('name', '')}": meta.get("annotations") or {}
            for meta in (item.get("metadata", {}) for item in resp.json().get("items", []))
        }


async def list_deployments(cluster_id: str, namespace: str, *, project_id: str) -> list[dict]:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/apis/apps/v1/namespaces/{namespace}/deployments")
        if resp.status_code != 200:
            _raise_k8s_error(resp, "list deployments")
        return [_deploy_from_k8s(item) for item in resp.json().get("items", [])]


async def list_replicasets(cluster_id: str, namespace: str, *, project_id: str) -> list[dict]:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(f"{server_url}/apis/apps/v1/namespaces/{namespace}/replicasets")
        if resp.status_code != 200:
            _raise_k8s_error(resp, "list replicasets")
        return [_rs_from_k8s(item) for item in resp.json().get("items", [])]


async def get_pod_log(
    cluster_id: str, namespace: str, name: str, *, container: str | None = None, tail_lines: int = 200, project_id: str
) -> str:
    params: dict = {"tailLines": str(tail_lines)}
    if container:
        params["container"] = container
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.get(
            f"{server_url}/api/v1/namespaces/{namespace}/pods/{name}/log",
            params=params,
        )
        if resp.status_code == 404:
            raise K3sApiError(404, "Pod not found")
        if resp.status_code != 200:
            raise K3sApiError(502, f"K8s log error: {resp.status_code}")
        return resp.text


async def delete_service(cluster_id: str, namespace: str, name: str, *, project_id: str) -> None:
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.delete(f"{server_url}/api/v1/namespaces/{namespace}/services/{name}")
        if resp.status_code not in (200, 202, 404):
            _raise_k8s_error(resp, f"delete service {name}")


async def restart_deployment(cluster_id: str, namespace: str, name: str, *, project_id: str) -> dict:
    import datetime

    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    patch = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": now,
                    }
                }
            }
        }
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.patch(
            f"{server_url}/apis/apps/v1/namespaces/{namespace}/deployments/{name}",
            json=patch,
            headers={"Content-Type": "application/strategic-merge-patch+json"},
        )
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"restart deployment {name}")
        return _deploy_from_k8s(resp.json())


async def scale_deployment(cluster_id: str, namespace: str, name: str, replicas: int, *, project_id: str) -> dict:
    scale_body = {
        "apiVersion": "autoscaling/v1",
        "kind": "Scale",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"replicas": replicas},
    }
    async with _kube_client(cluster_id, project_id=project_id) as (client, server_url):
        resp = await client.put(
            f"{server_url}/apis/apps/v1/namespaces/{namespace}/deployments/{name}/scale",
            json=scale_body,
        )
        if resp.status_code not in (200, 201):
            _raise_k8s_error(resp, f"scale deployment {name}")
        # scale 응답은 Scale 객체 — deployment 정보를 재조회
        resp2 = await client.get(f"{server_url}/apis/apps/v1/namespaces/{namespace}/deployments/{name}")
        if resp2.status_code != 200:
            _raise_k8s_error(resp2, f"get deployment {name} after scale")
        return _deploy_from_k8s(resp2.json())


# ---------------------------------------------------------------------------
# Stampede 오토스케일 전용 함수 (admin kubeconfig, observation failures propagate)
# ---------------------------------------------------------------------------


_QUANTITY = re.compile(r"([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))([eE][+-]?[0-9]+|[numkKMGTPE]|[KMGTPE]i)?")
_DECIMAL_SUFFIXES = {"n": -9, "u": -6, "m": -3, "k": 3, "K": 3, "M": 6, "G": 9, "T": 12, "P": 15, "E": 18}


def _parse_quantity(value: str) -> Decimal:
    """Parse Kubernetes quantities exactly; invalid/negative values fail closed."""
    match = _QUANTITY.fullmatch(str(value).strip())
    if match is None:
        raise ValueError(f"Invalid Kubernetes quantity: {value!r}")
    number, suffix = match.groups()
    with localcontext() as ctx:
        ctx.prec = max(64, len(number) + 32)
        result = Decimal(number)
        if suffix:
            if suffix.endswith("i"):
                result *= 1024 ** ("KMGTPE".index(suffix[0]) + 1)
            elif suffix in _DECIMAL_SUFFIXES:
                result *= Decimal(10) ** _DECIMAL_SUFFIXES[suffix]
            else:
                result *= Decimal(10) ** int(suffix[1:])
        if result < 0 or not result.is_finite():
            raise ValueError(f"Invalid Kubernetes quantity: {value!r}")
        return result


def _ceil_quantity(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _parse_cpu_millicores(s: str) -> int:
    return _ceil_quantity(_parse_quantity(s) * 1000)


def _parse_memory_bytes(s: str) -> int:
    return _ceil_quantity(_parse_quantity(s))


def _resource_dict(quantities: dict[str, Decimal], *, pods: int = 0) -> dict:
    """Keep nonstandard keys separate: never equate MIG/RDMA with NVIDIA GPUs."""
    return {
        "cpu_m": _ceil_quantity(quantities.get("cpu", Decimal(0)) * 1000),
        "memory_bytes": _ceil_quantity(quantities.get("memory", Decimal(0))),
        "gpu": _ceil_quantity(quantities.get("nvidia.com/gpu", Decimal(0))),
        "pods": pods,
        "extended_resources": {
            key: _ceil_quantity(value)
            for key, value in quantities.items()
            if key not in {"cpu", "memory", "nvidia.com/gpu", "pods"}
        },
    }


def _container_requests(container: dict) -> dict[str, Decimal]:
    resources = container.get("resources") or {}
    # Kubernetes defaults a request to its limit only if that request is absent.
    values = dict(resources.get("limits") or {})
    values.update(resources.get("requests") or {})
    return {key: _parse_quantity(value) for key, value in values.items()}


def _add_requests(target: dict[str, Decimal], values: dict[str, Decimal]) -> None:
    for key, value in values.items():
        target[key] = target.get(key, Decimal(0)) + value


def _max_requests(target: dict[str, Decimal], values: dict[str, Decimal]) -> None:
    for key, value in values.items():
        target[key] = max(target.get(key, Decimal(0)), value)


def _effective_pod_requests(spec: dict) -> dict:
    """Scheduler demand: max(apps + sidecars, each init + preceding sidecars) + overhead."""
    with localcontext() as ctx:
        ctx.prec = 64
        apps: dict[str, Decimal] = {}
        sidecars: dict[str, Decimal] = {}
        init_peak: dict[str, Decimal] = {}
        for container in spec.get("containers") or []:
            _add_requests(apps, _container_requests(container))
        for container in spec.get("initContainers") or []:
            requests = _container_requests(container)
            if container.get("restartPolicy") == "Always":
                _add_requests(sidecars, requests)
                stage = sidecars
            else:
                stage = dict(sidecars)
                _add_requests(stage, requests)
            _max_requests(init_peak, stage)
        _add_requests(apps, sidecars)
        _max_requests(apps, init_peak)
        # Pod-level requests cannot make the observation smaller than container demand.
        _max_requests(apps, _container_requests(spec))
        _add_requests(apps, {key: _parse_quantity(value) for key, value in (spec.get("overhead") or {}).items()})
        return _resource_dict(apps, pods=1)


def _stampede_pod_metadata(item: dict) -> dict:
    meta = item.get("metadata") or {}
    spec = item.get("spec") or {}
    annotations = meta.get("annotations") or {}
    owners = meta.get("ownerReferences") or []
    volumes = spec.get("volumes") or []
    is_daemonset = any(owner.get("kind") == "DaemonSet" for owner in owners)
    is_mirror = "kubernetes.io/config.mirror" in annotations
    has_controller = any(owner.get("controller") is True for owner in owners)
    has_local_storage = any("emptyDir" in volume or "hostPath" in volume for volume in volumes)
    protected = (
        str(annotations.get("cluster-autoscaler.kubernetes.io/safe-to-evict", "")).lower() == "false"
        or str(annotations.get("cluster-autoscaler.kubernetes.io/scale-down-disabled", "")).lower() == "true"
        or spec.get("priorityClassName") in {"system-cluster-critical", "system-node-critical"}
        or spec.get("priority", 0) >= 2000000000
        or meta.get("namespace", "default") == "kube-system"
    )
    return {
        "name": meta["name"],
        "namespace": meta.get("namespace", "default"),
        "uid": meta.get("uid", ""),
        "node_name": spec.get("nodeName") or "",
        "node_selector": spec.get("nodeSelector") or {},
        "tolerations": spec.get("tolerations") or [],
        "affinity": spec.get("affinity") or {},
        "topology_spread_constraints": spec.get("topologySpreadConstraints") or [],
        "host_ports": [
            {"host_ip": port.get("hostIP", ""), "port": port["hostPort"], "protocol": port.get("protocol", "TCP")}
            for container in (spec.get("containers") or []) + (spec.get("initContainers") or [])
            for port in container.get("ports") or []
            if port.get("hostPort", 0) > 0
        ],
        "scheduler_name": spec.get("schedulerName") or "default-scheduler",
        "is_daemonset": is_daemonset,
        "is_mirror": is_mirror,
        "has_controller": has_controller,
        "safe_to_evict": has_controller and not (is_daemonset or is_mirror or protected or has_local_storage),
        "has_local_storage": has_local_storage,
        "has_pvc": any("persistentVolumeClaim" in volume for volume in volumes),
        "deleting": bool(meta.get("deletionTimestamp")),
    }


async def _stampede_list_items(client, server_url: str, resource: str, *, params: dict | None = None, deadline: float | None = None) -> list[dict]:
    """A failed, malformed or incomplete list is never an empty-cluster observation."""
    import time

    query = dict(params or {})
    items = []
    seen_tokens = set()
    while True:
        request_args = {"headers": {"Accept": "application/json"}}
        if query:
            request_args["params"] = query
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Kubernetes observation deadline expired")
            request_args["timeout"] = min(15.0, remaining)
        resp = await client.get(f"{server_url}/api/v1/{resource}", **request_args)
        if resp.status_code != 200:
            _raise_k8s_error(resp, f"Stampede list {resource}")
        body = resp.json()
        page = body["items"]
        if not isinstance(page, list) or any(not isinstance(item, dict) for item in page):
            raise ValueError("Malformed Kubernetes list")
        items.extend(page)
        token = (body.get("metadata") or {}).get("continue")
        if not token:
            return items
        if token in seen_tokens:
            raise ValueError("Repeated Kubernetes continuation token")
        seen_tokens.add(token)
        query["continue"] = token


async def list_unschedulable_pods(cluster_id: str) -> list[dict]:
    """Unschedulable Pending demand, using the same requests as assigned usage."""
    async with _kube_client(cluster_id) as (client, server_url):
        items = await _stampede_list_items(client, server_url, "pods", params={"fieldSelector": "status.phase=Pending"})
    result = []
    for item in items:
        status = item.get("status") or {}
        if status.get("phase") != "Pending":
            continue
        for condition in status.get("conditions") or []:
            if condition.get("type") == "PodScheduled" and condition.get("status") == "False" and condition.get("reason") == "Unschedulable":
                result.append({
                    **_stampede_pod_metadata(item),
                    "resource_requests": _effective_pod_requests(item.get("spec") or {}),
                    "message": condition.get("message", ""),
                })
                break
    return result


async def get_node_capacity(cluster_id: str) -> list[dict]:
    """Node allocatable capacity and scheduling metadata; failures propagate."""
    async with _kube_client(cluster_id) as (client, server_url):
        items = await _stampede_list_items(client, server_url, "nodes")
    result = []
    for item in items:
        meta = item.get("metadata") or {}
        status = item.get("status") or {}
        spec = item.get("spec") or {}
        quantities = {key: _parse_quantity(value) for key, value in (status.get("allocatable") or {}).items()}
        result.append({
            "name": meta["name"],
            "allocatable": _resource_dict(quantities, pods=_ceil_quantity(quantities.get("pods", Decimal(0)))),
            "labels": meta.get("labels") or {},
            "taints": spec.get("taints") or [],
            "ready": any(cond.get("type") == "Ready" and cond.get("status") == "True" for cond in status.get("conditions") or []),
            "unschedulable": bool(spec.get("unschedulable")),
            "removal_vm_id": (meta.get("annotations") or {}).get(REMOVAL_VM_ANNOTATION),
        })
    return result


async def get_pod_resource_usage(cluster_id: str) -> list[dict]:
    """All assigned live pods consume capacity, including Pending and terminating."""
    async with _kube_client(cluster_id) as (client, server_url):
        items = await _stampede_list_items(client, server_url, "pods")
    result = []
    for item in items:
        spec = item.get("spec") or {}
        if not spec.get("nodeName") or (item.get("status") or {}).get("phase") in {"Succeeded", "Failed"}:
            continue
        requests = _effective_pod_requests(spec)
        result.append({
            **_stampede_pod_metadata(item),
            "node": spec["nodeName"],
            **requests,
            "resource_requests": requests,
        })
    return result


REMOVAL_VM_ANNOTATION = "drover.io/removing-vm-id"


async def _set_node_unschedulable(cluster_id: str, node_name: str, unschedulable: bool, removal_vm_id: str | None) -> bool:
    try:
        async with _kube_client(cluster_id) as (client, server_url):
            resp = await client.patch(
                f"{server_url}/api/v1/nodes/{node_name}",
                # Merge-patch null removes the annotation together with the cordon.
                json={"metadata": {"annotations": {REMOVAL_VM_ANNOTATION: removal_vm_id}}, "spec": {"unschedulable": unschedulable}},
                headers={
                    "Content-Type": "application/merge-patch+json",
                    "Accept": "application/json",
                },
            )
            if resp.status_code in (200, 201):
                return True
            _logger.warning("stampede: node scheduling patch failed HTTP %d", resp.status_code)
            return False
    except Exception:
        _logger.warning("stampede: node scheduling patch failed")
        return False


async def cordon_node(cluster_id: str, node_name: str, *, removal_vm_id: str) -> bool:
    """Prevent scheduling for one VM removal; False means callers must not proceed."""
    return await _set_node_unschedulable(cluster_id, node_name, True, removal_vm_id)


async def uncordon_node(cluster_id: str, node_name: str) -> bool:
    """Restore scheduling after an aborted drain, returning False on failure."""
    return await _set_node_unschedulable(cluster_id, node_name, False, None)


async def drain_node(cluster_id: str, node_name: str, *, timeout: float = 120.0) -> bool:
    """Evict managed, unprotected pods respecting PDBs, then wait until absent.

    DaemonSet/mirror and terminal pods are ignored. Any other unsafe pod blocks
    the entire drain before eviction. False is never permission to force-delete.
    """
    import asyncio
    import time

    deadline = time.monotonic() + timeout
    accepted: set[tuple[str, str, str]] = set()
    attempts: dict[tuple[str, str, str], int] = {}
    try:
        async with asyncio.timeout(timeout), _kube_client(cluster_id) as (client, server_url):
            while True:
                items = await _stampede_list_items(
                    client, server_url, "pods",
                    params={"fieldSelector": f"spec.nodeName={node_name}"},
                    deadline=deadline,
                )
                live = []
                for item in items:
                    if (item.get("status") or {}).get("phase") in {"Succeeded", "Failed"}:
                        continue
                    pod = _stampede_pod_metadata(item)
                    if pod["is_daemonset"] or pod["is_mirror"]:
                        continue
                    if not pod["safe_to_evict"]:
                        _logger.info("stampede: drain blocked by protected/unmanaged pod")
                        return False
                    live.append(pod)
                if not live:
                    return True

                wait_seconds = 2.0
                for pod in live:
                    identity = (pod["namespace"], pod["name"], pod["uid"])
                    # An accepted eviction and a deletionTimestamp still consume
                    # the node until a subsequent authoritative list says gone.
                    if pod["deleting"] or identity in accepted:
                        continue
                    count = attempts.get(identity, 0)
                    if count >= 5:
                        return False
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    body = {
                        "apiVersion": "policy/v1",
                        "kind": "Eviction",
                        "metadata": {"name": pod["name"], "namespace": pod["namespace"]},
                    }
                    if pod["uid"]:
                        body["deleteOptions"] = {"preconditions": {"uid": pod["uid"]}}
                    resp = await client.post(
                        f"{server_url}/api/v1/namespaces/{pod['namespace']}/pods/{pod['name']}/eviction",
                        json=body,
                        headers={"Accept": "application/json"},
                        timeout=min(15.0, remaining),
                    )
                    attempts[identity] = count + 1
                    if resp.status_code in (200, 201, 202, 404):
                        accepted.add(identity)
                    elif resp.status_code == 429:
                        if count + 1 >= 5:
                            return False
                        wait_seconds = max(wait_seconds, min(5.0 * 2**count, 30.0))
                    else:
                        _logger.warning("stampede: eviction failed HTTP %d", resp.status_code)
                        return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                await asyncio.sleep(min(wait_seconds, remaining))
    except Exception:
        _logger.warning("stampede: drain failed or timed out")
        return False
