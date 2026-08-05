"""K8s API 직접 호출 유틸리티.

kubeconfig의 클라이언트 인증서를 사용해 K8s API 서버에 직접 요청.
노드 삭제 등 클러스터 관리 작업에 사용.
"""

import base64
import contextlib
import logging
import ssl
import tempfile

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
# Stampede 오토스케일 전용 함수 (admin kubeconfig, K3sApiError 미사용)
# ---------------------------------------------------------------------------


def _parse_cpu_millicores(s: str) -> int:
    """K8s CPU 수량 문자열 → 밀리코어(int). 예: '500m'→500, '2'→2000."""
    if not s:
        return 0
    s = s.strip()
    if s.endswith("m"):
        return int(s[:-1])
    return int(float(s) * 1000)


def _parse_memory_bytes(s: str) -> int:
    """K8s 메모리 수량 문자열 → 바이트(int). 예: '512Mi'→536870912."""
    if not s:
        return 0
    s = s.strip()
    _suffixes = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    for suffix, mul in _suffixes.items():
        if s.endswith(suffix):
            return int(float(s[: -len(suffix)]) * mul)
    return int(s)


async def list_unschedulable_pods(cluster_id: str) -> list[dict]:
    """전체 네임스페이스에서 Unschedulable Pending pod 목록 반환 (admin kubeconfig).

    반환 구조:
      {name, namespace, node_selector, resource_requests{cpu_m, memory_bytes, gpu},
       tolerations, affinity, message}
    """
    try:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
        if not kubeconfig_yaml:
            return []
        cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
        ssl_ctx = _make_ssl_context(cert_pem, key_pem)
        async with httpx.AsyncClient(verify=ssl_ctx, timeout=15.0) as client:
            resp = await client.get(
                f"{server_url}/api/v1/pods",
                params={"fieldSelector": "status.phase=Pending"},
                headers={"Accept": "application/json"},
            )
            if resp.status_code != 200:
                _logger.warning("list_unschedulable_pods HTTP %d: %s", resp.status_code, resp.text[:200])
                return []
            items = resp.json().get("items", [])
    except Exception as e:
        _logger.warning("list_unschedulable_pods 오류 (cluster=%s): %s", cluster_id, e)
        return []

    result = []
    for item in items:
        meta = item.get("metadata", {})
        spec = item.get("spec", {})
        status = item.get("status", {})

        # PodScheduled=False, reason=Unschedulable 조건 확인
        conditions = status.get("conditions", [])
        unschedulable = False
        msg = ""
        for cond in conditions:
            if cond.get("type") == "PodScheduled" and cond.get("status") == "False":
                if cond.get("reason") == "Unschedulable":
                    unschedulable = True
                    msg = cond.get("message", "")
                break
        if not unschedulable:
            continue

        # resource requests 집계 (모든 컨테이너 합산)
        cpu_m = 0
        memory_bytes = 0
        gpu = 0
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            req = container.get("resources", {}).get("requests", {})
            cpu_m += _parse_cpu_millicores(req.get("cpu", "0"))
            memory_bytes += _parse_memory_bytes(req.get("memory", "0"))
            gpu += int(req.get("nvidia.com/gpu", 0))

        result.append(
            {
                "name": meta.get("name", ""),
                "namespace": meta.get("namespace", "default"),
                "node_selector": spec.get("nodeSelector") or {},
                "resource_requests": {"cpu_m": cpu_m, "memory_bytes": memory_bytes, "gpu": gpu},
                "tolerations": spec.get("tolerations") or [],
                "affinity": spec.get("affinity") or {},
                "message": msg,
                "node_name": spec.get("nodeName") or "",
            }
        )
    return result


async def get_node_capacity(cluster_id: str) -> list[dict]:
    """전체 노드의 capacity/allocatable/Ready 상태 반환 (admin kubeconfig).

    반환 구조:
      {name, allocatable{cpu_m, memory_bytes, gpu}, labels, taints, ready}
    """
    try:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
        if not kubeconfig_yaml:
            return []
        cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
        ssl_ctx = _make_ssl_context(cert_pem, key_pem)
        async with httpx.AsyncClient(verify=ssl_ctx, timeout=15.0) as client:
            resp = await client.get(
                f"{server_url}/api/v1/nodes",
                headers={"Accept": "application/json"},
            )
            if resp.status_code != 200:
                _logger.warning("get_node_capacity HTTP %d", resp.status_code)
                return []
            items = resp.json().get("items", [])
    except Exception as e:
        _logger.warning("get_node_capacity 오류 (cluster=%s): %s", cluster_id, e)
        return []

    result = []
    for item in items:
        meta = item.get("metadata", {})
        status = item.get("status", {})
        spec = item.get("spec", {})
        allocatable = status.get("allocatable", {})

        ready = False
        for cond in status.get("conditions", []):
            if cond.get("type") == "Ready" and cond.get("status") == "True":
                ready = True
                break

        result.append(
            {
                "name": meta.get("name", ""),
                "allocatable": {
                    "cpu_m": _parse_cpu_millicores(allocatable.get("cpu", "0")),
                    "memory_bytes": _parse_memory_bytes(allocatable.get("memory", "0")),
                    "gpu": int(allocatable.get("nvidia.com/gpu", 0)),
                },
                "labels": meta.get("labels") or {},
                "taints": spec.get("taints") or [],
                "ready": ready,
            }
        )
    return result


async def get_pod_resource_usage(cluster_id: str) -> list[dict]:
    """전체 Running pod의 resource requests 반환 (admin kubeconfig).

    반환 구조:
      {node, namespace, name, cpu_m, memory_bytes, gpu, is_daemonset, is_mirror}
    """
    try:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
        if not kubeconfig_yaml:
            return []
        cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
        ssl_ctx = _make_ssl_context(cert_pem, key_pem)
        async with httpx.AsyncClient(verify=ssl_ctx, timeout=15.0) as client:
            resp = await client.get(
                f"{server_url}/api/v1/pods",
                params={"fieldSelector": "status.phase=Running"},
                headers={"Accept": "application/json"},
            )
            if resp.status_code != 200:
                return []
            items = resp.json().get("items", [])
    except Exception as e:
        _logger.warning("get_pod_resource_usage 오류 (cluster=%s): %s", cluster_id, e)
        return []

    result = []
    for item in items:
        meta = item.get("metadata", {})
        spec = item.get("spec", {})
        node_name = spec.get("nodeName", "")
        if not node_name:
            continue
        cpu_m = 0
        memory_bytes = 0
        gpu = 0
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            req = container.get("resources", {}).get("requests", {})
            cpu_m += _parse_cpu_millicores(req.get("cpu", "0"))
            memory_bytes += _parse_memory_bytes(req.get("memory", "0"))
            gpu += int(req.get("nvidia.com/gpu", 0))
        owners = meta.get("ownerReferences") or []
        is_daemonset = any(owner.get("kind") == "DaemonSet" for owner in owners if isinstance(owner, dict))
        is_mirror = "kubernetes.io/config.mirror" in (meta.get("annotations") or {})
        result.append(
            {
                "node": node_name,
                "namespace": meta.get("namespace", "default"),
                "name": meta.get("name", ""),
                "cpu_m": cpu_m,
                "memory_bytes": memory_bytes,
                "gpu": gpu,
                "is_daemonset": is_daemonset,
                "is_mirror": is_mirror,
            }
        )
    return result


async def cordon_node(cluster_id: str, node_name: str) -> bool:
    """노드를 cordon(스케줄 불가)으로 설정한다 (admin kubeconfig).

    Returns: True=성공, False=실패
    """
    try:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
        if not kubeconfig_yaml:
            return False
        cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
        ssl_ctx = _make_ssl_context(cert_pem, key_pem)
        patch_body = {"spec": {"unschedulable": True}}
        async with httpx.AsyncClient(verify=ssl_ctx, timeout=10.0) as client:
            resp = await client.patch(
                f"{server_url}/api/v1/nodes/{node_name}",
                json=patch_body,
                headers={
                    "Content-Type": "application/merge-patch+json",
                    "Accept": "application/json",
                },
            )
            if resp.status_code in (200, 201):
                _logger.info("stampede: node %s cordoned", node_name)
                return True
            _logger.warning("stampede: cordon %s 실패 HTTP %d: %s", node_name, resp.status_code, resp.text[:200])
            return False
    except Exception as e:
        _logger.warning("stampede: cordon %s 오류: %s", node_name, e)
        return False


async def drain_node(cluster_id: str, node_name: str, *, timeout: float = 120.0) -> bool:
    """노드를 drain한다 (DaemonSet/mirror pod 제외, PDB 존중, admin kubeconfig).

    Returns: True=drain 완료, False=timeout 또는 오류 (호출자가 강제 삭제 진행)
    """
    import asyncio
    import time

    try:
        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
        if not kubeconfig_yaml:
            return False
        cert_pem, key_pem, server_url = _parse_kubeconfig(kubeconfig_yaml)
        ssl_ctx = _make_ssl_context(cert_pem, key_pem)
    except Exception as e:
        _logger.warning("stampede: drain %s kubeconfig 오류: %s", node_name, e)
        return False

    async with httpx.AsyncClient(verify=ssl_ctx, timeout=15.0) as client:
        # 노드의 모든 pod 조회
        try:
            resp = await client.get(
                f"{server_url}/api/v1/pods",
                params={"fieldSelector": f"spec.nodeName={node_name}"},
                headers={"Accept": "application/json"},
            )
            if resp.status_code != 200:
                _logger.warning("stampede: drain %s pod 조회 실패 HTTP %d", node_name, resp.status_code)
                return False
            items = resp.json().get("items", [])
        except Exception as e:
            _logger.warning("stampede: drain %s pod 조회 오류: %s", node_name, e)
            return False

        # 제외 대상 필터링: DaemonSet pod / mirror pod / 이미 종료된 pod
        evict_pods = []
        for item in items:
            meta = item.get("metadata", {})
            annotations = meta.get("annotations") or {}
            owner_refs = meta.get("ownerReferences") or []
            phase = item.get("status", {}).get("phase", "")

            # 이미 종료된 pod 제외
            if phase in ("Succeeded", "Failed"):
                continue
            # mirror pod 제외 (static pod)
            if "kubernetes.io/config.mirror" in annotations:
                continue
            # DaemonSet pod 제외
            is_daemonset = any(ref.get("kind") == "DaemonSet" for ref in owner_refs)
            if is_daemonset:
                continue

            evict_pods.append(
                {
                    "name": meta.get("name", ""),
                    "namespace": meta.get("namespace", "default"),
                }
            )

        if not evict_pods:
            _logger.info("stampede: drain %s — evict 대상 없음 (빈 노드)", node_name)
            return True

        _logger.info("stampede: drain %s — %d개 pod eviction 시작", node_name, len(evict_pods))
        deadline = time.monotonic() + timeout

        for pod in evict_pods:
            if time.monotonic() >= deadline:
                _logger.warning("stampede: drain %s — timeout 초과", node_name)
                return False
            ns = pod["namespace"]
            name = pod["name"]
            eviction_body = {
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {"name": name, "namespace": ns},
            }
            retry = 0
            while time.monotonic() < deadline:
                try:
                    r = await client.post(
                        f"{server_url}/api/v1/namespaces/{ns}/pods/{name}/eviction",
                        json=eviction_body,
                        headers={"Accept": "application/json"},
                    )
                    if r.status_code in (200, 201, 404):
                        # 성공 또는 이미 없음
                        break
                    if r.status_code == 429:
                        # PDB 위반 — 백오프 재시도
                        wait = min(5 * (2**retry), 30)
                        _logger.info("stampede: drain %s/%s PDB 위반, %ds 후 재시도", ns, name, wait)
                        await asyncio.sleep(wait)
                        retry += 1
                        continue
                    _logger.warning("stampede: evict %s/%s HTTP %d: %s", ns, name, r.status_code, r.text[:100])
                    break
                except Exception as e:
                    _logger.warning("stampede: evict %s/%s 오류: %s", ns, name, e)
                    break

        _logger.info("stampede: drain %s 완료", node_name)
        return True
