"""K3s 클러스터 노드 헬스체크 서비스.

워커 파드가 주기적으로 호출하고, 결과를 Redis에 저장.
메인 API는 Redis에서 읽기만 함.
"""

import base64
import logging
import ssl
import tempfile
from datetime import UTC, datetime

import httpx
import yaml

from drover.models.schemas import K3sClusterHealth, K3sNodeHealth

_logger = logging.getLogger(__name__)

_HEALTH_KEY_PREFIX = "drover:health:"
_HEALTH_TTL = 600  # 10분


# ---------------------------------------------------------------------------
# Redis 저장/조회
# ---------------------------------------------------------------------------


async def _redis():
    from drover.services.cache import _get_client

    return _get_client()


async def store_health_result(cluster_id: str, result: K3sClusterHealth) -> None:
    """헬스체크 결과를 Redis에 저장 (TTL 10분)."""
    try:
        r = await _redis()
        key = f"{_HEALTH_KEY_PREFIX}{cluster_id}"
        await r.setex(key, _HEALTH_TTL, result.model_dump_json())
    except Exception:
        _logger.warning("k3s health: Redis 저장 실패 (cluster=%s)", cluster_id, exc_info=True)


async def get_health_result(cluster_id: str) -> K3sClusterHealth | None:
    """Redis에서 헬스체크 결과 조회."""
    try:
        r = await _redis()
        raw = await r.get(f"{_HEALTH_KEY_PREFIX}{cluster_id}")
        if raw:
            return K3sClusterHealth.model_validate_json(raw)
    except Exception:
        _logger.warning("k3s health: Redis 조회 실패 (cluster=%s)", cluster_id, exc_info=True)
    return None


async def get_health_results_for_clusters(cluster_ids: list[str]) -> list[K3sClusterHealth]:
    """주어진 cluster_id 목록의 헬스 결과를 Redis에서 일괄 조회."""
    results = []
    for cid in cluster_ids:
        result = await get_health_result(cid)
        if result is not None:
            results.append(result)
    return results


# ---------------------------------------------------------------------------
# 내부 헬퍼: kubeconfig 파싱 + SSL 컨텍스트 생성
# ---------------------------------------------------------------------------


def _parse_kubeconfig(kubeconfig_yaml: str) -> tuple[bytes, bytes]:
    """kubeconfig YAML에서 (client_cert_pem, client_key_pem) 반환."""
    kc = yaml.safe_load(kubeconfig_yaml)
    user = kc["users"][0]["user"]
    cert_data = base64.b64decode(user["client-certificate-data"])
    key_data = base64.b64decode(user["client-key-data"])
    return cert_data, key_data


def _make_ssl_context(cert_pem: bytes, key_pem: bytes) -> ssl.SSLContext:
    """클라이언트 인증서로 SSLContext 생성 (K3s 자체 서명 인증서 허용)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # httpx는 SSLContext에 파일 경로로 cert_chain을 로드해야 함
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


# ---------------------------------------------------------------------------
# 접근성 판단: floating IP 여부 확인
# ---------------------------------------------------------------------------


async def _get_probe_ip(
    project_id: str, server_vm_id: str | None, server_ip: str, api_fip_address: str | None = None
) -> str:
    """프로브 IP 결정: API LB FIP > Nova floating IP > private IP 순."""
    if api_fip_address:
        return api_fip_address
    if not server_vm_id or not project_id:
        return server_ip
    try:
        import asyncio

        from drover.services import keystone, nova

        conn = await asyncio.to_thread(keystone.get_admin_connection_for_project, project_id)
        server = await asyncio.to_thread(nova.get_server, conn, server_vm_id)
        for ip_info in server.ip_addresses:
            if ip_info.type == "floating":
                return ip_info.addr
    except Exception:
        _logger.debug("k3s health: floating IP 조회 실패 (vm=%s), private IP 사용", server_vm_id)
    return server_ip


# ---------------------------------------------------------------------------
# 헬스체크 로직
# ---------------------------------------------------------------------------


async def check_cluster_health(cluster: dict) -> K3sClusterHealth:
    """단일 클러스터 헬스체크 수행."""
    cluster_id = cluster["id"]
    cluster_name = cluster.get("name", cluster_id)
    project_id = cluster.get("project_id", "")
    server_vm_id = cluster.get("server_vm_id")
    server_ip = cluster.get("server_ip", "")
    api_fip_address = cluster.get("api_fip_address") or None
    checked_at = datetime.now(UTC).isoformat()

    if not server_ip and not api_fip_address:
        return K3sClusterHealth(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            status="UNKNOWN",
            api_server_reachable=False,
            healthz_ok=False,
            checked_at=checked_at,
            error="server_ip 미설정",
            reachability="unreachable",
        )

    # 접근 가능한 IP 결정 (API LB FIP > Nova floating IP > private IP)
    probe_ip = await _get_probe_ip(project_id, server_vm_id, server_ip, api_fip_address=api_fip_address)
    base_url = f"https://{probe_ip}:6443"

    # kubeconfig 사전 로드 (healthz + nodes 양쪽에서 사용)
    ssl_ctx = None
    try:
        from drover.services import store as k3s_db

        kubeconfig_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
        if kubeconfig_yaml:
            cert_pem, key_pem = _parse_kubeconfig(kubeconfig_yaml)
            ssl_ctx = _make_ssl_context(cert_pem, key_pem)
    except Exception:
        _logger.debug("k3s health: kubeconfig 로드 실패 (cluster=%s)", cluster_id, exc_info=True)

    # 1단계: /healthz 프로빙 (kubeconfig가 있으면 인증 포함)
    healthz_ok = False
    api_server_reachable = False
    error_msg = None

    try:
        verify = ssl_ctx if ssl_ctx else False
        async with httpx.AsyncClient(verify=verify, timeout=httpx.Timeout(5.0, connect=3.0)) as client:
            resp = await client.get(f"{base_url}/healthz")
            api_server_reachable = True
            healthz_ok = resp.status_code == 200 and resp.text.strip() == "ok"
    except (httpx.ConnectError, httpx.TimeoutException):
        error_msg = f"{probe_ip}:6443 에 연결할 수 없습니다 (tenant-only 네트워크일 수 있음)"
        return K3sClusterHealth(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            status="UNREACHABLE",
            api_server_reachable=False,
            healthz_ok=False,
            checked_at=checked_at,
            error=error_msg,
            reachability="unreachable",
        )
    except Exception as e:
        error_msg = f"healthz 프로빙 오류: {e}"

    if not healthz_ok:
        return K3sClusterHealth(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            status="UNHEALTHY",
            api_server_reachable=api_server_reachable,
            healthz_ok=False,
            checked_at=checked_at,
            error=error_msg,
            reachability="direct",
        )

    # 2단계: /api/v1/nodes 조회 (kubeconfig 클라이언트 인증서 사용)
    nodes: list[K3sNodeHealth] = []
    if ssl_ctx:
        try:
            async with httpx.AsyncClient(verify=ssl_ctx, timeout=10.0) as client:
                resp = await client.get(
                    f"{base_url}/api/v1/nodes",
                    headers={"Accept": "application/json"},
                )
                if resp.status_code == 200:
                    nodes = _parse_nodes(resp.json())
        except Exception:
            _logger.debug("k3s health: 노드 목록 조회 실패 (cluster=%s)", cluster_id, exc_info=True)

    # 상태 결정
    if not nodes:
        status = "HEALTHY" if healthz_ok else "UNHEALTHY"
    else:
        all_ready = all(n.ready for n in nodes)
        any_ready = any(n.ready for n in nodes)
        if all_ready:
            status = "HEALTHY"
        elif any_ready:
            status = "DEGRADED"
        else:
            status = "UNHEALTHY"

    return K3sClusterHealth(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        status=status,
        api_server_reachable=True,
        healthz_ok=True,
        nodes=nodes,
        checked_at=checked_at,
        reachability="direct",
    )


def _parse_nodes(api_response: dict) -> list[K3sNodeHealth]:
    """Kubernetes /api/v1/nodes 응답에서 K3sNodeHealth 목록 추출."""
    result = []
    for item in api_response.get("items", []):
        name = item.get("metadata", {}).get("name", "")
        labels = item.get("metadata", {}).get("labels", {})
        is_cp = "node-role.kubernetes.io/control-plane" in labels or "node-role.kubernetes.io/master" in labels
        role = "server" if is_cp else "agent"
        kubelet_version = item.get("status", {}).get("nodeInfo", {}).get("kubeletVersion")

        conditions = item.get("status", {}).get("conditions", [])
        ready = False
        cond_names = []
        for cond in conditions:
            cond_type = cond.get("type", "")
            cond_status = cond.get("status", "False")
            if cond_type == "Ready":
                ready = cond_status == "True"
            if cond_status == "True":
                cond_names.append(cond_type)

        result.append(
            K3sNodeHealth(
                name=name,
                role=role,
                ready=ready,
                conditions=cond_names,
                kubelet_version=kubelet_version,
            )
        )
    return result


# ---------------------------------------------------------------------------
# 전체 클러스터 순회 (워커에서 호출)
# ---------------------------------------------------------------------------


async def check_all_active_clusters() -> None:
    """모든 ACTIVE K3s 클러스터를 순회하며 헬스체크 후 Redis에 저장."""
    from drover.services import store as k3s_db

    try:
        all_clusters = await k3s_db.list_all_clusters(include_deleted=False)
    except Exception:
        _logger.warning("k3s health: 클러스터 목록 조회 실패", exc_info=True)
        return

    active = [c for c in all_clusters if c.get("status") == "ACTIVE"]
    if not active:
        _logger.debug("k3s health: ACTIVE 클러스터 없음")
        return

    _logger.info("k3s health: %d개 ACTIVE 클러스터 헬스체크 시작", len(active))
    for cluster in active:
        try:
            result = await check_cluster_health(cluster)
            await store_health_result(cluster["id"], result)
            _logger.debug(
                "k3s health: cluster=%s status=%s nodes=%d",
                cluster["id"],
                result.status,
                len(result.nodes),
            )
        except Exception:
            _logger.warning("k3s health: 클러스터 %s 체크 중 오류", cluster["id"], exc_info=True)
