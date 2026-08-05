"""k3s 인증서 API — CA 다운로드, 만료 조회, 인증서 회전."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, StreamingResponse

from drover.auth import CacheMode, cache_mode, get_token_info
from drover.config import get_settings
from drover.models.schemas import CertificateExpiryResponse, CertificateInfo
from drover.services import store as k3s_db
from drover.services.cache import cached_call
from drover.services.cache import keys as cache_keys
from drover.services.cert_rotation import acquire_rotation_lock, release_rotation_lock, rotate_certificates

router = APIRouter()
_logger = logging.getLogger(__name__)

_CERT_EXPIRY_TTL = 3600  # 1h

_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
    "Content-Encoding": "identity",
}


async def _get_kubeconfig_for_user(project_id: str, cluster_id: str) -> str:
    """클러스터 접근 권한 확인 + kubeconfig 반환. 없으면 404."""
    cluster = await k3s_db.get_cluster(project_id, cluster_id)
    if cluster is None:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다.")
    kc = await k3s_db.get_kubeconfig(project_id, cluster_id)
    if not kc:
        raise HTTPException(
            status_code=404, detail="kubeconfig를 찾을 수 없습니다. 클러스터가 아직 초기화 중일 수 있습니다."
        )
    return kc


@router.get("/{cluster_id}/ca-certificate")
async def download_ca_certificate(
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
):
    """k3s 클러스터 CA 인증서 PEM 다운로드."""
    project_id = token_info["project_id"]
    kc = await _get_kubeconfig_for_user(project_id, cluster_id)

    from drover.services.certs import extract_ca_pem

    try:
        pem = extract_ca_pem(kc)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CA 인증서 추출 실패: {e}")

    cluster = await k3s_db.get_cluster(project_id, cluster_id)
    cluster_name = (cluster or {}).get("name", cluster_id)
    _logger.info("k3s CA 다운로드: cluster=%s project=%s", cluster_id, project_id)

    return Response(
        content=pem.encode(),
        media_type="application/x-pem-file",
        headers={"Content-Disposition": f'attachment; filename="ca-{cluster_name}.pem"'},
    )


@router.get("/{cluster_id}/certificate-expiry", response_model=CertificateExpiryResponse)
async def get_certificate_expiry(
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
    cm: CacheMode = Depends(cache_mode),
):
    """k3s 클러스터 인증서 만료 조회 (CA, 클라이언트, 서버 TLS)."""
    project_id = token_info["project_id"]
    kc = await _get_kubeconfig_for_user(project_id, cluster_id)

    cache_key = cache_keys.project_key("k3s", project_id, cluster_id, sub="cert_expiry")

    async def _compute() -> dict:
        from drover.services.certs import parse_kubeconfig_certs, probe_tls_server_cert

        certs = parse_kubeconfig_certs(kc)

        # 서버 IP 추출 → TLS 프로브
        server_via_tls: list[dict] = []
        try:
            import yaml

            parsed = yaml.safe_load(kc)
            server_url = parsed["clusters"][0]["cluster"]["server"]
            # https://<host>:<port>
            host = server_url.split("//")[-1].split(":")[0]
            port_str = server_url.split(":")[-1].split("/")[0]
            port = int(port_str) if port_str.isdigit() else 6443
            server_via_tls = await probe_tls_server_cert(host, port)
        except Exception as e:
            _logger.debug("TLS 프로브 실패: %s", e)

        return {
            "ca": certs.get("ca"),
            "client": certs.get("client"),
            "server_via_tls": server_via_tls,
        }

    data = await cached_call(cache_key, _CERT_EXPIRY_TTL, _compute, enabled=cm.enabled, refresh=cm.refresh)

    def _to_info(d: dict | None) -> CertificateInfo | None:
        if not d:
            return None
        return CertificateInfo(**d)

    return CertificateExpiryResponse(
        ca=_to_info(data.get("ca")),
        client=_to_info(data.get("client")),
        server_via_tls=[CertificateInfo(**s) for s in (data.get("server_via_tls") or [])],
    )


@router.post("/{cluster_id}/rotate-certs")
async def rotate_cluster_certs(
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
):
    """k3s 인증서 rolling 회전 (SSE 스트림).

    각 control-plane 노드에 K8s Job을 생성해 systemctl restart k3s를 실행한다.
    k3s는 재시작 시 만료 90일 이내 인증서를 자동 갱신한다.
    단일 마스터 클러스터는 재시작 중 API 서버 다운타임이 발생하므로 422 반환.
    """
    project_id = token_info["project_id"]

    # 클러스터 확인
    cluster = await k3s_db.get_cluster(project_id, cluster_id)
    if cluster is None:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다.")
    if cluster.get("status") not in ("ACTIVE", "ERROR"):
        raise HTTPException(status_code=409, detail="클러스터가 ACTIVE 상태가 아닙니다.")

    master_count = cluster.get("master_count", 1)
    if master_count < 3:
        raise HTTPException(
            status_code=422,
            detail="단일 마스터 클러스터는 인증서 회전 중 API 서버 다운타임이 발생합니다. HA(master_count≥3) 클러스터에서만 지원합니다.",
        )

    # Redis 분산 락 (동시 회전 방지)
    if not await acquire_rotation_lock(cluster_id):
        raise HTTPException(status_code=409, detail="이미 진행 중인 인증서 회전 작업이 있습니다.")

    settings = get_settings()
    initiated_by = token_info.get("username", token_info.get("user_id", "unknown"))

    async def _gen():
        try:
            async for msg in rotate_certificates(
                cluster_id,
                project_id,
                initiated_by,
                node_timeout=float(settings.drover_cert_rotation_node_timeout_sec),
                job_image=settings.drover_cert_rotation_job_image,
            ):
                yield f"data: {msg.model_dump_json()}\n\n"
        finally:
            await release_rotation_lock(cluster_id)

    return StreamingResponse(_gen(), media_type="text/event-stream", headers=_SSE_HEADERS)
