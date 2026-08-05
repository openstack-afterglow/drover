"""K3s 클러스터 헬스 조회 API.

워커 파드가 Redis에 저장한 헬스 결과를 반환.
실시간 프로빙은 POST /{cluster_id}/health/check 엔드포인트 사용.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from drover.auth import get_token_info
from drover.models.schemas import K3sClusterHealth
from drover.rate_limit import limiter
from drover.services import store as k3s_cluster
from drover.services import health as k3s_health

router = APIRouter()
_logger = logging.getLogger(__name__)
_limiter = Limiter(key_func=get_remote_address)


@router.get("", response_model=list[K3sClusterHealth])
async def list_cluster_health(
    token_info: dict = Depends(get_token_info),
):
    """프로젝트 내 모든 K3s 클러스터의 헬스 상태 목록 반환."""
    project_id = token_info.get("project_id", "")
    try:
        clusters = await k3s_cluster.list_clusters(project_id)
        cluster_ids = [c["id"] for c in clusters if c.get("status") == "ACTIVE"]
        return await k3s_health.get_health_results_for_clusters(cluster_ids)
    except Exception:
        _logger.exception("k3s health 목록 조회 실패")
        raise HTTPException(status_code=500, detail="헬스 상태 조회 실패")


@router.get("/{cluster_id}/health", response_model=K3sClusterHealth)
async def get_cluster_health(
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
):
    """단일 K3s 클러스터의 최신 헬스 상태 반환 (Redis 캐시)."""
    project_id = token_info.get("project_id", "")

    # 클러스터 소유권 확인
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    result = await k3s_health.get_health_result(cluster_id)
    if result is None:
        # 캐시 미스 — 워커가 아직 체크하지 않은 경우
        raise HTTPException(status_code=404, detail="헬스 데이터 없음 (아직 체크되지 않았습니다)")

    return result


@router.post("/{cluster_id}/health/check", response_model=K3sClusterHealth)
@limiter.limit("3/minute")
async def trigger_health_check(
    request: Request,
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
):
    """즉시 헬스체크 트리거 (rate-limited: 3회/분).

    워커 주기를 기다리지 않고 즉시 결과를 얻을 때 사용.
    """
    project_id = token_info.get("project_id", "")

    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    try:
        # project_id를 cluster dict에 추가 (list_all_clusters 결과와 동일한 형태)
        cluster_with_pid = {**cluster, "project_id": project_id}
        result = await k3s_health.check_cluster_health(cluster_with_pid)
        await k3s_health.store_health_result(cluster_id, result)
        return result
    except Exception:
        _logger.exception("k3s health 즉시 체크 실패 (cluster=%s)", cluster_id)
        raise HTTPException(status_code=500, detail="헬스체크 실패")
