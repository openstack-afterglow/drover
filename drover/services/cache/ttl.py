"""TTL 카테고리 + jitter + dynamic 조정.

3-tier TTL 카테고리:
  identity_stable  — 86400s (1일): 개인 프로필, role/group 멤버십
  catalog_slow     — 900s  (15분): flavors, glance image 메타, 데이터스토어, 클러스터 템플릿
  project_meta     — 300s  (5분):  keypair, SG 정의, 네트워크 메타
  operational_live — 30s         : instances/volumes/FIP/컨테이너/k3s 상태 list & detail
  admin_overview   — 60s         : admin 토폴로지, 하이퍼바이저
  auth_token       — 60s         : Keystone 토큰 검증 결과 (deps.py 의 _TOKEN_CACHE_TTL 유지)

Dynamic 조정 (mutation count 기반):
  mutcount/hr < LOW_THRESHOLD           → 기본 카테고리 TTL 유지
  LOW_THRESHOLD ≤ mutcount < HIGH       → TTL × 0.5
  mutcount ≥ HIGH_THRESHOLD            → operational_live (30s) 강제 다운그레이드

Jitter: 모든 TTL 에 ±15% 랜덤 jitter 적용 (stampede 완화).
"""

from __future__ import annotations

import logging
import random

from drover.config import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 기존 4-티어 helpers (하위 호환 — 85 개 호출처 유지)
# ---------------------------------------------------------------------------


def ttl_fast() -> int:
    """빈번히 변하는 리소스 TTL (인스턴스, 볼륨, 플로팅IP 등)."""
    return get_settings().cache_ttl_fast


def ttl_normal() -> int:
    """일반 리소스 TTL (네트워크, 라우터, 토폴로지 등)."""
    return get_settings().cache_ttl_normal


def ttl_slow() -> int:
    """느리게 변하는 리소스 TTL (키페어, 보안그룹 등)."""
    return get_settings().cache_ttl_slow


def ttl_static() -> int:
    """거의 변하지 않는 리소스 TTL (이미지, 플레이버 등)."""
    return get_settings().cache_ttl_static


# ---------------------------------------------------------------------------
# 3-tier 카테고리 시스템
# ---------------------------------------------------------------------------

# 카테고리명 → config 필드명 매핑
_CATEGORY_FIELD: dict[str, str] = {
    "identity_stable": "cache_ttl_identity_stable",
    "catalog_slow": "cache_ttl_catalog_slow",
    "project_meta": "cache_ttl_project_meta",
    "operational_live": "cache_ttl_operational_live",
    "admin_overview": "cache_ttl_admin_overview",
    "auth_token": "cache_ttl_auth_token",
}

# 카테고리명 → 기본값 (config 필드 없을 때 fallback)
_CATEGORY_DEFAULT: dict[str, int] = {
    "identity_stable": 86400,
    "catalog_slow": 900,
    "project_meta": 300,
    "operational_live": 30,
    "admin_overview": 60,
    "auth_token": 60,
}

_JITTER_RATIO = 0.15  # ±15%


def jittered(ttl: int) -> int:
    """TTL 에 ±15% 랜덤 jitter 적용. stampede 완화."""
    delta = int(ttl * _JITTER_RATIO)
    return ttl + random.randint(-delta, delta)


def _base_for_category(category: str) -> int:
    """카테고리의 기본 TTL (jitter 미적용)."""
    settings = get_settings()
    field = _CATEGORY_FIELD.get(category)
    if field and hasattr(settings, field):
        return getattr(settings, field)
    return _CATEGORY_DEFAULT.get(category, settings.cache_ttl_normal)


def dynamic_adjust(base: int, mutcount: int) -> int:
    """mutation 횟수에 따라 TTL 을 동적 단축.

    mutcount < LOW  → 기본 TTL
    LOW ≤ count < HIGH → TTL × 0.5
    count ≥ HIGH        → operational_live (30s) 강제 다운그레이드
    """
    settings = get_settings()
    low = settings.cache_dynamic_threshold_low
    high = settings.cache_dynamic_threshold_high

    if mutcount < low:
        return base
    if mutcount < high:
        return max(int(base * 0.5), _CATEGORY_DEFAULT["operational_live"])
    return _CATEGORY_DEFAULT["operational_live"]


async def for_category(
    category: str,
    *,
    service: str | None = None,
    project_id: str | None = None,
) -> int:
    """카테고리 TTL 을 동적 조정 + jitter 적용하여 반환.

    operational_live 이하 카테고리이고 service/project_id 가 제공된 경우
    mutation count 를 조회해 dynamic 단축을 적용한다.
    """
    base = _base_for_category(category)

    if category in ("operational_live", "project_meta") and service and project_id:
        mutcount = await _read_mutcount(service, project_id)
        base = dynamic_adjust(base, mutcount)

    return jittered(base)


async def _read_mutcount(service: str, project_id: str) -> int:
    """Redis 에서 mutation count 를 읽는다. 실패 시 0 반환 (silent fail)."""
    from drover.services.cache.keys import mutcount_key  # 지연 임포트

    try:
        from drover.services.cache import _get_backend  # noqa: PLC0415

        raw = await _get_backend().get(mutcount_key(service, project_id))
        return int(raw.decode() if isinstance(raw, bytes) else raw) if raw else 0
    except Exception as exc:
        logger.debug("mutcount 조회 실패 (%s/%s): %s", service, project_id, exc)
        return 0
