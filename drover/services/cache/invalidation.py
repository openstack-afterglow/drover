"""캐시 무효화 헬퍼.

- invalidate_tag(tag): tag SET 에 속한 모든 키를 일괄 삭제.
- bump_version(service, project_id): versioned key 전략용 카운터 증가.
- invalidate_mutation_count(service, project_id): mutation 카운터 증가 (Phase B 의
  dynamic TTL 산출 입력값).

write-through forbidden 불변식: 어떤 mutation 후에도 set() 을 호출하지 않는다.
mutation 직후에는 delete() / invalidate_tag() / bump_version() / mutation_count
만 사용한다.
"""

from __future__ import annotations

import logging

from drover.services.cache import metrics
from drover.services.cache.keys import mutcount_key, version_key

logger = logging.getLogger(__name__)


MUTCOUNT_TTL = 3600  # mutation 카운터 TTL (1시간 sliding window)


async def invalidate_tag(tag: str) -> int:
    """tag 에 묶인 모든 키를 일괄 삭제. 삭제된 키 개수 반환."""
    from drover.services.cache import _get_backend  # 지연 임포트로 순환 방지

    return await _get_backend().invalidate_tag(tag)


async def bump_version(service: str, project_id: str) -> int:
    """리소스 버전 카운터를 1 증가시키고 새 값을 반환.

    Versioned key 전략: 키 구성 시 버전을 포함시키면 mutation 후 카운터만 올려도
    이전 버전 키들은 자연 만료된다. 삭제 비용 없이 무효화 가능.
    """
    from drover.services.cache import _get_backend

    return await _get_backend().incr(version_key(service, project_id))


async def invalidate_mutation_count(service: str, project_id: str) -> None:
    """뮤테이션 횟수를 1 증가 + EXPIRE 3600.

    Phase B 에서 dynamic TTL (mutation 빈도가 높은 프로젝트는 TTL 단축) 의
    입력값으로 사용된다. v1 에서는 단순히 카운터만 유지한다.
    """
    from drover.services.cache import _get_backend

    try:
        await _get_backend().incr(mutcount_key(service, project_id), ttl=MUTCOUNT_TTL)
    except Exception as e:
        metrics.increment("cache.error")
        logger.debug("mutation count 증가 실패 (%s/%s): %s", service, project_id, e)
