"""Redis 기반 캐시 레이어 — Phase A' 패키지 진입점.

레거시 cache.py 와 동일한 import 경로를 유지하기 위한 backwards-compat wrapper.

키 형식: afterglow:{service}:{project_id}:{resource}
예시: afterglow:nova:abc123:servers

레거시 API (계속 동작 — ~85 call sites):
- cached_call(key, ttl, fn, *, refresh=False, enabled=True)
- invalidate(pattern)
- invalidate_project(project_id, service="*")
- ttl_fast / ttl_normal / ttl_slow / ttl_static
- _get_redis() / _get_client() (object_storage, instance_health, admin_identity 등)

신규 API:
- backend (전역 RedisBackend 싱글톤)
- get_backend() (테스트 주입용 접근자)
- base.Cache (ABC)
- redis_backend.RedisBackend
- keys.* / metrics / invalidation
- write_through(key, ttl, value) — terminal mutation 직후 known-value 직접 set
- patch_list(key, ttl, *, match, update/remove/add) — list 엔트리 surgical 패치

캐시 write 정책:
- read-through (cached_call): 캐시 미스 시 fn 실행 후 저장 — 허용.
- write-through (write_through / patch_list): terminal mutation 직후 핸들러가 이미
  쥔 최종 값을 직접 set — 허용, 단 전이 상태(BUILD/creating/deleting 등) 리소스에는
  사용 금지. 전이 상태 mutation 에는 invalidate() 만 사용한다.
- 임의 set(): mutation 핸들러에서 직접 backend.set() / backend.get() 후 set() 패턴
  금지 — 위 두 헬퍼를 통해서만 캐시에 쓴다.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

import redis.asyncio as aioredis

from drover.services.cache import metrics
from drover.services.cache.base import Cache
from drover.services.cache.redis_backend import RedisBackend
from drover.services.cache.ttl import ttl_fast, ttl_normal, ttl_slow, ttl_static

_UNSET = object()  # patch_list 내부 sentinel

logger = logging.getLogger(__name__)


_backend: Cache | None = None


def _get_backend() -> Cache:
    """프로세스 전역 캐시 백엔드를 반환 (필요 시 lazy 생성)."""
    global _backend
    if _backend is None:
        _backend = RedisBackend()
    return _backend


def get_backend() -> Cache:
    """외부 노출 백엔드 접근자 — 테스트에서 set_backend() 와 페어로 사용."""
    return _get_backend()


def set_backend(backend: Cache | None) -> None:
    """테스트 전용 — 백엔드 주입 / 리셋."""
    global _backend
    _backend = backend


def _make_serializable(obj: Any) -> Any:
    """Pydantic 모델 등을 JSON 직렬화 가능한 형태로 변환."""
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, list):
        return [_make_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    return obj


def _get_client() -> aioredis.Redis:
    """레거시 호환 — 동기 진입점에서도 raw redis 클라이언트를 반환.

    cache.py 시절 일부 호출부 (compute/instance_health.py) 가 사용.
    """
    backend = _get_backend()
    if isinstance(backend, RedisBackend):
        return backend.raw_client()
    raise RuntimeError("legacy _get_client() requires RedisBackend; current backend is incompatible")


async def _get_redis() -> aioredis.Redis:
    """레거시 호환 — raw redis 클라이언트를 반환 (캐시 레이어 외부 직접 접근용)."""
    return _get_client()


async def cached_call(
    key: str,
    ttl: int,
    fn: Callable[[], Any],
    *,
    refresh: bool = False,
    enabled: bool = True,
) -> Any:
    """캐시에서 값을 가져오거나, 없으면 fn을 실행하여 저장 후 반환.

    fn 이 동기 함수인 경우 asyncio.to_thread 로 실행한다.
    Redis 연결 실패 시 캐시 없이 fn 을 직접 실행한다 (silent fail).
    refresh=True 이면 기존 캐시를 삭제하고 fn 을 강제 실행한다.
    enabled=False 이면 캐시 read/write 를 모두 건너뛰고 fn 만 실행한다
    (인프라 캐시 — 토큰 검증, prewarm 등은 enabled 를 생략해 기본 True 유지).
    """
    if not enabled:
        metrics.increment("cache.disabled")
        if asyncio.iscoroutinefunction(fn):
            return await fn()
        return await asyncio.to_thread(fn)

    backend = _get_backend()

    if refresh:
        try:
            await backend.delete(key)
        except Exception:
            pass

    # 1) 캐시 hit 시도
    try:
        cached = await backend.get(key)
        if cached is not None:
            metrics.increment("cache.hit")
            try:
                return json.loads(cached)
            except (TypeError, ValueError) as e:
                # 손상된 캐시: 메트릭만 올리고 fn 으로 폴백
                metrics.increment("cache.error")
                logger.warning("캐시 JSON 디코드 실패 (%s): %s", key, e)
    except Exception as e:
        metrics.increment("cache.error")
        logger.warning("캐시 읽기 실패 (%s): %s", key, e)

    # 2) 캐시 미스 — fn 실행
    metrics.increment("cache.miss")
    if asyncio.iscoroutinefunction(fn):
        result = await fn()
    else:
        result = await asyncio.to_thread(fn)

    # 3) 결과를 캐시에 저장 (read-through 경로 — 허용)
    try:
        payload = json.dumps(_make_serializable(result))
        await backend.set(key, payload, ttl)
    except Exception as e:
        metrics.increment("cache.error")
        logger.warning("캐시 쓰기 실패 (%s): %s", key, e)

    return result


async def invalidate(pattern: str) -> None:
    """패턴에 매칭되는 캐시 키를 모두 삭제.

    KEYS 대신 SCAN 을 사용해 Redis 블로킹을 방지. 와일드카드(`*`) 가 없는 경우
    delete() 단일 호출로 최적화한다. backend 가 RedisBackend 가 아닌 경우
    (Memcached v2) 와일드카드 패턴은 무시되고 정확 키만 삭제된다.
    """
    backend = _get_backend()
    try:
        # 와일드카드가 없으면 단일 키 삭제로 처리
        if "*" not in pattern and "?" not in pattern and "[" not in pattern:
            await backend.delete(pattern)
            return

        # 와일드카드 — SCAN 사용 (RedisBackend 전용 _scan)
        if isinstance(backend, RedisBackend):
            keys_to_delete = await backend._scan(pattern)
            if keys_to_delete:
                await backend.delete(*keys_to_delete)
        else:
            # SCAN 없는 백엔드 — Memcached v2 호환 경로
            logger.debug("backend does not support pattern invalidation (%s)", pattern)
    except Exception as e:
        metrics.increment("cache.error")
        logger.warning("캐시 삭제 실패 (%s): %s", pattern, e)


async def invalidate_project(project_id: str, service: str = "*") -> None:
    """특정 프로젝트의 특정 서비스(또는 전체) 캐시를 삭제."""
    await invalidate(f"afterglow:{service}:{project_id}:*")


async def write_through(key: str, ttl: int, value: Any) -> None:
    """terminal mutation 직후, 핸들러가 이미 쥔 최종 값을 캐시에 직접 set.

    origin 재조회·invalidate 선행 없음. 전이 상태(BUILD 등)에는 사용 금지.
    요청 핸들러 동기 경로(get_os_conn finally 이전)에서 호출해야 한다.
    """
    backend = _get_backend()
    try:
        payload = json.dumps(_make_serializable(value))
        await backend.set(key, payload, ttl)
        metrics.increment("cache.write_through")
    except Exception as e:
        metrics.increment("cache.error")
        logger.warning("write_through 실패 (%s): %s", key, e)


async def patch_list(
    key: str,
    ttl: int,
    *,
    match: Callable[[Any], bool] | str | None = None,
    update: Any = None,
    remove: bool = False,
    add: Any = None,
) -> None:
    """캐시된 list 를 읽어 match 로 찾은 엔트리만 surgical 수정/제거/추가 후 set.

    캐시 miss(키 없음)면 no-op — 다음 cache 조회가 read-through 로 채운다.
    origin 재조회 없음. terminal mutation 전용.

    Args:
        key:    캐시 키.
        ttl:    갱신 시 적용할 TTL (GET 핸들러와 동일한 값 사용 권장).
        match:  callable(item) → bool, 또는 id 문자열(dict 의 "id" 필드 비교).
                add 만 사용할 때는 None 가능.
        update: match 된 엔트리를 이 값으로 교체(Pydantic 모델 또는 dict).
        remove: True 면 match 된 엔트리를 목록에서 제거.
        add:    목록 끝에 추가할 객체(match 불필요). update/remove 보다 우선.
    """
    backend = _get_backend()
    try:
        cached = await backend.get(key)
        if cached is None:
            return  # 캐시 miss — no-op

        try:
            items: list = json.loads(cached)
        except (TypeError, ValueError):
            return  # 손상된 캐시 — no-op

        if not isinstance(items, list):
            return

        if add is not None:
            items = [*items, _make_serializable(add)]
        elif match is not None:
            if callable(match):
                matcher = match
            else:
                _mid = match

                def matcher(item: Any, _id: str = _mid) -> bool:  # noqa: E731
                    return isinstance(item, dict) and item.get("id") == _id

            if remove:
                items = [item for item in items if not matcher(item)]
            elif update is not None:
                serialized = _make_serializable(update)
                items = [serialized if matcher(item) else item for item in items]

        payload = json.dumps(items)
        await backend.set(key, payload, ttl)
        metrics.increment("cache.patch_list")
    except Exception as e:
        metrics.increment("cache.error")
        logger.warning("patch_list 실패 (%s): %s", key, e)


__all__ = [
    # 레거시 호환
    "cached_call",
    "invalidate",
    "invalidate_project",
    "ttl_fast",
    "ttl_normal",
    "ttl_slow",
    "ttl_static",
    "_get_redis",
    "_get_client",
    "_make_serializable",
    # 신규
    "Cache",
    "RedisBackend",
    "get_backend",
    "set_backend",
    "metrics",
    # write-through (terminal mutation 전용)
    "write_through",
    "patch_list",
]
