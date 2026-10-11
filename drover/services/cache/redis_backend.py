"""Redis/Valkey 백엔드 (Cache ABC 구현).

- 모든 메서드는 silent fail: 백엔드 장애 시 의미 있는 기본값을 반환.
- _scan(pattern) 은 Memcached v2 호환을 깨므로 ABC 에 노출되지 않는다.
  레거시 invalidate(pattern) 호환을 위해 backend 내부에서만 노출.
- decode_responses=True 로 클라이언트를 생성하므로 get() 의 반환 타입은
  실질적으로 `str | None` 이다. 기존 cache.py 동작과 일관성을 유지하기 위해
  ABC 시그니처는 bytes | None 으로 두되, redis-py 의 디코딩 모드에 위임한다.
"""

from __future__ import annotations

import logging

import redis.asyncio as aioredis
from redis.asyncio.connection import SSLConnection, UnixDomainSocketConnection, parse_url

from drover.config import get_settings
from drover.services.cache import metrics
from drover.services.cache.base import Cache

logger = logging.getLogger(__name__)


TAG_SET_TTL = 86400  # 태그 SET 자체의 TTL (1일) — 고아 태그 자동 정리


class RedisBackend(Cache):
    """Redis/Valkey 기반 Cache 구현체."""

    def __init__(self, *, client: aioredis.Redis | None = None) -> None:
        self._client = client
        self._sentinel = None

    # ── 클라이언트 lazy 초기화 ────────────────────────────────────────────────
    def _get_client(self) -> aioredis.Redis:
        if self._client is None:
            settings = get_settings()
            if settings.sentinel_enabled:
                self._client = self._build_sentinel_client(settings)
            else:
                self._client = aioredis.from_url(
                    settings.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=3,
                    socket_timeout=5,
                    socket_keepalive=True,
                    health_check_interval=30,
                )
        return self._client

    def _build_sentinel_client(self, settings) -> aioredis.Redis:
        """Redis Sentinel HA 클라이언트를 빌드한다.

        sentinel_hosts: 콤마 구분 "host:port" 목록.
        master_for() kwargs로 decode_responses=True 를 전달해 str 반환 보장.
        """
        from redis.asyncio.sentinel import Sentinel, SentinelManagedSSLConnection

        hosts = []
        for entry in settings.sentinel_hosts.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                host, port_str = entry.rsplit(":", 1)
                hosts.append((host, int(port_str)))
            else:
                hosts.append((entry, 26379))

        # Preserve the same database, credentials and URL options as from_url().
        # Sentinel supplies only the current master's address, not its identity.
        options = parse_url(settings.redis_url)
        options.pop("host", None)
        options.pop("port", None)
        connection_class = options.pop("connection_class", None)
        if connection_class is UnixDomainSocketConnection:
            raise ValueError("Sentinel requires a TCP Redis URL")
        if connection_class is SSLConnection:
            options["connection_class"] = SentinelManagedSSLConnection
        self._sentinel = Sentinel(hosts, socket_timeout=5, socket_connect_timeout=3)
        master_kwargs = {
            "encoding": "utf-8",
            "decode_responses": True,
            "socket_keepalive": True,
            "health_check_interval": 30,
            **options,
        }
        return self._sentinel.master_for(settings.sentinel_master_name, **master_kwargs)

    # ── 기본 ABC 구현 ────────────────────────────────────────────────────────
    async def get(self, key: str) -> bytes | None:
        try:
            value = await self._get_client().get(key)
            return value
        except Exception as e:
            metrics.increment("cache.error")
            logger.warning("캐시 읽기 실패 (%s): %s", key, e)
            return None

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        """read-through 경로 전용. mutation 응답 직후 호출 금지."""
        try:
            await self._get_client().setex(key, ttl, value)
            metrics.increment("cache.write")
        except Exception as e:
            metrics.increment("cache.error")
            logger.warning("캐시 쓰기 실패 (%s): %s", key, e)

    async def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        try:
            count = await self._get_client().delete(*keys)
            metrics.increment("cache.invalidate")
            return int(count or 0)
        except Exception as e:
            metrics.increment("cache.error")
            logger.warning("캐시 삭제 실패 (%s): %s", keys, e)
            return 0

    async def incr(self, key: str, *, ttl: int | None = None) -> int:
        try:
            client = self._get_client()
            value = await client.incr(key)
            if ttl is not None:
                try:
                    await client.expire(key, ttl)
                except Exception:
                    # EXPIRE 실패는 경고만 — incr 결과는 살린다
                    pass
            return int(value or 0)
        except Exception as e:
            metrics.increment("cache.error")
            logger.warning("INCR 실패 (%s): %s", key, e)
            return 0

    async def add_to_tag(self, tag: str, key: str) -> None:
        full_tag = self._normalize_tag(tag)
        try:
            client = self._get_client()
            await client.sadd(full_tag, key)
            try:
                await client.expire(full_tag, TAG_SET_TTL)
            except Exception:
                pass
        except Exception as e:
            metrics.increment("cache.error")
            logger.warning("태그 추가 실패 (%s, %s): %s", full_tag, key, e)

    async def invalidate_tag(self, tag: str) -> int:
        full_tag = self._normalize_tag(tag)
        try:
            client = self._get_client()
            members = await client.smembers(full_tag)
            if not members:
                # 태그 SET 자체만 정리 (있으면)
                await client.delete(full_tag)
                metrics.increment("cache.invalidate")
                return 0
            keys = list(members)
            keys.append(full_tag)
            count = await client.delete(*keys)
            metrics.increment("cache.invalidate")
            # tag 자체 삭제는 반환 카운트에서 제외 (실제 캐시 키만 카운트)
            return max(0, int(count or 0) - 1)
        except Exception as e:
            metrics.increment("cache.error")
            logger.warning("태그 무효화 실패 (%s): %s", full_tag, e)
            return 0

    async def ping(self) -> bool:
        try:
            return bool(await self._get_client().ping())
        except Exception:
            return False

    async def close(self) -> None:
        client, sentinel = self._client, self._sentinel
        self._client = self._sentinel = None
        clients = ([client] if client is not None else [])
        if sentinel is not None:
            clients.extend(sentinel.sentinels)
        for owned_client in clients:
            try:
                # Sentinel supplies a pool; explicitly close that pool on redis-py 5.0.
                await owned_client.close(close_connection_pool=True)
            except Exception:
                pass

    # ── 레거시 호환 (ABC 외부) ────────────────────────────────────────────────
    async def _scan(self, pattern: str) -> list[str]:
        """패턴에 매칭되는 모든 키를 반환 (SCAN 기반).

        Memcached v2 호환을 깨므로 ABC 에 노출하지 않는다.
        레거시 invalidate(pattern) 헬퍼 전용.
        """
        client = self._get_client()
        keys: list[str] = []
        try:
            async for key in client.scan_iter(match=pattern, count=100):
                keys.append(key)
        except Exception as e:
            metrics.increment("cache.error")
            logger.warning("SCAN 실패 (%s): %s", pattern, e)
        return keys

    def raw_client(self) -> aioredis.Redis:
        """Return the same writer used by cache, callback and lock consumers."""
        return self._get_client()

    # ── 내부 ─────────────────────────────────────────────────────────────────
    @staticmethod
    def _normalize_tag(tag: str) -> str:
        """`tag:` 접두가 없으면 붙여서 키 네임스페이스를 일관되게 유지."""
        if tag.startswith("tag:"):
            return tag
        # service 와 project_id 가 콜론 두 개로 들어온 경우 그대로 prefix
        return f"tag:{tag}"


__all__ = ["RedisBackend", "TAG_SET_TTL"]
