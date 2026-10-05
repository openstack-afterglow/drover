"""Redis client construction and pinned-client lifecycle contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from redis.asyncio import Redis

from drover import cache
from drover.services.cache.redis_backend import RedisBackend


def test_sentinel_master_receives_decoded_redis_password() -> None:
    settings = SimpleNamespace(
        sentinel_hosts="cache-1:26379,cache-2",
        sentinel_master_name="valkey",
        redis_url="redis://:p%40ssword@cache-master:6379/7",
    )
    sentinel = MagicMock()

    with patch("redis.asyncio.sentinel.Sentinel", return_value=sentinel) as sentinel_factory:
        RedisBackend._build_sentinel_client(settings)

    sentinel_factory.assert_called_once_with(
        [("cache-1", 26379), ("cache-2", 26379)],
        socket_timeout=5,
        socket_connect_timeout=3,
    )
    sentinel.master_for.assert_called_once_with(
        "valkey",
        decode_responses=True,
        socket_keepalive=True,
        health_check_interval=30,
        password="p@ssword",
    )


def test_sentinel_master_omits_password_for_unauthenticated_redis() -> None:
    settings = SimpleNamespace(
        sentinel_hosts="cache-1:26379",
        sentinel_master_name="valkey",
        redis_url="redis://cache-master:6379/7",
    )
    sentinel = MagicMock()

    with patch("redis.asyncio.sentinel.Sentinel", return_value=sentinel):
        RedisBackend._build_sentinel_client(settings)

    assert "password" not in sentinel.master_for.call_args.kwargs


async def test_close_cache_disconnects_pinned_redis_and_clears_client(monkeypatch):
    client = Redis.from_url("redis://127.0.0.1:1/0")
    disconnect = AsyncMock()
    monkeypatch.setattr(client.connection_pool, "disconnect", disconnect)
    monkeypatch.setattr(cache, "_client", client)

    await cache.close_cache()

    disconnect.assert_awaited_once_with()
    assert cache._client is None
    await cache.close_cache()
    disconnect.assert_awaited_once_with()
