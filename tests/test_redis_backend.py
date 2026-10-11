"""Sentinel callback storage must retain Drover's Redis database boundary."""

from types import SimpleNamespace
from unittest.mock import patch

import fakeredis.aioredis

from drover.services import redis_store
from drover.services.cache import redis_backend
from drover.services.cache.redis_backend import RedisBackend


async def test_sentinel_callback_tokens_remain_in_configured_database(monkeypatch):
    server = fakeredis.FakeServer()
    database_seven = fakeredis.aioredis.FakeRedis(server=server, db=7, decode_responses=True)
    database_zero = fakeredis.aioredis.FakeRedis(server=server, db=0, decode_responses=True)
    backend = RedisBackend()
    settings = SimpleNamespace(
        sentinel_enabled=True,
        sentinel_hosts="sentinel:26379",
        sentinel_master_name="kolla",
        redis_url="redis://former-master:6379/7",
    )

    class Sentinel:
        sentinels = ()

        def __init__(self, *args, **kwargs):
            pass

        def master_for(self, name, **kwargs):
            # fakeredis cannot interleave its handshake with redis-py's health PING.
            kwargs["health_check_interval"] = 0
            return fakeredis.aioredis.FakeRedis(server=server, **kwargs)

    monkeypatch.setattr(redis_backend, "get_settings", lambda: settings)
    monkeypatch.setattr(redis_store, "_get_client", backend.raw_client)
    with patch("redis.asyncio.sentinel.Sentinel", Sentinel):
        try:
            await database_zero.set("shared-service-state", "unchanged")
            token = await redis_store.create_callback_token("project", "cluster")
            key = redis_store._callback_key(token)
            payload = await database_seven.getdel(key)
            assert payload is not None
            await database_seven.set(key, payload)
            assert await redis_store.consume_callback_token(token) == {
                "project_id": "project", "cluster_id": "cluster",
            }
            assert await redis_store.consume_callback_token(token) is None
            assert await database_zero.get("shared-service-state") == "unchanged"
            assert await database_zero.dbsize() == 1
        finally:
            await backend.close()
            await database_seven.close()
            await database_zero.close()
