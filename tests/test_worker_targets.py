"""Native periodic target boundaries; local parent results recorded in section 5.6."""

from unittest.mock import AsyncMock, Mock

import pytest

from drover.services import health, stampede
from drover.services.worker_ownership import OwnershipLost


@pytest.mark.asyncio
async def test_revoked_health_owner_cannot_publish_late_probe(monkeypatch):
    cluster = {"id": "cluster-1", "project_id": "project-1", "status": "ACTIVE"}
    monkeypatch.setattr("drover.services.store.get_cluster_admin", AsyncMock(return_value=cluster))
    probe = AsyncMock(return_value=object())
    redis = AsyncMock()
    monkeypatch.setattr(health, "check_cluster_health", probe)
    monkeypatch.setattr(health, "_redis", AsyncMock(return_value=redis))
    monkeypatch.setattr(health, "checkpoint", AsyncMock(side_effect=[None, None, OwnershipLost("revoked")]))
    with pytest.raises(OwnershipLost):
        await health.check_active_cluster("cluster-1")
    probe.assert_awaited_once_with(cluster)
    redis.setex.assert_not_awaited()


@pytest.mark.asyncio
async def test_revoked_stampede_owner_cannot_merge_observed_state(monkeypatch):
    merge = AsyncMock()
    monkeypatch.setattr("drover.services.nodegroup.merge_stampede_state", merge)
    monkeypatch.setattr(stampede, "checkpoint", AsyncMock(side_effect=OwnershipLost("revoked")))
    with pytest.raises(OwnershipLost):
        await stampede._update_stampede_state("group-1", "cluster-1", {"idle_since": {}})
    merge.assert_not_awaited()


@pytest.mark.asyncio
async def test_stampede_target_does_not_rediscover_registry(monkeypatch):
    from drover.config import get_settings

    monkeypatch.setattr(get_settings(), "drover_stampede_enabled", True)
    cluster = {"id": "cluster-1", "status": "ACTIVE", "stampede_enabled": True}
    registry = AsyncMock()
    planner = AsyncMock()
    monkeypatch.setattr("drover.services.store.get_cluster_admin", AsyncMock(return_value=cluster))
    monkeypatch.setattr("drover.services.store.list_all_clusters", registry)
    monkeypatch.setattr(stampede, "reconcile_cluster", planner)
    await stampede.run_cluster("cluster-1")
    planner.assert_awaited_once_with(cluster)
    registry.assert_not_awaited()


def test_worker_coordinator_respects_native_sentinel_selection(monkeypatch):
    from drover.config import get_settings
    from drover.services.cache.redis_backend import RedisBackend
    from drover.worker import WorkerRuntime

    settings = get_settings()
    monkeypatch.setattr(settings, "sentinel_enabled", True)
    monkeypatch.setattr(settings, "sentinel_master_name", "synthetic-master")
    monkeypatch.setattr(settings, "sentinel_hosts", "127.0.0.1:26379")
    selected = Mock()
    build = Mock(return_value=selected)
    direct = Mock(side_effect=AssertionError("Sentinel enabled must not use direct Redis URL"))
    monkeypatch.setattr(RedisBackend, "_build_sentinel_client", staticmethod(build))
    monkeypatch.setattr("drover.services.cache.redis_backend.aioredis.from_url", direct)
    first = WorkerRuntime._coordinator()
    second = WorkerRuntime._coordinator()
    assert first.redis is selected and second.redis is selected
    assert build.call_count == 2  # Independent backend instances, no global loop-bound client.
    direct.assert_not_called()


@pytest.mark.asyncio
async def test_worker_coordinator_close_releases_its_backend_and_sentinel_pools(monkeypatch):
    from drover.services.cache.redis_backend import RedisBackend
    from drover.worker import WorkerRuntime

    closed = []

    async def close(self):
        closed.append(self)

    monkeypatch.setattr(RedisBackend, "raw_client", lambda self: Mock())
    monkeypatch.setattr(RedisBackend, "close", close)
    coordinator = WorkerRuntime._coordinator()
    await coordinator.close_client()
    # RedisBackend.close also closes Sentinel discovery clients, unlike the master client alone.
    assert len(closed) == 1 and isinstance(closed[0], RedisBackend)
    coordinator.redis.close.assert_not_called()
