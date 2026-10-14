"""Native list failures are not a successfully empty inventory."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import InterfaceError, OperationalError

from drover import db
from drover.services import store


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [OperationalError, InterfaceError])
@pytest.mark.parametrize("query", ["", "?no_cache=true", "?refresh_cache=true"])
async def test_cluster_inventory_database_failure_is_503(client, error_type, query):
    error = error_type("SELECT private-data", {}, RuntimeError("private database credentials"))
    with (
        patch("drover.api.clusters.k3s_cluster.list_clusters", AsyncMock(side_effect=error)) as listing,
        patch("drover.api.clusters.mark_db_unhealthy") as unhealthy,
    ):
        response = await client.get("/v1/clusters" + query)
    assert response.status_code == 503
    assert response.json() == {"detail": "Cluster inventory unavailable"}
    assert "private" not in response.text
    listing.assert_awaited_once_with("test-project-123", include_deleted=False)
    unhealthy.assert_called_once_with()


@pytest.mark.asyncio
async def test_cluster_inventory_failure_is_not_cached_as_empty(client):
    listing = AsyncMock(side_effect=[OperationalError("query", {}, RuntimeError("outage")), []])
    with (
        patch("drover.api.clusters.k3s_cluster.list_clusters", listing),
        patch("drover.api.clusters.mark_db_unhealthy"),
    ):
        failed = await client.get("/v1/clusters?no_cache=false")
        recovered = await client.get("/v1/clusters?no_cache=false")
        cached = await client.get("/v1/clusters?no_cache=false")
    assert failed.status_code == 503
    assert recovered.status_code == cached.status_code == 200
    assert recovered.json() == cached.json() == []
    assert listing.await_count == 2


@pytest.mark.asyncio
async def test_cluster_inventory_denied_before_native_cache(non_admin_client):
    with (
        patch("drover.api.clusters.cached_call", AsyncMock(return_value=[])) as cache,
        patch("drover.api.clusters.k3s_cluster.list_clusters", AsyncMock()) as listing,
    ):
        response = await non_admin_client.get("/v1/clusters?no_cache=false")
    assert response.status_code == 403
    cache.assert_not_awaited()
    listing.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [OperationalError, InterfaceError])
@pytest.mark.parametrize("query", ["", "?no_cache=true", "?refresh_cache=true"])
async def test_cluster_inventory_sql_outage_remains_503_until_real_store_recovers(client, monkeypatch, error_type, query):
    # Keep the real store, SQL health breaker, native handler and cache path.
    # Only SQL I/O and the clock are synthetic; Redis must never replace SQL.
    now = [1000.0]
    monkeypatch.setattr(db.time, "time", lambda: now[0])
    monkeypatch.setattr(db, "_engine", object())
    monkeypatch.setattr(db, "_db_unhealthy_until", 0.0)
    monkeypatch.setattr(db, "_default_unhealthy_seconds", 15)
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    error = error_type("SELECT private-data", {}, RuntimeError(2003, "private connection failure"))
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.execute.side_effect = [error, result]
    monkeypatch.setattr(db, "_session_factory", lambda: session)
    redis_listing = AsyncMock(return_value=[])
    monkeypatch.setattr("drover.services.redis_store.list_clusters", redis_listing)

    first = await client.get("/v1/clusters" + query)
    assert first.status_code == 503
    assert db.is_db_configured() and not db.is_db_available()
    for subsequent_query in ("", "?no_cache=false", "?no_cache=true", "?refresh_cache=true"):
        subsequent = await client.get("/v1/clusters" + subsequent_query)
        assert subsequent.status_code == 503
        assert subsequent.json() == {"detail": "Cluster inventory unavailable"}
        assert "private" not in subsequent.text
    assert session.execute.await_count == 1
    with pytest.raises(store.ClusterInventoryUnavailable):
        await store.list_clusters("test-project-123")
    redis_listing.assert_not_awaited()

    now[0] = db._db_unhealthy_until + 1
    assert db.is_db_available()
    recovered = await client.get("/v1/clusters?refresh_cache=true")
    cached = await client.get("/v1/clusters?no_cache=false")
    assert recovered.status_code == cached.status_code == 200
    assert recovered.json() == cached.json() == []
    assert session.execute.await_count == 2
    redis_listing.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["", "?no_cache=true", "?refresh_cache=true"])
async def test_cluster_inventory_unconfigured_database_retains_legacy_redis_list(client, monkeypatch, query):
    monkeypatch.setattr(db, "_engine", None)
    redis_listing = AsyncMock(return_value=[])
    monkeypatch.setattr("drover.services.redis_store.list_clusters", redis_listing)
    response = await client.get("/v1/clusters" + query)
    assert response.status_code == 200
    assert response.json() == []
    redis_listing.assert_awaited_once_with("test-project-123")


@pytest.mark.asyncio
@pytest.mark.parametrize("include_deleted", [False, True])
async def test_cluster_inventory_open_breaker_does_not_serve_warm_cache(client, monkeypatch, include_deleted):
    now = [1000.0]
    monkeypatch.setattr(db.time, "time", lambda: now[0])
    monkeypatch.setattr(db, "_engine", object())
    monkeypatch.setattr(db, "_db_unhealthy_until", 0.0)
    monkeypatch.setattr(db, "_default_unhealthy_seconds", 15)
    suffix = "?include_deleted=" + str(include_deleted).lower()
    with patch("drover.api.clusters.k3s_cluster.list_clusters", AsyncMock(return_value=[])) as listing:
        primed = await client.get("/v1/clusters" + suffix)
        assert primed.status_code == 200
        assert db.mark_db_unhealthy(OperationalError("query", {}, RuntimeError(2003, "connection refused")))
        for query in ("", "&no_cache=false", "&no_cache=true", "&refresh_cache=true"):
            response = await client.get("/v1/clusters" + suffix + query)
            assert response.status_code == 503
            assert response.json() == {"detail": "Cluster inventory unavailable"}
        listing.assert_awaited_once_with("test-project-123", include_deleted=include_deleted)


@pytest.mark.asyncio
async def test_cluster_inventory_store_breaker_exception_is_503(client):
    with patch("drover.api.clusters.k3s_cluster.list_clusters", AsyncMock(side_effect=store.ClusterInventoryUnavailable)):
        response = await client.get("/v1/clusters")
    assert response.status_code == 503
    assert response.json() == {"detail": "Cluster inventory unavailable"}
