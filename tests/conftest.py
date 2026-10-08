"""Shared fixtures for the standalone Drover service tests."""

from __future__ import annotations

import contextlib
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

os.environ.setdefault("DROVER_KUBECONFIG_ENCRYPTION_KEY", "0123456789abcdef" * 4)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/7")

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from drover.api.clusters import limiter as clusters_limiter
from drover.auth import _get_admin_ks_session, get_os_conn, get_token_info, require_token
from drover.config import get_settings
from drover.main import app
from drover.rate_limit import limiter
from drover.services import cache as cache_module
from drover.services import redis_store
from drover.services.cache.redis_backend import RedisBackend

limiter.enabled = False


def make_mock_conn(project_id: str = "test-project-123") -> MagicMock:
    conn = MagicMock()
    conn._afterglow_token = "test-token"
    conn._afterglow_project_id = project_id
    conn._afterglow_user_id = "test-user-123"
    conn.close = MagicMock()
    conn.compute.find_server.return_value = None
    conn.load_balancer.find_load_balancer.return_value = None
    conn.network.find_ip.return_value = None
    conn.block_storage.find_volume.return_value = None
    conn.network.find_security_group.return_value = None
    conn.network.find_port.return_value = None
    return conn


def make_token_info(*, roles: list[str], is_system_admin: bool = False) -> dict:
    return {
        "token": "test-token",
        "project_id": "test-project-123",
        "project_name": "test-project",
        "user_id": "test-user-123",
        "username": "testuser",
        "roles": roles,
        "expires_at": "2099-01-01T00:00:00Z",
        "is_system_admin": is_system_admin,
        "auth_method": "password",
    }


@pytest.fixture(autouse=True)
def _reset_settings_and_rate_limit(monkeypatch):
    get_settings.cache_clear()
    _get_admin_ks_session.cache_clear()
    for current in (limiter, clusters_limiter):
        monkeypatch.setattr(current, "enabled", False)
        current._storage.reset()
    yield
    get_settings.cache_clear()
    _get_admin_ks_session.cache_clear()


@pytest.fixture(autouse=True)
async def _fake_redis(monkeypatch):
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    cache_module.set_backend(None)
    cache_module.set_backend(RedisBackend(client=fake))

    async def get_fake():
        return fake

    monkeypatch.setattr(cache_module, "_get_redis", get_fake, raising=False)
    monkeypatch.setattr(cache_module, "_get_client", lambda: fake, raising=False)
    monkeypatch.setattr(redis_store, "_get_client", lambda: fake, raising=False)
    yield fake
    cache_module.set_backend(None)


@pytest.fixture
def mock_conn():
    return make_mock_conn()


async def _client_for(mock_conn, *, roles: list[str], is_system_admin: bool = False):
    async def override_get_os_conn():
        yield mock_conn

    async def override_get_token_info():
        return make_token_info(roles=roles, is_system_admin=is_system_admin)

    app.dependency_overrides[get_os_conn] = override_get_os_conn
    app.dependency_overrides[get_token_info] = override_get_token_info
    app.dependency_overrides[require_token] = override_get_token_info
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-Auth-Token": "test-token", "X-Project-Id": "test-project-123"},
    )


@pytest.fixture
async def client(mock_conn):
    async with await _client_for(mock_conn, roles=["member", "reader", "drover-inventory_reader", "drover-access_user", "drover-clusters_editor", "drover-workloads_editor", "drover-clusters_admin", "drover-access_admin"]) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
async def admin_client(mock_conn):
    async with await _client_for(mock_conn, roles=["admin", "member"], is_system_admin=True) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
async def non_admin_client(mock_conn):
    async with await _client_for(mock_conn, roles=["member"]) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
def admitted_authority(monkeypatch):
    """Fake only Keystone admission for API tests; scope and policy guards still run."""
    from drover.services import cluster_authority, delegation

    @contextlib.asynccontextmanager
    async def admission(token_info, *, project_id, cluster_id, action):
        if token_info.get("project_id") != project_id:
            raise delegation.DelegationDenied("Requester token belongs to another project")
        yield delegation.AdmittedDelegation(
            id="delegation-1", project_id=project_id, cluster_id=cluster_id, action=action,
            trust_id="trust-1", trustor_user_id=token_info["user_id"], trustee_user_id="drover-service",
            role_ids=["member-id"], role_names=["member"], expires_at=datetime(2099, 1, 1, tzinfo=UTC),
        )

    async def issue(conn, token_info, *, purposes, **scope):
        return [cluster_authority.IssuedCredential(
            purpose=purpose, app_credential_id=f"credential-{purpose}", owner_user_id=token_info["user_id"],
            role_ids=["member-id"], role_names=["member"], secret="credential-secret-123",
        ) for purpose in purposes]

    monkeypatch.setattr(delegation, "admission", admission)
    monkeypatch.setattr(cluster_authority, "issue", issue)
    return "delegation-1"


@pytest.fixture
def credential_retirement(monkeypatch):
    """Keep cloud-deletion tests independent of credential persistence (covered separately)."""
    monkeypatch.setattr("drover.services.cluster_authority.retire_owned", AsyncMock(return_value=[]))
    monkeypatch.setattr("drover.services.cluster_authority.retire_remaining_for_deleted_cluster",
                        AsyncMock(return_value=0))


@pytest.fixture
def callback_delegation(monkeypatch):
    """Callback continuations reuse the create operation's already admitted trust."""
    monkeypatch.setattr("drover.services.delegation.active_delegation_for_operation",
                        AsyncMock(return_value="delegation-1"))
