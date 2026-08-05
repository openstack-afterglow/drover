"""k3s Secret CRUD 엔드포인트 단위 테스트."""

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app


def _make_cluster_record():
    return {
        "id": "k3s-1",
        "name": "mycluster",
        "status": "ACTIVE",
        "status_reason": None,
        "server_vm_id": "vm-1",
        "agent_vm_ids": [],
        "agent_count": 0,
        "api_address": None,
        "server_ip": "10.0.0.1",
        "network_id": "net-1",
        "key_name": None,
        "k3s_version": "v1.31.4+k3s1",
        "server_vm_name": "mycluster-server",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }


def _make_secret_record(name: str = "mysecret", namespace: str = "default"):
    # data 는 base64 인코딩된 값 (K8s API 응답 형식)
    return {
        "name": name,
        "namespace": namespace,
        "type": "Opaque",
        "data": {"key": "cGxhaW4="},  # "plain"
        "labels": {},
        "annotations": {},
        "created_at": "2024-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_list_secrets_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/k3s-1/secrets")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_list_secrets_cluster_not_found(client):
    with (
        patch("drover.api.secrets.k3s_cluster") as mock_cluster,
        patch("drover.api.secrets.k3s_kube") as mock_kube,
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=None)
        mock_kube.list_secrets = AsyncMock(return_value=[])
        resp = await client.get("/v1/clusters/k3s-1/secrets")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_secrets_success(client):
    with (
        patch("drover.api.secrets.k3s_cluster") as mock_cluster,
        patch("drover.api.secrets.k3s_kube") as mock_kube,
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.list_secrets = AsyncMock(return_value=[_make_secret_record("s1"), _make_secret_record("s2")])
        resp = await client.get("/v1/clusters/k3s-1/secrets?namespace=default")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "s1"


@pytest.mark.asyncio
async def test_get_secret_success(client):
    with (
        patch("drover.api.secrets.k3s_cluster") as mock_cluster,
        patch("drover.api.secrets.k3s_kube") as mock_kube,
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.get_secret = AsyncMock(return_value=_make_secret_record("mysecret"))
        resp = await client.get("/v1/clusters/k3s-1/namespaces/default/secrets/mysecret")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "mysecret"
    assert data["type"] == "Opaque"


@pytest.mark.asyncio
async def test_create_secret_success(client):
    with (
        patch("drover.api.secrets.k3s_cluster") as mock_cluster,
        patch("drover.api.secrets.k3s_kube") as mock_kube,
        patch("drover.api.secrets.rec", new_callable=AsyncMock),
        patch("drover.api.secrets.invalidate", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.create_secret = AsyncMock(return_value=_make_secret_record("newsecret"))
        resp = await client.post(
            "/v1/clusters/k3s-1/namespaces/default/secrets",
            json={"name": "newsecret", "data": {"key": "plain"}},
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "newsecret"


@pytest.mark.asyncio
async def test_create_secret_encodes_base64(client):
    """Secret 생성 시 plain text 가 그대로 service layer 로 전달되어야 한다.

    실제 base64 인코딩은 k3s_kube.create_secret 내부에서 처리되므로 API 레이어는
    plain text 그대로 전달한다.
    """
    with (
        patch("drover.api.secrets.k3s_cluster") as mock_cluster,
        patch("drover.api.secrets.k3s_kube") as mock_kube,
        patch("drover.api.secrets.rec", new_callable=AsyncMock),
        patch("drover.api.secrets.invalidate", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.create_secret = AsyncMock(return_value=_make_secret_record("mysecret"))
        resp = await client.post(
            "/v1/clusters/k3s-1/namespaces/default/secrets",
            json={"name": "mysecret", "data": {"key": "plain"}},
        )
    assert resp.status_code == 201
    call_kwargs = mock_kube.create_secret.call_args
    # plain text 그대로 service layer 로 전달되었는지 검증 (encoding 은 k3s_kube 책임)
    assert call_kwargs.kwargs.get("data") == {"key": "plain"} or call_kwargs.args[3] == {"key": "plain"}


@pytest.mark.asyncio
async def test_update_secret_success(client):
    with (
        patch("drover.api.secrets.k3s_cluster") as mock_cluster,
        patch("drover.api.secrets.k3s_kube") as mock_kube,
        patch("drover.api.secrets.rec", new_callable=AsyncMock),
        patch("drover.api.secrets.invalidate", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.update_secret = AsyncMock(return_value=_make_secret_record("mysecret"))
        resp = await client.put(
            "/v1/clusters/k3s-1/namespaces/default/secrets/mysecret",
            json={"data": {"key": "newplain"}},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "mysecret"


@pytest.mark.asyncio
async def test_delete_secret_success(client):
    with (
        patch("drover.api.secrets.k3s_cluster") as mock_cluster,
        patch("drover.api.secrets.k3s_kube") as mock_kube,
        patch("drover.api.secrets.rec", new_callable=AsyncMock),
        patch("drover.api.secrets.invalidate", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.delete_secret = AsyncMock(return_value=None)
        resp = await client.delete("/v1/clusters/k3s-1/namespaces/default/secrets/mysecret")
    assert resp.status_code == 204
