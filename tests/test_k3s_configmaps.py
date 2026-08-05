"""k3s ConfigMap CRUD 엔드포인트 단위 테스트."""

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


def _make_cm_record(name: str = "myconfig", namespace: str = "default"):
    return {
        "name": name,
        "namespace": namespace,
        "data": {"key1": "value1"},
        "binary_data": None,
        "labels": {},
        "annotations": {},
        "created_at": "2024-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_list_namespaces_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/k3s-1/namespaces")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_list_namespaces_cluster_not_found(client):
    with (
        patch("drover.api.configmaps.k3s_cluster") as mock_cluster,
        patch("drover.api.configmaps.k3s_kube") as mock_kube,
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=None)
        mock_kube.list_namespaces = AsyncMock(return_value=[])
        resp = await client.get("/v1/clusters/k3s-1/namespaces")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_namespaces_success(client):
    with (
        patch("drover.api.configmaps.k3s_cluster") as mock_cluster,
        patch("drover.api.configmaps.k3s_kube") as mock_kube,
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.list_namespaces = AsyncMock(return_value=["default", "kube-system"])
        resp = await client.get("/v1/clusters/k3s-1/namespaces")
    assert resp.status_code == 200
    assert resp.json() == ["default", "kube-system"]


@pytest.mark.asyncio
async def test_list_configmaps_success(client):
    with (
        patch("drover.api.configmaps.k3s_cluster") as mock_cluster,
        patch("drover.api.configmaps.k3s_kube") as mock_kube,
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.list_configmaps = AsyncMock(return_value=[_make_cm_record("cm1"), _make_cm_record("cm2")])
        resp = await client.get("/v1/clusters/k3s-1/configmaps?namespace=default")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "cm1"
    assert data[0]["namespace"] == "default"


@pytest.mark.asyncio
async def test_get_configmap_success(client):
    with (
        patch("drover.api.configmaps.k3s_cluster") as mock_cluster,
        patch("drover.api.configmaps.k3s_kube") as mock_kube,
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.get_configmap = AsyncMock(return_value=_make_cm_record("myconfig"))
        resp = await client.get("/v1/clusters/k3s-1/namespaces/default/configmaps/myconfig")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "myconfig"
    assert data["data"] == {"key1": "value1"}


@pytest.mark.asyncio
async def test_create_configmap_success(client):
    with (
        patch("drover.api.configmaps.k3s_cluster") as mock_cluster,
        patch("drover.api.configmaps.k3s_kube") as mock_kube,
        patch("drover.api.configmaps.rec", new_callable=AsyncMock),
        patch("drover.api.configmaps.invalidate", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.create_configmap = AsyncMock(return_value=_make_cm_record("newcm"))
        resp = await client.post(
            "/v1/clusters/k3s-1/namespaces/default/configmaps",
            json={"name": "newcm", "data": {"key1": "value1"}},
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "newcm"


@pytest.mark.asyncio
async def test_update_configmap_success(client):
    with (
        patch("drover.api.configmaps.k3s_cluster") as mock_cluster,
        patch("drover.api.configmaps.k3s_kube") as mock_kube,
        patch("drover.api.configmaps.rec", new_callable=AsyncMock),
        patch("drover.api.configmaps.invalidate", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.update_configmap = AsyncMock(return_value=_make_cm_record("myconfig"))
        resp = await client.put(
            "/v1/clusters/k3s-1/namespaces/default/configmaps/myconfig",
            json={"data": {"key1": "newvalue"}},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "myconfig"


@pytest.mark.asyncio
async def test_delete_configmap_success(client):
    with (
        patch("drover.api.configmaps.k3s_cluster") as mock_cluster,
        patch("drover.api.configmaps.k3s_kube") as mock_kube,
        patch("drover.api.configmaps.rec", new_callable=AsyncMock),
        patch("drover.api.configmaps.invalidate", new_callable=AsyncMock),
    ):
        mock_cluster.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_kube.delete_configmap = AsyncMock(return_value=None)
        resp = await client.delete("/v1/clusters/k3s-1/namespaces/default/configmaps/myconfig")
    assert resp.status_code == 204
