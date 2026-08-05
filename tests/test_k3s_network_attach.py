"""k3s 노드 네트워크 attach/detach API 단위 테스트.

FastAPI Depends() 는 라우트 정의 시점에 함수 참조를 캡처하므로 모듈 이름 patch 가
재라우팅되지 않는다. 따라서 `app.dependency_overrides` 로 인증/conn 의존성을 교체하고,
핸들러 내부에서 import 되는 nova/k3s_cluster/invalidate/rec 만 patch 한다.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.auth import get_os_conn, get_token_info
from drover.main import app


@pytest.fixture
def mock_cluster():
    return {
        "id": "cluster-1",
        "name": "test-cluster",
        "server_vm_id": "server-vm-1",
        "status": "ACTIVE",
        "network_id": "net-primary",
        "agent_vm_ids": ["agent-vm-1", "agent-vm-2"],
        "deleted_at": None,
    }


@pytest.fixture
def mock_conn():
    """k3s 핸들러가 사용하는 conn._afterglow_project_id 만 stub."""
    conn = MagicMock()
    conn._afterglow_project_id = "proj-1"
    conn._afterglow_token = "test-token"
    conn._afterglow_user_id = "test-user-1"
    conn.compute.server_interfaces = MagicMock(return_value=[])
    conn.close = MagicMock()
    return conn


@pytest.fixture
async def client(mock_conn):
    async def override_get_os_conn():
        try:
            yield mock_conn
        finally:
            pass

    async def override_get_token_info():
        return {
            "token": "test-token",
            "project_id": "proj-1",
            "project_name": "test-project",
            "user_id": "test-user-1",
            "username": "testuser",
            "roles": ["member"],
            "expires_at": "2099-01-01T00:00:00Z",
            "is_system_admin": False,
        }

    app.dependency_overrides[get_os_conn] = override_get_os_conn
    app.dependency_overrides[get_token_info] = override_get_token_info
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-Auth-Token": "test-token", "X-Project-Id": "proj-1"},
    ) as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_attach_interface_wrong_vm_403(client, mock_cluster):
    """클러스터에 속하지 않은 vm_id로 attach 시 403."""
    with patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster:
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        resp = await client.post(
            "/v1/clusters/cluster-1/nodes/unknown-vm/interfaces",
            json={"net_id": "net-1"},
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_attach_interface_cluster_not_found_404(client):
    """존재하지 않는 cluster_id로 attach 시 404."""
    with patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster:
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=None)
        resp = await client.post(
            "/v1/clusters/nonexistent/nodes/server-vm-1/interfaces",
            json={"net_id": "net-1"},
        )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_attach_server_vm_success(client, mock_cluster):
    """서버 VM에 인터페이스 attach 성공."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster,
        patch("drover.api.clusters.nova") as mock_nova,
        patch("drover.api.clusters.invalidate", new_callable=AsyncMock),
        patch("drover.api.clusters.rec", new_callable=AsyncMock),
    ):
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        mock_nova.attach_interface.return_value = {
            "port_id": "port-99",
            "net_id": "net-1",
            "fixed_ips": [{"ip_address": "192.168.100.5", "subnet_id": "sub-1"}],
        }
        resp = await client.post(
            "/v1/clusters/cluster-1/nodes/server-vm-1/interfaces",
            json={"net_id": "net-1"},
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["is_primary"] is False
    assert data["port_id"] == "port-99"
    assert data["node_role"] == "server"


@pytest.mark.asyncio
async def test_detach_agent_vm_success(client, mock_cluster, mock_conn):
    """에이전트 VM에서 인터페이스 detach 성공."""
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster,
        patch("drover.api.clusters.nova") as mock_nova,
        patch("drover.api.clusters.invalidate", new_callable=AsyncMock),
        patch("drover.api.clusters.rec", new_callable=AsyncMock),
    ):
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        mock_conn.compute.server_interfaces.return_value = [
            SimpleNamespace(port_id="port-old", net_id="net-secondary", fixed_ips=[])
        ]
        mock_nova.detach_interface.return_value = None
        resp = await client.delete(
            "/v1/clusters/cluster-1/nodes/agent-vm-1/interfaces/port-old",
        )
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_list_interfaces_marks_stored_primary_even_when_not_first(client, mock_cluster, mock_conn):
    mock_conn.compute.server_interfaces.return_value = [
        SimpleNamespace(port_id="port-secondary", net_id="net-secondary", fixed_ips=[]),
        SimpleNamespace(
            port_id="port-primary",
            net_id="net-primary",
            fixed_ips=[{"ip_address": "192.0.2.10", "subnet_id": "sub-primary"}],
        ),
    ]
    with patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster:
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        resp = await client.get("/v1/clusters/cluster-1/nodes/server-vm-1/interfaces")
    assert resp.status_code == 200
    assert [item["is_primary"] for item in resp.json()] == [False, True]


@pytest.mark.asyncio
async def test_list_interfaces_with_missing_primary_marks_none(client, mock_cluster, mock_conn):
    mock_cluster["network_id"] = ""
    mock_conn.compute.server_interfaces.return_value = [SimpleNamespace(port_id="port-1", net_id="net-1", fixed_ips=[])]
    with patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster:
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        resp = await client.get("/v1/clusters/cluster-1/nodes/server-vm-1/interfaces")
    assert resp.status_code == 200
    assert resp.json()[0]["is_primary"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "net_id", "current"),
    [
        ("BUILD", "net-new", []),
        ("ACTIVE", "net-primary", []),
        ("ACTIVE", "net-secondary", [SimpleNamespace(port_id="port-existing", net_id="net-secondary")]),
    ],
)
async def test_attach_rejects_invalid_state_or_duplicate_network(
    client, mock_cluster, mock_conn, status, net_id, current
):
    mock_cluster["status"] = status
    mock_conn.compute.server_interfaces.return_value = current
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster,
        patch("drover.api.clusters.nova") as mock_nova,
        patch("drover.api.clusters.rec", new_callable=AsyncMock) as mock_rec,
    ):
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        resp = await client.post(
            "/v1/clusters/cluster-1/nodes/server-vm-1/interfaces",
            json={"net_id": net_id},
        )
    assert resp.status_code == 409
    assert resp.json()["detail"] in {
        "ACTIVE 상태의 클러스터만 네트워크를 연결할 수 있습니다",
        "이미 연결된 네트워크입니다",
    }
    mock_nova.attach_interface.assert_not_called()
    assert mock_rec.await_args.kwargs["status"] == "failed"
    assert mock_rec.await_args.kwargs["error_message"] == resp.json()["detail"]


@pytest.mark.asyncio
async def test_detach_rejects_port_not_on_cluster_vm(client, mock_cluster, mock_conn):
    mock_conn.compute.server_interfaces.return_value = [
        SimpleNamespace(port_id="port-real", net_id="net-secondary", fixed_ips=[])
    ]
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster,
        patch("drover.api.clusters.nova") as mock_nova,
        patch("drover.api.clusters.rec", new_callable=AsyncMock) as mock_rec,
    ):
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        resp = await client.delete("/v1/clusters/cluster-1/nodes/server-vm-1/interfaces/port-missing")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "인터페이스를 찾을 수 없습니다"
    mock_nova.detach_interface.assert_not_called()
    assert mock_rec.await_args.kwargs["error_message"] == "인터페이스를 찾을 수 없습니다"


@pytest.mark.asyncio
async def test_detach_rejects_primary_port(client, mock_cluster, mock_conn):
    mock_conn.compute.server_interfaces.return_value = [
        SimpleNamespace(port_id="port-primary", net_id="net-primary", fixed_ips=[])
    ]
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster,
        patch("drover.api.clusters.nova") as mock_nova,
        patch("drover.api.clusters.rec", new_callable=AsyncMock) as mock_rec,
    ):
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        resp = await client.delete("/v1/clusters/cluster-1/nodes/server-vm-1/interfaces/port-primary")
    assert resp.status_code == 409
    assert resp.json()["detail"] == "기본 인터페이스는 제거할 수 없습니다"
    mock_nova.detach_interface.assert_not_called()
    assert mock_rec.await_args.kwargs["error_message"] == "기본 인터페이스는 제거할 수 없습니다"


@pytest.mark.asyncio
async def test_detach_fails_closed_when_primary_network_missing(client, mock_cluster, mock_conn):
    mock_cluster["network_id"] = None
    mock_conn.compute.server_interfaces.return_value = [
        SimpleNamespace(port_id="port-secondary", net_id="net-secondary", fixed_ips=[])
    ]
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_k3s_cluster,
        patch("drover.api.clusters.nova") as mock_nova,
        patch("drover.api.clusters.rec", new_callable=AsyncMock) as mock_rec,
    ):
        mock_k3s_cluster.get_cluster = AsyncMock(return_value=mock_cluster)
        resp = await client.delete("/v1/clusters/cluster-1/nodes/server-vm-1/interfaces/port-secondary")
    assert resp.status_code == 409
    assert resp.json()["detail"] == "기본 인터페이스를 판별할 수 없습니다"
    mock_nova.detach_interface.assert_not_called()
    assert mock_rec.await_args.kwargs["error_message"] == "기본 인터페이스를 판별할 수 없습니다"
