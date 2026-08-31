"""k3s/clusters.py 엔드포인트 단위 테스트 (6개, k3s 서비스 필요)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app
from drover.models.orm import ManagedOpenStackResource


def _make_cluster_record():
    return {
        "id": "k3s-1",
        "project_id": "test-project-123",
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


@pytest.mark.asyncio
async def test_list_drover_clusters_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_list_drover_clusters_success(client):
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.list_clusters = AsyncMock(return_value=[_make_cluster_record()])
        resp = await client.get("/v1/clusters")
    assert resp.status_code == 200

    assert resp.json()[0]["project_id"] == "test-project-123"

@pytest.mark.asyncio
async def test_get_drover_cluster_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/k3s-1")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_get_drover_cluster_success(client):
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        resp = await client.get("/v1/clusters/k3s-1")
    assert resp.status_code == 200



@pytest.mark.asyncio
async def test_get_drover_cluster_includes_reconciliation_fields(client):
    rec = _make_cluster_record()
    rec["last_reconciled_at"] = "2026-08-25T12:00:00Z"
    rec["drift_status"] = {"has_drift": False, "mismatches": []}
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=rec)
        resp = await client.get("/v1/clusters/k3s-1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["last_reconciled_at"] == "2026-08-25T12:00:00Z"
    assert data["drift_status"] == {"has_drift": False, "mismatches": []}

@pytest.mark.asyncio
async def test_get_drover_cluster_not_found(client):
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=None)
        resp = await client.get("/v1/clusters/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_download_kubeconfig_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/k3s-1/kubeconfig")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_download_kubeconfig_not_ready(client):
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_db.get_kubeconfig = AsyncMock(return_value=None)
        resp = await client.get("/v1/clusters/k3s-1/kubeconfig")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_download_kubeconfig_success(client):
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_db.get_kubeconfig = AsyncMock(return_value=b"apiVersion: v1\n...")
        resp = await client.get("/v1/clusters/k3s-1/kubeconfig")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_head_kubeconfig_ready(client):
    """kubeconfig 준비된 경우 HEAD 요청이 200을 반환해야 한다."""
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_db.get_kubeconfig = AsyncMock(return_value=b"apiVersion: v1\n...")
        resp = await client.request("HEAD", "/v1/clusters/k3s-1/kubeconfig")
    assert resp.status_code == 200
    assert resp.content == b""  # HEAD는 body 없어야 함


@pytest.mark.asyncio
async def test_head_kubeconfig_not_ready(client):
    """kubeconfig 미준비 시 HEAD도 404를 반환해야 한다."""
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=_make_cluster_record())
        mock_db.get_kubeconfig = AsyncMock(return_value=None)
        resp = await client.request("HEAD", "/v1/clusters/k3s-1/kubeconfig")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_scale_drover_cluster_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.patch("/v1/clusters/k3s-1/scale", json={"agent_count": 2})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_scale_drover_cluster_success(client):
    cluster = _make_cluster_record()
    enqueue = AsyncMock(return_value="job-1")
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters._jobs.enqueue_job", new=enqueue),
    ):
        mock_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        resp = await client.patch("/v1/clusters/k3s-1/scale", json={"agent_count": 1})
    assert resp.status_code == 200
    enqueue.assert_awaited_once_with(
        cluster_id="k3s-1",
        project_id="test-project-123",
        kind="scale",
        payload={"desired_count": 1},
        user_id="test-user-123",
        username="testuser",
    )

@pytest.mark.asyncio
async def test_delete_drover_cluster_unauthenticated():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.delete("/v1/clusters/k3s-1")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_delete_drover_cluster_not_found(client):
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=None)
        resp = await client.delete("/v1/clusters/nonexistent")
    assert resp.status_code == 404

@pytest.mark.asyncio
async def test_delete_drover_cluster_enqueues_durable_job(client):
    cluster = _make_cluster_record()
    enqueue = AsyncMock(return_value="job-1")
    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters._jobs.enqueue_job", new=enqueue),
    ):
        mock_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        resp = await client.delete("/v1/clusters/k3s-1")
    assert resp.status_code == 204
    enqueue.assert_awaited_once_with(
        cluster_id="k3s-1",
        project_id="test-project-123",
        kind="delete",
        payload={"user_id": "test-user-123", "username": "testuser"},
        user_id="test-user-123",
        username="testuser",
    )
    mock_db.update_cluster_status.assert_awaited_once_with(
        "test-project-123",
        "k3s-1",
        "DELETING",
    )


@pytest.mark.asyncio
async def test_delete_drover_cluster_cleans_occm_lbs(client):
    """recorded inventory에 있는 Octavia LB를 정리해야 한다."""
    cluster = _make_cluster_record()
    cluster["occm_enabled"] = True
    mock_managed = [
        ManagedOpenStackResource(service="octavia", resource_type="load_balancer", resource_id="lb-1"),
        ManagedOpenStackResource(service="octavia", resource_type="load_balancer", resource_id="lb-2"),
    ]

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.inventory.list_managed_resources", AsyncMock(return_value=mock_managed)),
        patch("drover.services.deletion.nova") as mock_nova,
        patch("drover.services.deletion.neutron") as mock_neutron,
        patch("drover.services.deletion.octavia") as mock_octavia,
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_nova.delete_server_safe = MagicMock()
        mock_neutron.delete_security_group_safe = MagicMock()
        mock_octavia.delete_load_balancer_safe = MagicMock()
        mock_kube.delete_k8s_nodes = AsyncMock()

        resp = await client.post("/v1/clusters/k3s-1/delete-async")

    assert resp.status_code == 200
    assert mock_octavia.delete_load_balancer_safe.call_count == 2
    deleted_ids = {call.args[1] for call in mock_octavia.delete_load_balancer_safe.call_args_list}
    assert deleted_ids == {"lb-1", "lb-2"}


@pytest.mark.asyncio
async def test_delete_drover_cluster_lb_cleanup_failure_continues(client):
    """LB 정리 실패해도 클러스터 삭제는 계속 진행되어야 한다."""
    cluster = _make_cluster_record()
    cluster["occm_enabled"] = True
    mock_managed = [
        ManagedOpenStackResource(service="octavia", resource_type="load_balancer", resource_id="lb-1"),
    ]

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.inventory.list_managed_resources", AsyncMock(return_value=mock_managed)),
        patch("drover.services.deletion.nova"),
        patch("drover.services.deletion.neutron"),
        patch("drover.services.deletion.octavia") as mock_octavia,
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_octavia.delete_load_balancer_safe = MagicMock(side_effect=Exception("Octavia unavailable"))
        mock_kube.delete_k8s_nodes = AsyncMock()

        resp = await client.post("/v1/clusters/k3s-1/delete-async")

    assert resp.status_code == 200
    mock_db.delete_cluster_record.assert_called_once()


@pytest.mark.asyncio
async def test_delete_drover_cluster_no_occm_skips_lb_cleanup(client):
    """기록된 LB가 없는 클러스터는 LB 정리를 스킵해야 한다."""
    cluster = _make_cluster_record()
    cluster["occm_enabled"] = False

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.inventory.list_managed_resources", AsyncMock(return_value=[])),
        patch("drover.services.deletion.nova"),
        patch("drover.services.deletion.neutron"),
        patch("drover.services.deletion.octavia") as mock_octavia,
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_octavia.delete_load_balancer_safe = MagicMock()
        mock_kube.delete_k8s_nodes = AsyncMock()

        resp = await client.post("/v1/clusters/k3s-1/delete-async")

    assert resp.status_code == 200
    mock_octavia.delete_load_balancer_safe.assert_not_called()


@pytest.mark.asyncio
async def test_delete_drover_cluster_deletes_k8s_nodes(client):
    """클러스터 삭제 시 agent 노드와 server 노드를 K8s API로 삭제해야 한다."""
    cluster = _make_cluster_record()
    cluster["occm_enabled"] = False
    cluster["agent_vm_ids"] = ["vm-agent-1", "vm-agent-2"]

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.nova"),
        patch("drover.services.deletion.neutron"),
        patch("drover.services.deletion.octavia"),
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(
            return_value={
                "vm-agent-1": "mycluster-agent-1",
                "vm-agent-2": "mycluster-agent-2",
            }
        )
        mock_kube.delete_k8s_nodes = AsyncMock()

        resp = await client.post("/v1/clusters/k3s-1/delete-async")

    assert resp.status_code == 200
    mock_kube.delete_k8s_nodes.assert_called_once()
    node_names = mock_kube.delete_k8s_nodes.call_args.args[1]
    assert "mycluster-agent-1" in node_names
    assert "mycluster-agent-2" in node_names
    assert "mycluster-server" in node_names


@pytest.mark.asyncio
async def test_delete_drover_cluster_continues_if_k8s_node_delete_fails(client):
    """K8s 노드 삭제 실패해도 VM 삭제와 soft-delete는 계속 진행되어야 한다."""
    cluster = _make_cluster_record()
    cluster["occm_enabled"] = False

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.nova"),
        patch("drover.services.deletion.neutron"),
        patch("drover.services.deletion.octavia"),
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_kube.delete_k8s_nodes = AsyncMock(side_effect=Exception("K8s API unreachable"))

        resp = await client.post("/v1/clusters/k3s-1/delete-async")

    assert resp.status_code == 200
    mock_db.delete_cluster_record.assert_called_once()


@pytest.mark.asyncio
async def test_delete_drover_cluster_vm_already_deleted(client):
    """VM이 이미 삭제된 상태(delete_server 404)여도 soft-delete까지 정상 완료해야 한다."""
    cluster = _make_cluster_record()
    cluster["id"] = "k3s-vm-already-del"
    cluster["occm_enabled"] = False

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.nova") as mock_nova,
        patch("drover.services.deletion.neutron"),
        patch("drover.services.deletion.octavia"),
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_nova.delete_server_safe = MagicMock()
        mock_kube.delete_k8s_nodes = AsyncMock()

        resp = await client.post("/v1/clusters/k3s-vm-already-del/delete-async")

    assert resp.status_code == 200
    mock_db.delete_cluster_record.assert_called_once()


@pytest.mark.asyncio
async def test_delete_drover_cluster_vm_wait_timeout(client):
    """VM 삭제 대기 타임아웃이 발생해도 SG 삭제와 soft-delete는 계속 진행해야 한다."""
    cluster = _make_cluster_record()
    cluster["id"] = "k3s-vm-wait-timeout"
    cluster["occm_enabled"] = False
    cluster["security_group_id"] = "sg-1"

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.nova") as mock_nova,
        patch("drover.services.deletion.neutron") as mock_neutron,
        patch("drover.services.deletion.octavia"),
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_nova.delete_server_safe = MagicMock(side_effect=TimeoutError("timeout"))
        mock_neutron.delete_security_group_safe = MagicMock()
        mock_kube.delete_k8s_nodes = AsyncMock()

        resp = await client.post("/v1/clusters/k3s-vm-wait-timeout/delete-async")

    assert resp.status_code == 200
    mock_db.delete_cluster_record.assert_called_once()
    mock_neutron.delete_security_group_safe.assert_called()

# ---------------------------------------------------------------------------
# API LB octavia 서비스 단위 테스트 (LB-first 전략)
# ---------------------------------------------------------------------------


def test_octavia_create_lb_with_subnet():
    """vip_subnet_id로 LB 생성 시 vip_subnet_id가 API 호출에 전달되어야 한다."""
    from drover.services import octavia

    mock_conn = MagicMock()
    mock_lb = MagicMock()
    mock_lb.id = "lb-1"
    mock_lb.name = "test-lb"
    mock_lb.description = ""
    mock_lb.provisioning_status = "ACTIVE"
    mock_lb.operating_status = "ONLINE"
    mock_lb.vip_address = "192.168.1.100"
    mock_lb.vip_subnet_id = "subnet-1"
    mock_lb.vip_network_id = "net-1"
    mock_lb.vip_port_id = "port-1"
    mock_lb.project_id = "proj-1"
    mock_conn.load_balancer.create_load_balancer.return_value = mock_lb

    result = octavia.create_load_balancer(mock_conn, "test-lb", "subnet-1", "desc")

    mock_conn.load_balancer.create_load_balancer.assert_called_once_with(
        name="test-lb", description="desc", vip_subnet_id="subnet-1"
    )
    assert result["id"] == "lb-1"


def test_octavia_create_lb_with_vip_network_id():
    """vip_network_id 설정 시 vip_subnet_id 대신 vip_network_id가 API 호출에 전달되어야 한다."""
    from drover.services import octavia

    mock_conn = MagicMock()
    mock_lb = MagicMock()
    mock_lb.id = "lb-2"
    mock_lb.name = "provider-lb"
    mock_lb.description = ""
    mock_lb.provisioning_status = "ACTIVE"
    mock_lb.operating_status = "ONLINE"
    mock_lb.vip_address = "10.100.0.50"
    mock_lb.vip_subnet_id = None
    mock_lb.vip_network_id = "provider-net-1"
    mock_lb.vip_port_id = "port-2"
    mock_lb.project_id = "proj-1"
    mock_conn.load_balancer.create_load_balancer.return_value = mock_lb

    result = octavia.create_load_balancer(
        mock_conn, "provider-lb", description="provider VIP LB", vip_network_id="provider-net-1"
    )

    call_kwargs = mock_conn.load_balancer.create_load_balancer.call_args[1]
    assert "vip_network_id" in call_kwargs
    assert call_kwargs["vip_network_id"] == "provider-net-1"
    assert "vip_subnet_id" not in call_kwargs
    assert result["id"] == "lb-2"


def test_octavia_listener_and_pool_wait_for_load_balancer_active():
    from drover.services import octavia

    mock_conn = MagicMock()
    listener = MagicMock()
    listener.id = "listener-1"
    listener.name = "api-listener"
    listener.protocol = "TCP"
    listener.protocol_port = 6443
    listener.provisioning_status = "PENDING_CREATE"
    listener.default_pool_id = None
    listener.load_balancer_id = "lb-1"
    mock_conn.load_balancer.create_listener.return_value = listener

    pool = MagicMock()
    pool.id = "pool-1"
    pool.name = "api-pool"
    pool.protocol = "TCP"
    pool.lb_algorithm = "ROUND_ROBIN"
    pool.provisioning_status = "PENDING_CREATE"
    pool.health_monitor_id = None
    pool.load_balancers = [{"id": "lb-1"}]
    mock_conn.load_balancer.create_pool.return_value = pool

    with patch.object(octavia, "wait_for_load_balancer") as wait_for_active:
        octavia.create_listener(mock_conn, "lb-1", "TCP", 6443, name="api-listener")
        wait_for_active.assert_called_once_with(mock_conn, "lb-1")

    with patch.object(octavia, "wait_for_load_balancer") as wait_for_active:
        octavia.create_pool(
            mock_conn,
            "lb-1",
            "TCP",
            name="api-pool",
            listener_id="listener-1",
        )
        wait_for_active.assert_called_once_with(mock_conn, "lb-1")


def test_octavia_member_and_delete_mutations_wait_for_load_balancer_active():
    from drover.services import octavia

    mock_conn = MagicMock()
    pool = MagicMock()
    pool.load_balancer_id = None
    pool.load_balancers = [{"id": "lb-1"}]
    mock_conn.load_balancer.find_pool.return_value = pool

    listener = MagicMock()
    listener.load_balancer_id = "lb-1"
    mock_conn.load_balancer.find_listener.return_value = listener

    member = MagicMock()
    member.id = "member-1"
    member.name = "server-1"
    member.address = "192.0.2.10"
    member.protocol_port = 6443
    member.weight = 1
    member.provisioning_status = "PENDING_CREATE"
    member.subnet_id = "subnet-1"
    monitor = MagicMock()
    monitor.id = "monitor-1"
    monitor.name = "api-monitor"
    monitor.type = "TCP"
    monitor.delay = 5
    monitor.timeout = 5
    monitor.max_retries = 3
    monitor.provisioning_status = "PENDING_CREATE"
    mock_conn.load_balancer.create_health_monitor.return_value = monitor
    mock_conn.load_balancer.find_health_monitor.return_value = {"pool_id": "pool-1"}
    mock_conn.load_balancer.create_member.return_value = member

    with patch.object(octavia, "wait_for_load_balancer") as wait_for_active:
        octavia.add_member(
            mock_conn,
            "pool-1",
            "192.0.2.10",
            6443,
            subnet_id="subnet-1",
        )
        octavia.remove_member(mock_conn, "pool-1", "member-1")
        octavia.delete_pool(mock_conn, "pool-1")
        octavia.create_health_monitor(mock_conn, "pool-1", type="TCP")
        octavia.delete_health_monitor(mock_conn, "monitor-1")
        octavia.delete_listener(mock_conn, "listener-1")
    assert [call.args for call in wait_for_active.call_args_list] == [
        (mock_conn, "lb-1"),
        (mock_conn, "lb-1"),
        (mock_conn, "lb-1"),
        (mock_conn, "lb-1"),
        (mock_conn, "lb-1"),
        (mock_conn, "lb-1"),
    ]
    mock_conn.load_balancer.find_listener.return_value = {"load_balancer_id": "lb-dict"}
    assert octavia._listener_load_balancer_id(mock_conn, "listener-dict") == "lb-dict"
    mock_conn.load_balancer.find_pool.return_value = {"load_balancer_id": "lb-direct"}
    assert octavia._pool_load_balancer_id(mock_conn, "pool-direct") == "lb-direct"
    mock_conn.load_balancer.find_pool.return_value = {"load_balancers": [{"id": "lb-list"}]}
    assert octavia._pool_load_balancer_id(mock_conn, "pool-list") == "lb-list"


def test_wait_for_load_balancer_fails_immediately_when_lb_disappears():
    from openstack import exceptions as openstack_exceptions

    from drover.services import octavia

    mock_conn = MagicMock()
    mock_conn.load_balancer.get_load_balancer.side_effect = openstack_exceptions.ResourceNotFound(
        "missing"
    )

    with pytest.raises(RuntimeError, match="disappeared while waiting"):
        octavia.wait_for_load_balancer(mock_conn, "lb-missing", wait=300)


# ---------------------------------------------------------------------------
# CreateK3sClusterRequest 모델 — allowed_cidrs 검증
# ---------------------------------------------------------------------------


def test_create_request_allowed_cidrs_default_is_none():
    """allowed_cidrs 미지정 시 None이어야 한다."""
    from drover.models.schemas import CreateK3sClusterRequest

    req = CreateK3sClusterRequest(name="test-cluster")
    assert req.allowed_cidrs is None


def test_create_request_allowed_cidrs_accepts_list():
    """allowed_cidrs에 CIDR 목록을 지정하면 그대로 저장되어야 한다."""
    from drover.models.schemas import CreateK3sClusterRequest

    cidrs = ["10.0.0.0/8", "192.168.1.0/24"]
    req = CreateK3sClusterRequest(name="test-cluster", allowed_cidrs=cidrs)
    assert req.allowed_cidrs == cidrs


def test_create_request_allowed_cidrs_empty_list():
    """allowed_cidrs에 빈 리스트를 지정하면 빈 리스트가 저장되어야 한다."""
    from drover.models.schemas import CreateK3sClusterRequest

    req = CreateK3sClusterRequest(name="test-cluster", allowed_cidrs=[])
    assert req.allowed_cidrs == []


def test_create_request_allowed_cidrs_rejects_invalid():
    """잘못된 CIDR 은 422 (ValidationError) 로 거부."""
    import pytest as _pytest
    from pydantic import ValidationError

    from drover.models.schemas import CreateK3sClusterRequest

    for bad in ["not-a-cidr", "999.0.0.0/8", "10.0.0.0/33", "10.0.0.0; rm -rf /"]:
        with _pytest.raises(ValidationError):
            CreateK3sClusterRequest(name="test-cluster", allowed_cidrs=[bad])


def test_create_request_allowed_cidrs_normalized():
    """호스트 비트 포함 CIDR 은 네트워크 주소로 정규화된다 (strict=False)."""
    from drover.models.schemas import CreateK3sClusterRequest

    req = CreateK3sClusterRequest(name="test-cluster", allowed_cidrs=["10.0.0.5/24"])
    assert req.allowed_cidrs == ["10.0.0.0/24"]


def test_create_request_allowed_cidrs_max_items():
    """allowed_cidrs 21개 이상은 거부."""
    import pytest as _pytest
    from pydantic import ValidationError

    from drover.models.schemas import CreateK3sClusterRequest

    too_many = [f"10.{i}.0.0/24" for i in range(21)]
    with _pytest.raises(ValidationError):
        CreateK3sClusterRequest(name="test-cluster", allowed_cidrs=too_many)


# ---------------------------------------------------------------------------
# K3sCallbackRequest — node_token / server_ip 형식 검증
# ---------------------------------------------------------------------------


def test_callback_node_token_pattern_rejects_metachars():
    """node_token 에 shell 메타문자가 들어오면 거부 (cloud-init 인젝션 차단)."""
    import pytest as _pytest
    from pydantic import ValidationError

    from drover.models.schemas import K3sCallbackRequest

    for bad in ['"; rm -rf /', "$(whoami)", "tok with space", "tok\nrm"]:
        with _pytest.raises(ValidationError):
            K3sCallbackRequest(token="callbacktok", success=True, node_token=bad)


def test_callback_node_token_accepts_typical_drover_token():
    """K3s 가 발급하는 일반적인 형태의 node_token 은 통과."""
    from drover.models.schemas import K3sCallbackRequest

    req = K3sCallbackRequest(
        token="callbacktok",
        success=True,
        node_token="K10abc123def456::server:7890abcdef==",
    )
    assert req.node_token is not None


def test_callback_server_ip_rejects_invalid():
    """server_ip 에 IP 가 아닌 값이 들어오면 거부."""
    import pytest as _pytest
    from pydantic import ValidationError

    from drover.models.schemas import K3sCallbackRequest

    for bad in ["not-an-ip", "256.256.256.256", "1.2.3.4; rm"]:
        with _pytest.raises(ValidationError):
            K3sCallbackRequest(token="callbacktok", success=True, server_ip=bad)


# ---------------------------------------------------------------------------
# Plugin 게이팅 — Barbican KMS / Keystone Auth (8.14 데드락 해소 후 정상 분기)
# ---------------------------------------------------------------------------


def _make_plugin_settings(**kwargs):
    """플러그인 테스트용 Settings MagicMock."""
    from unittest.mock import MagicMock

    defaults = {
        "drover_barbican_kms_enabled": False,
        "drover_barbican_kms_kek_id": "",
        "drover_barbican_kms_image": "registry.k8s.io/provider-os/barbican-kms-plugin:v1.31.0",
        "drover_keystone_auth_enabled": False,
        "drover_keystone_auth_image": "registry.k8s.io/provider-os/k8s-keystone-auth:v1.34.1",
        "os_auth_url": "http://keystone:5000/v3",
        "os_username": "admin",
        "os_password": "secret",
    }
    defaults.update(kwargs)
    mock = MagicMock()
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


def test_barbican_kms_plugin_enabled_when_settings_enabled():
    """8.14 후속 — 설정 활성화 + KEK + 자격증명 충족 시 should_deploy()=True."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    plugin = BarbicanKmsPlugin()
    settings = _make_plugin_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="kek-uuid")
    assert plugin.should_deploy(settings) is True


def test_keystone_auth_plugin_enabled_when_settings_enabled():
    """8.14 후속 — 설정 활성화 + image + os_auth_url 충족 시 should_deploy()=True."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    plugin = KeystoneAuthPlugin()
    settings = _make_plugin_settings(drover_keystone_auth_enabled=True)
    assert plugin.should_deploy(settings) is True


def test_barbican_kms_plugin_disabled_when_settings_disabled():
    """Barbican KMS는 설정이 비활성화되면 should_deploy()가 False를 반환해야 한다."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    plugin = BarbicanKmsPlugin()
    settings = _make_plugin_settings(drover_barbican_kms_enabled=False)
    assert plugin.should_deploy(settings) is False


def test_keystone_auth_plugin_disabled_when_settings_disabled():
    """Keystone Auth는 설정이 비활성화되면 should_deploy()가 False를 반환해야 한다."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    plugin = KeystoneAuthPlugin()
    settings = _make_plugin_settings(drover_keystone_auth_enabled=False)
    assert plugin.should_deploy(settings) is False


class _DashboardStatsPipeline:
    def __init__(self, redis: "_DashboardStatsRedis"):
        self._redis = redis
        self.keys: list[str] = []
        self.type_keys: list[str] = []

    def hmget(self, key: str, *_fields: str) -> None:
        self.keys.append(key)

    def type(self, key: str) -> None:
        self.type_keys.append(key)

    async def execute(self) -> list[list[object]] | list[bytes]:
        if self.type_keys:
            return [self._redis.key_types.get(key, self._redis.key_type) for key in self.type_keys]
        return [self._redis.rows.get(key, [None, None, None]) for key in self.keys]


class _DashboardStatsRedis:
    def __init__(
        self,
        *,
        key_type: bytes,
        members: list[bytes] | None = None,
        scan_keys: list[bytes] | None = None,
        key_types: dict[str, bytes] | None = None,
    ):
        self.key_type = key_type
        self.members = members or []
        self.scan_keys = scan_keys or []
        self.key_types = key_types or {}
        self.rows: dict[str, list[object]] = {}
        self.pipeline_calls = 0

    async def type(self, key: str) -> bytes:
        return self.key_types.get(key, self.key_type)

    async def sscan(self, _key: str, *, cursor: int, count: int):
        assert count == 200
        return 0, self.members

    async def scan(self, *, cursor: int, match: str, count: int):
        assert match.endswith(":cluster:*")
        assert count == 200
        return 0, self.scan_keys

    def pipeline(self, *, transaction: bool):
        assert transaction is False
        self.pipeline_calls += 1
        return _DashboardStatsPipeline(self)


@pytest.mark.asyncio
async def test_dashboard_cluster_stats_reads_set_without_mutating_source():
    from drover.services import redis_store as drover_cluster

    project_id = "project-a"
    redis = _DashboardStatsRedis(key_type=b"set", members=[b"active", b"deleted", b"pending", b"stale"])
    redis.rows = {
        f"afterglow:k3s:{project_id}:cluster:active": [b"ACTIVE", None, b""],
        f"afterglow:k3s:{project_id}:cluster:deleted": [b"ACTIVE", None, b"2026-01-01T00:00:00Z"],
        f"afterglow:k3s:{project_id}:cluster:pending": [b"CREATING", b"ACTIVE", b""],
    }
    source_before = (redis.key_type, list(redis.members), dict(redis.rows))

    with patch("drover.services.redis_store._get_client", return_value=redis):
        stats = await drover_cluster.dashboard_cluster_stats(project_id)

    assert stats == {"total": 2, "active": 2}
    assert (redis.key_type, list(redis.members), dict(redis.rows)) == source_before


@pytest.mark.asyncio
async def test_dashboard_cluster_stats_scans_hashes_when_membership_type_collides():
    from drover.services import redis_store as drover_cluster

    project_id = "project-a"
    cluster_key = f"afterglow:k3s:{project_id}:cluster:from-scan"
    non_hash_key = f"afterglow:k3s:{project_id}:cluster:non-hash"
    redis = _DashboardStatsRedis(
        key_type=b"string",
        scan_keys=[cluster_key.encode(), non_hash_key.encode()],
        key_types={cluster_key: b"hash", non_hash_key: b"string"},
    )
    redis.rows = {cluster_key: [b"ACTIVE", None, b""]}
    source_before = (
        redis.key_type,
        list(redis.scan_keys),
        dict(redis.key_types),
        dict(redis.rows),
    )

    with patch("drover.services.redis_store._get_client", return_value=redis):
        stats = await drover_cluster.dashboard_cluster_stats(project_id)

    assert stats == {"total": 1, "active": 1}
    assert (
        redis.key_type,
        list(redis.scan_keys),
        dict(redis.key_types),
        dict(redis.rows),
    ) == source_before


@pytest.mark.asyncio
async def test_dashboard_cluster_stats_rejects_nonterminating_cursor_without_partial_data():
    from drover.services import redis_store as drover_cluster

    class _NonTerminatingRedis(_DashboardStatsRedis):
        async def sscan(self, _key: str, *, cursor: int, count: int):
            return 1, [b"only-cluster"]

    redis = _NonTerminatingRedis(key_type=b"set")
    with patch("drover.services.redis_store._get_client", return_value=redis):
        with pytest.raises(drover_cluster.K3sStatsUnavailable):
            await drover_cluster.dashboard_cluster_stats("project-a")


@pytest.mark.asyncio
async def test_dashboard_cluster_stats_handles_empty_and_missing_membership_read_only():
    from drover.services import redis_store as drover_cluster

    for redis in (
        _DashboardStatsRedis(key_type=b"set"),
        _DashboardStatsRedis(key_type=b"none"),
    ):
        source_before = (redis.key_type, list(redis.members), list(redis.scan_keys), dict(redis.rows))
        with patch("drover.services.redis_store._get_client", return_value=redis):
            assert await drover_cluster.dashboard_cluster_stats("project-a") == {"total": 0, "active": 0}
        assert (redis.key_type, list(redis.members), list(redis.scan_keys), dict(redis.rows)) == source_before


@pytest.mark.asyncio
async def test_dashboard_cluster_stats_rejects_oversized_set_without_partial_count():
    from drover.services import redis_store as drover_cluster

    redis = _DashboardStatsRedis(key_type=b"set")
    redis.members = [f"cluster-{index}".encode() for index in range(1001)]
    with patch("drover.services.redis_store._get_client", return_value=redis):
        with pytest.raises(drover_cluster.K3sStatsUnavailable):
            await drover_cluster.dashboard_cluster_stats("project-a")


@pytest.mark.asyncio
async def test_dashboard_cluster_stats_rejects_nonterminating_scan_and_candidate_cap():
    from drover.services import redis_store as drover_cluster

    class _NonTerminatingScanRedis(_DashboardStatsRedis):
        async def scan(self, *, cursor: int, match: str, count: int):
            return 1, []

    class _OversizedCandidateRedis(_DashboardStatsRedis):
        async def scan(self, *, cursor: int, match: str, count: int):
            return 0, [f"afterglow:k3s:project-a:cluster:{index}".encode() for index in range(1001)]

    for redis in (_NonTerminatingScanRedis(key_type=b"none"), _OversizedCandidateRedis(key_type=b"none")):
        with patch("drover.services.redis_store._get_client", return_value=redis):
            with pytest.raises(drover_cluster.K3sStatsUnavailable):
                await drover_cluster.dashboard_cluster_stats("project-a")


@pytest.mark.asyncio
async def test_dashboard_cluster_stats_chunks_hmget_and_times_out_without_partial_count():
    from drover.services import redis_store as drover_cluster

    project_id = "project-a"
    redis = _DashboardStatsRedis(key_type=b"set", members=[f"cluster-{index}".encode() for index in range(201)])
    redis.rows = {f"afterglow:k3s:{project_id}:cluster:cluster-{index}": [b"ACTIVE", None, b""] for index in range(201)}
    with patch("drover.services.redis_store._get_client", return_value=redis):
        assert await drover_cluster.dashboard_cluster_stats(project_id) == {"total": 201, "active": 201}
    assert redis.pipeline_calls == 2

    class _SlowRedis(_DashboardStatsRedis):
        async def sscan(self, _key: str, *, cursor: int, count: int):
            await asyncio.sleep(0.6)
            return 0, []

    with patch("drover.services.redis_store._get_client", return_value=_SlowRedis(key_type=b"set")):
        with pytest.raises(drover_cluster.K3sStatsUnavailable):
            await drover_cluster.dashboard_cluster_stats(project_id)


# ---------------------------------------------------------------------------
# DB OperationalError → 빈 목록 graceful degrade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_drover_clusters_db_operational_error_returns_empty(client):
    """DB OperationalError 발생 시 list_drover_clusters가 200 + 빈 배열을 반환한다."""
    from sqlalchemy.exc import OperationalError

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.mark_db_unhealthy") as mock_mark,
    ):
        mock_db.list_clusters = AsyncMock(side_effect=OperationalError("lost connection", None, None))
        resp = await client.get("/v1/clusters")

    assert resp.status_code == 200
    assert resp.json() == []
    mock_mark.assert_called_once()


@pytest.mark.asyncio
async def test_list_drover_clusters_db_interface_error_returns_empty(client):
    """DB InterfaceError 발생 시 list_drover_clusters가 200 + 빈 배열을 반환한다."""
    from sqlalchemy.exc import InterfaceError

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_db,
        patch("drover.api.clusters.mark_db_unhealthy") as mock_mark,
    ):
        mock_db.list_clusters = AsyncMock(side_effect=InterfaceError("connection reset", None, None))
        resp = await client.get("/v1/clusters")

    assert resp.status_code == 200
    assert resp.json() == []
    mock_mark.assert_called_once()


# ---------------------------------------------------------------------------
# POST /{cluster_id}/delete-async — SSE 비동기 삭제 엔드포인트
# ---------------------------------------------------------------------------


async def _consume_sse(resp) -> list[dict]:
    """SSE 응답에서 data: 라인을 파싱해 메시지 목록을 반환한다."""
    import json

    msgs = []
    async for line in resp.aiter_lines():
        if line.startswith("data: "):
            try:
                msgs.append(json.loads(line[6:]))
            except Exception:
                pass
    return msgs


@pytest.mark.asyncio
async def test_delete_drover_cluster_async_unauthenticated():
    """인증 없이 호출하면 401이 반환된다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/v1/clusters/k3s-1/delete-async")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_delete_drover_cluster_async_not_found(client):
    """존재하지 않는 cluster_id이면 404가 반환된다 (SSE 스트림 시작 전)."""
    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=None)
        resp = await client.post("/v1/clusters/nonexistent/delete-async")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_drover_cluster_async_happy_path(client):
    """정상 흐름: delete_init → ... → delete_record → completed 순서로 이벤트가 수신되고 delete_cluster_record가 호출된다."""
    cluster = _make_cluster_record()
    cluster["id"] = "k3s-async-1"
    cluster["occm_enabled"] = False
    cluster["security_group_id"] = "sg-async-1"

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.nova") as mock_nova,
        patch("drover.services.deletion.neutron") as mock_neutron,
        patch("drover.services.deletion.octavia") as mock_octavia,
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_nova.delete_server_safe = MagicMock()
        mock_neutron.delete_security_group_safe = MagicMock()
        mock_octavia.delete_load_balancer_safe = MagicMock()
        mock_kube.delete_k8s_nodes = AsyncMock()

        async with client.stream("POST", "/v1/clusters/k3s-async-1/delete-async") as resp:
            assert resp.status_code == 200
            msgs = await _consume_sse(resp)

    steps = [m["step"] for m in msgs]
    assert "delete_init" in steps
    assert "delete_record" in steps
    assert steps[-1] == "completed"
    assert steps.index("delete_init") < steps.index("delete_record")
    mock_db.delete_cluster_record.assert_called_once()


@pytest.mark.asyncio
async def test_delete_drover_cluster_async_partial_failure_continues(client):
    """LB 목록 조회가 실패해도 최종 completed 이벤트 + delete_cluster_record 호출."""
    cluster = _make_cluster_record()
    cluster["id"] = "k3s-async-partial"
    cluster["occm_enabled"] = True

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
        patch("drover.services.deletion.nova"),
        patch("drover.services.deletion.neutron"),
        patch("drover.services.deletion.octavia") as mock_octavia,
        patch("drover.services.deletion.k3s_kube") as mock_kube,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock()
        mock_db.delete_cluster_record = AsyncMock()
        mock_db.get_agent_vm_names = AsyncMock(return_value={})
        mock_octavia.delete_load_balancer_safe = MagicMock(side_effect=Exception("octavia error"))
        mock_kube.delete_k8s_nodes = AsyncMock()

        async with client.stream(
            "POST", "/v1/clusters/k3s-async-partial/delete-async"
        ) as resp:
            assert resp.status_code == 200
            msgs = await _consume_sse(resp)

    steps = [m["step"] for m in msgs]
    assert steps[-1] == "completed"
    mock_db.delete_cluster_record.assert_called_once()

@pytest.mark.asyncio
async def test_delete_drover_cluster_async_already_deleted(client):
    """이미 삭제된 클러스터는 단일 completed 이벤트를 반환하고 delete_cluster_record를 호출하지 않는다."""
    cluster = _make_cluster_record()
    cluster["id"] = "k3s-async-already"
    cluster["deleted_at"] = "2024-01-01T00:00:00Z"

    with patch("drover.api.clusters.k3s_cluster") as mock_db:
        mock_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.delete_cluster_record = AsyncMock()
        async with client.stream("POST", "/v1/clusters/k3s-async-already/delete-async") as resp:
            assert resp.status_code == 200
            msgs = await _consume_sse(resp)

    assert len(msgs) == 1
    assert msgs[0]["step"] == "completed"
    mock_db.delete_cluster_record.assert_not_called()


# ---------------------------------------------------------------------------
# Runtime policy storage failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("os_type", ["ubuntu", "fcos"])
async def test_create_cluster_returns_503_when_policy_storage_is_unavailable(client, os_type):
    resp = await client.post(
        "/v1/clusters/async",
        json={"name": f"test-{os_type}", "os_type": os_type},
    )

    assert resp.status_code == 503
    assert resp.json() == {"detail": "resource policy storage is unavailable"}


@pytest.mark.asyncio
async def test_delete_drover_cluster_async_fatal_failure(client):
    """update_cluster_status가 치명적 예외를 던지면 failed 이벤트가 수신되고 delete_cluster_record는 호출되지 않는다."""
    cluster = _make_cluster_record()
    cluster["id"] = "k3s-async-fatal"
    cluster["occm_enabled"] = False

    with (
        patch("drover.api.clusters.k3s_cluster") as mock_api_db,
        patch("drover.services.deletion.k3s_cluster") as mock_db,
    ):
        mock_api_db.get_cluster = AsyncMock(return_value=cluster)
        mock_db.update_cluster_status = AsyncMock(side_effect=Exception("DB 연결 실패"))
        mock_db.delete_cluster_record = AsyncMock()
        async with client.stream("POST", "/v1/clusters/k3s-async-fatal/delete-async") as resp:
            assert resp.status_code == 200
            msgs = await _consume_sse(resp)

    steps = [m["step"] for m in msgs]
    assert "failed" in steps
    mock_db.delete_cluster_record.assert_not_called()
