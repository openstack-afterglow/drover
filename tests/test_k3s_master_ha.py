"""PR 2B — k3s Master HA 테스트."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drover.models.schemas import CreateK3sClusterRequest

# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

_VALID_TOKEN_INFO = {
    "project_id": "test-project",
    "user_id": "uid1",
    "username": "tester",
    "is_admin": False,
}


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


# ---------------------------------------------------------------------------
# Pydantic 모델 유효성
# ---------------------------------------------------------------------------


def test_master_count_default_is_1():
    req = CreateK3sClusterRequest(name="my-cluster")
    assert req.master_count == 1


def test_master_count_3_allowed():
    req = CreateK3sClusterRequest(name="ha-cluster", master_count=3)
    assert req.master_count == 3


def test_master_count_2_raises():
    with pytest.raises(Exception):
        CreateK3sClusterRequest(name="bad", master_count=2)


def test_master_count_5_raises():
    with pytest.raises(Exception):
        CreateK3sClusterRequest(name="bad", master_count=5)


# ---------------------------------------------------------------------------
# k3s_cloudinit — cluster_init 분기
# ---------------------------------------------------------------------------


def test_generate_server_userdata_cluster_init():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="ha-test",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="token123",
        cluster_init=True,
    )
    assert result.data  # base64 인코딩됨
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "--cluster-init" in raw
    assert "--server" not in raw


def test_generate_server_userdata_ha_join():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="ha-test",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="token456",
        cluster_init=False,
        join_url="https://10.0.0.5:6443",
        ha_node_token="K10abc::server:xyz",
    )
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "--server" in raw
    assert "10.0.0.5" in raw
    assert "--cluster-init" not in raw


def test_generate_server_userdata_single_master():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="single",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="tok789",
    )
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "--cluster-init" not in raw
    assert "--server" not in raw


def test_generate_server_userdata_uses_direct_drover_callback():
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="single",
        k3s_version="v1.28.0+k3s1",
        callback_url="http://cb.test",
        callback_token="tok789",
    )
    import base64
    import gzip

    raw = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "http://cb.test/v1/callback" in raw
    assert "/api/k3s/callback" not in raw


# ---------------------------------------------------------------------------
# HA 콜백 토큰
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_and_consume_ha_callback_token():
    from unittest.mock import AsyncMock, patch

    mock_client = AsyncMock()
    mock_client.setex = AsyncMock()
    mock_client.getdel = AsyncMock(
        return_value=json.dumps({"project_id": "proj1", "cluster_id": "clus1", "server_index": 2}).encode()
    )

    with patch("drover.services.redis_store._get_client", return_value=mock_client):
        from drover.services.redis_store import consume_ha_callback_token, create_ha_callback_token

        token = await create_ha_callback_token("proj1", "clus1", 2)
        assert isinstance(token, str) and len(token) > 20

        data = await consume_ha_callback_token(token)
        assert data is not None
        assert data["server_index"] == 2
        assert data["project_id"] == "proj1"


@pytest.mark.asyncio
async def test_incr_ha_join_count():
    from unittest.mock import AsyncMock, patch

    mock_client = AsyncMock()
    mock_client.incr = AsyncMock(return_value=2)
    mock_client.expire = AsyncMock()

    with patch("drover.services.redis_store._get_client", return_value=mock_client):
        from drover.services.redis_store import incr_ha_join_count

        count = await incr_ha_join_count("clus1")
        assert count == 2
        mock_client.expire.assert_called_once()


# ---------------------------------------------------------------------------
# callback.py — HA 조인 콜백 처리
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_ha_joiner_adds_lb_member_and_triggers_agents():
    """조인 카운터가 master_count-1에 도달하면 provision_agents가 스폰된다."""
    cluster_info = {
        "project_id": "proj1",
        "name": "ha-cluster",
        "master_count": 3,
        "api_lb_pool_id": "pool-xyz",
        "network_id": "net-abc",
        "server_ip": "10.0.0.5",
        "node_token": "K10abc::server:xyz",
    }

    with (
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=cluster_info)),
        patch("drover.services.store.incr_ha_join_count", new=AsyncMock(return_value=2)),
        patch("drover.services.keystone.get_admin_connection_for_project") as mock_conn,
        patch("drover.services.octavia.add_member") as mock_add,
        patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()) as enqueue_job,
    ):
        mock_conn.return_value = MagicMock()
        mock_conn.return_value.network.subnets = MagicMock(return_value=[MagicMock(id="sub-1")])
        mock_add.return_value = {}

        from drover.api.callback import _handle_ha_joiner
        from drover.models.schemas import K3sCallbackRequest

        req = K3sCallbackRequest(
            token="dummytoken123",
            success=True,
            server_ip="10.0.0.6",
            node_token="K10abc::server:xyz",
        )
        result = await _handle_ha_joiner("proj1", "clus1", 2, req)
        assert result == {"ok": True}
        enqueue_job.assert_awaited_once_with(
            cluster_id="clus1",
            project_id="proj1",
            kind="provision_agents",
            payload={"server_ip": "10.0.0.5", "node_token": "K10abc::server:xyz"},
        )


@pytest.mark.asyncio
async def test_handle_ha_joiner_no_agents_if_not_all_joined():
    """조인 카운터가 master_count-1 미만이면 provision_agents가 스폰되지 않는다."""
    cluster_info = {
        "name": "ha-cluster",
        "master_count": 3,
        "api_lb_pool_id": "pool-xyz",
        "network_id": "net-abc",
        "server_ip": "10.0.0.5",
        "node_token": "K10abc::server:xyz",
    }

    with (
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=cluster_info)),
        patch("drover.services.store.incr_ha_join_count", new=AsyncMock(return_value=1)),
        patch("drover.services.keystone.get_admin_connection_for_project") as mock_conn,
        patch("drover.services.octavia.add_member"),
        patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()) as enqueue_job,
    ):
        mock_conn.return_value = MagicMock()
        mock_conn.return_value.network.subnets = MagicMock(return_value=[])

        from drover.api.callback import _handle_ha_joiner
        from drover.models.schemas import K3sCallbackRequest

        req = K3sCallbackRequest(token="dummytoken456", success=True, server_ip="10.0.0.7")
        await _handle_ha_joiner("proj1", "clus1", 3, req)
        enqueue_job.assert_not_awaited()
@pytest.mark.asyncio
async def test_ha_join_endpoint_falls_back_to_floating_ip_when_vip_lookup_fails():
    from drover.services import provisioner

    with patch(
        "drover.services.octavia.get_load_balancer",
        side_effect=RuntimeError("Octavia unavailable"),
    ):
        join_url, tls_sans = await provisioner._resolve_ha_join_endpoint(
            MagicMock(),
            "lb-1",
            "198.51.100.50",
            "10.0.0.10",
        )

    assert join_url == "https://198.51.100.50:6443"
    assert tls_sans == ["198.51.100.50"]


@pytest.mark.asyncio
async def test_ha_endpoint_snapshot_is_persisted_before_primary_callback():
    from drover.services import provisioner

    with patch(
        "drover.services.provisioner.k3s_cluster.update_cluster_status",
        new=AsyncMock(),
    ) as update_status:
        await provisioner._persist_ha_endpoint_snapshot(
            "project-1",
            "cluster-1",
            lb_id="lb-1",
            pool_id="pool-1",
            fip_id="fip-1",
            fip_address="198.51.100.50",
        )

    update_status.assert_awaited_once_with(
        "project-1",
        "cluster-1",
        "CREATING",
        api_lb_id="lb-1",
        api_lb_pool_id="pool-1",
        api_fip_id="fip-1",
        api_fip_address="198.51.100.50",
    )


@pytest.mark.asyncio
async def test_three_master_topology_inventory_deletion_reconciliation(monkeypatch):
    """Test 3-master HA topology inventory recording, reconciliation, and deletion.

    Creates a 3-master topology:
    - Master 1: server VM, boot volume, Octavia pool member
    - Master 2: server VM, boot volume, Octavia pool member
    - Master 3: server VM, boot volume, Octavia pool member

    Asserts all master/volume/member inventory records and metadata, then verifies
    both reconcile and delete consume all 3 master server, volume, and member IDs.
    """
    from drover.api.callback import _handle_ha_joiner
    from drover.models.schemas import K3sCallbackRequest
    from drover.services import (
        deletion,
        inventory,
        provisioner,
        reconciliation,
    )
    from tests.test_managed_resources_lifecycle import _factory, _TestSession

    project_id = "proj-ha-3m"
    cluster_id = "clus-ha-3m"
    op_id = "op-ha-create-999"
    cluster_name = "ha-3m-cluster"

    session = _TestSession()
    monkeypatch.setattr("drover.services.inventory.get_session_factory", lambda: (lambda: _factory(session)))

    cluster_info = {
        "id": cluster_id,
        "project_id": project_id,
        "name": cluster_name,
        "master_count": 3,
        "k3s_version": "v1.28.0+k3s1",
        "os_type": "ubuntu",
        "server_image_id": "img-ubuntu",
        "server_flavor_id": "flavor-m1",
        "network_id": "net-primary",
        "resource_policy_snapshot": {
            "k3s.volume_availability_zone": {"id": "nova-az"}
        },
        "api_lb_id": "lb-ha-1",
        "api_lb_pool_id": "pool-ha-1",
        "api_fip_address": "198.51.100.50",
        "server_vm_id": "srv-vm-master-1",
        "server_vm_name": f"{cluster_name}-server-1",
        "server_ip": "10.0.0.10",
        "node_token": "K10node::token123",
    }
    # Record primary server#1 VM and boot volume as created by create_cluster_job
    await inventory.record_resource(
        session,
        cluster_id=cluster_id,
        service="nova",
        resource_type="server",
        resource_id="srv-vm-master-1",
        operation_id=op_id,
        name=f"{cluster_name}-server-1",
        metadata={"role": "primary_server", "server_index": 1},
    )
    await inventory.record_resource(
        session,
        cluster_id=cluster_id,
        service="cinder",
        resource_type="volume",
        resource_id="vol-master-1",
        operation_id=op_id,
        name=f"{cluster_name}-server-1-boot",
        metadata={"role": "primary_server_boot_volume", "server_index": 1},
    )

    # Mock OpenStack SDK for provisioner.bootstrap_ha_servers
    mock_conn = MagicMock()
    mock_conn.network.subnets.return_value = [MagicMock(id="sub-1")]

    # Octavia member #1
    mock_mem1 = {"id": "member-master-1"}
    # Cinder volumes for server #2 and #3
    mock_vol2 = MagicMock(id="vol-master-2")
    mock_vol3 = MagicMock(id="vol-master-3")
    # Nova VMs for server #2 and #3
    mock_vm2 = MagicMock(id="srv-vm-master-2")
    mock_vm3 = MagicMock(id="srv-vm-master-3")
    agent_userdata = MagicMock()
    agent_userdata.data = b"cloud-init"
    agent_userdata.config_drive = False

    with (
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=cluster_info)),
        patch("drover.services.keystone.get_admin_connection_for_project", return_value=mock_conn),
        patch("drover.services.octavia.add_member", return_value=mock_mem1),
        patch(
            "drover.services.octavia.get_load_balancer",
            return_value={"vip_address": "192.168.240.50"},
        ),
        patch(
            "drover.services.cloudinit.generate_server_userdata",
            return_value=agent_userdata,
        ) as generate_userdata,
        patch("drover.services.cinder.create_volume_from_image", side_effect=[mock_vol2, mock_vol3]),
        patch("drover.services.nova.create_server", side_effect=[mock_vm2, mock_vm3]),
        patch("drover.services.store.create_ha_callback_token", new=AsyncMock(return_value="ha-tok-123")),
    ):
        await provisioner.bootstrap_ha_servers(
            project_id=project_id,
            cluster_id=cluster_id,
            server_ip="10.0.0.10",
            node_token="K10node::token123",
            master_count=3,
            lb_pool_id="pool-ha-1",
            lb_fip_address="198.51.100.50",
            operation_id=op_id,
        )
    assert generate_userdata.call_count == 2
    assert all(
        call.kwargs["join_url"] == "https://192.168.240.50:6443"
        for call in generate_userdata.call_args_list
    )
    assert all(
        call.kwargs["extra_tls_sans"] == ["192.168.240.50", "198.51.100.50"]
        for call in generate_userdata.call_args_list
    )

    # Server #2 and Server #3 callback -> _handle_ha_joiner
    mock_mem2 = {"id": "member-master-2"}
    mock_mem3 = {"id": "member-master-3"}

    with (
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=cluster_info)),
        patch("drover.services.store.incr_ha_join_count", new=AsyncMock(side_effect=[1, 2])),
        patch("drover.services.keystone.get_admin_connection_for_project", return_value=mock_conn),
        patch("drover.services.octavia.add_member", side_effect=[mock_mem2, mock_mem3]),
        patch("drover.services.operations.get_active_operation", new=AsyncMock(return_value=MagicMock(id=op_id))),
        patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()),
    ):
        req2 = K3sCallbackRequest(token="ha-tok-2", success=True, server_ip="10.0.0.11")
        await _handle_ha_joiner(project_id, cluster_id, 2, req2)

        req3 = K3sCallbackRequest(token="ha-tok-3", success=True, server_ip="10.0.0.12")
        await _handle_ha_joiner(project_id, cluster_id, 3, req3)

    # Assert active inventory records for all 3 masters
    active_resources = await inventory.list_managed_resources(session, cluster_id=cluster_id, active_only=True)

    servers = [r for r in active_resources if r.service == "nova" and r.resource_type == "server"]
    volumes = [r for r in active_resources if r.service == "cinder" and r.resource_type == "volume"]
    members = [r for r in active_resources if r.service == "octavia" and r.resource_type == "member"]

    assert len(servers) == 3
    assert {s.resource_id for s in servers} == {"srv-vm-master-1", "srv-vm-master-2", "srv-vm-master-3"}

    assert len(volumes) == 3
    assert {v.resource_id for v in volumes} == {"vol-master-1", "vol-master-2", "vol-master-3"}

    assert len(members) == 3
    assert {m.resource_id for m in members} == {"member-master-1", "member-master-2", "member-master-3"}

    # Assert metadata distinguishes HA resources
    server2_rec = next(s for s in servers if s.resource_id == "srv-vm-master-2")
    assert server2_rec.metadata_json == {"role": "ha_server", "server_index": 2}

    volume2_rec = next(v for v in volumes if v.resource_id == "vol-master-2")
    assert volume2_rec.metadata_json == {"role": "ha_server_boot_volume", "server_index": 2}

    member2_rec = next(m for m in members if m.resource_id == "member-master-2")
    assert member2_rec.metadata_json == {"pool_id": "pool-ha-1", "role": "ha_server_member", "server_index": 2}

    # Verify reconciliation consumes all 3 master IDs
    checked_servers = []
    checked_volumes = []
    checked_members = []

    def mock_fetch(conn, svc, rtype, rid, metadata=None):
        if svc == "nova" and rtype == "server":
            checked_servers.append(rid)
            srv = MagicMock()
            srv.project_id = project_id
            srv.metadata = {"drover.cluster_id": cluster_id, "drover.managed": "true"}
            srv.status = "ACTIVE"
            return srv
        elif svc == "cinder" and rtype == "volume":
            checked_volumes.append(rid)
            vol = MagicMock()
            vol.project_id = project_id
            vol.metadata = {"drover.cluster_id": cluster_id, "drover.managed": "true"}
            vol.status = "in-use"
            return vol
        elif svc == "octavia" and rtype == "member":
            checked_members.append(rid)
            mem = MagicMock()
            mem.project_id = project_id
            mem.provisioning_status = "ACTIVE"
            mem.operating_status = "ONLINE"
            return mem
        elif svc == "octavia" and rtype == "load_balancer":
            lb = MagicMock()
            lb.project_id = project_id
            lb.provisioning_status = "ACTIVE"
            lb.operating_status = "ONLINE"
            return lb
    with (
        patch("drover.services.reconciliation.fetch_recorded_resource", side_effect=mock_fetch),
        patch("drover.services.store.get_cluster", new=AsyncMock(return_value=cluster_info)),
        patch("drover.services.store.update_cluster_reconciliation", new=AsyncMock()),
        patch("drover.services.operations.append_operation_event", new=AsyncMock()),
    ):
        drift = await reconciliation.reconcile_cluster(project_id, cluster_id, conn=mock_conn)
        assert len(drift["missing"]) == 0
        assert set(checked_servers) == {"srv-vm-master-1", "srv-vm-master-2", "srv-vm-master-3"}
        assert set(checked_volumes) == {"vol-master-1", "vol-master-2", "vol-master-3"}
        assert set(checked_members) == {"member-master-1", "member-master-2", "member-master-3"}

    # Verify deletion consumes all 3 master IDs
    deleted_servers = []
    deleted_volumes = []
    deleted_members = []

    def mock_del_srv(conn, vid, proj, cid):
        deleted_servers.append(vid)

    def mock_del_vol(conn, vid, proj, cid):
        deleted_volumes.append(vid)

    def mock_del_mem(conn, pid, mid):
        deleted_members.append(mid)

    with (
        patch("drover.services.nova.delete_server_safe", side_effect=mock_del_srv),
        patch("drover.services.cinder.delete_volume_safe", side_effect=mock_del_vol),
        patch("drover.services.octavia.remove_member", side_effect=mock_del_mem),
        patch("drover.services.octavia.delete_pool", return_value=None),
        patch("drover.services.octavia.delete_listener", return_value=None),
        patch("drover.services.octavia.delete_load_balancer_safe", return_value=None),
        patch("drover.services.neutron.delete_floating_ip_safe", return_value=None),
        patch("drover.services.neutron.wait_port_deleted", return_value=None),
        patch("drover.services.neutron.delete_security_group_rule", return_value=None),
        patch("drover.services.neutron.delete_security_group_safe", return_value=None),
        patch("drover.services.keystone.delete_app_credential", AsyncMock()),
        patch("drover.services.kube.delete_k8s_nodes", AsyncMock()),
        patch("drover.services.store.delete_cluster_record", AsyncMock()),
        patch("drover.services.operations.append_operation_event", AsyncMock()),
    ):
        async for _ in deletion.delete_cluster_progress(mock_conn, project_id, cluster_info, token_info={"user_id": "u1"}):
            pass

    assert set(deleted_servers) == {"srv-vm-master-1", "srv-vm-master-2", "srv-vm-master-3"}
    assert set(deleted_volumes) == {"vol-master-1", "vol-master-2", "vol-master-3"}
    assert set(deleted_members) == {"member-master-1", "member-master-2", "member-master-3"}

    # Verify inventory marked deleted for all resources
    all_resources = await inventory.list_managed_resources(session, cluster_id=cluster_id, active_only=False)
    assert len(all_resources) > 0
    for r in all_resources:
        assert r.deleted_at is not None
