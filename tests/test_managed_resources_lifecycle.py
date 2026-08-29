"""Focused mocked lifecycle tests for ManagedOpenStackResource write/tag/delete safety semantics."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

pytest_plugins = ("pytest_asyncio",)
import pytest

from drover.models.orm import ManagedOpenStackResource
from drover.services import deletion, inventory


class _TestSession:
    def __init__(self, store=None):
        self.store = store if store is not None else {}
        self.added = []

    def add(self, obj):
        self.added.append(obj)
        key = (type(obj), getattr(obj, "id", None))
        self.store[key] = obj

    def begin(self):
        class _Tx:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *exc):
                pass
        return _Tx()

    async def execute(self, stmt):
        class _Result:
            def __init__(self, items):
                self._items = items

            def scalar_one_or_none(self):
                return self._items[0] if self._items else None

            def scalars(self):
                class _Scalars:
                    def __init__(self, items):
                        self._items = items

                    def all(self):
                        return self._items

                return _Scalars(self._items)

        params = {}
        try:
            compiled = stmt.compile()
            params = dict(compiled.params or {})
        except Exception:
            pass

        srv = params.get("service_1") or params.get("service")
        rtype = params.get("resource_type_1") or params.get("resource_type")
        rid = params.get("resource_id_1") or params.get("resource_id")
        cid = params.get("cluster_id_1") or params.get("cluster_id")

        res_items = []
        for obj in self.store.values():
            if isinstance(obj, ManagedOpenStackResource):
                match = True
                if srv and getattr(obj, "service", None) != srv:
                    match = False
                if rtype and getattr(obj, "resource_type", None) != rtype:
                    match = False
                if rid and getattr(obj, "resource_id", None) != rid:
                    match = False
                if cid and getattr(obj, "cluster_id", None) != cid:
                    match = False
                if match:
                    res_items.append(obj)

        return _Result(res_items)
    async def flush(self):
        pass


def _factory(session):
    class _CM:
        async def __aenter__(self):
            return session

        async def __aexit__(self, *exc):
            pass

        def begin(self):
            return self

    return _CM()


@pytest.mark.asyncio
async def test_build_drover_tags_and_metadata():
    """Verify build_drover_tags and build_drover_metadata formatting."""
    cluster_id = "c1234567-89ab-cdef-0123-456789abcdef"
    op_id = "op987654-3210-fedc-ba98-76543210fedc"

    tags = inventory.build_drover_tags(cluster_id, op_id, "security_group")
    assert f"drover.cluster_id={cluster_id}" in tags
    assert f"drover.operation_id={op_id}" in tags
    assert "drover.resource_type=security_group" in tags
    assert "drover.managed=true" in tags

    meta = inventory.build_drover_metadata(cluster_id, op_id, "server")
    assert meta["drover.cluster_id"] == cluster_id
    assert meta["drover.operation_id"] == op_id
    assert meta["drover.resource_type"] == "server"
    assert meta["drover.managed"] == "true"


@pytest.mark.asyncio
async def test_inventory_recording_and_deletion(monkeypatch):
    """Verify record_resource, mark_resource_deleted, and list_managed_resources."""
    cluster_id = "cluster-test-111"
    op_id = "op-test-222"
    session = _TestSession()

    monkeypatch.setattr("drover.services.inventory.get_session_factory", lambda: (lambda: _factory(session)))

    # Record Nova server
    r1 = await inventory.record_resource(
        session,
        cluster_id=cluster_id,
        service="nova",
        resource_type="server",
        resource_id="srv-vm-1",
        operation_id=op_id,
        name="server-node-1",
        metadata={"drover.managed": "true"},
    )
    assert r1 is not None
    assert r1.resource_id == "srv-vm-1"

    # Record Cinder volume
    r2 = await inventory.record_resource(
        session,
        cluster_id=cluster_id,
        service="cinder",
        resource_type="volume",
        resource_id="vol-boot-1",
        operation_id=op_id,
        name="server-node-1-boot",
    )
    assert r2 is not None

    active_res = await inventory.list_managed_resources(session, cluster_id=cluster_id, active_only=True)
    assert len(active_res) == 2

    # Mark volume as deleted
    await inventory.mark_resource_deleted(session, service="cinder", resource_type="volume", resource_id="vol-boot-1")
    assert r2.deleted_at is not None


@pytest.mark.asyncio
async def test_validate_resource_ownership():
    """Verify project + owner marker enforcement on OpenStack objects."""
    project_id = "proj-aaa"
    cluster_id = "cluster-bbb"

    # Valid Nova server object
    srv_valid = MagicMock()
    srv_valid.project_id = project_id
    srv_valid.metadata = {"drover.cluster_id": cluster_id, "drover.managed": "true"}
    assert inventory.validate_resource_ownership(srv_valid, project_id, cluster_id, "server") is True

    # Invalid project ID
    srv_wrong_proj = MagicMock()
    srv_wrong_proj.project_id = "proj-other"
    srv_wrong_proj.metadata = {"drover.cluster_id": cluster_id, "drover.managed": "true"}
    assert inventory.validate_resource_ownership(srv_wrong_proj, project_id, cluster_id, "server") is False

    # Invalid cluster ID in metadata
    srv_wrong_cluster = MagicMock()
    srv_wrong_cluster.project_id = project_id
    srv_wrong_cluster.metadata = {"drover.cluster_id": "cluster-wrong", "drover.managed": "true"}
    assert inventory.validate_resource_ownership(srv_wrong_cluster, project_id, cluster_id, "server") is False

    # Valid Neutron SG tags
    sg_valid = MagicMock()
    sg_valid.project_id = project_id
    sg_valid.tags = [f"drover.cluster_id={cluster_id}", "drover.managed=true"]
    assert inventory.validate_resource_ownership(sg_valid, project_id, cluster_id, "security_group") is True

    # Invalid SG tag
    sg_wrong_tag = MagicMock()
    sg_wrong_tag.project_id = project_id
    sg_wrong_tag.tags = ["drover.cluster_id=cluster-wrong"]
    assert inventory.validate_resource_ownership(sg_wrong_tag, project_id, cluster_id, "security_group") is False


@pytest.mark.asyncio
async def test_deletion_order_and_ownership_constraints():
    """Assert deletion executes in strict dependency order:

    VMs+members -> pools/listeners -> LBs -> FIPs -> volumes -> ports+SG -> app credentials.
    """
    cluster_id = "cluster-del-order-123"
    project_id = "proj-del-order-456"

    # Create dummy records
    nova_srv = ManagedOpenStackResource(id="1", cluster_id=cluster_id, service="nova", resource_type="server", resource_id="srv-vm-del")
    oct_mem = ManagedOpenStackResource(id="2", cluster_id=cluster_id, service="octavia", resource_type="member", resource_id="lb-mem-del", metadata_json={"pool_id": "lb-pool-del"})
    oct_pool = ManagedOpenStackResource(id="3", cluster_id=cluster_id, service="octavia", resource_type="pool", resource_id="lb-pool-del")
    oct_list = ManagedOpenStackResource(id="4", cluster_id=cluster_id, service="octavia", resource_type="listener", resource_id="lb-list-del")
    oct_lb = ManagedOpenStackResource(id="5", cluster_id=cluster_id, service="octavia", resource_type="load_balancer", resource_id="lb-main-del")
    net_fip = ManagedOpenStackResource(id="6", cluster_id=cluster_id, service="neutron", resource_type="floating_ip", resource_id="fip-del")
    cin_vol = ManagedOpenStackResource(id="7", cluster_id=cluster_id, service="cinder", resource_type="volume", resource_id="vol-del")
    net_port = ManagedOpenStackResource(id="8", cluster_id=cluster_id, service="neutron", resource_type="port", resource_id="port-del")
    net_rule = ManagedOpenStackResource(id="9", cluster_id=cluster_id, service="neutron", resource_type="security_group_rule", resource_id="sg-rule-del")
    net_sg = ManagedOpenStackResource(id="10", cluster_id=cluster_id, service="neutron", resource_type="security_group", resource_id="sg-del")
    ks_cred = ManagedOpenStackResource(id="11", cluster_id=cluster_id, service="keystone", resource_type="app_credential", resource_id="app-cred-del")

    mock_res_list = [nova_srv, oct_mem, oct_pool, oct_list, oct_lb, net_fip, cin_vol, net_port, net_rule, net_sg, ks_cred]

    conn = MagicMock()
    # Mock compute server find
    srv_mock = MagicMock()
    srv_mock.project_id = project_id
    srv_mock.metadata = {"drover.cluster_id": cluster_id, "drover.managed": "true"}
    conn.compute.find_server.return_value = srv_mock

    # Mock octavia LB find
    lb_mock = MagicMock()
    lb_mock.project_id = project_id
    lb_mock.tags = [f"drover.cluster_id={cluster_id}", "drover.managed=true"]
    conn.load_balancer.find_load_balancer.return_value = lb_mock

    # Mock FIP find
    fip_mock = MagicMock()
    fip_mock.project_id = project_id
    fip_mock.tags = [f"drover.cluster_id={cluster_id}"]
    conn.network.find_ip.return_value = fip_mock

    # Mock volume find
    vol_mock = MagicMock()
    vol_mock.project_id = project_id
    vol_mock.metadata = {"drover.cluster_id": cluster_id}
    conn.block_storage.find_volume.return_value = vol_mock

    # Mock SG find
    sg_mock = MagicMock()
    sg_mock.project_id = project_id
    sg_mock.tags = [f"drover.cluster_id={cluster_id}"]
    conn.network.find_security_group.return_value = sg_mock

    # Track deletion call order
    call_order = []

    def mock_del_server_safe(*args, **kwargs):
        call_order.append("nova_server")

    def mock_del_member(*args, **kwargs):
        call_order.append("octavia_member")

    def mock_del_pool(*args, **kwargs):
        call_order.append("octavia_pool")

    def mock_del_listener(*args, **kwargs):
        call_order.append("octavia_listener")

    def mock_del_lb_safe(*args, **kwargs):
        call_order.append("octavia_lb")

    def mock_del_fip_safe(*args, **kwargs):
        call_order.append("neutron_fip")

    def mock_del_vol_safe(*args, **kwargs):
        call_order.append("cinder_volume")

    def mock_del_port(*args, **kwargs):
        call_order.append("neutron_port")

    def mock_del_sg_safe(*args, **kwargs):
        call_order.append("neutron_sg")

    def mock_del_app_cred(*args, **kwargs):
        call_order.append("keystone_app_cred")

    cluster_dict = {
        "id": cluster_id,
        "name": "test-cluster",
        "server_vm_id": "srv-vm-del",
        "security_group_id": "sg-del",
        "api_lb_id": "lb-main-del",
        "api_fip_id": "fip-del",
        "app_credential_id": "app-cred-del",
    }

    with (
        patch("drover.services.inventory.list_managed_resources", AsyncMock(return_value=mock_res_list)),
        patch("drover.services.inventory.mark_resource_deleted", AsyncMock()),
        patch("drover.services.nova.delete_server_safe", side_effect=mock_del_server_safe),
        patch("drover.services.octavia.remove_member", side_effect=mock_del_member),
        patch("drover.services.octavia.delete_pool", side_effect=mock_del_pool),
        patch("drover.services.octavia.delete_listener", side_effect=mock_del_listener),
        patch("drover.services.octavia.delete_load_balancer_safe", side_effect=mock_del_lb_safe),
        patch("drover.services.neutron.delete_floating_ip_safe", side_effect=mock_del_fip_safe),
        patch("drover.services.cinder.delete_volume_safe", side_effect=mock_del_vol_safe),
        patch("drover.services.neutron.wait_port_deleted", return_value=None),
        patch("drover.services.neutron.delete_security_group_rule", return_value=None),
        patch("drover.services.neutron.delete_security_group_safe", side_effect=mock_del_sg_safe),
        patch("drover.services.keystone.delete_app_credential", side_effect=mock_del_app_cred),
        patch("drover.services.kube.delete_k8s_nodes", AsyncMock()),
        patch("drover.services.store.delete_cluster_record", AsyncMock()),
    ):
        conn.network.delete_port = MagicMock(side_effect=mock_del_port)
        async for _ in deletion.delete_cluster_progress(conn, project_id, cluster_dict, token_info={"user_id": "u1"}):
            pass

    # Verify exact ordering:
    assert "nova_server" in call_order
    assert "octavia_member" in call_order
    assert "octavia_pool" in call_order
    assert "octavia_listener" in call_order
    assert "octavia_lb" in call_order
    assert "neutron_fip" in call_order
    assert "cinder_volume" in call_order
    assert "neutron_port" in call_order
    assert "neutron_sg" in call_order
    assert "keystone_app_cred" in call_order

    idx_vm = call_order.index("nova_server")
    idx_mem = call_order.index("octavia_member")
    idx_pool = call_order.index("octavia_pool")
    idx_list = call_order.index("octavia_listener")
    idx_lb = call_order.index("octavia_lb")
    idx_fip = call_order.index("neutron_fip")
    idx_vol = call_order.index("cinder_volume")
    idx_port = call_order.index("neutron_port")
    idx_sg = call_order.index("neutron_sg")
    idx_cred = call_order.index("keystone_app_cred")

    # VMs/members before pools/listeners
    assert idx_vm < idx_pool
    assert idx_mem < idx_pool
    # Pools/listeners before LBs
    assert idx_pool < idx_lb
    assert idx_list < idx_lb
    # LBs before FIPs
    assert idx_lb < idx_fip
    # FIPs before volumes
    assert idx_fip < idx_vol
    # Volumes before ports/SGs
    assert idx_vol < idx_port
    assert idx_vol < idx_sg
    # SGs before app creds
    assert idx_sg < idx_cred


@pytest.mark.asyncio
async def test_no_legacy_prefix_cleanup_and_no_fixed_sleeps():
    """Verify that octavia.list_load_balancers with prefix filtering and fixed sleeps are absent from deletion service."""
    import inspect
    source = inspect.getsource(deletion)
    assert "kube_service_" not in source
    assert "kube_ingress_" not in source
    assert "asyncio.sleep(5)" not in source
