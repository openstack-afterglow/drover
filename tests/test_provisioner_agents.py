"""Provisioner guest rendering, persistence, and scaling-token contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openstack import exceptions as os_exceptions

from drover.config import Settings
from drover.services import autoscale, provisioner

pytestmark = pytest.mark.asyncio


def _cluster() -> dict:
    return {
        "id": "cluster-1",
        "project_id": "project-1",
        "name": "staging-cluster",
        "agent_count": 1,
        "agent_flavor_id": "flavor-1",
        "network_id": "network-1",
        "security_group_id": "sg-1",
        "ssh_public_key": None,
        "k3s_version": "v1.34.1+k3s1",
        "os_type": "ubuntu",
        "occm_enabled": False,
        "server_ip": "192.0.2.10",
        "resource_policy_snapshot": {
            "effective_agent_image": {"id": "image-1"},
            "k3s.volume_availability_zone": {"id": "nova"},
        },
    }


async def test_provision_agents_persists_created_vm_ids() -> None:
    cluster = _cluster()
    volume = SimpleNamespace(id="volume-1")
    server = SimpleNamespace(id="server-1")
    userdata = SimpleNamespace(data="cloud-init", config_drive=False)
    connection = MagicMock()

    with (
        patch("drover.services.provisioner.k3s_cluster.get_cluster", new=AsyncMock(return_value=cluster)),
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
        patch("drover.services.keystone.get_project_manager_connection", return_value=connection),
        patch(
            "drover.services.plugins.with_resource_policy_snapshot", side_effect=lambda settings, _snapshot: settings
        ),
        patch("drover.services.plugins.aggregate_agent_args", return_value=[]),
        patch("drover.services.cinder.create_volume_from_image", return_value=volume),
        patch("drover.services.cloudinit.generate_agent_userdata", return_value=userdata),
        patch("drover.services.nova.create_server", return_value=server),
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
        patch("drover.services.provisioner.k3s_cluster.add_agent_vms", new=AsyncMock()) as add_agent_vms,
        patch("drover.services.nodegroup.get_default_agent_nodegroup_id", new=AsyncMock(return_value="nodegroup-1")),
        patch("drover.services.nodegroup.add_nodegroup_vms", new=AsyncMock()) as add_nodegroup_vms,
        patch("drover.services.provisioner.k3s_cluster.update_cluster_status", new=AsyncMock()) as update_status,
    ):
        await provisioner.provision_agents("project-1", "cluster-1", "192.0.2.10", "node-token")

    expected_entries = [{"vm_id": "server-1", "name": add_agent_vms.await_args.args[1][0]["name"]}]
    add_agent_vms.assert_awaited_once_with("cluster-1", expected_entries)
    add_nodegroup_vms.assert_awaited_once_with("nodegroup-1", "cluster-1", expected_entries)
    assert update_status.await_args.kwargs["agent_vm_ids"] == ["server-1"]


async def test_provision_agents_reuses_persisted_ssh_key() -> None:
    cluster = _cluster()
    cluster["key_name"] = "caller-key"
    cluster["ssh_public_key"] = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 caller@example"
    connection = MagicMock()
    userdata = SimpleNamespace(data="cloud-init", config_drive=False)

    with (
        patch("drover.services.provisioner.k3s_cluster.get_cluster", new=AsyncMock(return_value=cluster)),
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
        patch("drover.services.keystone.get_project_manager_connection", return_value=connection),
        patch("drover.services.plugins.with_resource_policy_snapshot", side_effect=lambda settings, _snapshot: settings),
        patch("drover.services.plugins.aggregate_agent_args", return_value=[]),
        patch("drover.services.cinder.create_volume_from_image", return_value=SimpleNamespace(id="volume-1")),
        patch("drover.services.cloudinit.generate_agent_userdata", return_value=userdata) as generate_userdata,
        patch("drover.services.nova.create_server", return_value=SimpleNamespace(id="server-1")) as create_server,
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
        patch("drover.services.provisioner.k3s_cluster.add_agent_vms", new=AsyncMock()),
        patch("drover.services.nodegroup.get_default_agent_nodegroup_id", new=AsyncMock(return_value=None)),
        patch("drover.services.provisioner.k3s_cluster.update_cluster_status", new=AsyncMock()),
    ):
        await provisioner.provision_agents("project-1", "cluster-1", "192.0.2.10", "node-token")

    assert generate_userdata.call_args.kwargs["ssh_public_key"] == "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5"
    assert "key_name" not in create_server.call_args.kwargs


async def test_named_key_without_snapshot_fails_before_manager_connection() -> None:
    payload = {"key_name": "caller-key"}
    manager_connection = AsyncMock()

    with patch("drover.services.keystone.get_project_manager_connection", new=manager_connection):
        with pytest.raises(RuntimeError, match="SSH public key snapshot is missing"):
            await provisioner.create_cluster_job("project-1", "cluster-1", payload)

    manager_connection.assert_not_awaited()


async def test_primary_server_uses_snapshot_without_manager_key_name() -> None:
    payload = {
        "name": "staging-cluster",
        "master_count": 1,
        "allowed_cidrs": ["203.0.113.0/24"],
        "server_image_id": "image-1",
        "server_flavor_id": "flavor-1",
        "network_id": "network-1",
        "key_name": "caller-key",
        "ssh_public_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 caller@example",
        "k3s_version": "v1.34.1+k3s1",
        "resource_policy_snapshot": {"k3s.volume_availability_zone": {"id": "nova"}},
    }
    connection = MagicMock()
    connection.network.get_network.return_value = SimpleNamespace(name="private")
    userdata = SimpleNamespace(data="cloud-init", config_drive=False)

    with (
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30, drover_callback_base_url="https://drover.test")),
        patch("drover.services.keystone.get_project_manager_connection", new=AsyncMock(return_value=connection)),
        patch("drover.services.neutron.create_security_group", return_value={"id": "sg-1"}),
        patch("drover.services.neutron.create_security_group_rule", return_value={"id": "rule-1"}),
        patch("drover.services.cinder.create_volume_from_image", return_value=SimpleNamespace(id="volume-1")),
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
        patch("drover.services.cloudinit.generate_server_userdata", return_value=userdata) as generate_userdata,
        patch("drover.services.nova.create_server", return_value=SimpleNamespace(id="server-1")) as create_server,
        patch("drover.services.provisioner.k3s_cluster.create_callback_token", new=AsyncMock(return_value="callback-token")),
        patch("drover.services.provisioner.k3s_cluster.update_cluster_status", new=AsyncMock()),
        patch("drover.services.plugins.with_resource_policy_snapshot", side_effect=lambda settings, _snapshot: settings),
        patch("drover.services.plugins.get_active_plugin_names", return_value={}),
        patch("drover.services.plugins.aggregate_cloud_conf", return_value=""),
        patch("drover.services.plugins.aggregate_manifests", return_value=([], [])),
        patch("drover.services.plugins.aggregate_server_args", return_value=[]),
        patch("drover.services.plugins.aggregate_extra_write_files", return_value=[]),
        patch("drover.services.plugins.needs_external_cloud_provider", return_value=False),
        patch("drover.services.plugins.get_active_plugins", return_value=[]),
    ):
        await provisioner.create_cluster_job("project-1", "cluster-1", payload)

    assert generate_userdata.call_args.kwargs["ssh_public_key"] == "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5"
    assert "key_name" not in create_server.call_args.kwargs
    connection.close.assert_called_once_with()


async def test_create_cluster_renders_public_identity_url_without_changing_backend_settings() -> None:
    backend_url = "http://management.example.test:5000/v3"
    public_url = "https://identity.example.test:5000/v3"
    settings = Settings(
        os_auth_url=backend_url,
        os_region_name="RegionOne",
        drover_callback_base_url="https://drover.test",
        drover_occm_enabled=True,
        drover_cinder_csi_enabled=False,
        drover_manila_csi_enabled=False,
        drover_octavia_ingress_enabled=False,
        drover_keystone_auth_enabled=False,
        drover_barbican_kms_enabled=False,
    )
    snapshot = {
        "k3s.volume_availability_zone": {"id": "nova"},
        "k3s.occm_floating_network": {"id": "external-id"},
    }
    payload = {
        "name": "staging-cluster",
        "network_id": "network-1",
        "server_image_id": "image-1",
        "server_flavor_id": "flavor-1",
        "k3s_version": "v1.34.1+k3s1",
        "resource_policy_snapshot": snapshot,
    }
    connection = MagicMock()
    connection.session.get_endpoint.return_value = public_url
    connection.network.get_network.return_value = SimpleNamespace(name="private")
    userdata = SimpleNamespace(data="cloud-init", config_drive=False)

    with (
        patch("drover.config.get_settings", return_value=settings),
        patch("drover.services.keystone.get_project_manager_connection", new=AsyncMock(return_value=connection)),
        patch(
            "drover.services.keystone.create_app_credential_for_cluster",
            new=AsyncMock(return_value={"id": "cred-1", "secret": "secret-1"}),
        ),
        patch("drover.services.neutron.create_security_group", return_value={"id": "sg-1"}),
        patch("drover.services.neutron.create_security_group_rule", return_value={"id": "rule-1"}),
        patch("drover.services.cinder.create_volume_from_image", return_value=SimpleNamespace(id="volume-1")),
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
        patch("drover.services.cloudinit.generate_server_userdata", return_value=userdata) as generate_userdata,
        patch("drover.services.nova.create_server", return_value=SimpleNamespace(id="server-1")),
        patch(
            "drover.services.provisioner.k3s_cluster.create_callback_token",
            new=AsyncMock(return_value="callback-token"),
        ),
        patch("drover.services.provisioner.k3s_cluster.update_cluster_status", new=AsyncMock()),
    ):
        await provisioner.create_cluster_job("project-1", "cluster-1", payload)

    connection.session.get_endpoint.assert_called_once_with(
        service_type="identity", interface="public", region_name="RegionOne"
    )
    cloud_conf = generate_userdata.call_args.kwargs["cloud_conf"]
    assert f"auth-url={public_url}" in cloud_conf
    assert backend_url not in cloud_conf
    assert "floating-network-id=external-id" in cloud_conf
    assert settings.os_auth_url == backend_url


async def test_plugin_free_guest_settings_do_not_resolve_catalog_endpoint() -> None:
    settings = Settings(
        os_auth_url="http://management.example.test:5000/v3",
        drover_occm_enabled=False,
        drover_cinder_csi_enabled=False,
        drover_manila_csi_enabled=False,
        drover_octavia_ingress_enabled=False,
        drover_keystone_auth_enabled=False,
        drover_barbican_kms_enabled=False,
    )
    connection = MagicMock()
    guest = await provisioner._guest_plugin_settings(connection, settings, {})

    connection.session.get_endpoint.assert_not_called()
    assert guest.settings is settings


@pytest.mark.parametrize("endpoint", [None, "not-a-url", "ftp://identity.example.test/v3"])
async def test_missing_or_invalid_public_identity_endpoint_fails_before_resource_mutation(endpoint) -> None:
    settings = Settings(
        os_auth_url="http://management.example.test:5000/v3",
        drover_occm_enabled=True,
        drover_cinder_csi_enabled=False,
        drover_manila_csi_enabled=False,
        drover_octavia_ingress_enabled=False,
        drover_keystone_auth_enabled=False,
        drover_barbican_kms_enabled=False,
    )
    connection = MagicMock()
    connection.session.get_endpoint.return_value = endpoint
    payload = {"network_id": "network-1", "resource_policy_snapshot": {}}

    with (
        patch("drover.config.get_settings", return_value=settings),
        patch("drover.services.keystone.get_project_manager_connection", new=AsyncMock(return_value=connection)),
        patch("drover.services.neutron.create_security_group") as create_security_group,
        patch("drover.services.provisioner.k3s_cluster.update_cluster_status", new=AsyncMock()) as update_status,
    ):
        with pytest.raises(RuntimeError, match="Public identity endpoint is missing or invalid"):
            await provisioner.create_cluster_job("project-1", "cluster-1", payload)

    create_security_group.assert_not_called()
    assert update_status.await_args.args[2] == "ERROR"
    connection.close.assert_called_once_with()


async def test_occm_ha_joiners_boot_from_cluster_secret_without_app_credential() -> None:
    settings = Settings(
        os_auth_url="http://management.example.test:5000/v3",
        drover_callback_base_url="https://drover.test",
        drover_occm_enabled=True,
        drover_cinder_csi_enabled=False,
        drover_manila_csi_enabled=False,
        drover_octavia_ingress_enabled=False,
        drover_keystone_auth_enabled=False,
        drover_barbican_kms_enabled=False,
    )
    cluster = {
        "name": "ha-cluster",
        "k3s_version": "v1.34.1+k3s1",
        "server_image_id": "image-1",
        "server_flavor_id": "flavor-1",
        "network_id": "network-1",
        "resource_policy_snapshot": {"k3s.volume_availability_zone": {"id": "nova"}},
    }
    connection = MagicMock()
    connection.session.get_endpoint.return_value = "https://identity.example.test/v3"
    userdata = SimpleNamespace(data="cloud-init", config_drive=False)

    with (
        patch("drover.config.get_settings", return_value=settings),
        patch("drover.services.provisioner.k3s_cluster.get_cluster", new=AsyncMock(return_value=cluster)),
        patch("drover.services.keystone.get_project_manager_connection", new=AsyncMock(return_value=connection)),
        patch("drover.services.octavia.add_member", return_value={"id": "member-1"}),
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
        patch("drover.services.provisioner.k3s_cluster.create_ha_callback_token", new=AsyncMock(return_value="t")),
        patch("drover.services.cinder.create_volume_from_image", return_value=SimpleNamespace(id="volume-1")),
        patch("drover.services.cloudinit.generate_server_userdata", return_value=userdata) as generate_userdata,
        patch("drover.services.nova.create_server", return_value=SimpleNamespace(id="server-2")) as create_server,
    ):
        await provisioner.bootstrap_ha_servers(
            "project-1", "cluster-1", "192.0.2.10", "K10node::token", 3, "pool-1", "198.51.100.5"
        )

    # Rendering OCCM config here would need the unrecoverable app-credential secret and abort HA bootstrap.
    assert create_server.call_count == 2
    for call in generate_userdata.call_args_list:
        assert call.kwargs["cloud_conf"] is None
        assert "--disable=servicelb" in call.kwargs["extra_server_args"]



async def test_nodegroup_provisioning_reads_token_from_database_store() -> None:
    cluster = _cluster()
    get_token = AsyncMock(return_value="node-token")
    connection = MagicMock()
    with (
        patch("drover.services.store.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.services.store.get_cluster_node_token", new=get_token),
        patch("drover.services.autoscale.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
        patch("drover.services.keystone.get_project_manager_connection", return_value=connection),
        patch(
            "drover.services.plugins.with_resource_policy_snapshot", side_effect=lambda settings, _snapshot: settings
        ),
        patch("drover.services.plugins.aggregate_agent_args", return_value=[]),
    ):
        created = await autoscale.provision_nodegroup_vms(
            "project-1",
            "cluster-1",
            "nodegroup-1",
            0,
            flavor_id="flavor-1",
        )

    assert created == []
    get_token.assert_awaited_once_with("project-1", "cluster-1")
    connection.close.assert_called_once_with()


async def test_stampede_replays_submitting_intent_without_direct_openstack_create() -> None:
    cluster = _cluster()
    userdata = SimpleNamespace(data="Y2xvdWQtaW5pdA==", config_drive=False)
    submit_intent = AsyncMock(
        return_value={
            "state": "succeeded",
            "server_id": "server-1",
            "volume_id": "volume-1",
            "name": "staging-cluster-stampede-a",
        }
    )

    with (
        patch("drover.services.store.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.services.store.get_cluster_node_token", new=AsyncMock(return_value="node-token")),
        patch("drover.services.plugins.aggregate_agent_args", return_value=[]),
        patch("drover.services.autoscale.get_settings", return_value=SimpleNamespace(
            drover_boot_volume_size_gb=30, drover_afterglow_provisioning_url="https://afterglow.test",
        )),
        patch("drover.services.cloudinit.generate_agent_userdata", return_value=userdata),
        patch(
            "drover.services.afterglow.create_provisioning_intent", new=AsyncMock(return_value={"state": "submitting"})
        ),
        patch("drover.services.afterglow.submit_provisioning_intent", new=submit_intent),
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
        patch("drover.services.nodegroup.get_nodegroup", new=AsyncMock(return_value={"vms": []})),
        patch("drover.services.nodegroup.add_nodegroup_vms", new=AsyncMock()) as add_nodegroup_vms,
        patch("drover.services.cinder.create_volume_from_image") as create_volume,
        patch("drover.services.nova.create_server") as create_server,
    ):
        created = await autoscale.provision_nodegroup_vms(
            "project-1",
            "cluster-1",
            "nodegroup-1",
            1,
            flavor_id="flavor-1",
            provisioning_key_prefix="stampede-cluster-1-nodegroup-1-key",
        )

    expected_name = autoscale._stampede_node_name("staging-cluster", "stampede-cluster-1-nodegroup-1-key-node-0")
    assert created == [{"vm_id": "server-1", "name": expected_name}]
    submit_intent.assert_awaited_once()
    create_volume.assert_not_called()
    create_server.assert_not_called()
    add_nodegroup_vms.assert_awaited_once_with("nodegroup-1", "cluster-1", created)


async def test_stampede_defers_remote_submitting_intent_without_local_mutation() -> None:
    from drover.services.afterglow import ProvisioningRemoteError

    cluster = _cluster()
    userdata = SimpleNamespace(data="Y2xvdWQtaW5pdA==", config_drive=False)
    remote_in_progress = ProvisioningRemoteError(
        409,
        state="submitting",
        no_duplicate=True,
    )

    with (
        patch("drover.services.store.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.services.store.get_cluster_node_token", new=AsyncMock(return_value="node-token")),
        patch("drover.services.plugins.aggregate_agent_args", return_value=[]),
        patch("drover.services.autoscale.get_settings", return_value=SimpleNamespace(
            drover_boot_volume_size_gb=30, drover_afterglow_provisioning_url="https://afterglow.test",
        )),
        patch("drover.services.cloudinit.generate_agent_userdata", return_value=userdata),
        patch(
            "drover.services.afterglow.create_provisioning_intent",
            new=AsyncMock(return_value={"state": "submitting"}),
        ),
        patch(
            "drover.services.afterglow.submit_provisioning_intent",
            new=AsyncMock(side_effect=remote_in_progress),
        ),
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
        patch("drover.services.nodegroup.add_nodegroup_vms", new=AsyncMock()) as add_nodegroup_vms,
        patch("drover.services.cinder.create_volume_from_image") as create_volume,
        patch("drover.services.nova.create_server") as create_server,
    ):
        with pytest.raises(autoscale.ProvisioningInProgress):
            await autoscale.provision_nodegroup_vms(
                "project-1",
                "cluster-1",
                "nodegroup-1",
                1,
                flavor_id="flavor-1",
                provisioning_key_prefix="stampede-cluster-1-nodegroup-1-key",
            )

    create_volume.assert_not_called()
    create_server.assert_not_called()
    add_nodegroup_vms.assert_not_awaited()


async def test_stampede_retries_non_deferred_remote_error_without_local_mutation() -> None:
    from drover.services.afterglow import ProvisioningRemoteError

    cluster = _cluster()
    remote_error = ProvisioningRemoteError(
        409,
        state="submitting",
        no_duplicate=False,
    )

    with (
        patch("drover.services.store.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.services.store.get_cluster_node_token", new=AsyncMock(return_value="node-token")),
        patch("drover.services.plugins.aggregate_agent_args", return_value=[]),
        patch("drover.services.autoscale.get_settings", return_value=SimpleNamespace(
            drover_boot_volume_size_gb=30, drover_afterglow_provisioning_url="https://afterglow.test",
        )),
        patch(
            "drover.services.cloudinit.generate_agent_userdata",
            return_value=SimpleNamespace(data="Y2xvdWQtaW5pdA==", config_drive=False),
        ),
        patch(
            "drover.services.afterglow.create_provisioning_intent",
            new=AsyncMock(return_value={"state": "submitting"}),
        ),
        patch(
            "drover.services.afterglow.submit_provisioning_intent",
            new=AsyncMock(side_effect=remote_error),
        ),
        patch("drover.services.nodegroup.add_nodegroup_vms", new=AsyncMock()) as add_nodegroup_vms,
        patch("drover.services.cinder.create_volume_from_image") as create_volume,
        patch("drover.services.nova.create_server") as create_server,
    ):
        with pytest.raises(ProvisioningRemoteError) as raised:
            await autoscale.provision_nodegroup_vms(
                "project-1",
                "cluster-1",
                "nodegroup-1",
                1,
                flavor_id="flavor-1",
                provisioning_key_prefix="stampede-cluster-1-nodegroup-1-key",
            )

    assert raised.value is remote_error
    create_volume.assert_not_called()
    create_server.assert_not_called()
    add_nodegroup_vms.assert_not_awaited()


@pytest.fixture
def native_nodegroup(monkeypatch):
    """No real DB, API, cloud-init, sleeps or external configuration."""
    connection = MagicMock()
    connection.compute.servers.return_value = []
    connection.block_storage.volumes.return_value = []
    connection.compute.find_server.return_value = None
    connection.block_storage.find_volume.return_value = None
    tracked = []
    add = AsyncMock(side_effect=lambda _ng, _cluster, entries: tracked.extend(entries))
    volume = SimpleNamespace(id="volume-1", status="available")
    server = SimpleNamespace(id="server-1", status="ACTIVE")
    create_volume = MagicMock(return_value=volume)
    create_server = MagicMock(return_value=server)
    remote = AsyncMock()
    monkeypatch.setattr(autoscale, "get_settings", lambda: SimpleNamespace(
        drover_boot_volume_size_gb=30, drover_afterglow_provisioning_url="",
    ))
    monkeypatch.setattr("drover.services.store.get_cluster_admin", AsyncMock(return_value=_cluster()))
    monkeypatch.setattr("drover.services.store.get_cluster_node_token", AsyncMock(return_value="node-token"))
    monkeypatch.setattr("drover.services.keystone.get_project_manager_connection", AsyncMock(return_value=connection))
    monkeypatch.setattr("drover.services.keystone.close_connection", AsyncMock())
    monkeypatch.setattr("drover.services.plugins.aggregate_agent_args", lambda _settings: [])
    monkeypatch.setattr("drover.services.cloudinit.generate_agent_userdata", MagicMock(
        return_value=SimpleNamespace(data="cloud-init", config_drive=False),
    ))
    monkeypatch.setattr("drover.services.nodegroup.get_nodegroup", AsyncMock(return_value={"vms": tracked}))
    monkeypatch.setattr("drover.services.nodegroup.add_nodegroup_vms", add)
    monkeypatch.setattr("drover.services.inventory.list_managed_resources", AsyncMock(return_value=[]))
    monkeypatch.setattr("drover.services.inventory.record_resource", AsyncMock())
    monkeypatch.setattr("drover.services.cinder.create_volume_from_image", create_volume)
    monkeypatch.setattr("drover.services.nova.create_server", create_server)
    monkeypatch.setattr("drover.services.afterglow.create_provisioning_intent", remote)
    return SimpleNamespace(
        connection=connection, tracked=tracked, add=add, volume=volume, server=server,
        create_volume=create_volume, create_server=create_server, remote=remote,
    )


async def _provision_native(add_count=1):
    return await autoscale.provision_nodegroup_vms(
        "project-1", "cluster-1", "nodegroup-1", add_count,
        flavor_id="flavor-1", provisioning_key_prefix="stampede-native-key",
    )


async def test_native_stampede_without_afterglow_records_stable_keyed_resources(native_nodegroup):
    mocks = native_nodegroup
    created = await _provision_native()
    expected_name = autoscale._stampede_node_name("staging-cluster", "stampede-native-key-node-0")
    assert created == [{"vm_id": "server-1", "name": expected_name}]
    mocks.remote.assert_not_awaited()
    assert mocks.create_volume.call_args.args[1] == f"{expected_name}-boot"
    assert mocks.create_volume.call_args.kwargs["metadata"]["drover.provisioning_idempotency_key"] == (
        "stampede-native-key-node-0"
    )
    assert mocks.create_server.call_args.kwargs["metadata"]["drover.provisioning_idempotency_key"] == (
        "stampede-native-key-node-0"
    )
    mocks.add.assert_awaited_once_with("nodegroup-1", "cluster-1", created)


async def test_native_stampede_retry_reuses_recorded_server_without_duplicate_vm_or_volume(native_nodegroup):
    mocks = native_nodegroup
    created = await _provision_native()
    mocks.connection.compute.find_server.return_value = mocks.server
    replayed = await _provision_native()
    assert replayed == created
    mocks.create_volume.assert_called_once()
    mocks.create_server.assert_called_once()
    mocks.add.assert_awaited_once()


async def test_manual_operation_retry_reuses_vm_and_boot_volume(native_nodegroup, monkeypatch):
    from drover.services import jobs

    mocks = native_nodegroup
    servers = {}
    volumes = {}
    group = {"id": "nodegroup-1", "flavor_id": "flavor-1", "vms": mocks.tracked, "node_count": 0}

    def create_volume(_conn, name, *_args, metadata=None, **_kwargs):
        volume = SimpleNamespace(id=f"volume-{len(volumes) + 1}", name=name, status="available", metadata=metadata)
        volumes[volume.id] = volume
        return volume

    def create_server(_conn, name, *_args, metadata=None, **_kwargs):
        server = SimpleNamespace(
            id=f"server-{len(servers) + 1}", name=name, status="ACTIVE", task_state=None,
            project_id="project-1", metadata=metadata,
        )
        servers[server.id] = server
        return server

    async def set_count(_, __, count):
        group["node_count"] = count

    mocks.create_volume.side_effect = create_volume
    mocks.create_server.side_effect = create_server
    mocks.connection.compute.servers.side_effect = lambda **_: list(servers.values())
    mocks.connection.compute.find_server.side_effect = lambda vm_id, **_: servers.get(vm_id)
    mocks.connection.block_storage.volumes.side_effect = lambda **_: list(volumes.values())
    monkeypatch.setattr("drover.services.nova.list_flavors", lambda _: [SimpleNamespace(id="flavor-1", extra_specs={})])
    monkeypatch.setattr("drover.services.nova.observe_server", lambda _, vm_id: servers.get(vm_id))
    monkeypatch.setattr("drover.services.nodegroup.get_nodegroup", AsyncMock(side_effect=lambda *_: group))
    monkeypatch.setattr("drover.services.nodegroup.set_nodegroup_count", set_count)
    monkeypatch.setattr("drover.services.operations.append_operation_event", AsyncMock())
    payload = {"action": "provision", "nodegroup": group, "add_count": 1}
    for _ in range(2):
        await jobs._execute_job_direct("nodegroup_reconcile", payload, "cluster-1", "project-1", operation_id="operation-1")
    assert set(servers) == {"server-1"} and set(volumes) == {"volume-1"}
    assert [vm["vm_id"] for vm in mocks.tracked] == ["server-1"]
    assert group["node_count"] == 1
    assert servers["server-1"].metadata["drover.provisioning_idempotency_key"] == "nodegroup-cluster-1-nodegroup-1-operation-1-node-0"

    await jobs._execute_job_direct("nodegroup_reconcile", payload, "cluster-1", "project-1", operation_id="operation-2")
    assert set(servers) == {"server-1", "server-2"} and set(volumes) == {"volume-1", "volume-2"}
    assert {vm["vm_id"] for vm in mocks.tracked} == {"server-1", "server-2"}
    assert group["node_count"] == 2
    assert servers["server-2"].metadata["drover.provisioning_idempotency_key"] == "nodegroup-cluster-1-nodegroup-1-operation-2-node-0"



async def test_native_stampede_recovers_nova_acceptance_before_database_record(native_nodegroup):
    mocks = native_nodegroup
    name = autoscale._stampede_node_name("staging-cluster", "stampede-native-key-node-0")
    mocks.server.name = name
    mocks.server.metadata = {
        "drover.cluster_id": "cluster-1", "drover.provisioning_idempotency_key": "stampede-native-key-node-0",
    }
    mocks.connection.compute.servers.return_value = [mocks.server]
    created = await _provision_native()
    assert created == [{"vm_id": "server-1", "name": name}]
    mocks.create_volume.assert_not_called()
    mocks.create_server.assert_not_called()
    mocks.add.assert_awaited_once()


async def test_native_stampede_retry_reuses_orphan_boot_volume(native_nodegroup):
    mocks = native_nodegroup
    name = autoscale._stampede_node_name("staging-cluster", "stampede-native-key-node-0")
    mocks.volume.name = f"{name}-boot"
    mocks.volume.metadata = {
        "drover.cluster_id": "cluster-1", "drover.provisioning_idempotency_key": "stampede-native-key-node-0",
    }
    mocks.connection.block_storage.volumes.return_value = [mocks.volume]
    await _provision_native()
    mocks.create_volume.assert_not_called()
    assert mocks.create_server.call_args.args[4] == "volume-1"


async def test_native_stampede_records_first_vm_before_second_failure(native_nodegroup):
    mocks = native_nodegroup
    mocks.create_server.side_effect = [mocks.server, RuntimeError("Nova unavailable")]
    with pytest.raises(RuntimeError, match="Nova unavailable"):
        await _provision_native(add_count=2)
    assert len(mocks.tracked) == 1
    assert mocks.tracked[0]["vm_id"] == "server-1"


async def test_native_stampede_observation_failure_never_creates_duplicate(native_nodegroup):
    mocks = native_nodegroup
    mocks.connection.compute.servers.side_effect = RuntimeError("Nova listing unavailable")
    with pytest.raises(RuntimeError, match="Nova listing unavailable"):
        await _provision_native()
    mocks.create_volume.assert_not_called()
    mocks.create_server.assert_not_called()


@pytest.fixture
def nodegroup_deletion(monkeypatch):
    connection = MagicMock()
    connection.compute.get_server.return_value = SimpleNamespace(
        id="server-1", status="ACTIVE", task_state=None, project_id="project-1",
        metadata={"drover.cluster_id": "cluster-1", "drover.managed": "true"},
    )
    cordon = AsyncMock(return_value=True)
    drain = AsyncMock(return_value=True)
    uncordon = AsyncMock(return_value=True)
    delete_node = AsyncMock(return_value=True)
    delete_server = connection.compute.delete_server
    wait_deleted = MagicMock()
    remove = AsyncMock()
    marked = AsyncMock()
    monkeypatch.setattr("drover.services.keystone.get_project_manager_connection", AsyncMock(return_value=connection))
    monkeypatch.setattr("drover.services.keystone.close_connection", AsyncMock())
    monkeypatch.setattr("drover.services.kube.cordon_node", cordon)
    monkeypatch.setattr("drover.services.kube.drain_node", drain)
    monkeypatch.setattr("drover.services.kube.get_node_capacity", AsyncMock(return_value=[]))
    monkeypatch.setattr("drover.services.kube.uncordon_node", uncordon)
    monkeypatch.setattr("drover.services.kube.delete_k8s_node", delete_node)
    monkeypatch.setattr("drover.services.nova.wait_server_deleted", wait_deleted)
    monkeypatch.setattr("drover.services.nodegroup.remove_nodegroup_vms", remove)
    monkeypatch.setattr("drover.services.inventory.list_managed_resources", AsyncMock(return_value=[]))
    monkeypatch.setattr("drover.services.inventory.mark_resource_deleted", marked)
    monkeypatch.setattr("drover.services.activity.record", AsyncMock())
    return SimpleNamespace(
        connection=connection, cordon=cordon, drain=drain, uncordon=uncordon, wait_deleted=wait_deleted,
        delete_node=delete_node, delete_server=delete_server, remove=remove, marked=marked,
    )


async def _delete_nodegroup():
    await autoscale.delete_nodegroup_vms(
        "project-1", "cluster-1", "nodegroup-1", [{"vm_id": "server-1", "name": "worker-1"}],
    )


@pytest.mark.parametrize("boundary", ["cordon", "drain"])
async def test_nodegroup_delete_preserves_vm_and_record_when_drain_blocked(nodegroup_deletion, boundary):
    mocks = nodegroup_deletion
    getattr(mocks, boundary).return_value = False
    with pytest.raises(autoscale.NodegroupDeletionError, match=f"{boundary}_failed:worker-1"):
        await _delete_nodegroup()
    mocks.uncordon.assert_awaited_once_with("cluster-1", "worker-1")
    mocks.delete_server.assert_not_called()
    mocks.delete_node.assert_not_awaited()
    mocks.remove.assert_not_awaited()
    mocks.marked.assert_not_awaited()




async def test_nodegroup_delete_reports_uncordon_failure(nodegroup_deletion):
    mocks = nodegroup_deletion
    mocks.drain.return_value = False
    mocks.uncordon.return_value = False
    with pytest.raises(autoscale.NodegroupDeletionError) as raised:
        await _delete_nodegroup()
    assert raised.value.reason == "drain_failed:worker-1"
    assert raised.value.uncordon_failed == ["worker-1"]


async def test_nodegroup_delete_success_orders_drain_nova_node_and_inventory(nodegroup_deletion):
    mocks = nodegroup_deletion
    order = []
    mocks.drain.side_effect = lambda *_args: order.append("drain") or True
    mocks.delete_server.side_effect = lambda *_args, **_kwargs: order.append("nova")
    mocks.delete_node.side_effect = lambda *_args: order.append("node") or True
    mocks.remove.side_effect = lambda *_args: order.append("db")
    await _delete_nodegroup()
    assert order == ["drain", "nova", "node", "db"]
    mocks.cordon.assert_awaited_once_with("cluster-1", "worker-1", removal_vm_id="server-1")
    # Normal deletion only, with verified disappearance.
    mocks.delete_server.assert_called_once_with("server-1", ignore_missing=True)
    mocks.wait_deleted.assert_called_once_with(mocks.connection, "server-1")
    mocks.uncordon.assert_not_awaited()
    mocks.marked.assert_awaited_once_with(service="nova", resource_type="server", resource_id="server-1")


async def test_nodegroup_delete_retry_after_nova_gone_only_cleans_node_and_record(nodegroup_deletion):
    mocks = nodegroup_deletion
    mocks.connection.compute.get_server.side_effect = os_exceptions.NotFoundException("gone")
    await _delete_nodegroup()
    mocks.cordon.assert_not_awaited()
    mocks.drain.assert_not_awaited()
    mocks.delete_node.assert_awaited_once()
    mocks.remove.assert_awaited_once_with("nodegroup-1", ["server-1"])


async def test_nodegroup_delete_keeps_record_when_node_cleanup_fails(nodegroup_deletion):
    mocks = nodegroup_deletion
    mocks.delete_node.return_value = False
    with pytest.raises(autoscale.NodegroupDeletionError, match="node_delete_failed:worker-1"):
        await _delete_nodegroup()
    mocks.remove.assert_not_awaited()


async def test_nodegroup_delete_final_ownership_recheck_preserves_server(nodegroup_deletion):
    mocks = nodegroup_deletion
    owned = mocks.connection.compute.get_server.return_value
    foreign = SimpleNamespace(**{**vars(owned), "project_id": "project-2"})
    mocks.connection.compute.get_server.side_effect = [owned, foreign]
    with pytest.raises(autoscale.NodegroupDeletionError, match="server_delete_failed:server-1"):
        await _delete_nodegroup()
    mocks.delete_server.assert_not_called()
    mocks.uncordon.assert_awaited_once_with("cluster-1", "worker-1")
    mocks.remove.assert_not_awaited()


async def test_native_stampede_ambiguous_resources_never_duplicate(native_nodegroup):
    mocks = native_nodegroup
    name = autoscale._stampede_node_name("staging-cluster", "stampede-native-key-node-0")
    metadata = {"drover.cluster_id": "cluster-1", "drover.provisioning_idempotency_key": "stampede-native-key-node-0"}
    mocks.connection.block_storage.volumes.return_value = [
        SimpleNamespace(id="vol-a", name=f"{name}-boot", metadata=metadata),
        SimpleNamespace(id="vol-b", name=f"{name}-boot", metadata=metadata),
    ]
    with pytest.raises(RuntimeError, match="Ambiguous provisioning resources"):
        await _provision_native()
    mocks.create_volume.assert_not_called()
    mocks.create_server.assert_not_called()


async def test_native_stampede_building_server_recorded_before_active_wait(native_nodegroup):
    mocks = native_nodegroup
    name = autoscale._stampede_node_name("staging-cluster", "stampede-native-key-node-0")
    mocks.server.name = name
    mocks.server.status = "BUILD"
    mocks.server.metadata = {
        "drover.cluster_id": "cluster-1", "drover.provisioning_idempotency_key": "stampede-native-key-node-0",
    }
    mocks.connection.compute.servers.return_value = [mocks.server]
    mocks.connection.compute.wait_for_server.side_effect = TimeoutError("server ACTIVE timeout")
    with pytest.raises(TimeoutError, match="server ACTIVE timeout"):
        await _provision_native()
    assert mocks.tracked == [{"vm_id": "server-1", "name": name}]
    mocks.create_server.assert_not_called()


async def test_nodegroup_delete_boot_volume_timeout_retains_cleanup_record(nodegroup_deletion, monkeypatch):
    mocks = nodegroup_deletion
    monkeypatch.setattr("drover.services.inventory.list_managed_resources", AsyncMock(return_value=[
        SimpleNamespace(service="cinder", resource_type="volume", resource_id="volume-1", name="worker-1-boot"),
    ]))
    cleanup = MagicMock(side_effect=TimeoutError("volume still exists"))
    monkeypatch.setattr("drover.services.cinder.delete_detached_boot_volume", cleanup)
    with pytest.raises(autoscale.NodegroupDeletionError, match="boot_volume_delete_unverified:volume-1"):
        await _delete_nodegroup()
    mocks.delete_server.assert_called_once()
    mocks.delete_node.assert_not_awaited()
    mocks.remove.assert_not_awaited()
    mocks.marked.assert_awaited_once_with(service="nova", resource_type="server", resource_id="server-1")


async def test_nodegroup_delete_drains_entire_batch_before_any_nova_delete(nodegroup_deletion):
    mocks = nodegroup_deletion
    mocks.drain.side_effect = [True, False]
    with pytest.raises(autoscale.NodegroupDeletionError, match="drain_failed:worker-2"):
        await autoscale.delete_nodegroup_vms(
            "project-1", "cluster-1", "nodegroup-1",
            [{"vm_id": "server-1", "name": "worker-1"}, {"vm_id": "server-2", "name": "worker-2"}],
        )
    mocks.delete_server.assert_not_called()
    mocks.remove.assert_not_awaited()
    assert mocks.uncordon.await_count == 2


async def test_native_stampede_new_server_is_tracked_before_active_timeout(native_nodegroup):
    mocks = native_nodegroup
    mocks.connection.compute.wait_for_server.side_effect = TimeoutError("server ACTIVE timeout")
    with pytest.raises(TimeoutError, match="server ACTIVE timeout"):
        await _provision_native()
    assert mocks.create_server.call_args.kwargs["wait"] is False
    assert len(mocks.tracked) == 1
    assert mocks.tracked[0]["vm_id"] == "server-1"


@pytest.mark.parametrize("wait", [True, False])
async def test_nova_create_can_return_resource_for_immediate_tracking(wait):
    from openstack.compute.v2.server import Server

    from drover.models.openstack import InstanceInfo
    from drover.services import nova

    connection = MagicMock()
    # Nova server-create-resp.json does not contain name, status or flavor.
    accepted = Server(id="server-1", links=[], security_groups=[{"name": "default"}])
    active = Server(id="server-1", name="worker", status="ACTIVE", flavor={"id": "flavor-1"})
    connection.compute.create_server.return_value = accepted
    connection.compute.wait_for_server.return_value = active
    result = nova.create_server(connection, "worker", "flavor-1", "network-1", "volume-1", wait=wait)
    if wait:
        assert isinstance(result, InstanceInfo)
        assert result.id == "server-1"
        assert result.status == "ACTIVE"
        assert result.flavor_id == "flavor-1"
        connection.compute.wait_for_server.assert_called_once_with(accepted, status="ACTIVE", wait=600)
    else:
        assert result is accepted
        assert result.id == "server-1"
        assert result.status is None
        assert result.flavor is None
        connection.compute.wait_for_server.assert_not_called()


async def test_stampede_succeeded_intent_replay_does_not_duplicate_tracking(native_nodegroup, monkeypatch):
    mocks = native_nodegroup
    monkeypatch.setattr(autoscale, "get_settings", lambda: SimpleNamespace(
        drover_boot_volume_size_gb=30, drover_afterglow_provisioning_url="https://afterglow.test",
    ))
    mocks.remote.return_value = {"state": "succeeded", "server_id": "server-1", "volume_id": "volume-1"}
    first = await _provision_native()
    replayed = await _provision_native()
    assert replayed == first
    assert mocks.remote.await_count == 2
    mocks.add.assert_awaited_once()
    mocks.create_volume.assert_not_called()
    mocks.create_server.assert_not_called()


async def test_native_gpu_worker_forwards_required_bootstrap(native_nodegroup):
    with patch("drover.services.cloudinit.generate_agent_userdata", return_value=SimpleNamespace(
        data="gpu-cloud-init", config_drive=False,
    )) as userdata:
        await autoscale.provision_nodegroup_vms(
            "project-1", "cluster-1", "nodegroup-1", 1, flavor_id="gpu-flavor",
            provisioning_key_prefix="stampede-gpu-key", gpu_required=True,
        )
    assert userdata.call_args.kwargs["gpu_required"] is True


@pytest.mark.parametrize("key_prefix", [None, "nodegroup-cluster-1-nodegroup-1-operation-1"])
async def test_manual_gpu_worker_detects_flavor_and_ensures_device_plugin(native_nodegroup, key_prefix):
    with (
        patch("drover.services.nova.list_flavors", return_value=[
            SimpleNamespace(id="gpu-flavor", extra_specs={"gpu_count": "1"}),
        ]),
        patch("drover.services.gpu.ensure_device_plugin", new=AsyncMock()) as plugin,
        patch("drover.services.cloudinit.generate_agent_userdata", return_value=SimpleNamespace(
            data="gpu-cloud-init", config_drive=False,
        )) as userdata,
    ):
        await autoscale.provision_nodegroup_vms(
            "project-1", "cluster-1", "nodegroup-1", 1, flavor_id="gpu-flavor",
            provisioning_key_prefix=key_prefix, inspect_flavor_gpu=True,
        )
    plugin.assert_awaited_once_with("cluster-1")
    assert userdata.call_args.kwargs["gpu_required"] is True


async def test_remote_gpu_worker_forwards_required_bootstrap(native_nodegroup, monkeypatch):
    mocks = native_nodegroup
    monkeypatch.setattr(autoscale, "get_settings", lambda: SimpleNamespace(
        drover_boot_volume_size_gb=30, drover_afterglow_provisioning_url="https://afterglow.test",
    ))
    mocks.remote.return_value = {"state": "pending"}
    with (
        patch("drover.services.afterglow.submit_provisioning_intent", new=AsyncMock(return_value={
            "state": "succeeded", "server_id": "server-1", "volume_id": "volume-1",
        })),
        patch("drover.services.cloudinit.generate_agent_userdata", return_value=SimpleNamespace(
            data="gpu-cloud-init", config_drive=False,
        )) as userdata,
    ):
        await autoscale.provision_nodegroup_vms(
            "project-1", "cluster-1", "nodegroup-1", 1, flavor_id="gpu-flavor",
            provisioning_key_prefix="stampede-gpu-key", gpu_required=True,
        )
    assert userdata.call_args.kwargs["gpu_required"] is True
    mocks.create_server.assert_not_called()
