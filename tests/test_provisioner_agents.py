"""Provisioner guest rendering, persistence, and scaling-token contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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


async def test_nodegroup_provisioning_reads_token_from_database_store() -> None:
    cluster = _cluster()
    get_token = AsyncMock(return_value="node-token")
    connection = MagicMock()
    with (
        patch("drover.services.store.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.services.store.get_cluster_node_token", new=get_token),
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
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
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
        patch("drover.services.cloudinit.generate_agent_userdata", return_value=userdata),
        patch(
            "drover.services.afterglow.create_provisioning_intent", new=AsyncMock(return_value={"state": "submitting"})
        ),
        patch("drover.services.afterglow.submit_provisioning_intent", new=submit_intent),
        patch("drover.services.inventory.record_resource", new=AsyncMock()),
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
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
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
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
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
