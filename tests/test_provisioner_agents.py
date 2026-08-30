"""Agent provisioning persistence and scaling-token contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
        patch("drover.services.keystone.get_admin_connection_for_project", return_value=connection),
        patch("drover.services.plugins.with_resource_policy_snapshot", side_effect=lambda settings, _snapshot: settings),
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


async def test_nodegroup_provisioning_reads_token_from_database_store() -> None:
    cluster = _cluster()
    get_token = AsyncMock(return_value="node-token")

    with (
        patch("drover.services.store.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.services.store.get_cluster_node_token", new=get_token),
        patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
        patch("drover.services.keystone.get_admin_connection_for_project", return_value=MagicMock()),
        patch("drover.services.plugins.with_resource_policy_snapshot", side_effect=lambda settings, _snapshot: settings),
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
