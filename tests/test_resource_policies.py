"""Drover-owned resource-policy and runtime-setting contracts."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from drover.services import resource_policies
from drover.services import resource_policy_store as store


class _NetworkAPI:
    def __init__(self, networks):
        self._networks = {network.id: network for network in networks}

    def get_network(self, resource_id):
        return self._networks.get(resource_id)


class _NetworkConnection:
    def __init__(self, networks):
        self.network = _NetworkAPI(networks)


def test_registry_contains_only_drover_owned_policy_keys():
    assert {spec.key for spec in resource_policies.list_specs()} == {
        "k3s.server_image",
        "k3s.fcos_image",
        "k3s.server_flavor",
        "k3s.default_agent_flavor",
        "k3s.volume_availability_zone",
        "k3s.default_network",
        "k3s.occm_floating_network",
        "k3s.occm_public_network",
        "k3s.lb_subnet",
        "k3s.api_lb_vip_network",
        "k3s.api_lb_floating_network",
        "k3s.octavia_ingress_floating_network",
    }


@pytest.mark.asyncio
async def test_external_and_shared_network_policies_filter_exact_catalog(monkeypatch):
    networks = [
        SimpleNamespace(
            id="private",
            name="private",
            is_external=False,
            is_router_external=False,
            is_shared=False,
        ),
        SimpleNamespace(
            id="shared",
            name="shared",
            is_external=False,
            is_router_external=False,
            is_shared=True,
        ),
        SimpleNamespace(
            id="public",
            name="public",
            is_external=True,
            is_router_external=True,
            is_shared=False,
        ),
    ]
    conn = _NetworkConnection(networks)
    monkeypatch.setattr(resource_policies.neutron, "list_networks", lambda _conn: networks)

    assert [option["id"] for option in await resource_policies.discover_options(conn, "k3s.default_network")] == [
        "shared"
    ]
    assert [
        option["id"] for option in await resource_policies.discover_options(conn, "k3s.occm_floating_network")
    ] == ["public"]
    with pytest.raises(resource_policies.ResourcePolicyValidationError):
        await resource_policies.validate_selection(conn, "k3s.default_network", "private")


@pytest.mark.parametrize("value", ["", "   ", True, 1, None])
def test_k3s_version_runtime_setting_rejects_invalid_values(value):
    with pytest.raises(store.RuntimeSettingValidationError):
        store._validate_runtime_value("k3s.version", value)


def test_runtime_setting_registry_is_drover_scoped():
    assert set(store.RUNTIME_SETTING_SPECS) == {"k3s.version"}
    assert store._validate_runtime_value("k3s.version", " v1.31.5+k3s1 ") == "v1.31.5+k3s1"
