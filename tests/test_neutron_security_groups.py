"""Security-group creation contracts for Neutron deployments without create-time tags."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from drover.services import neutron


def _security_group():
    return SimpleNamespace(
        id="sg-1",
        name="k3s-test",
        description="managed",
        security_group_rules=[],
    )


def _floating_ip():
    return SimpleNamespace(
        id="fip-1",
        floating_ip_address="198.51.100.10",
        fixed_ip_address="192.0.2.10",
        status="ACTIVE",
        port_id="port-1",
        floating_network_id="public-net",
        project_id="project-1",
    )


def test_create_security_group_applies_tags_after_creation() -> None:
    connection = MagicMock()
    security_group = _security_group()
    connection.network.create_security_group.return_value = security_group

    result = neutron.create_security_group(
        connection,
        "k3s-test",
        "managed",
        tags=["drover.managed=true"],
    )

    connection.network.create_security_group.assert_called_once_with(
        name="k3s-test",
        description="managed",
    )
    connection.network.set_tags.assert_called_once_with(
        security_group,
        ["drover.managed=true"],
    )
    assert result["id"] == "sg-1"


def test_create_security_group_deletes_resource_when_tagging_fails() -> None:
    connection = MagicMock()
    security_group = _security_group()
    connection.network.create_security_group.return_value = security_group
    connection.network.set_tags.side_effect = RuntimeError("tagging failed")

    with pytest.raises(RuntimeError, match="tagging failed"):
        neutron.create_security_group(
            connection,
            "k3s-test",
            "managed",
            tags=["drover.managed=true"],
        )

    connection.network.delete_security_group.assert_called_once_with(
        security_group,
        ignore_missing=True,
    )


def test_create_floating_ip_applies_tags_after_creation() -> None:
    connection = MagicMock()
    floating_ip = _floating_ip()
    connection.network.create_ip.return_value = floating_ip

    result = neutron.create_floating_ip(
        connection,
        "public-net",
        port_id="port-1",
        tags=["drover.managed=true"],
    )

    connection.network.create_ip.assert_called_once_with(
        floating_network_id="public-net",
        port_id="port-1",
    )
    connection.network.set_tags.assert_called_once_with(
        floating_ip,
        ["drover.managed=true"],
    )
    assert result.id == "fip-1"


def test_create_floating_ip_deletes_resource_when_tagging_fails() -> None:
    connection = MagicMock()
    floating_ip = _floating_ip()
    connection.network.create_ip.return_value = floating_ip
    connection.network.set_tags.side_effect = RuntimeError("tagging failed")

    with pytest.raises(RuntimeError, match="tagging failed"):
        neutron.create_floating_ip(
            connection,
            "public-net",
            port_id="port-1",
            tags=["drover.managed=true"],
        )

    connection.network.delete_ip.assert_called_once_with(
        floating_ip,
        ignore_missing=True,
    )
