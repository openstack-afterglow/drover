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
