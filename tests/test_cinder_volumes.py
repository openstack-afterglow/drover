"""Cinder volume normalization contracts."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from drover.services import cinder


def test_volume_info_maps_sdk_size_to_size_gb() -> None:
    volume = SimpleNamespace(
        id="volume-1",
        name="k3s-boot",
        status="available",
        size=30,
        volume_type="ceph",
        attachments=[],
        is_bootable="true",
        volume_image_metadata={"image_id": "image-1"},
    )

    result = cinder._vol_to_info(volume)

    assert result.size_gb == 30
    assert result.bootable is True
    assert result.volume_image_metadata == {"image_id": "image-1"}


def _owned_volume(*, status: str, attachments: list[dict]):
    return SimpleNamespace(
        id="volume-1",
        project_id="project-1",
        status=status,
        attachments=attachments,
        metadata={
            "drover.cluster_id": "cluster-1",
            "drover.managed": "true",
            "drover.resource_type": "volume",
        },
    )


def test_delete_volume_safe_accepts_nova_auto_deleted_volume() -> None:
    connection = MagicMock()
    attached = _owned_volume(status="in-use", attachments=[{"server_id": "server-1"}])
    connection.block_storage.find_volume.side_effect = [attached, attached, None]

    with patch("time.sleep"):
        cinder.delete_volume_safe(connection, "volume-1", "project-1", "cluster-1")

    connection.block_storage.delete_volume.assert_not_called()


def test_delete_volume_safe_waits_for_detach_before_delete() -> None:
    connection = MagicMock()
    attached = _owned_volume(status="in-use", attachments=[{"server_id": "server-1"}])
    available = _owned_volume(status="available", attachments=[])
    connection.block_storage.find_volume.side_effect = [attached, attached, available, None]

    with patch("time.sleep"):
        cinder.delete_volume_safe(connection, "volume-1", "project-1", "cluster-1")

    connection.block_storage.delete_volume.assert_called_once_with("volume-1", ignore_missing=True)


def test_delete_volume_safe_rejects_foreign_volume_before_polling() -> None:
    connection = MagicMock()
    foreign = _owned_volume(status="in-use", attachments=[{"server_id": "server-1"}])
    foreign.project_id = "other-project"
    connection.block_storage.find_volume.return_value = foreign

    with pytest.raises(ValueError, match="ownership validation failed"):
        cinder.delete_volume_safe(connection, "volume-1", "project-1", "cluster-1")

    connection.block_storage.find_volume.assert_called_once_with("volume-1", ignore_missing=True)
