"""Cinder volume normalization contracts."""

from types import SimpleNamespace

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
