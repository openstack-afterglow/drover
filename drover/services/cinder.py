from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from drover.models.openstack import VolumeInfo


def create_volume_from_image(
    conn: openstack.connection.Connection,
    name: str,
    image_id: str,
    size_gb: int,
    availability_zone: str | None = None,
    metadata: dict | None = None,
) -> VolumeInfo:
    """OS 이미지를 소스로 부트 볼륨 생성."""
    kwargs: dict = {
        "name": name,
        "size": size_gb,
        "imageRef": image_id,
    }
    if availability_zone:
        kwargs["availability_zone"] = availability_zone
    if metadata:
        kwargs["metadata"] = metadata

    vol = conn.block_storage.create_volume(**kwargs)
    vol = conn.block_storage.wait_for_status(vol, status="available", wait=300)
    return _vol_to_info(vol)


def create_empty_volume(
    conn: openstack.connection.Connection,
    name: str,
    size_gb: int,
    availability_zone: str | None = None,
    metadata: dict | None = None,
) -> VolumeInfo:
    """upperdir 용 빈 볼륨 생성."""
    kwargs: dict = {"name": name, "size": size_gb}
    if availability_zone:
        kwargs["availability_zone"] = availability_zone
    if metadata:
        kwargs["metadata"] = metadata

    vol = conn.block_storage.create_volume(**kwargs)
    vol = conn.block_storage.wait_for_status(vol, status="available", wait=120)
    return _vol_to_info(vol)

def rename_volume(conn: openstack.connection.Connection, volume_id: str, new_name: str) -> None:
    conn.block_storage.update_volume(volume_id, name=new_name)


def delete_volume(conn: openstack.connection.Connection, volume_id: str) -> None:
    conn.block_storage.delete_volume(volume_id, ignore_missing=True)


def wait_volume_deleted(conn: openstack.connection.Connection, volume_id: str, timeout: int = 120) -> None:
    """볼륨이 완전히 삭제될 때까지 폴링."""
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        vol = conn.block_storage.find_volume(volume_id, ignore_missing=True)
        if vol is None:
            return
        time.sleep(3)
    raise TimeoutError(f"볼륨 {volume_id} 삭제 대기 타임아웃 ({timeout}s)")


def delete_volume_safe(
    conn: openstack.connection.Connection,
    volume_id: str,
    expected_project_id: str,
    expected_cluster_id: str,
) -> None:
    """프로젝트 및 Drover 소유권 검증 후 볼륨을 안전하게 삭제."""
    from drover.services.inventory import validate_resource_ownership

    vol = conn.block_storage.find_volume(volume_id, ignore_missing=True)
    if vol is None:
        return
    if not validate_resource_ownership(vol, expected_project_id, expected_cluster_id, "volume"):
        raise ValueError(f"Volume {volume_id} ownership validation failed for project {expected_project_id}")
    delete_volume(conn, volume_id)
    wait_volume_deleted(conn, volume_id)

def reset_volume_status(
    conn: openstack.connection.Connection,
    volume_id: str,
    status: str = "error",
    attach_status: str | None = None,
) -> None:
    """볼륨 상태를 강제로 변경한다 (Cinder os-reset_status action).

    attach_status 지정 시 함께 변경한다 (예: 'attached'). 기존 호출부 호환 유지.
    """
    conn.block_storage.reset_volume_status(volume_id, status, attach_status)


def force_delete_volume(conn: openstack.connection.Connection, volume_id: str) -> None:
    """볼륨을 강제 삭제한다 (Cinder os-force_delete action). 관리자 전용."""
    endpoint = conn.block_storage.get_endpoint()
    resp = conn.session.post(
        f"{endpoint}/volumes/{volume_id}/action",
        json={"os-force_delete": {}},
    )
    if hasattr(resp, "raise_for_status"):
        resp.raise_for_status()


def extend_volume(conn: openstack.connection.Connection, volume_id: str, new_size: int) -> None:
    conn.block_storage.extend_volume(volume_id, new_size)


def get_volume(conn: openstack.connection.Connection, volume_id: str) -> VolumeInfo:
    vol = conn.block_storage.get_volume(volume_id)
    return _vol_to_info(vol)


def list_volumes(conn: openstack.connection.Connection) -> list[VolumeInfo]:
    return [_vol_to_info(v) for v in conn.block_storage.volumes(details=True)]


def get_volume_limits(conn: openstack.connection.Connection) -> dict:
    """프로젝트의 Cinder 리소스 사용량/한도 조회."""
    limits = conn.block_storage.get_limits()
    a = limits.absolute
    return {
        "volumes_used": getattr(a, "total_volumes_used", 0),
        "volumes_limit": getattr(a, "max_total_volumes", -1),
        "gigabytes_used": getattr(a, "total_gigabytes_used", 0),
        "gigabytes_limit": getattr(a, "max_total_volume_gigabytes", -1),
    }


_QUOTA_KEYS = ("volumes", "snapshots", "gigabytes", "backups", "backup_gigabytes")


def get_volume_quota(conn: openstack.connection.Connection, project_id: str, *, strict: bool = False) -> dict:
    """프로젝트의 상세 Cinder 할당량 (usage 포함).

    Overview callers use ``strict`` to require explicit usage fields for the
    rendered resources.  Default callers keep legacy sentinel fallback
    semantics intact.
    """

    def _normalize(q) -> dict:
        if isinstance(q, dict):
            return {"limit": int(q.get("limit", -1)), "in_use": int(q.get("in_use", 0))}
        if isinstance(q, int):
            return {"limit": q, "in_use": 0}
        return {"limit": -1, "in_use": 0}

    def _strict_entry(q, key: str) -> dict:
        if not isinstance(q, dict) or "limit" not in q or "in_use" not in q:
            raise ValueError(f"Cinder quota usage is missing for {key}")
        limit, in_use = q["limit"], q["in_use"]
        if (
            not isinstance(limit, (int, float))
            or isinstance(limit, bool)
            or not isinstance(in_use, (int, float))
            or isinstance(in_use, bool)
        ):
            raise ValueError(f"Cinder quota data is malformed for {key}")
        return {"limit": limit, "in_use": in_use}

    def _strict_limits_entry(limits, limit_attr: str, used_attr: str, key: str) -> dict:
        limit = getattr(limits, limit_attr, None)
        in_use = getattr(limits, used_attr, None)
        if limit is None or in_use is None:
            raise ValueError(f"Cinder limits usage is missing for {key}")
        return _strict_entry({"limit": limit, "in_use": in_use}, key)

    try:
        bs_endpoint = conn.block_storage.get_endpoint()
        resp = conn.session.get(f"{bs_endpoint}/os-quota-sets/{project_id}", params={"usage": "true"})
        if strict:
            resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise ValueError("Cinder quota payload is malformed")
        qs = payload.get("quota_set", {})
        if not isinstance(qs, dict):
            raise ValueError("Cinder quota_set is malformed")
        if strict:
            return {key: _strict_entry(qs.get(key), key) for key in ("volumes", "gigabytes")}
        return {k: _normalize(qs.get(k)) for k in _QUOTA_KEYS}
    except Exception as exc:
        if strict and isinstance(exc, ValueError):
            raise
        import logging as _logging

        _logging.getLogger(__name__).warning("Cinder quota_set 조회 실패 — limits API로 fallback", exc_info=True)
        limits = conn.block_storage.get_limits()
        absolute = limits.absolute
        if strict:
            return {
                "volumes": _strict_limits_entry(absolute, "max_total_volumes", "total_volumes_used", "volumes"),
                "gigabytes": _strict_limits_entry(
                    absolute, "max_total_volume_gigabytes", "total_gigabytes_used", "gigabytes"
                ),
            }
        return {
            "volumes": {
                "limit": getattr(absolute, "max_total_volumes", -1),
                "in_use": getattr(absolute, "total_volumes_used", 0),
            },
            "snapshots": {
                "limit": getattr(absolute, "max_total_snapshots", -1),
                "in_use": getattr(absolute, "total_snapshots_used", 0),
            },
            "gigabytes": {
                "limit": getattr(absolute, "max_total_volume_gigabytes", -1),
                "in_use": getattr(absolute, "total_gigabytes_used", 0),
            },
            "backups": {
                "limit": getattr(absolute, "max_total_backups", -1),
                "in_use": getattr(absolute, "total_backups_used", 0),
            },
            "backup_gigabytes": {
                "limit": getattr(absolute, "max_total_backup_gigabytes", -1),
                "in_use": getattr(absolute, "total_backup_gigabytes_used", 0),
            },
        }


def get_volume_image_metadata(conn: openstack.connection.Connection, volume_id: str) -> dict | None:
    """부트 볼륨의 원본 이미지 메타데이터 반환 (volume_image_metadata 필드)."""
    try:
        vol = conn.block_storage.get_volume(volume_id)
        raw = vol.to_dict() if hasattr(vol, "to_dict") else {}
        return raw.get("volume_image_metadata") or getattr(vol, "volume_image_metadata", None)
    except Exception:
        return None


def list_backups(conn: openstack.connection.Connection) -> list[dict]:
    return [_backup_to_dict(b) for b in conn.block_storage.backups(details=True)]


def get_backup(conn: openstack.connection.Connection, backup_id: str) -> dict:
    b = conn.block_storage.get_backup(backup_id)
    return _backup_to_dict(b)


def create_backup(
    conn: openstack.connection.Connection,
    volume_id: str,
    name: str,
    description: str | None = None,
    incremental: bool = False,
) -> dict:
    kwargs: dict = {"volume_id": volume_id, "name": name, "is_incremental": incremental}
    if description:
        kwargs["description"] = description
    b = conn.block_storage.create_backup(**kwargs)
    return _backup_to_dict(b)


def delete_backup(conn: openstack.connection.Connection, backup_id: str) -> None:
    conn.block_storage.delete_backup(backup_id, ignore_missing=True)


def restore_backup(conn: openstack.connection.Connection, backup_id: str, volume_id: str | None = None) -> dict:
    kwargs: dict = {}
    if volume_id:
        kwargs["volume_id"] = volume_id
    result = conn.block_storage.restore_backup(backup_id, **kwargs)
    return {"volume_id": getattr(result, "volume_id", None), "volume_name": getattr(result, "volume_name", None)}


def list_snapshots(
    conn: openstack.connection.Connection,
    volume_id: str | None = None,
    caller_project_id: str | None = None,
) -> list[dict]:
    kwargs = {}
    if volume_id:
        kwargs["volume_id"] = volume_id
    snapshots = [_snapshot_to_dict(s) for s in conn.block_storage.snapshots(details=True, **kwargs)]
    if caller_project_id is not None:
        snapshots = [s for s in snapshots if s.get("project_id") == caller_project_id]
    return snapshots


def get_snapshot(conn: openstack.connection.Connection, snapshot_id: str) -> dict:
    s = conn.block_storage.get_snapshot(snapshot_id)
    return _snapshot_to_dict(s)


def create_snapshot(
    conn: openstack.connection.Connection,
    volume_id: str,
    name: str,
    description: str | None = None,
    force: bool = False,
) -> dict:
    kwargs: dict = {"volume_id": volume_id, "name": name, "is_forced": force}
    if description:
        kwargs["description"] = description
    s = conn.block_storage.create_snapshot(**kwargs)
    return _snapshot_to_dict(s)


def delete_snapshot(conn: openstack.connection.Connection, snapshot_id: str) -> None:
    conn.block_storage.delete_snapshot(snapshot_id, ignore_missing=True)


def _snapshot_to_dict(s) -> dict:
    return {
        "id": s.id,
        "name": s.name or "",
        "status": s.status,
        "volume_id": s.volume_id,
        "size": s.size,
        "description": getattr(s, "description", "") or "",
        "created_at": str(s.created_at) if getattr(s, "created_at", None) else None,
        "project_id": getattr(s, "project_id", None),
    }


def _backup_to_dict(b) -> dict:
    return {
        "id": b.id,
        "name": b.name or "",
        "status": b.status,
        "volume_id": b.volume_id,
        "size": b.size,
        "is_incremental": getattr(b, "is_incremental", False),
        "description": b.description or "",
        "created_at": str(b.created_at) if getattr(b, "created_at", None) else None,
    }


def wait_volume_available(
    conn: openstack.connection.Connection,
    volume_id: str,
    timeout: int = 120,
) -> VolumeInfo:
    """볼륨이 'available' 상태가 될 때까지 폴링."""
    vol = conn.block_storage.get_volume(volume_id)
    vol = conn.block_storage.wait_for_status(vol, status="available", wait=timeout)
    return _vol_to_info(vol)


def create_volume_transfer(
    conn: openstack.connection.Connection,
    volume_id: str,
    name: str | None = None,
) -> dict:
    """볼륨 이전(transfer) 생성. auth_key를 반환 — 수락 측에 전달 필요."""
    body: dict = {"volume_id": volume_id}
    if name:
        body["name"] = name
    result = conn.block_storage.post(
        "/os-volume-transfer",
        json={"transfer": body},
    )
    transfer = result.json().get("transfer", result.json())
    return {
        "id": transfer.get("id"),
        "name": transfer.get("name") or "",
        "volume_id": transfer.get("volume_id"),
        "auth_key": transfer.get("auth_key"),
        "created_at": transfer.get("created_at"),
    }


def accept_volume_transfer(
    conn: openstack.connection.Connection,
    transfer_id: str,
    auth_key: str,
) -> dict:
    """볼륨 이전 수락."""
    result = conn.block_storage.post(
        f"/os-volume-transfer/{transfer_id}/accept",
        json={"accept": {"auth_key": auth_key}},
    )
    transfer = result.json().get("transfer", result.json())
    return {
        "id": transfer.get("id"),
        "name": transfer.get("name") or "",
        "volume_id": transfer.get("volume_id"),
    }


def list_volume_transfers(conn: openstack.connection.Connection) -> list[dict]:
    """볼륨 이전 목록 조회."""
    result = conn.block_storage.get("/os-volume-transfer/detail")
    transfers = result.json().get("transfers", [])
    return [
        {
            "id": t.get("id"),
            "name": t.get("name") or "",
            "volume_id": t.get("volume_id"),
            "created_at": t.get("created_at"),
        }
        for t in transfers
    ]


def delete_volume_transfer(conn: openstack.connection.Connection, transfer_id: str) -> None:
    """볼륨 이전 취소."""
    conn.block_storage.delete(f"/os-volume-transfer/{transfer_id}", ignore_missing=True)


def _vol_to_info(vol) -> VolumeInfo:
    # Use original API field names (bootable, volume_image_metadata) since the SDK
    # Python alias for bootable is `is_bootable`, not `bootable`.
    raw = vol.to_dict(original_names=True, computed=False) if hasattr(vol, "to_dict") else {}
    raw_bootable = raw.get("bootable", getattr(vol, "is_bootable", getattr(vol, "bootable", False)))
    bootable = raw_bootable if isinstance(raw_bootable, bool) else str(raw_bootable).lower() == "true"
    raw_vim = raw.get("volume_image_metadata", getattr(vol, "volume_image_metadata", None))
    volume_image_metadata = raw_vim if isinstance(raw_vim, dict) else None
    return VolumeInfo(
        id=vol.id,
        name=vol.name or "",
        status=vol.status,
        size_gb=vol.size,
        volume_type=vol.volume_type,
        attachments=list(vol.attachments or []),
        bootable=bootable,
        volume_image_metadata=volume_image_metadata,
    )


def _safe_float(v) -> float:
    try:
        return round(float(v), 2)
    except Exception:
        return 0.0


def list_storage_pools(conn) -> list[dict]:
    """Cinder backend pool 목록 — total/free/allocated capacity_gb 정규화 반환."""
    bs_ep = conn.block_storage.get_endpoint()
    resp = conn.session.get(f"{bs_ep}/scheduler-stats/get_pools", params={"detail": "True"})
    out = []
    for pool in resp.json().get("pools", []):
        caps = pool.get("capabilities", {})
        total_gb = caps.get("total_capacity_gb", 0)
        free_gb = caps.get("free_capacity_gb", 0)
        allocated_gb = caps.get("allocated_capacity_gb", 0)
        if isinstance(total_gb, str) and total_gb in ("infinite", "unknown"):
            total_gb = 0
        if isinstance(free_gb, str) and free_gb in ("infinite", "unknown"):
            free_gb = 0
        out.append(
            {
                "name": pool.get("name", ""),
                "volume_backend_name": caps.get("volume_backend_name", ""),
                "storage_protocol": caps.get("storage_protocol", ""),
                "total_capacity_gb": _safe_float(total_gb),
                "free_capacity_gb": _safe_float(free_gb),
                "allocated_capacity_gb": _safe_float(allocated_gb),
            }
        )
    return out


def list_volume_types(conn: openstack.connection.Connection) -> list[dict]:
    """볼륨 타입 목록. DB 인스턴스 생성 시 volume.type 선택에 사용."""
    try:
        return [
            {
                "id": vt.id,
                "name": vt.name or "",
                "description": getattr(vt, "description", "") or "",
                "is_public": getattr(vt, "is_public", True),
            }
            for vt in conn.block_storage.types()
            if vt.name
        ]
    except Exception:
        return []
