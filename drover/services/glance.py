from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from drover.models.openstack import ImageDetail, ImageInfo
from drover.services.image_refs import image_reference_fields, normalize_image_reference


def list_images(conn: openstack.connection.Connection, project_id: str | None = None) -> list[ImageInfo]:
    seen: set[str] = set()
    images: list[ImageInfo] = []

    def _add(img) -> None:
        if img.id in seen:
            return
        seen.add(img.id)
        display_name, repository, tag = image_reference_fields(getattr(img, "name", None))
        props = img.properties or {}
        os_distro = getattr(img, "os_distro", None) or props.get("os_distro") or _guess_distro(display_name)
        images.append(
            ImageInfo(
                id=img.id,
                name=display_name,
                status=img.status,
                repository=repository,
                tag=tag,
                size=img.size,
                min_disk=img.min_disk or 0,
                min_ram=img.min_ram or 0,
                disk_format=img.disk_format,
                os_type=props.get("os_type"),
                os_distro=os_distro,
                created_at=str(img.created_at) if img.created_at else None,
                owner=getattr(img, "owner", None) or getattr(img, "project_id", None),
                visibility=getattr(img, "visibility", None),
            )
        )

    # public, community, shared: 접근 가능한 공개/공유 이미지
    for vis in ("public", "community", "shared"):
        try:
            for img in conn.image.images(status="active", visibility=vis):
                _add(img)
        except Exception:
            pass

    # 현재 프로젝트의 private 이미지만 (deactivated 포함 — 소유자는 모든 상태를 봐야 함)
    pid = project_id or getattr(conn, "_afterglow_project_id", None)
    try:
        kwargs: dict = {"visibility": "private"}
        if pid:
            kwargs["owner"] = pid
        for img in conn.image.images(**kwargs):
            _add(img)
    except Exception:
        pass

    return sorted(images, key=lambda x: x.name)


def get_image(conn: openstack.connection.Connection, image_id: str) -> ImageDetail:
    img = conn.image.get_image(image_id)
    display_name, repository, tag = image_reference_fields(getattr(img, "name", None))
    raw_props = dict(img.properties or {})
    os_distro = getattr(img, "os_distro", None) or raw_props.get("os_distro") or _guess_distro(display_name)

    # SDK 가 Image 본문 필드로 따로 노출하는 키들을 properties 에 병합 (OpenStack CLI 와 동일한 뷰)
    _sdk_fields: dict = {
        "os_distro": getattr(img, "os_distro", None),
        "os_hash_algo": getattr(img, "os_hash_algo", None),
        "os_hash_value": getattr(img, "os_hash_value", None),
        "direct_url": getattr(img, "direct_url", None),
    }
    is_hidden = getattr(img, "is_hidden", None)
    if is_hidden is not None:
        _sdk_fields["os_hidden"] = "True" if is_hidden else "False"
    for k, v in _sdk_fields.items():
        if v is not None and k not in raw_props:
            raw_props[k] = v if isinstance(v, str) else str(v)

    return ImageDetail(
        id=img.id,
        name=display_name,
        status=img.status,
        repository=repository,
        tag=tag,
        size=img.size,
        min_disk=img.min_disk or 0,
        min_ram=img.min_ram or 0,
        disk_format=img.disk_format,
        os_type=raw_props.get("os_type"),
        os_distro=os_distro,
        created_at=str(img.created_at) if img.created_at else None,
        owner=getattr(img, "owner", None) or getattr(img, "project_id", None),
        visibility=getattr(img, "visibility", None),
        checksum=getattr(img, "checksum", None),
        container_format=img.container_format,
        virtual_size=getattr(img, "virtual_size", None),
        updated_at=str(img.updated_at) if getattr(img, "updated_at", None) else None,
        protected=getattr(img, "is_protected", False) or False,
        tags=list(getattr(img, "tags", None) or []),
        properties=raw_props,
        os_hash_algo=getattr(img, "os_hash_algo", None),
        os_hash_value=getattr(img, "os_hash_value", None),
        direct_url=getattr(img, "direct_url", None),
    )


_ALLOWED_DISK_FORMATS: frozenset[str] = frozenset(
    {"raw", "qcow2", "vmdk", "vdi", "vhd", "vhdx", "iso", "ami", "aki", "ari"}
)

_UPLOAD_PROPERTY_WHITELIST: frozenset[str] = frozenset(
    {"os_distro", "os_version", "hw_disk_bus", "hw_qemu_guest_agent"}
)


def create_image(
    conn: openstack.connection.Connection,
    *,
    name: str,
    disk_format: str,
    container_format: str = "bare",
    visibility: str = "private",
    data,
    properties: dict | None = None,
):
    """Glance 이미지 생성 + 바이너리 업로드 (openstacksdk 단일 PUT 스트리밍).

    data는 파일 유사 객체(UploadFile.file)를 그대로 전달.
    """
    normalized_name = normalize_image_reference(name)
    safe_props: dict = {}
    if properties:
        safe_props = {k: str(v) for k, v in properties.items() if k in _UPLOAD_PROPERTY_WHITELIST}
    return conn.image.create_image(
        name=normalized_name,
        disk_format=disk_format,
        container_format=container_format,
        visibility=visibility,
        data=data,
        allow_duplicates=True,
        **safe_props,
    )


def delete_image(conn: openstack.connection.Connection, image_id: str) -> None:
    conn.image.delete_image(image_id, ignore_missing=False)


def deactivate_image(conn: openstack.connection.Connection, image_id: str) -> None:
    conn.image.deactivate_image(image_id)


def reactivate_image(conn: openstack.connection.Connection, image_id: str) -> None:
    conn.image.reactivate_image(image_id)


def update_image_metadata(
    conn: openstack.connection.Connection,
    image_id: str,
    name: str | None = None,
    os_distro: str | None = None,
    os_type: str | None = None,
    min_disk: int | None = None,
    min_ram: int | None = None,
    visibility: str | None = None,
) -> ImageInfo:
    """이미지 메타데이터 업데이트 (소유한 이미지만 가능)."""
    kwargs: dict = {}
    if name is not None:
        kwargs["name"] = normalize_image_reference(name)
    if os_distro is not None:
        kwargs["os_distro"] = os_distro
    if os_type is not None:
        kwargs["os_type"] = os_type
    if min_disk is not None:
        kwargs["min_disk"] = min_disk
    if min_ram is not None:
        kwargs["min_ram"] = min_ram
    if visibility is not None:
        kwargs["visibility"] = visibility
    img = conn.image.update_image(image_id, **kwargs)
    props = img.properties or {}
    display_name, repository, tag = image_reference_fields(getattr(img, "name", None))
    od = getattr(img, "os_distro", None) or props.get("os_distro") or _guess_distro(display_name)
    return ImageInfo(
        id=img.id,
        name=display_name,
        status=img.status,
        repository=repository,
        tag=tag,
        size=img.size,
        min_disk=img.min_disk or 0,
        min_ram=img.min_ram or 0,
        disk_format=img.disk_format,
        os_type=props.get("os_type"),
        os_distro=od,
        created_at=str(img.created_at) if img.created_at else None,
        owner=getattr(img, "owner", None) or getattr(img, "project_id", None),
    )


# Glance 가 자동 관리하거나 hash/url 등 무결성 관련 키 — 사용자 편집 금지
_RESERVED_PROPERTY_KEYS: frozenset[str] = frozenset(
    {
        "id",
        "name",
        "status",
        "visibility",
        "owner",
        "size",
        "virtual_size",
        "disk_format",
        "container_format",
        "checksum",
        "os_hash_algo",
        "os_hash_value",
        "min_disk",
        "min_ram",
        "tags",
        "self",
        "file",
        "schema",
        "direct_url",
        "locations",
        "created_at",
        "updated_at",
        "protected",
    }
)


def _is_protected_key(key: str) -> bool:
    """Glance 예약/내부 관리 키인지 검사 (편집/삭제 금지)."""
    if key in _RESERVED_PROPERTY_KEYS:
        return True
    return key.startswith("os_glance_")


def update_image_properties(
    conn: openstack.connection.Connection,
    image_id: str,
    set_properties: dict[str, str] | None = None,
    remove_keys: list[str] | None = None,
) -> ImageDetail:
    """이미지 임의 properties 추가/수정/삭제.

    set_properties: key→value 추가 또는 갱신
    remove_keys: 삭제할 key 목록
    예약 키(`_RESERVED_PROPERTY_KEYS`, `os_glance_*`)는 무시.

    set/remove 모두 JSON-Patch 로 단일 호출 처리.
    update_image(**kwargs) 는 알려진 Image 속성만 반영하므로
    임의 사용자 정의 키에는 사용하지 않는다.
    """
    set_properties = set_properties or {}
    remove_keys = remove_keys or []

    safe_set = {k: str(v) for k, v in set_properties.items() if not _is_protected_key(k)}
    safe_remove = [k for k in remove_keys if not _is_protected_key(k)]

    existing_props = (conn.image.get_image(image_id).properties or {}) if safe_set else {}

    patch_ops: list[dict] = []
    for k, v in safe_set.items():
        op = "replace" if k in existing_props else "add"
        patch_ops.append({"op": op, "path": f"/{k}", "value": v})
    for k in safe_remove:
        patch_ops.append({"op": "remove", "path": f"/{k}"})

    if patch_ops:
        conn.image.patch(
            f"/v2/images/{image_id}",
            json=patch_ops,
            headers={"Content-Type": "application/openstack-images-v2.1-json-patch"},
        )

    return get_image(conn, image_id)


def _guess_distro(name: str) -> str | None:
    """이미지 이름에서 OS 배포판 추정."""
    lower = name.lower()
    for distro in ("ubuntu", "centos", "rocky", "debian", "fedora-coreos", "fedora", "rhel", "windows", "cirros"):
        if distro in lower:
            return distro
    return None


# ---------------------------------------------------------------------------
# 이미지 멤버 (공유 프로젝트 관리)
# ---------------------------------------------------------------------------


def list_image_members(conn: openstack.connection.Connection, image_id: str) -> list[dict]:
    """이미지 멤버(공유 프로젝트) 목록 조회."""
    members = conn.image.members(image_id)
    return [
        {
            "member_id": m.id,
            "status": m.status,
            "created_at": str(m.created_at) if m.created_at else None,
        }
        for m in members
    ]


def add_image_member(conn: openstack.connection.Connection, image_id: str, member_id: str) -> dict:
    """이미지에 프로젝트 멤버 추가."""
    m = conn.image.add_member(image_id, member_id=member_id)
    return {"member_id": m.id, "status": m.status}


def remove_image_member(conn: openstack.connection.Connection, image_id: str, member_id: str) -> None:
    """이미지에서 프로젝트 멤버 삭제."""
    conn.image.remove_member(member_id, image_id)


def get_total_image_bytes(conn) -> int:
    """active 상태 모든 이미지의 size 합 (bytes). 실패 시 0."""
    total = 0
    try:
        for img in conn.image.images():
            if getattr(img, "status", None) != "active":
                continue
            total += int(getattr(img, "size", 0) or 0)
    except Exception:
        pass
    return total
