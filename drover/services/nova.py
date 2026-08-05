from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

from drover.models.openstack import FlavorInfo, InstanceInfo, IpAddress
from drover.services.image_refs import image_reference_fields

_logger = logging.getLogger(__name__)


def list_flavors(conn: openstack.connection.Connection) -> list[FlavorInfo]:
    flavors = []
    # is_public=None → 공개 + 접근 권한이 부여된 비공개 플레이버 모두 반환
    for f in conn.compute.flavors(is_public=None):
        extra = dict(f.extra_specs) if f.extra_specs else {}
        # List API는 extra_specs를 포함하지 않을 수 있으므로 개별 조회 fallback
        if not extra:
            try:
                detail = conn.compute.get_flavor(f.id)
                extra = dict(detail.extra_specs) if detail.extra_specs else {}
            except Exception:
                pass
        flavors.append(
            FlavorInfo(
                id=f.id,
                name=f.name,
                vcpus=f.vcpus,
                ram=f.ram,
                disk=f.disk,
                is_public=getattr(f, "is_public", True),
                extra_specs=extra,
            )
        )
    return sorted(flavors, key=lambda x: (x.vcpus, x.ram))


def list_servers(conn: openstack.connection.Connection) -> list[InstanceInfo]:
    servers = []
    for s in conn.compute.servers(details=True):
        servers.append(_server_to_info(s))
    return servers


def get_server(conn: openstack.connection.Connection, server_id: str) -> InstanceInfo:
    s = conn.compute.get_server(server_id)
    return _server_to_info(s)


def create_server(
    conn: openstack.connection.Connection,
    name: str,
    flavor_id: str,
    network_id: str,
    boot_volume_id: str,
    userdata: str | None = None,
    key_name: str | None = None,
    admin_pass: str | None = None,
    availability_zone: str | None = None,
    metadata: dict | None = None,
    delete_boot_volume_on_termination: bool = False,
    security_groups: list[str] | None = None,
    config_drive: bool = False,
) -> InstanceInfo:
    body = {
        "name": name,
        "flavorRef": flavor_id,
        "block_device_mapping_v2": [
            {
                "boot_index": 0,
                "uuid": boot_volume_id,
                "source_type": "volume",
                "destination_type": "volume",
                "delete_on_termination": delete_boot_volume_on_termination,
            }
        ],
    }
    if config_drive:
        body["config_drive"] = True
    if userdata:
        body["user_data"] = userdata
    # network_id가 있으면 지정, 없으면 자동 할당
    if network_id:
        body["networks"] = [{"uuid": network_id}]
    if key_name:
        body["key_name"] = key_name
    if admin_pass:
        body["adminPass"] = admin_pass
    if availability_zone:
        body["availability_zone"] = availability_zone
    if metadata:
        body["metadata"] = metadata
    if security_groups:
        body["security_groups"] = [{"name": sg} for sg in security_groups]

    s = conn.compute.create_server(**body)
    s = conn.compute.wait_for_server(s, status="ACTIVE", wait=600)
    return _server_to_info(s)


def get_console_output(conn: openstack.connection.Connection, server_id: str, length: int | None = 100) -> str:
    """서버 serial console 출력을 반환한다. length=None 이면 전체 출력."""
    kwargs: dict = {} if length is None else {"length": length}
    output = conn.compute.get_server_console_output(server_id, **kwargs)
    return output.get("output", "")


def list_volume_attachments(conn: openstack.connection.Connection, server_id: str) -> list[dict]:
    return [
        {
            "id": a.id,
            "volume_id": a.volume_id,
            "device": a.device,
            "server_id": a.server_id,
            "delete_on_termination": getattr(a, "delete_on_termination", False),
        }
        for a in conn.compute.volume_attachments(server_id)
    ]


def update_volume_attachment_delete_flag(
    conn: openstack.connection.Connection,
    server_id: str,
    volume_id: str,
    delete_on_termination: bool,
) -> None:
    """Nova microversion 2.85: 기존 attachment의 delete_on_termination 변경."""
    endpoint = conn.compute.get_endpoint()
    resp = conn.session.put(
        f"{endpoint}/servers/{server_id}/os-volume_attachments/{volume_id}",
        headers={"OpenStack-API-Version": "compute 2.85"},
        json={
            "volumeAttachment": {
                "volumeId": volume_id,
                "delete_on_termination": delete_on_termination,
            }
        },
    )
    resp.raise_for_status()


def attach_volume(conn: openstack.connection.Connection, server_id: str, volume_id: str) -> dict:
    a = conn.compute.create_volume_attachment(server_id, volume_id=volume_id)
    return {"id": a.id, "volume_id": a.volume_id, "device": a.device}


def detach_volume(conn: openstack.connection.Connection, server_id: str, volume_id: str) -> None:
    conn.compute.delete_volume_attachment(volume_id, server_id)


def attach_interface(conn: openstack.connection.Connection, server_id: str, net_id: str) -> dict:
    iface = conn.compute.create_server_interface(server_id, net_id=net_id)
    return {
        "port_id": iface.port_id,
        "net_id": iface.net_id,
        "fixed_ips": iface.fixed_ips or [],
    }


def detach_interface(conn: openstack.connection.Connection, server_id: str, port_id: str) -> None:
    conn.compute.delete_server_interface(port_id, server=server_id)


def get_project_limits(conn: openstack.connection.Connection) -> dict:
    """프로젝트의 Nova 리소스 사용량/한도 조회."""
    limits = conn.compute.get_limits()
    a = limits.absolute
    return {
        "instances_used": getattr(a, "total_instances_used", 0),
        "instances_limit": getattr(a, "max_total_instances", -1),
        "vcpus_used": getattr(a, "total_cores_used", 0),
        "vcpus_limit": getattr(a, "max_total_cores", -1),
        "ram_used_mb": getattr(a, "total_ram_used", 0),
        "ram_limit_mb": getattr(a, "max_total_ram_size", -1),
    }


_NOVA_QUOTA_KEYS = ("instances", "cores", "ram", "key_pairs", "server_groups")


def get_project_quota(conn: openstack.connection.Connection, project_id: str, *, strict: bool = False) -> dict:
    """프로젝트의 상세 Nova 할당량 (usage 포함).

    ``strict`` is reserved for dashboard overview callers.  It rejects
    malformed usage-bearing raw quota data before legacy sentinel
    normalization, while default callers retain the historic REST-to-limits
    fallback behavior and complete wire shape.
    """

    def _normalize(q) -> dict:
        if isinstance(q, dict):
            return {"limit": int(q.get("limit", -1)), "in_use": int(q.get("in_use", 0))}
        if isinstance(q, int):
            return {"limit": q, "in_use": 0}
        return {"limit": -1, "in_use": 0}

    def _strict_entry(q, key: str) -> dict:
        if not isinstance(q, dict) or "limit" not in q or "in_use" not in q:
            raise ValueError(f"Nova quota usage is missing for {key}")
        limit, in_use = q["limit"], q["in_use"]
        if (
            not isinstance(limit, (int, float))
            or isinstance(limit, bool)
            or not isinstance(in_use, (int, float))
            or isinstance(in_use, bool)
        ):
            raise ValueError(f"Nova quota data is malformed for {key}")
        return {"limit": limit, "in_use": in_use}

    def _strict_limits_entry(limits, limit_attr: str, used_attr: str, key: str) -> dict:
        limit = getattr(limits, limit_attr, None)
        in_use = getattr(limits, used_attr, None)
        if limit is None or in_use is None:
            raise ValueError(f"Nova limits usage is missing for {key}")
        return _strict_entry({"limit": limit, "in_use": in_use}, key)

    try:
        compute_endpoint = conn.compute.get_endpoint()
        resp = conn.session.get(f"{compute_endpoint}/os-quota-sets/{project_id}/detail")
        if strict:
            resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise ValueError("Nova quota payload is malformed")
        qs = payload.get("quota_set", {})
        if not isinstance(qs, dict):
            raise ValueError("Nova quota_set is malformed")
        if strict:
            return {key: _strict_entry(qs.get(key), key) for key in ("instances", "cores", "ram")}
        return {k: _normalize(qs.get(k)) for k in _NOVA_QUOTA_KEYS}
    except Exception as exc:
        # A successful but incomplete/malformed detailed response is never
        # converted into a fake zero for strict overview callers.  Legacy
        # callers retain the established limits fallback; strict transport
        # failures may use that usage-bearing fallback too.
        if strict and isinstance(exc, ValueError):
            raise
        _logger.warning("Nova quota_set 조회 실패 — limits API로 fallback", exc_info=True)
        limits = conn.compute.get_limits()
        absolute = limits.absolute
        if strict:
            return {
                "instances": _strict_limits_entry(absolute, "max_total_instances", "total_instances_used", "instances"),
                "cores": _strict_limits_entry(absolute, "max_total_cores", "total_cores_used", "cores"),
                "ram": _strict_limits_entry(absolute, "max_total_ram_size", "total_ram_used", "ram"),
            }
        return {
            "instances": {
                "limit": getattr(absolute, "max_total_instances", -1),
                "in_use": getattr(absolute, "total_instances_used", 0),
            },
            "cores": {
                "limit": getattr(absolute, "max_total_cores", -1),
                "in_use": getattr(absolute, "total_cores_used", 0),
            },
            "ram": {
                "limit": getattr(absolute, "max_total_ram_size", -1),
                "in_use": getattr(absolute, "total_ram_used", 0),
            },
            "key_pairs": {"limit": getattr(absolute, "max_total_keypairs", -1), "in_use": 0},
            "server_groups": {"limit": getattr(absolute, "max_server_groups", -1), "in_use": 0},
        }


def get_project_usage(conn: openstack.connection.Connection, project_id: str, start: str, end: str) -> dict:
    """Nova simple-tenant-usage API로 기간별 사용량 조회."""
    try:
        # openstacksdk get_usage는 datetime 객체를 기대함 (문자열 전달 시 AttributeError)
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        usage = conn.compute.get_usage(project_id, start=start_dt, end=end_dt)
        server_usages = getattr(usage, "server_usages", []) or []
        return {
            "total_vcpus_usage": getattr(usage, "total_vcpus_usage", 0.0),
            "total_memory_mb_usage": getattr(usage, "total_memory_mb_usage", 0.0),
            "total_local_gb_usage": getattr(usage, "total_local_gb_usage", 0.0),
            "total_hours": getattr(usage, "total_hours", 0.0),
            "server_usages": [
                {
                    "name": s.get("name", "") if isinstance(s, dict) else getattr(s, "name", ""),
                    "instance_id": s.get("instance_id", "") if isinstance(s, dict) else getattr(s, "instance_id", ""),
                    "vcpus": s.get("vcpus", 0) if isinstance(s, dict) else getattr(s, "vcpus", 0),
                    "memory_mb": s.get("memory_mb", 0) if isinstance(s, dict) else getattr(s, "memory_mb", 0),
                    "local_gb": s.get("local_gb", 0) if isinstance(s, dict) else getattr(s, "local_gb", 0),
                    "hours": s.get("hours", 0.0) if isinstance(s, dict) else getattr(s, "hours", 0.0),
                    "state": s.get("state", "") if isinstance(s, dict) else getattr(s, "state", ""),
                }
                for s in server_usages
            ],
        }
    except Exception:
        _logger.warning("Nova get_usage 조회 실패", exc_info=True)
        return {
            "total_vcpus_usage": 0.0,
            "total_memory_mb_usage": 0.0,
            "total_local_gb_usage": 0.0,
            "total_hours": 0.0,
            "server_usages": [],
        }


def list_availability_zones(conn: openstack.connection.Connection) -> list[dict]:
    """사용 가능한 가용 영역 목록 조회."""
    return [
        {
            "name": az.name,
            "state": getattr(az, "state", {}).get("available", True)
            if isinstance(getattr(az, "state", {}), dict)
            else True,
        }
        for az in conn.compute.availability_zones()
        if getattr(az, "name", "") not in ("internal",)
    ]


def list_keypairs(conn: openstack.connection.Connection) -> list[dict]:
    return [
        {"name": kp.name, "fingerprint": kp.fingerprint, "type": getattr(kp, "type", "ssh")}
        for kp in conn.compute.keypairs()
    ]


def create_keypair(
    conn: openstack.connection.Connection,
    name: str,
    public_key: str | None = None,
    key_type: str = "ssh",
) -> dict:
    """키페어 생성. public_key가 없으면 Nova가 자동 생성하고 private_key를 반환."""
    body: dict = {"name": name, "type": key_type}
    if public_key:
        body["public_key"] = public_key
    kp = conn.compute.create_keypair(**body)
    return {
        "name": kp.name,
        "fingerprint": kp.fingerprint,
        "type": getattr(kp, "type", "ssh"),
        "public_key": getattr(kp, "public_key", None),
        "private_key": getattr(kp, "private_key", None),
    }


def delete_keypair(conn: openstack.connection.Connection, name: str) -> None:
    conn.compute.delete_keypair(name, ignore_missing=True)


def delete_server(conn: openstack.connection.Connection, server_id: str) -> None:
    try:
        conn.compute.delete_server(server_id, force=True)
    except Exception as e:
        # 이미 삭제됐거나 존재하지 않으면 무시 (404 ResourceNotFound, 409 Conflict)
        err_str = str(e).lower()
        if "404" in err_str or "not found" in err_str or "409" in err_str or "conflict" in err_str:
            _logger.debug("delete_server %s: already gone or conflict, ignoring (%s)", server_id, e)
            return
        raise


def wait_server_deleted(conn: openstack.connection.Connection, server_id: str, timeout: int = 120) -> None:
    """서버가 완전히 사라질 때까지 폴링. 타임아웃 초과 시 TimeoutError 발생."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        srv = conn.compute.find_server(server_id, ignore_missing=True)
        if srv is None:
            return
        time.sleep(3)
    raise TimeoutError(f"서버 {server_id} 삭제 대기 타임아웃 ({timeout}s)")


def start_server(conn: openstack.connection.Connection, server_id: str) -> None:
    conn.compute.start_server(server_id)


def stop_server(conn: openstack.connection.Connection, server_id: str) -> None:
    conn.compute.stop_server(server_id)


def reboot_server(conn: openstack.connection.Connection, server_id: str, reboot_type: str = "SOFT") -> None:
    conn.compute.reboot_server(server_id, reboot_type)


def reset_server_state(conn: openstack.connection.Connection, server_id: str, state: str = "active") -> None:
    """인스턴스 상태를 강제로 변경한다 (Nova os-resetState action). 관리자 전용."""
    conn.compute.reset_server_state(server_id, state)


def shelve_server(conn: openstack.connection.Connection, server_id: str) -> None:
    conn.compute.shelve_server(server_id)


def unshelve_server(conn: openstack.connection.Connection, server_id: str) -> None:
    conn.compute.unshelve_server(server_id)


def get_console_url(conn: openstack.connection.Connection, server_id: str) -> str:
    s = conn.compute.get_server(server_id)
    result = conn.compute.create_console(s, console_type="novnc")
    return result.get("url", "")


def live_migrate_server(
    conn: openstack.connection.Connection, server_id: str, host: str | None = None, block_migration: str = "auto"
) -> None:
    """라이브 마이그레이션 (인스턴스 실행 중 이동)."""
    conn.compute.live_migrate_server(server_id, host=host, block_migration=block_migration)


def cold_migrate_server(conn: openstack.connection.Connection, server_id: str, host: str | None = None) -> None:
    """콜드 마이그레이션 (인스턴스 종료 후 이동).

    host가 지정된 경우 Nova microversion 2.56을 사용해 대상 호스트를 직접 지정한다.
    host가 None이면 Nova 스케줄러 자동 배치.
    """
    if host:
        endpoint = conn.compute.get_endpoint()
        conn.session.post(
            f"{endpoint}/servers/{server_id}/action",
            json={"migrate": {"host": host}},
            headers={"OpenStack-API-Version": "compute 2.56"},
        )
    else:
        conn.compute.migrate_server(server_id)


def confirm_resize_server(conn: openstack.connection.Connection, server_id: str) -> None:
    """콜드 마이그레이션 후 리사이즈 확인."""
    conn.compute.confirm_server_resize(server_id)


def resize_server(conn: openstack.connection.Connection, server_id: str, flavor_id: str) -> None:
    """인스턴스 플레이버 변경 (cold resize). VERIFY_RESIZE 상태로 전이, confirm 필요."""
    conn.compute.resize_server(server_id, flavor_id)


def evacuate_server(
    conn: openstack.connection.Connection,
    server_id: str,
    host: str | None = None,
    on_shared_storage: bool = False,
) -> None:
    """호스트 장애 시 인스턴스를 다른 호스트로 강제 이주(evacuate).

    host를 지정하면 해당 호스트로, 생략하면 Nova 스케줄러가 자동 선택한다.
    on_shared_storage=True이면 디스크 복사 없이 공유 스토리지 경로로 처리한다.
    """
    endpoint = conn.compute.get_endpoint()
    body: dict = {"evacuate": {"onSharedStorage": on_shared_storage}}
    if host:
        body["evacuate"]["host"] = host
    conn.session.post(
        f"{endpoint}/servers/{server_id}/action",
        json=body,
    )


def revert_resize_server(conn: openstack.connection.Connection, server_id: str) -> None:
    """리사이즈 취소 — VERIFY_RESIZE 상태에서 이전 플레이버로 복귀."""
    conn.compute.revert_server_resize(server_id)


def change_server_password(
    conn: openstack.connection.Connection,
    server_id: str,
    new_password: str,
) -> None:
    """QEMU Guest Agent를 통해 게스트 OS의 관리자 패스워드 변경.
    이미지에 hw_qemu_guest_agent=yes + 게스트 내 QGA 데몬 실행 중이어야 동작.
    """
    conn.compute.change_server_password(server_id, new_password=new_password)


def get_server_image_meta(conn: openstack.connection.Connection, server_id: str) -> dict:
    """서버의 부트 이미지 메타데이터 조회.
    Returns: {"qga_enabled": bool, "os_admin_user": str|None, "image_id": str|None, "image_name": str|None}
    볼륨 부팅 인스턴스는 cinder volume_image_metadata에서 fallback.
    """
    from drover.services import cinder  # 순환 임포트 방지

    s = conn.compute.get_server(server_id)
    image_id = s.image.get("id") if isinstance(s.image, dict) else None

    if not image_id:
        # 볼륨 부팅: /dev/vda 볼륨의 image_metadata에서 조회
        try:
            for att in conn.compute.volume_attachments(server_id):
                if getattr(att, "device", "") == "/dev/vda":
                    meta = cinder.get_volume_image_metadata(conn, att.volume_id)
                    if meta:
                        return {
                            "qga_enabled": str(meta.get("hw_qemu_guest_agent", "")).lower() in ("yes", "true", "1"),
                            "os_admin_user": meta.get("os_admin_user"),
                            "image_id": meta.get("image_id"),
                            "image_name": image_reference_fields(meta.get("image_name"))[0] or None,
                        }
        except Exception:
            pass
        return {"qga_enabled": False, "os_admin_user": None, "image_id": None, "image_name": None}

    try:
        img = conn.image.get_image(image_id)
        props = dict(img.properties or {})
        qga = props.get("hw_qemu_guest_agent") or getattr(img, "hw_qemu_guest_agent", None)
        admin_user = props.get("os_admin_user") or getattr(img, "os_admin_user", None)
        return {
            "qga_enabled": str(qga or "").lower() in ("yes", "true", "1"),
            "os_admin_user": admin_user,
            "image_id": image_id,
            "image_name": image_reference_fields(getattr(img, "name", None))[0] or None,
        }
    except Exception:
        _logger.warning("이미지 메타 조회 실패 server=%s image=%s", server_id, image_id, exc_info=True)
        return {"qga_enabled": False, "os_admin_user": None, "image_id": image_id, "image_name": None}


def extract_cpu_model(h: dict) -> str | None:
    """하이퍼바이저 raw dict에서 CPU model 문자열을 추출한다.

    cpu_info가 JSON 문자열이면 파싱 후 'model' 키를 반환하고,
    dict이면 바로 'model' 키를 반환한다. 파싱 실패·필드 없으면 None.
    """
    ci = h.get("cpu_info") or {}
    if isinstance(ci, str):
        try:
            ci = json.loads(ci)
        except Exception:
            return None
    return ci.get("model") if isinstance(ci, dict) else None


def list_compute_hosts(
    conn: openstack.connection.Connection,
    source_host: str | None = None,
    cpu_filter: bool = True,
) -> list[dict]:
    """마이그레이션 대상 컴퓨트 호스트 목록.

    source_host가 주어지면 소스 자신을 제외한다.
    cpu_filter=True(기본)이고 source_host가 있으면 동일 CPU 모델 호스트만 반환한다.
    cpu_filter=False이면 CPU 모델 관계 없이 enabled 전체를 반환(콜드 마이그레이션용).
    CPU 정보 판별 불가 시 fail-open(전체 목록 반환).
    """
    endpoint = conn.compute.get_endpoint()
    try:
        resp = conn.session.get(
            f"{endpoint}/os-hypervisors/detail",
            headers={"OpenStack-API-Version": "compute 2.53"},
        )
        hypervisors = resp.json().get("hypervisors", [])
    except Exception:
        return []

    enabled = [h for h in hypervisors if h.get("state") == "up" and h.get("status") == "enabled"]

    def _to_dict(h: dict) -> dict:
        return {
            "name": h.get("hypervisor_hostname", ""),
            "state": h.get("state", ""),
            "status": h.get("status", ""),
            "cpu_model": extract_cpu_model(h),
        }

    if not source_host:
        return [_to_dict(h) for h in enabled]

    # 소스 호스트 제외 (공통)
    candidates = [h for h in enabled if h.get("hypervisor_hostname", "") != source_host]

    if not cpu_filter:
        # 콜드 마이그레이션 — CPU 모델 무관, 소스만 제외
        return [_to_dict(h) for h in candidates]

    # 라이브 마이그레이션 — 동일 CPU 모델 필터
    source_model: str | None = None
    for h in hypervisors:
        hostname = h.get("hypervisor_hostname", "")
        service_host = h.get("service", {}).get("host", "") if isinstance(h.get("service"), dict) else ""
        if hostname == source_host or service_host == source_host:
            source_model = extract_cpu_model(h)
            break

    if not source_model:
        # CPU 정보 미제공 환경 — fail-open: 소스 자신만 제외하고 전체 반환
        return [_to_dict(h) for h in candidates]

    return [_to_dict(h) for h in candidates if extract_cpu_model(h) == source_model]


def _extract_os_error(e: Exception) -> str:
    """OpenStack 예외에서 사람이 읽을 수 있는 메시지를 추출한다.

    내부 경로·스택트레이스를 노출하지 않는다(CLAUDE.md 보안 #6).
    """
    # openstack.exceptions.HttpException 등에는 .details 또는 .message 속성이 있다.
    for attr in ("message", "details", "response"):
        val = getattr(e, attr, None)
        if val is None:
            continue
        # response 객체인 경우 JSON에서 상세 추출 시도
        if hasattr(val, "json"):
            try:
                body = val.json()
                for key in ("faultstring", "message", "detail"):
                    msg = body.get(key) or (body.get("error") or {}).get("message")
                    if msg:
                        return str(msg)
            except Exception:
                pass
        msg = str(val).strip()
        if msg:
            return msg
    return str(e).strip() or "알 수 없는 오류"


def get_server_migration_status(conn: openstack.connection.Connection, server_id: str) -> dict:
    """인스턴스의 현재 호스트 + 진행 중 마이그레이션 상태를 반환한다.

    MIGRATING 중이면 source→dest·메모리 진행률을 포함한다.
    진행 중이 아니고 최근 마이그레이션이 error 상태였다면 실패 사유를 반환한다.
    예외는 fail-soft(빈 결과)로 처리해 폴링을 안전하게 유지한다.
    """
    result: dict = {"host": None, "migration": None, "error": None}
    try:
        srv = conn.compute.get_server(server_id)
        result["host"] = getattr(srv, "compute_host", None)
    except Exception:
        pass

    # 진행 중 라이브 마이그레이션 조회 (MV 2.23+)
    try:
        active: list = list(conn.compute.server_migrations(server_id))
        if active:
            m = active[0]  # 진행 중은 보통 1개
            total = getattr(m, "memory_total_bytes", 0) or 0
            processed = getattr(m, "memory_processed_bytes", 0) or 0
            memory_percent = round(processed / total * 100, 1) if total > 0 else None
            dest = getattr(m, "dest_host", None) or getattr(m, "dest_compute", None)
            result["migration"] = {
                "id": getattr(m, "id", None),
                "source": getattr(m, "source_compute", None),
                "dest": dest,
                "status": getattr(m, "status", None),
                "type": getattr(m, "migration_type", None),
                "memory_percent": memory_percent,
            }
            return result
    except Exception:
        pass

    # 진행 중이 아닌 경우 — 최근 실패 사유 회수
    try:
        fault = getattr(conn.compute.get_server(server_id), "fault", None)
        if fault:
            msg = fault.get("message") if isinstance(fault, dict) else getattr(fault, "message", None)
            if msg:
                result["error"] = str(msg)
                return result
    except Exception:
        pass

    try:
        # os-migrations에서 최근 error 항목 확인
        migs = list(conn.compute.migrations(instance_uuid=server_id))
        # 최신 순으로 정렬 후 error 상태 탐색
        migs.sort(key=lambda x: getattr(x, "created_at", "") or "", reverse=True)
        for mg in migs[:3]:
            if (getattr(mg, "status", "") or "").lower() == "error":
                dest = getattr(mg, "dest_compute", None)
                if dest:
                    result["migration"] = {
                        "id": getattr(mg, "id", None),
                        "source": getattr(mg, "source_compute", None),
                        "dest": dest,
                        "status": "error",
                        "type": getattr(mg, "migration_type", None),
                        "memory_percent": None,
                    }
                break
    except Exception:
        pass

    return result


def abort_live_migration(
    conn: openstack.connection.Connection,
    server_id: str,
    migration_id: str | None = None,
) -> None:
    """진행 중 라이브 마이그레이션을 중단한다."""
    if migration_id is None:
        active = list(conn.compute.server_migrations(server_id))
        if not active:
            raise ValueError("진행 중인 라이브 마이그레이션이 없습니다")
        migration_id = active[0].id
    conn.compute.abort_server_migration(migration_id, server_id)


def force_complete_live_migration(
    conn: openstack.connection.Connection,
    server_id: str,
    migration_id: str | None = None,
) -> None:
    """진행 중 라이브 마이그레이션을 강제 완료한다."""
    if migration_id is None:
        active = list(conn.compute.server_migrations(server_id))
        if not active:
            raise ValueError("진행 중인 라이브 마이그레이션이 없습니다")
        migration_id = active[0].id
    conn.compute.force_complete_server_migration(migration_id, server_id)


def _server_to_info(s) -> InstanceInfo:
    ips = []
    for net_name, net_addrs in (s.addresses or {}).items():
        for addr in net_addrs:
            ips.append(
                IpAddress(
                    addr=addr["addr"],
                    type=addr.get("OS-EXT-IPS:type", "fixed"),
                    network_name=net_name,
                )
            )

    meta = dict(s.metadata) if s.metadata else {}

    image_id = s.image.get("id") if isinstance(s.image, dict) else None
    flavor_id = None
    flavor_name = None
    if isinstance(s.flavor, dict):
        flavor_id = s.flavor.get("id")
        # 마이크로버전 2.47+에서는 "id" 없이 "original_name"만 반환
        flavor_name = s.flavor.get("original_name")

    fault = None
    if s.status == "ERROR":
        raw_fault = getattr(s, "fault", None)
        if raw_fault:
            fault = dict(raw_fault) if not isinstance(raw_fault, dict) else raw_fault

    _h = getattr(s, "compute_host", None)
    host = _h if isinstance(_h, str) else None

    return InstanceInfo(
        id=s.id,
        name=s.name,
        status=s.status,
        image_id=image_id,
        flavor_id=flavor_id,
        flavor_name=flavor_name,
        ip_addresses=ips,
        created_at=str(s.created_at) if s.created_at else None,
        metadata=meta,
        union_libraries=meta.get("union_libraries", "").split(",") if meta.get("union_libraries") else [],
        union_strategy=meta.get("union_strategy"),
        union_share_ids=[
            share_id for share_id in meta.get("union_share_ids", "").split(",") if share_id and share_id != "none"
        ],
        union_upper_volume_id=(
            meta.get("union_upper_volume_id") if meta.get("union_upper_volume_id") not in {None, "", "none"} else None
        ),
        scheduling=meta.get("scheduling"),
        key_name=getattr(s, "key_name", None),
        user_id=getattr(s, "user_id", None),
        project_id=getattr(s, "project_id", None) or getattr(s, "tenant_id", None),
        fault=fault,
        # 관리자 스코프에서만 채워짐 (OS-EXT-SRV-ATTR:host → compute_host)
        host=host,
    )
