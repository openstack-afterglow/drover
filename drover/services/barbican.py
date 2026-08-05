"""Barbican (key-manager) 서비스 헬퍼.

§ PR2 — per-project KEK auto-provisioning.

owner project 의 Barbican 에서 k8s 용 KEK (key encryption key) 를 조회하거나,
없으면 신규 발급. cluster 별이 아니라 **project 별 공유 KEK** 패턴 — 같은 project
내 모든 cluster 가 같은 KEK 를 재사용 (lifecycle 단순, cluster delete 시 KEK 유지).

발급자는 § 28 의 manager user (`afterglow-cluster-mgr-<proj>`) — 같은 user 의
app credential 이 자동으로 read 권한을 가짐 (Barbican default policy: creator 접근).
"""

from __future__ import annotations

import asyncio
import logging

from drover.config import get_settings

_logger = logging.getLogger(__name__)

KEK_NAME = "afterglow-k8s-kek"


async def ensure_project_kek(project_id: str) -> str:
    """프로젝트 owner Barbican 에서 k8s KEK 조회/발급. 반환: KEK UUID.

    동작:
    1. manager user 로 project-scoped Barbican 검색 (이름='afterglow-k8s-kek')
    2. 발견 + ACTIVE 면 그 UUID 반환 (idempotent)
    3. 없으면 secret order 발급 (aes/cbc/256, async). PENDING → ACTIVE 폴링.
    4. Secret ref URL 의 마지막 segment 가 UUID.
    """
    from drover.services import keystone as _keystone

    user_id, password = await _keystone.ensure_cluster_manager_user(project_id)
    settings = get_settings()
    return await asyncio.to_thread(_ensure_project_kek_sync, project_id, password, settings)


def _ensure_project_kek_sync(project_id: str, password: str, settings) -> str:
    from drover.services.keystone import _connect_as_manager

    conn = _connect_as_manager(project_id, password, settings)
    try:
        ep = conn.session.get_endpoint(service_type="key-manager")

        # 1. 기존 KEK 검색
        r = conn.session.get(f"{ep}/v1/secrets", params={"name": KEK_NAME})
        for s in r.json().get("secrets", []) or []:
            if s.get("status") == "ACTIVE":
                kek_id = s["secret_ref"].rsplit("/", 1)[-1]
                _logger.info("기존 KEK 재사용: project=%s kek=%s", project_id, kek_id)
                return kek_id

        # 2. 신규 order 발급
        _logger.info("신규 KEK order 발급: project=%s", project_id)
        order_resp = conn.session.post(
            f"{ep}/v1/orders",
            json={
                "type": "key",
                "meta": {
                    "name": KEK_NAME,
                    "algorithm": "aes",
                    "mode": "cbc",
                    "bit_length": 256,
                    "payload_content_type": "application/octet-stream",
                },
            },
        )
        order_resp.raise_for_status()
        order_ref = order_resp.json().get("order_ref")
        if not order_ref:
            raise RuntimeError(f"Barbican order 응답에 order_ref 없음: {order_resp.text[:200]}")

        # 3. order PENDING → ACTIVE 폴링 (최대 30초)
        import time

        for _ in range(30):
            o = conn.session.get(order_ref)
            o_body = o.json()
            status = o_body.get("status")
            if status == "ACTIVE":
                kek_id = o_body["secret_ref"].rsplit("/", 1)[-1]
                _logger.info("KEK 발급 완료: project=%s kek=%s", project_id, kek_id)
                return kek_id
            if status == "ERROR":
                raise RuntimeError(f"Barbican order 실패: {o_body.get('error_reason', 'unknown')}")
            time.sleep(1)
        raise RuntimeError("Barbican KEK order timeout (30s)")
    finally:
        conn.close()


# ── Key Manager API helpers ───────────────────────────────────────────────────

SYSTEM_MANAGED_PREFIXES = ("afterglow-",)


def _is_system_managed(name: str | None) -> bool:
    if not name:
        return False
    return any(name.startswith(p) for p in SYSTEM_MANAGED_PREFIXES)


def _bep(conn) -> str:
    """Barbican endpoint."""
    return conn.session.get_endpoint(service_type="key-manager")


def _secret_uuid(s) -> str:
    """openstacksdk Secret 객체에서 UUID만 추출.

    openstacksdk 버전에 따라 s.id가 전체 URL을 반환하는 경우가 있어
    secret_ref → rsplit("/")[-1] 로 UUID를 안전하게 추출한다.
    """
    ref = getattr(s, "secret_ref", None) or getattr(s, "id", "") or ""
    return str(ref).rsplit("/", 1)[-1]


# ── Secrets ──────────────────────────────────────────────────────────────────


def list_secrets(conn, **filters) -> list[dict]:
    """프로젝트 범위 secret 목록."""
    return [
        {
            "id": _secret_uuid(s),
            "name": s.name,
            "secret_type": s.secret_type,
            "status": s.status,
            "algorithm": s.algorithm,
            "bit_length": s.bit_length,
            "mode": s.mode,
            "created": str(s.created_at) if s.created_at else None,
            "expires": str(s.expires_at) if s.expires_at else None,
            "content_types": s.content_types,
            "system_managed": _is_system_managed(s.name),
        }
        for s in conn.key_manager.secrets(**filters)
    ]


def get_secret_meta(conn, secret_id: str) -> dict:
    s = conn.key_manager.get_secret(secret_id)
    return {
        "id": _secret_uuid(s),
        "name": s.name,
        "secret_type": s.secret_type,
        "status": s.status,
        "algorithm": s.algorithm,
        "bit_length": s.bit_length,
        "mode": s.mode,
        "created": str(s.created_at) if s.created_at else None,
        "expires": str(s.expires_at) if s.expires_at else None,
        "content_types": s.content_types,
        "system_managed": _is_system_managed(s.name),
    }


def get_secret_payload(conn, secret_id: str) -> bytes:
    """payload 복호화 — raw REST (캐시/로그 금지)."""
    ep = _bep(conn)
    resp = conn.session.get(
        f"{ep}/v1/secrets/{secret_id}/payload",
        headers={"Accept": "application/octet-stream"},
    )
    resp.raise_for_status()
    return resp.content


def create_secret(
    conn,
    *,
    name: str,
    secret_type: str = "opaque",
    payload: str | None = None,
    payload_content_type: str = "text/plain",
    algorithm: str | None = None,
    bit_length: int | None = None,
    mode: str | None = None,
    expiration: str | None = None,
) -> dict:
    kwargs: dict = {"name": name, "secret_type": secret_type}
    if payload is not None:
        kwargs["payload"] = payload
        kwargs["payload_content_type"] = payload_content_type
    if algorithm:
        kwargs["algorithm"] = algorithm
    if bit_length:
        kwargs["bit_length"] = bit_length
    if mode:
        kwargs["mode"] = mode
    if expiration:
        kwargs["expiration"] = expiration
    s = conn.key_manager.create_secret(**kwargs)
    return {"id": s.id, "name": s.name, "secret_ref": getattr(s, "secret_ref", None)}


def delete_secret(conn, secret_id: str) -> None:
    meta = conn.key_manager.get_secret(secret_id)
    if _is_system_managed(meta.name):
        raise ValueError(f"시스템 관리 secret은 삭제할 수 없습니다: {meta.name}")
    conn.key_manager.delete_secret(secret_id, ignore_missing=False)


# ── Containers ───────────────────────────────────────────────────────────────


def list_containers(conn) -> list[dict]:
    return [
        {
            "id": c.id,
            "name": c.name,
            "type": c.type,
            "status": c.status,
            "created": str(c.created_at) if c.created_at else None,
            "secret_refs": getattr(c, "secret_refs", []),
        }
        for c in conn.key_manager.containers()
    ]


def create_container(conn, *, name: str, container_type: str, secret_refs: list[dict]) -> dict:
    c = conn.key_manager.create_container(name=name, type=container_type, secret_refs=secret_refs)
    return {"id": c.id, "name": c.name, "type": c.type}


def delete_container(conn, container_id: str) -> None:
    conn.key_manager.delete_container(container_id, ignore_missing=False)


# ── Orders ───────────────────────────────────────────────────────────────────


def list_orders(conn) -> list[dict]:
    return [
        {
            "id": o.id,
            "type": o.type,
            "status": o.status,
            "created": str(o.created_at) if o.created_at else None,
            "secret_ref": getattr(o, "secret_ref", None),
            "container_ref": getattr(o, "container_ref", None),
            "meta": getattr(o, "meta", {}),
        }
        for o in conn.key_manager.orders()
    ]


def create_order(conn, *, order_type: str, meta: dict) -> dict:
    o = conn.key_manager.create_order(type=order_type, meta=meta)
    return {"id": o.id, "type": o.type, "status": o.status}


def get_order(conn, order_id: str) -> dict:
    o = conn.key_manager.get_order(order_id)
    return {
        "id": o.id,
        "type": o.type,
        "status": o.status,
        "secret_ref": getattr(o, "secret_ref", None),
        "container_ref": getattr(o, "container_ref", None),
        "meta": getattr(o, "meta", {}),
        "error_reason": getattr(o, "error_reason", None),
    }


def delete_order(conn, order_id: str) -> None:
    conn.key_manager.delete_order(order_id, ignore_missing=False)


# ── ACL ──────────────────────────────────────────────────────────────────────


def get_acl(conn, resource_type: str, resource_id: str) -> dict:
    ep = _bep(conn)
    r = conn.session.get(f"{ep}/v1/{resource_type}/{resource_id}/acl")
    r.raise_for_status()
    return r.json()


def set_acl(conn, resource_type: str, resource_id: str, users: list[str], project_access: bool) -> dict:
    ep = _bep(conn)
    r = conn.session.put(
        f"{ep}/v1/{resource_type}/{resource_id}/acl",
        json={"read": {"users": users, "project-access": project_access}},
    )
    r.raise_for_status()
    return r.json()


def delete_acl(conn, resource_type: str, resource_id: str) -> None:
    ep = _bep(conn)
    r = conn.session.delete(f"{ep}/v1/{resource_type}/{resource_id}/acl")
    r.raise_for_status()


# ── Consumers ────────────────────────────────────────────────────────────────


def list_secret_consumers(conn, secret_id: str) -> list[dict]:
    ep = _bep(conn)
    r = conn.session.get(f"{ep}/v1/secrets/{secret_id}/consumers")
    r.raise_for_status()
    return r.json().get("consumers", [])


def list_container_consumers(conn, container_id: str) -> list[dict]:
    ep = _bep(conn)
    r = conn.session.get(f"{ep}/v1/containers/{container_id}/consumers")
    r.raise_for_status()
    return r.json().get("consumers", [])


# ── Quotas ───────────────────────────────────────────────────────────────────


def get_effective_quota(conn) -> dict:
    ep = _bep(conn)
    r = conn.session.get(f"{ep}/v1/quotas")
    r.raise_for_status()
    return r.json().get("quotas", {})


def list_project_quotas(conn) -> list[dict]:
    ep = _bep(conn)
    r = conn.session.get(f"{ep}/v1/project-quotas")
    r.raise_for_status()
    return r.json().get("project_quotas", [])


def get_project_quota(conn, project_id: str) -> dict:
    ep = _bep(conn)
    r = conn.session.get(f"{ep}/v1/project-quotas/{project_id}")
    r.raise_for_status()
    return r.json()


def set_project_quota(conn, project_id: str, quotas: dict) -> dict:
    ep = _bep(conn)
    r = conn.session.put(
        f"{ep}/v1/project-quotas/{project_id}",
        json={"project_quotas": quotas},
    )
    r.raise_for_status()
    return r.json()


def delete_project_quota(conn, project_id: str) -> None:
    ep = _bep(conn)
    r = conn.session.delete(f"{ep}/v1/project-quotas/{project_id}")
    r.raise_for_status()
