"""Octavia (OpenStack Load Balancer) 서비스 래퍼."""

from __future__ import annotations

import logging
import time
from threading import Lock
from typing import TYPE_CHECKING

from openstack import exceptions as openstack_exceptions

if TYPE_CHECKING:
    import openstack

_logger = logging.getLogger(__name__)

def _lb_to_dict(lb) -> dict:
    prov = getattr(lb, "provisioning_status", "") or ""
    return {
        "id": lb.id,
        "name": lb.name or "",
        "description": getattr(lb, "description", "") or "",
        "status": prov,
        "provisioning_status": prov,
        "operating_status": getattr(lb, "operating_status", "") or "",
        "vip_address": getattr(lb, "vip_address", None),
        "vip_subnet_id": getattr(lb, "vip_subnet_id", None),
        "vip_network_id": getattr(lb, "vip_network_id", None),
        "vip_port_id": getattr(lb, "vip_port_id", None),
        "project_id": getattr(lb, "project_id", None),
    }


def _listener_to_dict(listener) -> dict:
    return {
        "id": listener.id,
        "name": listener.name or "",
        "protocol": getattr(listener, "protocol", "") or "",
        "protocol_port": getattr(listener, "protocol_port", 0),
        "status": getattr(listener, "provisioning_status", "") or "",
        "default_pool_id": getattr(listener, "default_pool_id", None),
        "load_balancer_id": getattr(listener, "load_balancer_id", None),
    }


def _pool_to_dict(p) -> dict:
    return {
        "id": p.id,
        "name": p.name or "",
        "protocol": getattr(p, "protocol", "") or "",
        "lb_algorithm": getattr(p, "lb_algorithm", "") or "",
        "status": getattr(p, "provisioning_status", "") or "",
        "health_monitor_id": getattr(p, "health_monitor_id", None),
        "load_balancer_id": (getattr(p, "load_balancers", None) or [{}])[0].get("id")
        if getattr(p, "load_balancers", None)
        else None,
    }


def _member_to_dict(m) -> dict:
    return {
        "id": m.id,
        "name": m.name or "",
        "address": getattr(m, "address", "") or "",
        "protocol_port": getattr(m, "protocol_port", 0),
        "weight": getattr(m, "weight", 1),
        "status": getattr(m, "provisioning_status", "") or "",
        "subnet_id": getattr(m, "subnet_id", None),
    }


def _hm_to_dict(hm) -> dict:
    return {
        "id": hm.id,
        "name": hm.name or "",
        "type": getattr(hm, "type", "") or "",
        "delay": getattr(hm, "delay", 5),
        "timeout": getattr(hm, "timeout", 5),
        "max_retries": getattr(hm, "max_retries", 3),
        "status": getattr(hm, "provisioning_status", "") or "",
    }


# ---------------------------------------------------------------------------
# Load Balancers
# ---------------------------------------------------------------------------


def list_load_balancers(conn: openstack.connection.Connection, project_id: str | None = None) -> list[dict]:
    kwargs = {}
    if project_id:
        kwargs["project_id"] = project_id
    return [_lb_to_dict(lb) for lb in conn.load_balancer.load_balancers(**kwargs)]


def get_load_balancer(conn: openstack.connection.Connection, lb_id: str) -> dict:
    return _lb_to_dict(conn.load_balancer.get_load_balancer(lb_id))


def create_load_balancer(
    conn: openstack.connection.Connection,
    name: str,
    vip_subnet_id: str = "",
    description: str = "",
    *,
    vip_network_id: str | None = None,
    tags: list[str] | None = None,
) -> dict:
    """Octavia LB 생성.

    vip_network_id가 있으면 provider 네트워크에 직접 VIP를 생성 (FIP 불필요).
    없으면 vip_subnet_id로 tenant 서브넷에 VIP를 생성.
    """
    kwargs: dict = {"name": name, "description": description}
    if vip_network_id:
        kwargs["vip_network_id"] = vip_network_id
    else:
        kwargs["vip_subnet_id"] = vip_subnet_id
    if tags:
        kwargs["tags"] = tags
    lb = conn.load_balancer.create_load_balancer(**kwargs)
    return _lb_to_dict(lb)


def delete_load_balancer(conn: openstack.connection.Connection, lb_id: str, cascade: bool = True) -> None:
    conn.load_balancer.delete_load_balancer(lb_id, cascade=cascade, ignore_missing=True)

def wait_load_balancer_deleted(conn: openstack.connection.Connection, lb_id: str, timeout: int = 120) -> None:
    """LB가 완전히 삭제될 때까지 폴링."""
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            lb = conn.load_balancer.find_load_balancer(lb_id, ignore_missing=True)
            if lb is None or getattr(lb, "provisioning_status", "") in {"DELETED", ""}:
                return
        except Exception:
            return
        time.sleep(3)
    raise TimeoutError(f"LB {lb_id} 삭제 대기 타임아웃 ({timeout}s)")


def delete_load_balancer_safe(
    conn: openstack.connection.Connection,
    lb_id: str,
    expected_project_id: str,
    expected_cluster_id: str,
    cascade: bool = True,
) -> None:
    """프로젝트 및 Drover 소유권 검증 후 LB를 안전하게 삭제."""
    from drover.services.inventory import validate_resource_ownership

    try:
        lb = conn.load_balancer.find_load_balancer(lb_id, ignore_missing=True)
    except Exception:
        lb = None
    if lb is None:
        return
    if not validate_resource_ownership(lb, expected_project_id, expected_cluster_id, "load_balancer"):
        raise ValueError(f"Load balancer {lb_id} ownership validation failed for project {expected_project_id}")
    delete_load_balancer(conn, lb_id, cascade=cascade)
    wait_load_balancer_deleted(conn, lb_id)

def wait_for_load_balancer(
    conn: openstack.connection.Connection,
    lb_id: str,
    status: str = "ACTIVE",
    wait: int = 300,
    interval: int = 5,
) -> dict:
    """LB가 target provisioning_status에 도달할 때까지 폴링. 타임아웃 시 TimeoutError."""
    import time

    deadline = time.time() + wait
    while time.time() < deadline:
        try:
            lb = conn.load_balancer.get_load_balancer(lb_id)
        except openstack_exceptions.ResourceNotFound as exc:
            raise RuntimeError(f"LB {lb_id} disappeared while waiting for {status}") from exc
        prov_status = getattr(lb, "provisioning_status", "")
        if prov_status == status:
            return _lb_to_dict(lb)
        if prov_status == "ERROR":
            raise RuntimeError(f"LB {lb_id} entered ERROR state")
        time.sleep(interval)
    raise TimeoutError(f"LB {lb_id} did not reach {status} within {wait}s")


def get_lb_status_tree(conn: openstack.connection.Connection, lb_id: str) -> dict:
    """로드밸런서 상태 트리 조회 (Octavia status tree API).
    오류 발생 위치를 계층적으로 확인하는 데 사용.
    """
    try:
        # openstacksdk가 status tree를 직접 지원하지 않으므로 raw session 사용
        lb = conn.load_balancer.get_load_balancer(lb_id)
        endpoint = conn.load_balancer.get_endpoint()
        resp = conn.load_balancer._session.get(f"{endpoint.rstrip('/')}/lbaas/loadbalancers/{lb_id}/status")
        data = resp.json() if hasattr(resp, "json") else {}
        return data.get("statuses", {}).get("loadbalancer", {})
    except Exception:
        pass
    # fallback: 기본 LB 정보만 반환
    try:
        lb = conn.load_balancer.get_load_balancer(lb_id)
        return {
            "id": lb.id,
            "name": lb.name,
            "provisioning_status": getattr(lb, "provisioning_status", ""),
            "operating_status": getattr(lb, "operating_status", ""),
            "listeners": [],
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Listeners
# ---------------------------------------------------------------------------


def list_listeners(conn: openstack.connection.Connection, lb_id: str | None = None) -> list[dict]:
    kwargs = {}
    if lb_id:
        kwargs["load_balancer_id"] = lb_id
    return [_listener_to_dict(listener) for listener in conn.load_balancer.listeners(**kwargs)]


def _wait_for_parent_load_balancer(conn: openstack.connection.Connection, lb_id: str) -> None:
    if lb_id:
        wait_for_load_balancer(conn, lb_id)


def create_listener(
    conn: openstack.connection.Connection,
    lb_id: str,
    protocol: str,
    protocol_port: int,
    name: str = "",
    default_pool_id: str | None = None,
) -> dict:
    kwargs: dict = {
        "load_balancer_id": lb_id,
        "protocol": protocol,
        "protocol_port": protocol_port,
    }
    if name:
        kwargs["name"] = name
    if default_pool_id:
        kwargs["default_pool_id"] = default_pool_id
    _wait_for_parent_load_balancer(conn, lb_id)
    listener = conn.load_balancer.create_listener(**kwargs)
    _wait_for_parent_load_balancer(conn, lb_id)
    return _listener_to_dict(listener)


def _listener_load_balancer_id(conn: openstack.connection.Connection, listener_id: str) -> str:
    try:
        listener = conn.load_balancer.find_listener(listener_id, ignore_missing=True)
    except openstack_exceptions.ResourceNotFound:
        return ""
    except Exception:
        _logger.warning("Failed to resolve listener %s parent load balancer", listener_id, exc_info=True)
        raise
    if isinstance(listener, dict):
        return str(listener.get("load_balancer_id") or "")
    return str(getattr(listener, "load_balancer_id", "") or "") if listener is not None else ""

def delete_listener(conn: openstack.connection.Connection, listener_id: str) -> None:
    lb_id = _listener_load_balancer_id(conn, listener_id)
    _wait_for_parent_load_balancer(conn, lb_id)
    conn.load_balancer.delete_listener(listener_id, ignore_missing=True)
    _wait_for_parent_load_balancer(conn, lb_id)


# ---------------------------------------------------------------------------
# Pools
# ---------------------------------------------------------------------------


def list_pools(conn: openstack.connection.Connection, lb_id: str | None = None) -> list[dict]:
    kwargs = {}
    if lb_id:
        kwargs["load_balancer_id"] = lb_id
    return [_pool_to_dict(p) for p in conn.load_balancer.pools(**kwargs)]


def create_pool(
    conn: openstack.connection.Connection,
    lb_id: str,
    protocol: str,
    lb_algorithm: str = "ROUND_ROBIN",
    name: str = "",
    listener_id: str | None = None,
) -> dict:
    kwargs: dict = {
        "load_balancer_id": lb_id,
        "protocol": protocol,
        "lb_algorithm": lb_algorithm,
    }
    if name:
        kwargs["name"] = name
    if listener_id:
        kwargs["listener_id"] = listener_id
    _wait_for_parent_load_balancer(conn, lb_id)
    pool = conn.load_balancer.create_pool(**kwargs)
    _wait_for_parent_load_balancer(conn, lb_id)
    return _pool_to_dict(pool)


def _pool_load_balancer_id(conn: openstack.connection.Connection, pool_id: str) -> str:
    try:
        pool = conn.load_balancer.find_pool(pool_id, ignore_missing=True)
    except openstack_exceptions.ResourceNotFound:
        return ""
    except Exception:
        _logger.warning("Failed to resolve pool %s parent load balancer", pool_id, exc_info=True)
        raise
    if pool is None:
        return ""
    if isinstance(pool, dict):
        direct_id = pool.get("load_balancer_id")
        load_balancers = pool.get("load_balancers") or []
    else:
        direct_id = getattr(pool, "load_balancer_id", None)
        load_balancers = getattr(pool, "load_balancers", None) or []
    if isinstance(direct_id, str) and direct_id:
        return direct_id
    first = load_balancers[0] if load_balancers else None
    if isinstance(first, dict):
        return str(first.get("id") or "")
    return str(getattr(first, "id", "") or "")

def delete_pool(conn: openstack.connection.Connection, pool_id: str) -> None:
    lb_id = _pool_load_balancer_id(conn, pool_id)
    _wait_for_parent_load_balancer(conn, lb_id)
    conn.load_balancer.delete_pool(pool_id, ignore_missing=True)
    _wait_for_parent_load_balancer(conn, lb_id)


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------


def list_members(conn: openstack.connection.Connection, pool_id: str) -> list[dict]:
    return [_member_to_dict(m) for m in conn.load_balancer.members(pool_id)]


def add_member(
    conn: openstack.connection.Connection,
    pool_id: str,
    address: str,
    protocol_port: int,
    subnet_id: str | None = None,
    name: str = "",
    weight: int = 1,
) -> dict:
    kwargs: dict = {
        "address": address,
        "protocol_port": protocol_port,
        "weight": weight,
    }
    if name:
        kwargs["name"] = name
    if subnet_id:
        kwargs["subnet_id"] = subnet_id
    lb_id = _pool_load_balancer_id(conn, pool_id)
    _wait_for_parent_load_balancer(conn, lb_id)
    member = conn.load_balancer.create_member(pool_id, **kwargs)
    _wait_for_parent_load_balancer(conn, lb_id)
    return _member_to_dict(member)


def remove_member(conn: openstack.connection.Connection, pool_id: str, member_id: str) -> None:
    lb_id = _pool_load_balancer_id(conn, pool_id)
    _wait_for_parent_load_balancer(conn, lb_id)
    conn.load_balancer.delete_member(member_id, pool_id, ignore_missing=True)
    _wait_for_parent_load_balancer(conn, lb_id)


# ---------------------------------------------------------------------------
# Health Monitors
# ---------------------------------------------------------------------------


def list_health_monitors(conn: openstack.connection.Connection, pool_id: str | None = None) -> list[dict]:
    kwargs = {}
    if pool_id:
        kwargs["pool_id"] = pool_id
    return [_hm_to_dict(hm) for hm in conn.load_balancer.health_monitors(**kwargs)]


def create_health_monitor(
    conn: openstack.connection.Connection,
    pool_id: str,
    type: str = "HTTP",
    delay: int = 5,
    timeout: int = 5,
    max_retries: int = 3,
    name: str = "",
) -> dict:
    kwargs: dict = {
        "pool_id": pool_id,
        "type": type,
        "delay": delay,
        "timeout": timeout,
        "max_retries": max_retries,
    }
    if name:
        kwargs["name"] = name
    lb_id = _pool_load_balancer_id(conn, pool_id)
    _wait_for_parent_load_balancer(conn, lb_id)
    monitor = conn.load_balancer.create_health_monitor(**kwargs)
    _wait_for_parent_load_balancer(conn, lb_id)
    return _hm_to_dict(monitor)


def _health_monitor_pool_id(conn: openstack.connection.Connection, hm_id: str) -> str:
    try:
        monitor = conn.load_balancer.find_health_monitor(hm_id, ignore_missing=True)
    except openstack_exceptions.ResourceNotFound:
        return ""
    except Exception:
        _logger.warning("Failed to resolve health monitor %s parent pool", hm_id, exc_info=True)
        raise
    if isinstance(monitor, dict):
        return str(monitor.get("pool_id") or "")
    return str(getattr(monitor, "pool_id", "") or "") if monitor is not None else ""


def delete_health_monitor(conn: openstack.connection.Connection, hm_id: str) -> None:
    pool_id = _health_monitor_pool_id(conn, hm_id)
    lb_id = _pool_load_balancer_id(conn, pool_id) if pool_id else ""
    _wait_for_parent_load_balancer(conn, lb_id)
    conn.load_balancer.delete_health_monitor(hm_id, ignore_missing=True)
    _wait_for_parent_load_balancer(conn, lb_id)


# ---------------------------------------------------------------------------
# 토폴로지 수집
# ---------------------------------------------------------------------------


def get_topology_lbs(
    conn: openstack.connection.Connection,
    project_id: str | None = None,
    instances: list[dict] | None = None,
) -> list[dict]:
    """프로젝트의 LB 목록을 토폴로지용 dict로 반환.

    LB별로 list_listeners + list_pools + list_members를 직접 호출해 listener/pool/member를
    평탄화한다 (status tree보다 정확 — listener에 묶이지 않은 추가 pool도 포함).
    member.address ↔ instance fixed_ip 매칭으로 server_id를 미리 채운다.
    Octavia가 catalog에 없으면 빈 리스트 반환.
    """
    try:
        lbs = list_load_balancers(conn, project_id=project_id)
    except Exception:
        return []

    ip_to_server: dict[str, str] = {}
    for srv in instances or []:
        for ip_info in srv.get("ip_addresses") or []:
            addr = ip_info.get("addr")
            if addr and srv.get("id"):
                ip_to_server[addr] = srv["id"]

    result = []
    for lb in lbs:
        lb_id = lb["id"]

        listeners_dicts: list[dict] = []
        try:
            for li in list_listeners(conn, lb_id=lb_id):
                listeners_dicts.append(
                    {
                        "id": li.get("id", ""),
                        "name": li.get("name", ""),
                        "protocol": li.get("protocol", ""),
                        "protocol_port": li.get("protocol_port", 0),
                        "default_pool_id": li.get("default_pool_id"),
                    }
                )
        except Exception:
            pass

        members_flat: list[dict] = []
        try:
            pools = list_pools(conn, lb_id=lb_id)
        except Exception:
            pools = []
        for pool in pools:
            pid = pool.get("id", "")
            try:
                pool_members = list_members(conn, pool_id=pid)
            except Exception:
                continue
            for m in pool_members:
                addr = m.get("address", "")
                members_flat.append(
                    {
                        "id": m.get("id", ""),
                        "address": addr,
                        "protocol_port": m.get("protocol_port", 0),
                        "status": m.get("status", ""),
                        "subnet_id": m.get("subnet_id"),
                        "pool_id": pid,
                        "server_id": ip_to_server.get(addr),
                    }
                )
        result.append({**lb, "listeners": listeners_dicts, "members": members_flat})
    return result


# ---------------------------------------------------------------------------
# LB 트래픽 stats (Octavia /stats API — 누적 카운터 차분으로 rate 계산)
# ---------------------------------------------------------------------------

_lb_snapshot: dict[str, tuple[int, int, float]] = {}  # lb_id → (bytes_in, bytes_out, ts)
_snapshot_lock = Lock()


def get_lb_stats(conn, lb_id: str) -> dict[str, int] | None:
    """Octavia 누적 stats 반환. 실패 시 None."""
    try:
        s = conn.load_balancer.get_load_balancer_statistics(lb_id)
        return {
            "bytes_in": int(getattr(s, "bytes_in", 0) or 0),
            "bytes_out": int(getattr(s, "bytes_out", 0) or 0),
            "active_connections": int(getattr(s, "active_connections", 0) or 0),
        }
    except Exception:
        return None


def lb_rate_from_snapshot(lb_id: str, current: dict[str, int]) -> dict[str, float]:
    """이전 스냅샷과 비교해 bps 계산 후 스냅샷 갱신.

    최초 호출은 rx_bps=tx_bps=0 (스냅샷이 없음). 다음 폴링(15s 후)부터 정상 값.
    """
    now = time.time()
    with _snapshot_lock:
        prev = _lb_snapshot.get(lb_id)
        _lb_snapshot[lb_id] = (current["bytes_in"], current["bytes_out"], now)
    if prev is None:
        return {"rx_bps": 0.0, "tx_bps": 0.0}
    dt = now - prev[2]
    if dt <= 0:
        return {"rx_bps": 0.0, "tx_bps": 0.0}
    # bytes_out = client 가 받는 응답 → client 입장 rx
    return {
        "rx_bps": max(0.0, (current["bytes_out"] - prev[1]) / dt) * 8,
        "tx_bps": max(0.0, (current["bytes_in"] - prev[0]) / dt) * 8,
    }
