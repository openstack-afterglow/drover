"""Rate limiting and client IP resolution helper for Drover."""

from __future__ import annotations

import ipaddress
import logging
from collections.abc import Sequence

from fastapi import Request
from slowapi import Limiter

from drover.config import get_settings

_logger = logging.getLogger(__name__)


def _parse_ip_networks(cidrs: str | Sequence[str] | None) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    if not cidrs:
        return []
    if isinstance(cidrs, str):
        cidrs = [c.strip() for c in cidrs.split(",") if c.strip()]
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for item in cidrs:
        item_str = str(item).strip()
        if item_str:
            try:
                networks.append(ipaddress.ip_network(item_str, strict=False))
            except ValueError as e:
                _logger.warning("Invalid CIDR format in config '%s': %s", item_str, e)
    return networks


def is_ip_in_cidrs(ip_str: str, cidrs: str | Sequence[str] | None) -> bool:
    if not ip_str or not cidrs:
        return False
    try:
        ip = ipaddress.ip_address(ip_str.strip())
    except ValueError:
        return False
    networks = _parse_ip_networks(cidrs)
    return any(ip in net for net in networks)


def get_trusted_client_ip(request: Request, trusted_proxies: str | Sequence[str] | None = None) -> str:
    """Resolve client IP using trusted-proxy semantics.

    If direct connection peer is not a trusted proxy, X-Forwarded-For / X-Real-IP are ignored.
    If direct peer is trusted, walk X-Forwarded-For chain right-to-left to find the non-trusted client IP.
    """
    if trusted_proxies is None:
        try:
            trusted_proxies = get_settings().trusted_proxies
        except Exception:
            trusted_proxies = "127.0.0.1/32,::1/128"

    trusted_nets = _parse_ip_networks(trusted_proxies)

    peer_host = request.client.host if request.client and request.client.host else "127.0.0.1"
    try:
        peer_ip = ipaddress.ip_address(peer_host)
    except ValueError:
        return peer_host

    peer_is_trusted = any(peer_ip in net for net in trusted_nets)

    if not peer_is_trusted:
        return peer_host

    x_forwarded_for = request.headers.get("X-Forwarded-For")
    if x_forwarded_for:
        hops = [h.strip() for h in x_forwarded_for.split(",") if h.strip()]
        for hop in reversed(hops):
            try:
                hop_ip = ipaddress.ip_address(hop)
            except ValueError:
                return hop
            if not any(hop_ip in net for net in trusted_nets):
                return hop
        return hops[0] if hops else peer_host

    x_real_ip = request.headers.get("X-Real-IP")
    if x_real_ip:
        return x_real_ip.strip()

    return peer_host


def _get_real_ip(request: Request) -> str:
    return get_trusted_client_ip(request)


limiter = Limiter(key_func=_get_real_ip)
