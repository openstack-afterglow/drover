"""Rate limiting helper for Drover."""

from __future__ import annotations

from fastapi import Request
from slowapi import Limiter


def _get_real_ip(request: Request) -> str:
    if x_forwarded_for := request.headers.get("X-Forwarded-For"):
        return x_forwarded_for.split(",")[0].strip()
    if x_real_ip := request.headers.get("X-Real-IP"):
        return x_real_ip.strip()
    return request.client.host if request.client else "127.0.0.1"


limiter = Limiter(key_func=_get_real_ip)
