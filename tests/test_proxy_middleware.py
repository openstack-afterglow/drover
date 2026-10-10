"""Trusted proxy scheme regressions; preserve the socket peer and client-IP boundaries."""

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from drover.middleware import CorrelationMiddleware, TrustedProxySchemeMiddleware
from drover.rate_limit import _get_real_ip, get_trusted_client_ip


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("peer", "headers", "scheme", "expected_client"),
    [
        ("192.0.2.10", [("X-Forwarded-Proto", "https"),
                       ("X-Forwarded-For", "198.51.100.20, 192.0.2.11")], "https", "198.51.100.20"),
        # HAProxy option forwardfor can append a second field after caller-supplied XFF.
        ("192.0.2.10", [("X-Forwarded-Proto", "https"),
                       ("X-Forwarded-For", "10.1.2.3"),
                       ("X-Forwarded-For", "198.51.100.20")], "https", "198.51.100.20"),
        ("203.0.113.50", [("X-Forwarded-Proto", "https"),
                         ("X-Forwarded-For", "192.0.2.10"),
                         ("X-Real-IP", "198.51.100.20")], "http", "203.0.113.50"),
        ("192.0.2.10", [("X-Forwarded-Proto", "https"),
                       ("X-Real-IP", "198.51.100.20")], "https", "198.51.100.20"),
        ("192.0.2.10", [("X-Forwarded-Proto", "https"),
                       ("X-Forwarded-Proto", "http")], "http", "192.0.2.10"),
        ("192.0.2.10", [("X-Forwarded-Proto", "https"),
                       ("X-Forwarded-Proto", "https")], "http", "192.0.2.10"),
        ("192.0.2.10", [("Forwarded", "proto=https")], "http", "192.0.2.10"),
    ],
)
async def test_scheme_handling_keeps_original_peer_and_client_resolution(
    monkeypatch, peer, headers, scheme, expected_client,
):
    monkeypatch.setenv("TRUSTED_PROXIES", "192.0.2.10/32,192.0.2.11/32")
    app = FastAPI()
    app.add_middleware(TrustedProxySchemeMiddleware)
    app.add_middleware(CorrelationMiddleware)

    @app.get("/inspect")
    async def inspect(request: Request):
        return {
            "scheme": request.scope["scheme"],
            "peer": request.client.host,
            "port": request.client.port,
            "client": get_trusted_client_ip(request),
            "rate_limit_key": _get_real_ip(request),
            "forwarded_for": request.headers.getlist("x-forwarded-for"),
        }

    async with AsyncClient(
        transport=ASGITransport(app=app, client=(peer, 43210)), base_url="http://api.example",
    ) as client:
        response = await client.get("/inspect", headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "scheme": scheme,
        "peer": peer,
        "port": 43210,
        "client": expected_client,
        "rate_limit_key": expected_client,
        "forwarded_for": [value for name, value in headers if name.lower() == "x-forwarded-for"],
    }
    assert response.headers["x-openstack-request-id"].startswith("req-")


@pytest.mark.asyncio
@pytest.mark.parametrize("peer", [None, ("not-an-ip", 43210)])
async def test_scheme_requires_a_known_ip_peer(monkeypatch, peer):
    monkeypatch.setenv("TRUSTED_PROXIES", "127.0.0.1/32")
    app = AsyncMock()
    scope = {
        "type": "http", "scheme": "http", "client": peer,
        "headers": [(b"x-forwarded-proto", b"https")],
    }
    receive, send = AsyncMock(), AsyncMock()
    await TrustedProxySchemeMiddleware(app)(scope, receive, send)
    assert scope["scheme"] == "http"
    app.assert_awaited_once_with(scope, receive, send)


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_type", ["websocket", "lifespan"])
async def test_non_http_scope_is_unchanged(scope_type):
    app = AsyncMock()
    scope = {"type": scope_type}
    receive, send = AsyncMock(), AsyncMock()
    await TrustedProxySchemeMiddleware(app)(scope, receive, send)
    assert scope == {"type": scope_type}
    app.assert_awaited_once_with(scope, receive, send)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("peer", "expected_second_status"),
    [("192.0.2.10", 200), ("203.0.113.50", 429)],
)
async def test_scheme_handling_preserves_rate_limit_buckets(monkeypatch, peer, expected_second_status):
    """Trusted forwarded clients get separate buckets; an untrusted peer cannot evade its bucket."""
    monkeypatch.setenv("TRUSTED_PROXIES", "192.0.2.10/32")
    app = FastAPI()
    limiter = Limiter(key_func=_get_real_ip, storage_uri="memory://")
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)
    app.add_middleware(TrustedProxySchemeMiddleware)
    app.add_middleware(CorrelationMiddleware)

    @app.get("/limited")
    @limiter.limit("1/minute")
    async def limited(request: Request):
        return {"scheme": request.scope["scheme"]}

    headers = {"X-Forwarded-Proto": "https", "X-Openstack-Request-Id": "req-proxy-limit"}
    async with AsyncClient(
        transport=ASGITransport(app=app, client=(peer, 43210)), base_url="http://api.example",
    ) as client:
        first = await client.get("/limited", headers={**headers, "X-Forwarded-For": "198.51.100.20"})
        second = await client.get("/limited", headers={**headers, "X-Forwarded-For": "198.51.100.21"})
        repeated = await client.get("/limited", headers={**headers, "X-Forwarded-For": "198.51.100.20"})
    assert first.status_code == 200
    assert first.json() == {"scheme": "https" if peer == "192.0.2.10" else "http"}
    assert second.status_code == expected_second_status
    assert repeated.status_code == 429
    for response in (first, second, repeated):
        assert response.headers["x-openstack-request-id"] == "req-proxy-limit"
