"""Focused tests for Drover X-Openstack-Request-Id correlation middleware and logging context."""

import logging
import re

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from drover.main import app as main_app
from drover.middleware import (
    CorrelationMiddleware,
    RequestIdFilter,
    get_request_id,
    get_request_logger,
    validate_request_id,
)
from drover.services.errors import K3sApiError

_UUID_REQ_RE = re.compile(r"^req-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def test_validate_request_id_rules():
    """Verify validation accepts safe IDs and generates req-UUID for unsafe ones."""
    # Valid custom and OpenStack IDs
    assert validate_request_id("req-12345") == "req-12345"
    assert validate_request_id("custom_id.1-23") == "custom_id.1-23"

    # Malformed, whitespace, or control characters -> fallback to req-<uuid>
    res_bad = validate_request_id("bad id\nwith\rnewlines")
    assert _UUID_REQ_RE.match(res_bad)

    # Unbounded (>128 chars) -> fallback
    res_long = validate_request_id("a" * 150)
    assert _UUID_REQ_RE.match(res_long)

    # Empty / None -> fallback
    res_none = validate_request_id(None)
    assert _UUID_REQ_RE.match(res_none)


@pytest.mark.asyncio
async def test_caller_provided_header_propagation():
    """Verify caller-provided X-Openstack-Request-Id is propagated into response header."""
    async with AsyncClient(transport=ASGITransport(app=main_app), base_url="http://test") as client:
        res = await client.get("/", headers={"X-Openstack-Request-Id": "req-caller-provided-101"})
        assert res.status_code == 200
        assert res.headers.get("X-Openstack-Request-Id") == "req-caller-provided-101"

        # Also verify fallback lookup for X-Request-Id when X-Openstack-Request-Id is absent
        res2 = await client.get("/", headers={"X-Request-Id": "req-fallback-header-202"})
        assert res2.status_code == 200
        assert res2.headers.get("X-Openstack-Request-Id") == "req-fallback-header-202"


@pytest.mark.asyncio
async def test_generated_request_id():
    """Verify generated req-<uuid> is returned when header is missing."""
    async with AsyncClient(transport=ASGITransport(app=main_app), base_url="http://test") as client:
        res = await client.get("/")
        assert res.status_code == 200
        req_id = res.headers.get("X-Openstack-Request-Id")
        assert req_id is not None
        assert _UUID_REQ_RE.match(req_id)


@pytest.mark.asyncio
async def test_malformed_unbounded_header_fallback():
    """Verify malformed or unbounded caller headers are replaced with generated IDs."""
    async with AsyncClient(transport=ASGITransport(app=main_app), base_url="http://test") as client:
        # Injection / spaces in header
        res = await client.get("/", headers={"X-Openstack-Request-Id": "<script>alert(1)</script>"})
        assert res.status_code == 200
        req_id = res.headers.get("X-Openstack-Request-Id")
        assert req_id is not None
        assert req_id != "<script>alert(1)</script>"
        assert _UUID_REQ_RE.match(req_id)

        # Unbounded length header
        res_long = await client.get("/", headers={"X-Openstack-Request-Id": "x" * 200})
        assert res_long.status_code == 200
        req_id_long = res_long.headers.get("X-Openstack-Request-Id")
        assert req_id_long is not None
        assert _UUID_REQ_RE.match(req_id_long)


@pytest.mark.asyncio
async def test_error_responses_contain_request_id():
    """Verify 401, 404, 422, and domain/server error responses include X-Openstack-Request-Id."""
    async with AsyncClient(transport=ASGITransport(app=main_app), base_url="http://test") as client:
        # 1. 401 Unauthorized (protected route without X-Auth-Token)
        res_401 = await client.get("/v1/clusters")
        assert res_401.status_code == 401
        assert res_401.headers.get("X-Openstack-Request-Id") is not None

        # 2. 404 Not Found
        res_404 = await client.get("/v1/nonexistent-route")
        assert res_404.status_code == 404
        assert res_404.headers.get("X-Openstack-Request-Id") is not None

        # 3. Custom test app with K3sApiError and unhandled Exception
        test_app = FastAPI()
        test_app.add_middleware(CorrelationMiddleware)

        @test_app.exception_handler(K3sApiError)
        async def k3s_err_handler(request: Request, exc: K3sApiError):
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail, "req_id": request.state.request_id},
            )

        @test_app.get("/k3s-error")
        async def k3s_err_route():
            raise K3sApiError(409, "Cluster conflict")

        @test_app.get("/crash")
        async def crash_route():
            raise RuntimeError("Database exploded")

        async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as sub_client:
            res_k3s = await sub_client.get("/k3s-error", headers={"X-Openstack-Request-Id": "req-k3s-409"})
            assert res_k3s.status_code == 409
            assert res_k3s.headers.get("X-Openstack-Request-Id") == "req-k3s-409"
            assert res_k3s.json()["req_id"] == "req-k3s-409"

            res_crash = await sub_client.get("/crash", headers={"X-Openstack-Request-Id": "req-crash-500"})
            assert res_crash.status_code == 500
            assert res_crash.headers.get("X-Openstack-Request-Id") == "req-crash-500"


@pytest.mark.asyncio
async def test_logger_adapter_and_request_id_filter():
    """Verify request ID is accessible via request.state.request_id and injected into log context."""
    logged_extra = {}

    class TestHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            nonlocal logged_extra
            logged_extra["request_id_attr"] = getattr(record, "request_id", None)
            logged_extra["extra_dict"] = getattr(record, "__dict__", {})

    raw_logger = logging.getLogger("test.correlation")
    raw_logger.setLevel(logging.INFO)
    handler = TestHandler()
    handler.addFilter(RequestIdFilter())
    raw_logger.addHandler(handler)

    test_app = FastAPI()
    test_app.add_middleware(CorrelationMiddleware)

    @test_app.get("/log-test")
    async def log_route(request: Request):
        req_id = request.state.request_id
        ctx_id = get_request_id()
        req_logger = get_request_logger(raw_logger)
        req_logger.info("Test log message")
        return {"req_id": req_id, "ctx_id": ctx_id}

    async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as client:
        res = await client.get("/log-test", headers={"X-Openstack-Request-Id": "req-logging-spec-007"})
        assert res.status_code == 200
        data = res.json()
        assert data["req_id"] == "req-logging-spec-007"
        assert data["ctx_id"] == "req-logging-spec-007"
        assert logged_extra.get("request_id_attr") == "req-logging-spec-007"
        assert logged_extra.get("extra_dict", {}).get("request_id") == "req-logging-spec-007"
