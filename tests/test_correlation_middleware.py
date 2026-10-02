"""Focused tests for Drover X-Openstack-Request-Id correlation middleware and logging context."""

import logging
import re

import pytest
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from drover.logging import configure_logging, safe_metadata
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


@pytest.mark.asyncio
async def test_api_completion_logs_outcomes_without_secret_query_path_or_response(caplog):
    test_app = FastAPI()
    test_app.add_middleware(CorrelationMiddleware)

    @test_app.get("/items/{item_id}")
    async def item(item_id: str, request: Request):
        request.state.kubeconfig = "KUBECONFIG_PRIVATE"
        if item_id == "denied":
            return JSONResponse(status_code=403, content={"password": "RESPONSE_SECRET"})
        if item_id == "crash":
            raise RuntimeError("KEYSTONE_PRIVATE")
        return {"token": "RESPONSE_SECRET"}

    logger = logging.getLogger("drover.api")
    with caplog.at_level(logging.DEBUG, logger="drover.api"):
        async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as client:
            for item_id, status in (("PATH_SECRET", 200), ("denied", 403), ("crash", 500)):
                res = await client.get(
                    f"/items/{item_id}?limit=2&token=QUERY_SECRET&password=OTHER_SECRET",
                    headers={"X-Openstack-Request-Id": "req-safe-log"},
                )
                assert res.status_code == status

    records = [record.getMessage() for record in caplog.records if record.name == logger.name]
    assert len([message for message in records if message.startswith("API completed")]) == 3
    assert all("route=/items/{item_id}" in message for message in records if message.startswith("API completed"))
    assert any("status=200 outcome=success" in message for message in records)
    assert any("status=403 outcome=error" in message for message in records)
    assert any("status=500 outcome=error" in message for message in records)
    assert all("query=mapping(count=1,keys=limit)" in message for message in records if message.startswith("API metadata"))
    text = "\n".join(records)
    for secret in ("QUERY_SECRET", "OTHER_SECRET", "RESPONSE_SECRET", "KUBECONFIG_PRIVATE", "KEYSTONE_PRIVATE", "PATH_SECRET", "password", "token"):
        assert secret not in text


@pytest.mark.asyncio
async def test_api_completion_logs_prefixed_template_for_included_routers(caplog):
    # FastAPI included routers expose the router-local route (path "" or "/{item_id}") in scope.
    router = APIRouter()

    @router.get("")
    async def list_items():
        return []

    @router.get("/{item_id}")
    async def get_item(item_id: str):
        return {}

    test_app = FastAPI()
    test_app.add_middleware(CorrelationMiddleware)
    test_app.include_router(router, prefix="/v1/items")
    test_app.include_router(router, prefix="/v1/admin/items")

    with caplog.at_level(logging.INFO, logger="drover.api"):
        async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as client:
            for path in ("/v1/items", "/v1/items/PATH_SECRET", "/v1/admin/items/PATH_SECRET"):
                assert (await client.get(path)).status_code == 200

    routes = [
        record.getMessage().split(" route=", 1)[1].split(" ", 1)[0]
        for record in caplog.records
        if record.name == "drover.api" and record.getMessage().startswith("API completed")
    ]
    assert routes == ["/v1/items", "/v1/items/{item_id}", "/v1/admin/items/{item_id}"]



def test_metadata_does_not_render_credential_bearing_objects():
    class Credential:
        def __str__(self):
            raise AssertionError("credential value must not be stringified")

    summary = safe_metadata({"status": "ok", "token": Credential(), "kubeconfig": Credential(), "identity": Credential()})
    assert summary == "mapping(count=4,keys=status)"
    assert safe_metadata([Credential()] * 10001) == "list(count=9999)"


def test_debug_opt_in_does_not_enable_dependency_debug(monkeypatch):
    drover_logger = logging.getLogger("drover")
    original_level = drover_logger.level
    try:
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        configure_logging()
        assert drover_logger.level == logging.INFO
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        configure_logging()
        assert drover_logger.level == logging.DEBUG
        assert not logging.getLogger("httpx").isEnabledFor(logging.DEBUG)
    finally:
        drover_logger.setLevel(original_level)
