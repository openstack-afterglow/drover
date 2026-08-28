"""Correlation middleware and logging context utilities for Drover."""

from __future__ import annotations

import contextvars
import logging
import re
import uuid
from collections.abc import MutableMapping
from typing import Any

from fastapi.responses import JSONResponse

_REQUEST_ID_RE = re.compile(r"^[a-zA-Z0-9_.-]{1,128}$")

request_id_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar("drover_request_id", default=None)


def get_request_id() -> str | None:
    """Return the current correlation request ID from context if set."""
    return request_id_ctx.get()


def validate_request_id(raw_id: str | None) -> str:
    """Validate caller-supplied request ID or generate a fresh OpenStack-compliant one.

    Rejects malformed, non-ASCII, unbounded (>128 chars), or whitespace-containing IDs.
    """
    if raw_id:
        stripped = raw_id.strip()
        if _REQUEST_ID_RE.match(stripped):
            return stripped
    return f"req-{uuid.uuid4()}"


class RequestLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that automatically attaches request_id to log record extra dict."""

    def process(self, msg: Any, kwargs: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        extra = kwargs.get("extra", {})
        req_id = get_request_id()
        if req_id and "request_id" not in extra:
            extra["request_id"] = req_id
        kwargs["extra"] = extra
        return msg, kwargs


def get_request_logger(name_or_logger: str | logging.Logger) -> RequestLoggerAdapter:
    """Return a RequestLoggerAdapter wrapping the specified logger name or Logger instance."""
    if isinstance(name_or_logger, str):
        logger = logging.getLogger(name_or_logger)
    else:
        logger = name_or_logger
    return RequestLoggerAdapter(logger, {})


class RequestIdFilter(logging.Filter):
    """Logging filter that injects `request_id` into LogRecord objects."""

    def filter(self, record: logging.LogRecord) -> bool:
        req_id = get_request_id()
        if not hasattr(record, "request_id") or record.request_id is None:
            record.request_id = req_id or ""
        return True


class CorrelationMiddleware:
    """FastAPI/ASGI middleware that attaches or generates X-Openstack-Request-Id.

    Puts request_id on request.state.request_id and sets request_id_ctx ContextVar.
    Injects X-Openstack-Request-Id header into every HTTP response (normal or exception).
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        raw_req_id = None
        for k, v in scope.get("headers", []):
            k_lower = k.lower()
            if k_lower == b"x-openstack-request-id" or (k_lower == b"x-request-id" and raw_req_id is None):
                try:
                    raw_req_id = v.decode("latin1")
                except Exception:
                    raw_req_id = None
                if k_lower == b"x-openstack-request-id":
                    break

        req_id = validate_request_id(raw_req_id)
        state = scope.setdefault("state", {})
        state["request_id"] = req_id

        token = request_id_ctx.set(req_id)
        response_started = False

        async def send_with_correlation(message: MutableMapping[str, Any]) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                res_headers = list(message.get("headers", []))
                has_header = any(k.lower() == b"x-openstack-request-id" for k, v in res_headers)
                if not has_header:
                    res_headers.append((b"x-openstack-request-id", req_id.encode("latin1")))
                    message["headers"] = res_headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_correlation)
        except Exception as exc:
            if not response_started:
                res = JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
                res.headers["X-Openstack-Request-Id"] = req_id
                await res(scope, receive, send)
            else:
                raise exc
        finally:
            request_id_ctx.reset(token)
