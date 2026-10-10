"""Correlation middleware and logging context utilities for Drover."""

from __future__ import annotations

import contextvars
import logging
import re
import time
import uuid
from collections.abc import MutableMapping
from typing import Any

from fastapi.responses import JSONResponse
from fastapi.routing import iter_route_contexts
from starlette.routing import compile_path

from drover.config import get_settings
from drover.rate_limit import is_ip_in_cidrs

_logger = logging.getLogger("drover.api")
_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})

_REQUEST_ID_RE = re.compile(r"^[a-zA-Z0-9_.-]{1,128}$")

# Only known, non-credential field *names* appear in diagnostics. Values are never rendered.
_SUMMARY_KEYS = frozenset({
    "attempt", "cluster_id", "include_deleted", "kind", "limit", "offset",
    "operation_id", "page", "request_id", "state", "status",
})


def safe_metadata(value: Any) -> str:
    """Summarize shape without converting untrusted values or secret field names to text."""
    if isinstance(value, MutableMapping):
        keys = sorted(key for key in _SUMMARY_KEYS if key in value)
        return f"mapping(count={min(len(value), 9999)},keys={','.join(keys)})"
    if isinstance(value, (list, tuple, set, frozenset)):
        return f"{type(value).__name__}(count={min(len(value), 9999)})"
    return "scalar"


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


class TrustedProxySchemeMiddleware:
    """Honor a single HTTP scheme from a trusted socket peer, without rewriting client IP.

    Disable Uvicorn proxy parsing so source-IP checks retain the original peer.
    The trusted proxy must overwrite incoming X-Forwarded-Proto.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http" and (peer := scope.get("client")):
            schemes = [value.strip() for name, value in scope.get("headers", [])
                       if name.lower() == b"x-forwarded-proto"]
            if (len(schemes) == 1 and schemes[0] in {b"http", b"https"}
                    and is_ip_in_cidrs(peer[0], get_settings().trusted_proxies)):
                scope["scheme"] = schemes[0].decode("ascii")
        await self.app(scope, receive, send)


class CorrelationMiddleware:
    """FastAPI/ASGI middleware that attaches or generates X-Openstack-Request-Id.

    Puts request_id on request.state.request_id and sets request_id_ctx ContextVar.
    Injects X-Openstack-Request-Id header into every HTTP response (normal or exception).
    """

    def __init__(self, app: Any) -> None:
        self.app = app
        # id(original route) -> (route, ((full template, compiled regex), ...)); the route reference
        # guards against id reuse. FastAPI's included routers put the router-local route in scope,
        # whose `path` lacks the include prefix (e.g. "" for GET /v1/clusters).
        self._route_templates: dict[int, tuple[Any, tuple[tuple[str, re.Pattern[str] | None], ...]]] = {}

    def _index_routes(self, app: Any) -> None:
        routes = getattr(app, "routes", None)
        if not routes:
            return
        grouped: dict[int, tuple[Any, list[tuple[str, re.Pattern[str] | None]]]] = {}
        for context in iter_route_contexts(routes):
            if context.path:
                original = context.original_route
                grouped.setdefault(id(original), (original, []))[1].append(
                    (context.path, compile_path(context.path)[0])
                )
        self._route_templates.update({key: (route, tuple(items)) for key, (route, items) in grouped.items()})

    def _route_template(self, scope: MutableMapping[str, Any]) -> str:
        """Return the static, prefixed route template; never the raw request path."""
        route = scope.get("route")
        if route is None:
            return "(unmatched)"
        entry = self._route_templates.get(id(route))
        if entry is None or entry[0] is not route:
            self._index_routes(scope.get("app"))
            entry = self._route_templates.get(id(route))
            if entry is None or entry[0] is not route:
                entry = (route, ((getattr(route, "path", "") or "(unmatched)", None),))
                self._route_templates[id(route)] = entry
        templates = entry[1]
        if len(templates) == 1:
            return templates[0][0]
        # The same route object included under several prefixes: pick the one this request matched.
        path = scope.get("path", "")
        for template, regex in templates:
            if regex is not None and regex.match(path):
                return template
        return "(ambiguous)"

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
        started = time.monotonic()
        response_started = False
        status = 500
        failed = False

        async def send_with_correlation(message: MutableMapping[str, Any]) -> None:
            nonlocal response_started, status
            if message["type"] == "http.response.start":
                status = message["status"]
                response_started = True
                res_headers = list(message.get("headers", []))
                has_header = any(k.lower() == b"x-openstack-request-id" for k, v in res_headers)
                if not has_header:
                    res_headers.append((b"x-openstack-request-id", req_id.encode("latin1")))
                    message["headers"] = res_headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_correlation)
        except Exception:
            failed = True
            if not response_started:
                res = JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
                res.headers["X-Openstack-Request-Id"] = req_id
                await res(scope, receive, send)
            else:
                raise
        finally:
            route_path = self._route_template(scope)
            method = scope.get("method", "")
            method = method if method in _METHODS else "OTHER"
            outcome = "error" if failed or status >= 400 else "success"
            _logger.info(
                "API completed method=%s route=%s status=%d outcome=%s duration_ms=%d request_id=%s",
                method, route_path, status, outcome, int((time.monotonic() - started) * 1000), req_id,
            )
            if _logger.isEnabledFor(logging.DEBUG):
                # Never inspect query values, request/response bodies, headers or path parameters.
                query_keys = {key.decode("ascii"): None for item in scope.get("query_string", b"")[:2048].split(b"&")
                              if (key := item.split(b"=", 1)[0]) in {k.encode("ascii") for k in _SUMMARY_KEYS}}
                _logger.debug("API metadata query=%s state=%s result=%s",
                              safe_metadata(query_keys), safe_metadata(state), safe_metadata({"status": status}))
            request_id_ctx.reset(token)
