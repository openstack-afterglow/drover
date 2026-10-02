"""Bounded lifecycle logging shared by the API and background worker."""

import logging
import os

from drover.middleware import (
    RequestIdFilter,
    RequestLoggerAdapter,
    get_request_id,
    get_request_logger,
    request_id_ctx,
    safe_metadata,
)


def configure_logging() -> None:
    """Enable Drover DEBUG explicitly; leave dependencies at their normal INFO level."""
    level = logging.DEBUG if os.environ.get("LOG_LEVEL", "").upper() == "DEBUG" else logging.INFO
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s [request_id=%(request_id)s]: %(message)s")
    logging.getLogger("drover").setLevel(level)
    for handler in logging.getLogger().handlers:
        if not any(isinstance(filter_, RequestIdFilter) for filter_ in handler.filters):
            handler.addFilter(RequestIdFilter())


__all__ = [
    "RequestIdFilter", "RequestLoggerAdapter", "configure_logging", "get_request_id",
    "get_request_logger", "request_id_ctx", "safe_metadata",
]
