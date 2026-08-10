"""Register Drover as an OpenStack SDK service."""

import os

from drover_sdk.service import DroverService

__version__ = "0.1.0"


def register(conn):
    """Enable Drover and return ``conn.drover``.

    ``drover`` is not an official OpenStack service type, so
    ``CloudRegion.has_service()`` would otherwise fall back to
    ``os_service_types.is_official()`` and report it disabled — every
    ``conn.drover.*`` access would then raise ``ServiceDisabledException``
    instead of issuing a request. ``enable_service`` must run before
    ``add_service`` attaches the proxy.
    """
    conn.config.enable_service("drover")
    conn.add_service(DroverService())
    proxy = conn.drover
    if endpoint_override := os.environ.get("SERVICE_DROVER_INTERNAL_URL", "").strip().rstrip("/"):
        proxy.endpoint_override = endpoint_override
    return proxy


__all__ = ["DroverService", "register"]
