"""Register Drover as an OpenStack SDK service."""

import os

from drover_sdk.service import DroverService

__version__ = "0.2.4"


def register(conn):
    """Enable Drover catalog discovery and return ``conn.drover``.

    Drover is registered in Keystone under the ``drover`` service type. The
    optional ``SERVICE_DROVER_INTERNAL_URL`` override may point either at the
    service root or at a versioned API base.
    """
    service_type = "drover"
    conn.config.enable_service(service_type)
    if endpoint_override := os.environ.get("SERVICE_DROVER_INTERNAL_URL", "").strip().rstrip("/"):
        conn.config.set_service_value("endpoint_override", service_type, endpoint_override)
        conn.config.set_service_value("api_version", service_type, "1")
    conn.add_service(DroverService())
    return conn.drover


__all__ = ["DroverService", "register"]
