"""Register Drover as an OpenStack SDK service."""

import os

from drover_sdk.service import DroverService

__version__ = "0.1.0"


def register(conn):
    """Enable catalog discovery and return the ``conn.drover`` proxy.

    Drover is registered in Keystone as ``container-infra`` at its ``/v1``
    base path, but exposed to callers through the stable ``drover`` alias.
    ``SERVICE_DROVER_INTERNAL_URL`` remains an emergency endpoint override for
    tests or catalog outages; it accepts either the service root or `/v1` base.
    """
    conn.config.enable_service("container-infra")
    if endpoint_override := os.environ.get("SERVICE_DROVER_INTERNAL_URL", "").strip().rstrip("/"):
        if not endpoint_override.endswith("/v1"):
            endpoint_override = f"{endpoint_override}/v1"
        conn.config.set_service_value("endpoint_override", "container-infra", endpoint_override)
        conn.config.set_service_value("api_version", "container-infra", "1")
    conn.add_service(DroverService())
    return conn.drover


__all__ = ["DroverService", "register"]
