"""Instance orchestration and placement helpers for Drover."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack

logger = logging.getLogger(__name__)


async def resolve_default_network(
    conn: openstack.connection.Connection,
    settings,
) -> str | None:
    """Resolve Drover's standalone default-network policy."""
    from drover.services.resource_policy_store import resolve_policies

    try:
        policies = await resolve_policies(conn=conn, keys=("k3s.default_network",))
        return policies.get("k3s.default_network")
    except Exception as e:
        logger.warning("Failed to resolve default network policy: %s", e)
        return None


