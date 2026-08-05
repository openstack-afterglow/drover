"""Drover activity recording & event tracking helper."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from drover.cache import _get_client

_logger = logging.getLogger("drover.activity")


async def record(
    project_id: str,
    user_id: str,
    username: str,
    resource_type: str,
    action: str,
    status: str = "success",
    resource_id: str | None = None,
    resource_name: str | None = None,
    error_message: str | None = None,
    extra: dict | None = None,
) -> None:
    """Best-effort activity log via Python logger.

    Also records k3s_stampede events to Redis capped list for stampede event API.
    """
    _logger.info(
        "ACTIVITY: [%s] user=%s(%s) project=%s action=%s type=%s id=%s name=%s error=%s extra=%s",
        status,
        username,
        user_id,
        project_id,
        action,
        resource_type,
        resource_id,
        resource_name,
        error_message,
        extra,
    )
    if resource_type == "k3s_stampede" and resource_id:
        try:
            client = _get_client()
            key = f"drover:stampede:{resource_id}:events"
            event_obj = {
                "id": f"evt-{int(time.time()*1000)}",
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "action": action,
                "status": status,
                "nodegroup_id": resource_name,
                "extra": extra or {},
            }
            await client.lpush(key, json.dumps(event_obj))
            await client.ltrim(key, 0, 199)
        except Exception as e:
            _logger.debug("Failed to store stampede event in Redis: %s", e)


async def rec(
    token_info: dict,
    conn: Any | None,
    *,
    resource_type: str,
    action: str,
    status: str = "success",
    resource_id: str | None = None,
    resource_name: str | None = None,
    error_message: str | None = None,
    extra: dict | None = None,
) -> None:
    project_id = token_info.get("project_id") or (getattr(conn, "_afterglow_project_id", "") if conn else "")
    user_id = token_info.get("user_id") or (getattr(conn, "_afterglow_user_id", "") if conn else "")
    username = token_info.get("username", "")
    if not project_id or not user_id:
        return
    await record(
        project_id=project_id,
        user_id=user_id,
        username=username,
        resource_type=resource_type,
        action=action,
        status=status,
        resource_id=resource_id,
        resource_name=resource_name,
        error_message=error_message,
        extra=extra,
    )


async def list_stampede_events(cluster_id: str, limit: int = 50) -> list[dict]:
    """Retrieve recent stampede events from Redis."""
    try:
        client = _get_client()
        key = f"drover:stampede:{cluster_id}:events"
        items = await client.lrange(key, 0, limit - 1)
        res = []
        for item in items:
            raw = item.decode() if isinstance(item, bytes) else item
            res.append(json.loads(raw))
        return res
    except Exception as e:
        _logger.warning("Failed to read stampede events from Redis: %s", e)
        return []
