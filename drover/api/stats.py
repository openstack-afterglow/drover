"""Drover stats API for cross-service aggregation."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import case, func, select

from drover.auth import require_token
from drover.db import get_session_factory, is_db_available
from drover.models.orm import K3sCluster
from drover.services import store

router = APIRouter()


@router.get("/clusters")
async def cluster_stats(token_info: dict = Depends(require_token)) -> dict:
    """Return cluster count stats for project."""
    project_id = token_info["project_id"]
    if is_db_available():
        factory = get_session_factory()
        async with factory() as session:
            stmt = select(
                func.count(K3sCluster.id),
                func.coalesce(func.sum(case((K3sCluster.status == "ACTIVE", 1), else_=0)), 0),
            ).where(K3sCluster.project_id == project_id, K3sCluster.deleted_at.is_(None))
            result = await session.execute(stmt)
            row = result.one_or_none()
            if row:
                return {"total": int(row[0] or 0), "active": int(row[1] or 0)}
    clusters = await store.list_clusters(project_id)
    total = len(clusters)
    active = sum(1 for c in clusters if c.get("status") == "ACTIVE")
    return {"total": total, "active": active}
