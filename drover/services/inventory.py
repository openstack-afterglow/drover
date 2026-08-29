"""Inventory and ownership tracking service for ManagedOpenStackResource."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from drover.db import get_session_factory
from drover.models.orm import ManagedOpenStackResource

_logger = logging.getLogger("drover.inventory")

SUPPORTED_TAGGED_TYPES = frozenset({"server", "volume", "security_group", "floating_ip", "load_balancer"})


def _now() -> datetime:
    return datetime.now(UTC)


def build_drover_metadata(
    cluster_id: str, operation_id: str | None = None, resource_type: str = ""
) -> dict[str, str]:
    """Build immutable key-value metadata dict for Nova servers and Cinder volumes."""
    return {
        "drover.cluster_id": cluster_id,
        "drover.operation_id": operation_id or "",
        "drover.resource_type": resource_type,
        "drover.managed": "true",
    }


def build_drover_tags(
    cluster_id: str, operation_id: str | None = None, resource_type: str = ""
) -> list[str]:
    """Build tag strings for Neutron and Octavia top-level resources."""
    return [
        f"drover.cluster_id={cluster_id}",
        f"drover.operation_id={operation_id or ''}",
        f"drover.resource_type={resource_type}",
        "drover.managed=true",
    ]


async def record_resource(
    session_or_factory: AsyncSession | Any = None,
    *,
    cluster_id: str,
    service: str,
    resource_type: str,
    resource_id: str,
    operation_id: str | None = None,
    name: str | None = None,
    state: str | None = None,
    metadata: dict | list | None = None,
) -> ManagedOpenStackResource | None:
    """Immediately persist or update a ManagedOpenStackResource record."""
    if isinstance(session_or_factory, AsyncSession):
        return await _record_resource_impl(
            session_or_factory,
            cluster_id=cluster_id,
            service=service,
            resource_type=resource_type,
            resource_id=resource_id,
            operation_id=operation_id,
            name=name,
            state=state,
            metadata=metadata,
        )

    factory = get_session_factory()
    if factory is None:
        _logger.warning("Database unavailable, resource %s/%s not recorded in DB", service, resource_id)
        return None
    async with factory() as session, session.begin():
        return await _record_resource_impl(
            session,
            cluster_id=cluster_id,
            service=service,
            resource_type=resource_type,
            resource_id=resource_id,
            operation_id=operation_id,
            name=name,
            state=state,
            metadata=metadata,
        )


async def _record_resource_impl(
    session: AsyncSession,
    *,
    cluster_id: str,
    service: str,
    resource_type: str,
    resource_id: str,
    operation_id: str | None = None,
    name: str | None = None,
    state: str | None = None,
    metadata: dict | list | None = None,
) -> ManagedOpenStackResource:
    stmt = select(ManagedOpenStackResource).where(
        ManagedOpenStackResource.service == service,
        ManagedOpenStackResource.resource_type == resource_type,
        ManagedOpenStackResource.resource_id == resource_id,
    )
    res = await session.execute(stmt)
    existing = res.scalar_one_or_none()
    now = _now()
    if existing is not None:
        existing.cluster_id = cluster_id
        if operation_id:
            existing.operation_id = operation_id
        if name is not None:
            existing.name = name
        if state is not None:
            existing.state = state
        if metadata is not None:
            existing.metadata_json = metadata
        existing.last_seen_at = now
        existing.deleted_at = None
        return existing

    record = ManagedOpenStackResource(
        id=str(uuid.uuid4()),
        cluster_id=cluster_id,
        operation_id=operation_id,
        service=service,
        resource_type=resource_type,
        resource_id=resource_id,
        name=name,
        state=state,
        metadata_json=metadata,
        created_at=now,
        last_seen_at=now,
        deleted_at=None,
    )
    session.add(record)
    return record


async def mark_resource_deleted(
    session_or_factory: AsyncSession | Any = None,
    *,
    service: str,
    resource_type: str,
    resource_id: str,
) -> None:
    """Mark a managed resource record as deleted."""
    if isinstance(session_or_factory, AsyncSession):
        await _mark_deleted_impl(session_or_factory, service, resource_type, resource_id)
        return

    factory = get_session_factory()
    if factory is None:
        return
    async with factory() as session, session.begin():
        await _mark_deleted_impl(session, service, resource_type, resource_id)


async def _mark_deleted_impl(session: AsyncSession, service: str, resource_type: str, resource_id: str) -> None:
    stmt = select(ManagedOpenStackResource).where(
        ManagedOpenStackResource.service == service,
        ManagedOpenStackResource.resource_type == resource_type,
        ManagedOpenStackResource.resource_id == resource_id,
    )
    res = await session.execute(stmt)
    existing = res.scalar_one_or_none()
    if existing is not None:
        existing.deleted_at = _now()


async def list_managed_resources(
    session_or_factory: AsyncSession | Any = None,
    cluster_id: str = "",
    operation_id: str = "",
    active_only: bool = True,
) -> list[ManagedOpenStackResource]:
    """Retrieve managed resources for a cluster and/or operation."""
    stmt = select(ManagedOpenStackResource)
    if cluster_id:
        stmt = stmt.where(ManagedOpenStackResource.cluster_id == cluster_id)
    if operation_id:
        stmt = stmt.where(ManagedOpenStackResource.operation_id == operation_id)
    if active_only:
        stmt = stmt.where(ManagedOpenStackResource.deleted_at.is_(None))
    stmt = stmt.order_by(ManagedOpenStackResource.created_at.desc())

    if isinstance(session_or_factory, AsyncSession):
        res = await session_or_factory.execute(stmt)
        return list(res.scalars().all())

    factory = get_session_factory()
    if factory is None:
        return []
    async with factory() as session:
        res = await session.execute(stmt)
        return list(res.scalars().all())

def validate_resource_ownership(
    resource: Any,
    expected_project_id: str,
    expected_cluster_id: str,
    resource_type: str = "",
) -> bool:
    """Validate project ownership and Drover cluster/managed marker on an OpenStack resource."""
    if resource is None:
        return True

    # Project check if project_id attribute exists
    proj_id = None
    if isinstance(resource, dict):
        proj_id = resource.get("project_id") or resource.get("tenant_id")
    else:
        proj_id = getattr(resource, "project_id", None) or getattr(resource, "tenant_id", None)
        if callable(proj_id):
            proj_id = None

    if proj_id and isinstance(proj_id, str) and expected_project_id and proj_id != expected_project_id:
        _logger.warning(
            "Ownership check failed: resource project %s != expected project %s",
            proj_id,
            expected_project_id,
        )
        return False

    # Check tags or metadata where supported
    if resource_type in SUPPORTED_TAGGED_TYPES or not resource_type:
        meta = None
        tags = None
        name = ""
        description = ""
        if isinstance(resource, dict):
            meta = resource.get("metadata")
            tags = resource.get("tags")
            name = resource.get("name") or ""
            description = resource.get("description") or ""
        else:
            raw_meta = getattr(resource, "metadata", None)
            if isinstance(raw_meta, dict):
                meta = raw_meta
            raw_tags = getattr(resource, "tags", None)
            if isinstance(raw_tags, (list, tuple, set)):
                tags = raw_tags
            raw_name = getattr(resource, "name", None)
            if isinstance(raw_name, str):
                name = raw_name
            raw_desc = getattr(resource, "description", None)
            if isinstance(raw_desc, str):
                description = raw_desc

        # Metadata check (Nova VM, Cinder Volume)
        if isinstance(meta, dict) and meta:
            cluster_marker = meta.get("drover.cluster_id") or meta.get("k3s_horse_generator_cluster_id")
            is_managed = meta.get("drover.managed") == "true" or meta.get("k3s_horse_generator_role") is not None
            if cluster_marker or is_managed:
                if cluster_marker and cluster_marker != expected_cluster_id:
                    _logger.warning(
                        "Ownership check failed: resource cluster_id %s != expected %s",
                        cluster_marker,
                        expected_cluster_id,
                    )
                    return False
                return True

        # Tags check (Neutron, Octavia)
        if isinstance(tags, (list, tuple, set)) and tags:
            tag_marker = f"drover.cluster_id={expected_cluster_id}"
            has_tag = any(isinstance(t, str) and (t == tag_marker or t == "drover.managed=true") for t in tags)
            if has_tag:
                return True
            wrong_cluster_tag = any(isinstance(t, str) and t.startswith("drover.cluster_id=") and t != tag_marker for t in tags)
            if wrong_cluster_tag:
                _logger.warning("Ownership check failed: resource tags %s lack %s", tags, tag_marker)
                return False

        # Name / Description fallback check
        if expected_cluster_id and len(expected_cluster_id) >= 8:
            short_id = expected_cluster_id[:8]
            if (name and short_id in name) or (description and short_id in description):
                return True

    return True
