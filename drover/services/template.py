"""k3s 클러스터 템플릿 CRUD 서비스 (DB 전용, Redis fallback 없음)."""

import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from drover.db import get_session_factory, is_db_available
from drover.models.orm import K3sClusterTemplate

_logger = logging.getLogger(__name__)


def _template_to_dict(t: K3sClusterTemplate) -> dict:
    return {
        "id": t.id,
        "name": t.name,
        "description": t.description,
        "k3s_version": t.k3s_version,
        "default_node_count": t.default_node_count,
        "default_agent_flavor_id": t.default_agent_flavor_id,
        "default_image_id": t.default_image_id,
        "plugins_enabled": t.plugins_enabled or {},
        "os_type": t.os_type,
        "public_visible": t.public_visible,
        "created_by": t.created_by,
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "updated_at": t.updated_at.isoformat() if t.updated_at else None,
    }


async def create_template(data: dict, created_by: str) -> dict:
    if not is_db_available():
        raise RuntimeError("DB가 설정되어 있지 않습니다. 템플릿 기능을 사용하려면 DB를 설정하세요.")

    factory = get_session_factory()
    async with factory() as session:
        t = K3sClusterTemplate(
            id=str(uuid.uuid4()),
            name=data["name"],
            description=data.get("description"),
            k3s_version=data.get("k3s_version"),
            default_node_count=int(data.get("default_node_count") or 1),
            default_agent_flavor_id=data.get("default_agent_flavor_id"),
            default_image_id=data.get("default_image_id"),
            plugins_enabled=data.get("plugins_enabled"),
            os_type=data.get("os_type") or "ubuntu",
            public_visible=bool(data.get("public_visible", True)),
            created_by=created_by,
        )
        session.add(t)
        await session.commit()
        await session.refresh(t)
        return _template_to_dict(t)


async def get_template(template_id: str) -> dict | None:
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sClusterTemplate).where(
            K3sClusterTemplate.id == template_id,
            K3sClusterTemplate.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        t = result.scalar_one_or_none()
        return _template_to_dict(t) if t else None


async def list_templates(user_id: str | None = None, admin: bool = False) -> list[dict]:
    """템플릿 목록.
    - admin=True: 전체 목록 (deleted 제외)
    - admin=False: public_visible=True 또는 본인 생성
    """
    if not is_db_available():
        return []

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sClusterTemplate).where(K3sClusterTemplate.deleted_at.is_(None))
        if not admin:
            from sqlalchemy import or_

            conditions = [K3sClusterTemplate.public_visible.is_(True)]
            if user_id:
                conditions.append(K3sClusterTemplate.created_by == user_id)
            stmt = stmt.where(or_(*conditions))
        stmt = stmt.order_by(K3sClusterTemplate.created_at.desc())
        result = await session.execute(stmt)
        return [_template_to_dict(t) for t in result.scalars().all()]


async def update_template(template_id: str, updates: dict) -> dict | None:
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sClusterTemplate).where(
            K3sClusterTemplate.id == template_id,
            K3sClusterTemplate.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        t = result.scalar_one_or_none()
        if t is None:
            return None

        for key, val in updates.items():
            if val is not None and hasattr(t, key):
                setattr(t, key, val)
        t.updated_at = datetime.now(UTC)
        await session.commit()
        await session.refresh(t)
        return _template_to_dict(t)


async def delete_template(template_id: str) -> bool:
    """Soft delete."""
    if not is_db_available():
        return False

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sClusterTemplate).where(
            K3sClusterTemplate.id == template_id,
            K3sClusterTemplate.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        t = result.scalar_one_or_none()
        if t is None:
            return False
        t.deleted_at = datetime.now(UTC)
        await session.commit()
        return True
