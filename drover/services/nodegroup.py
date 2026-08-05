"""k3s 노드그룹 CRUD 서비스."""

import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from drover.db import get_session_factory, is_db_available
from drover.models.orm import K3sCluster, K3sNodegroup, K3sNodegroupVM

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------


def _ng_to_dict(ng: K3sNodegroup) -> dict:
    vms = ng.vms or []
    return {
        "id": ng.id,
        "cluster_id": ng.cluster_id,
        "name": ng.name,
        "role": ng.role,
        "node_count": ng.node_count,
        "flavor_id": ng.flavor_id,
        "image_id": ng.image_id,
        "labels": ng.labels or {},
        "taints": ng.taints or [],
        "is_default": bool(ng.is_default),
        # Stampede 오토스케일
        "stampede_enabled": bool(ng.stampede_enabled),
        "min_size": ng.min_size,
        "max_size": ng.max_size,
        "stampede_state": ng.stampede_state or {},
        "vms": [{"vm_id": v.vm_id, "name": v.name, "status": v.status} for v in vms],
        "created_at": ng.created_at.isoformat() if ng.created_at else None,
        "updated_at": ng.updated_at.isoformat() if ng.updated_at else None,
    }


def _validate_scalable_invariants(role: str, node_count: int, flavor_id: str | None, stampede_enabled: bool) -> None:
    if role == "server":
        raise ValueError("커스텀 server 노드그룹은 아직 지원되지 않습니다.")
    if stampede_enabled and not flavor_id:
        raise ValueError("Stampede 노드그룹은 flavor_id가 필요합니다.")


def _deterministic_default_id(cluster_id: str, name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"afterglow:k3s-nodegroup:{cluster_id}:{name}"))


# ---------------------------------------------------------------------------
# 조회
# ---------------------------------------------------------------------------


async def list_nodegroups(cluster_id: str) -> list[dict]:
    """클러스터의 노드그룹 목록 (삭제되지 않은 것)."""
    if not is_db_available():
        return []

    factory = get_session_factory()
    async with factory() as session:
        stmt = (
            select(K3sNodegroup)
            .where(K3sNodegroup.cluster_id == cluster_id, K3sNodegroup.deleted_at.is_(None))
            .order_by(K3sNodegroup.created_at.asc())
        )
        result = await session.execute(stmt)
        ngs = result.scalars().all()
        out = []
        for ng in ngs:
            await session.refresh(ng, ["vms"])
            out.append(_ng_to_dict(ng))
        return out


async def get_nodegroup(cluster_id: str, nodegroup_id: str) -> dict | None:
    """단일 노드그룹 조회."""
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sNodegroup).where(
            K3sNodegroup.id == nodegroup_id,
            K3sNodegroup.cluster_id == cluster_id,
            K3sNodegroup.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        ng = result.scalar_one_or_none()
        if ng is None:
            return None
        await session.refresh(ng, ["vms"])
        return _ng_to_dict(ng)


# ---------------------------------------------------------------------------
# 생성
# ---------------------------------------------------------------------------


async def create_default_nodegroups(
    session,
    *,
    cluster_id: str,
    server_flavor_id: str | None,
    server_image_id: str | None,
    agent_flavor_id: str | None,
    agent_image_id: str | None,
    agent_count: int,
) -> None:
    """신규 클러스터용 기본 server/agent 노드그룹을 같은 DB 트랜잭션에 추가한다."""
    existing = await session.execute(
        select(K3sNodegroup.name).where(
            K3sNodegroup.cluster_id == cluster_id,
            K3sNodegroup.name.in_(["default-server", "default-agent"]),
            K3sNodegroup.deleted_at.is_(None),
        )
    )
    names = set(existing.scalars().all())
    if "default-server" not in names:
        session.add(
            K3sNodegroup(
                id=_deterministic_default_id(cluster_id, "default-server"),
                cluster_id=cluster_id,
                name="default-server",
                role="server",
                node_count=1,
                flavor_id=server_flavor_id,
                image_id=server_image_id,
                is_default=True,
                min_size=1,
                max_size=1,
            )
        )
    if "default-agent" not in names:
        session.add(
            K3sNodegroup(
                id=_deterministic_default_id(cluster_id, "default-agent"),
                cluster_id=cluster_id,
                name="default-agent",
                role="agent",
                node_count=max(0, int(agent_count or 0)),
                flavor_id=agent_flavor_id,
                image_id=agent_image_id,
                is_default=True,
                min_size=0,
                max_size=max(5, int(agent_count or 0)),
            )
        )


async def create_nodegroup(cluster_id: str, data: dict) -> dict:
    """노드그룹 생성. DB 미설정 시 RuntimeError."""
    if not is_db_available():
        raise RuntimeError("DB가 설정되지 않아 노드그룹 기능을 사용할 수 없습니다.")

    factory = get_session_factory()
    async with factory() as session:
        # 클러스터 존재 확인
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id, K3sCluster.deleted_at.is_(None))
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster is None:
            raise ValueError(f"클러스터 {cluster_id}를 찾을 수 없습니다.")

        # 동일 이름 중복 검사
        dup_stmt = select(K3sNodegroup).where(
            K3sNodegroup.cluster_id == cluster_id,
            K3sNodegroup.name == data["name"],
            K3sNodegroup.deleted_at.is_(None),
        )
        dup_result = await session.execute(dup_stmt)
        if dup_result.scalar_one_or_none() is not None:
            raise ValueError(f"이미 같은 이름의 노드그룹이 존재합니다: {data['name']}")

        role = data.get("role", "agent")
        node_count = int(data.get("node_count", 0))
        flavor_id = data.get("flavor_id") or None
        stampede_enabled = bool(data.get("stampede_enabled", False))
        _validate_scalable_invariants(role, node_count, flavor_id, stampede_enabled)

        ng = K3sNodegroup(
            id=str(uuid.uuid4()),
            cluster_id=cluster_id,
            name=data["name"],
            role=role,
            node_count=node_count,
            flavor_id=flavor_id,
            image_id=data.get("image_id") or None,
            labels=data.get("labels") or None,
            taints=data.get("taints") or None,
            is_default=False,
            stampede_enabled=stampede_enabled,
            min_size=int(data.get("min_size", 0)),
            max_size=int(data.get("max_size", 5)),
        )
        session.add(ng)
        await session.commit()
        await session.refresh(ng, ["vms"])
        return _ng_to_dict(ng)


# ---------------------------------------------------------------------------
# 수정
# ---------------------------------------------------------------------------


async def update_nodegroup(cluster_id: str, nodegroup_id: str, updates: dict) -> dict | None:
    """노드그룹 부분 업데이트. 없으면 None."""
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sNodegroup).where(
            K3sNodegroup.id == nodegroup_id,
            K3sNodegroup.cluster_id == cluster_id,
            K3sNodegroup.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        ng = result.scalar_one_or_none()
        if ng is None:
            return None

        next_role = updates.get("role", ng.role)
        next_count = int(updates.get("node_count", ng.node_count))
        next_flavor = updates.get("flavor_id", ng.flavor_id)
        next_stampede = bool(updates.get("stampede_enabled", ng.stampede_enabled))
        _validate_scalable_invariants(next_role, next_count, next_flavor, next_stampede)

        _allowed = {
            "node_count",
            "flavor_id",
            "image_id",
            "labels",
            "taints",
            "stampede_enabled",
            "min_size",
            "max_size",
            "stampede_state",
        }
        for k, v in updates.items():
            if k in _allowed:
                setattr(ng, k, v)
        ng.updated_at = datetime.now(UTC)
        await session.commit()
        await session.refresh(ng, ["vms"])
        return _ng_to_dict(ng)


# ---------------------------------------------------------------------------
# 삭제 (soft-delete)
# ---------------------------------------------------------------------------


async def delete_nodegroup(cluster_id: str, nodegroup_id: str) -> bool:
    """노드그룹 soft-delete. 기본 그룹(is_default=True)은 삭제 불가."""
    if not is_db_available():
        return False

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sNodegroup).where(
            K3sNodegroup.id == nodegroup_id,
            K3sNodegroup.cluster_id == cluster_id,
            K3sNodegroup.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        ng = result.scalar_one_or_none()
        if ng is None:
            return False
        if ng.is_default:
            raise ValueError("기본 노드그룹(default-server / default-agent)은 삭제할 수 없습니다.")
        ng.deleted_at = datetime.now(UTC)
        ng.updated_at = datetime.now(UTC)
        await session.commit()
        return True


# ---------------------------------------------------------------------------
# VM 추적 헬퍼
# ---------------------------------------------------------------------------


async def add_nodegroup_vms(nodegroup_id: str, cluster_id: str, vm_entries: list[dict]) -> None:
    """노드그룹에 VM 레코드 추가."""
    if not is_db_available():
        return

    factory = get_session_factory()
    async with factory() as session:
        for entry in vm_entries:
            vm = K3sNodegroupVM(
                nodegroup_id=nodegroup_id,
                cluster_id=cluster_id,
                vm_id=entry["vm_id"],
                name=entry.get("name"),
                status="CREATING",
            )
            session.add(vm)
        await session.commit()


async def remove_nodegroup_vms(nodegroup_id: str, vm_ids: list[str]) -> None:
    """노드그룹에서 VM 레코드 제거."""
    if not is_db_available():
        return

    from sqlalchemy import delete

    factory = get_session_factory()
    async with factory() as session:
        await session.execute(
            delete(K3sNodegroupVM).where(
                K3sNodegroupVM.nodegroup_id == nodegroup_id,
                K3sNodegroupVM.vm_id.in_(vm_ids),
            )
        )
        await session.commit()


async def count_creating_vms(nodegroup_id: str) -> int:
    """노드그룹에서 status='CREATING'인 VM 수 반환 (in_flight 재조정용)."""
    if not is_db_available():
        return 0

    from sqlalchemy import func

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(func.count()).where(
            K3sNodegroupVM.nodegroup_id == nodegroup_id,
            K3sNodegroupVM.status == "CREATING",
        )
        result = await session.execute(stmt)
        return result.scalar_one() or 0


async def set_nodegroup_count(cluster_id: str, nodegroup_id: str, node_count: int) -> None:
    if not is_db_available():
        return
    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sNodegroup).where(
            K3sNodegroup.id == nodegroup_id,
            K3sNodegroup.cluster_id == cluster_id,
            K3sNodegroup.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        ng = result.scalar_one_or_none()
        if ng:
            ng.node_count = max(0, int(node_count))
            ng.updated_at = datetime.now(UTC)
            await session.commit()


async def get_default_agent_nodegroup_id(cluster_id: str) -> str | None:
    """클러스터의 default-agent 노드그룹 ID 반환. 없으면 None."""
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sNodegroup.id).where(
            K3sNodegroup.cluster_id == cluster_id,
            K3sNodegroup.name == "default-agent",
            K3sNodegroup.deleted_at.is_(None),
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
