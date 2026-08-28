"""k3s 클러스터 DB CRUD.

DB가 초기화되어 있으면 MariaDB를 사용하고,
미설정 시 기존 Redis 방식(k3s_cluster.py)으로 폴백.
"""

import logging
from datetime import UTC, datetime

from sqlalchemy import delete, select

from drover.crypto import decrypt_kubeconfig, decrypt_node_token, encrypt_kubeconfig, encrypt_node_token
from drover.db import get_session_factory, is_db_available
from drover.models.orm import K3sAgentVM, K3sCluster

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------


def _cluster_to_dict(cluster: K3sCluster) -> dict:
    """ORM 모델 → API 응답 호환 dict 변환."""
    agent_vms = cluster.agent_vms or []
    return {
        "id": cluster.id,
        "project_id": cluster.project_id,
        "name": cluster.name,
        "status": cluster.status,
        "status_reason": cluster.status_reason,
        "server_vm_id": cluster.server_vm_id,
        "agent_vm_ids": [v.vm_id for v in agent_vms],
        "agent_count": cluster.agent_count,
        "server_flavor_id": cluster.server_flavor_id,
        "agent_flavor_id": cluster.agent_flavor_id,
        "server_image_id": cluster.server_image_id,
        "network_id": cluster.network_id,
        "security_group_id": cluster.security_group_id,
        "server_ip": cluster.server_ip,
        "api_address": cluster.api_address,
        "key_name": cluster.key_name,
        "ssh_public_key": cluster.ssh_public_key,
        "k3s_version": cluster.k3s_version,
        "created_by_user_id": cluster.created_by_user_id,
        "created_by_username": cluster.created_by_username,
        "created_at": cluster.created_at.isoformat() if cluster.created_at else None,
        "updated_at": cluster.updated_at.isoformat() if cluster.updated_at else None,
        "deleted_at": cluster.deleted_at.isoformat() if cluster.deleted_at else None,
        "deleted_by_user_id": cluster.deleted_by_user_id,
        "deleted_reason": cluster.deleted_reason,
        "occm_enabled": cluster.occm_enabled,
        "plugins_enabled": cluster.plugins_enabled or {},
        "plugin_status": cluster.plugin_status or {},
        "secret_cloud_config_status": cluster.secret_cloud_config_status,
        "app_credential_id": cluster.app_credential_id or "",
        "api_lb_id": cluster.api_lb_id or "",
        "api_lb_pool_id": cluster.api_lb_pool_id or "",
        "api_fip_id": cluster.api_fip_id or "",
        "api_fip_address": cluster.api_fip_address or "",
        "os_type": cluster.os_type or "ubuntu",
        "template_id": cluster.template_id or None,
        "template_snapshot": cluster.template_snapshot or None,
        "resource_policy_snapshot": cluster.resource_policy_snapshot or None,
        "master_count": cluster.master_count if hasattr(cluster, "master_count") else 1,
        "stampede_enabled": bool(cluster.stampede_enabled) if hasattr(cluster, "stampede_enabled") else False,
        "last_reconciled_at": cluster.last_reconciled_at.isoformat() if getattr(cluster, "last_reconciled_at", None) else None,
        "drift_status": cluster.drift_status if getattr(cluster, "drift_status", None) is not None else None,
    }


# ---------------------------------------------------------------------------
# Cluster CRUD
# ---------------------------------------------------------------------------


async def create_cluster_record(project_id: str, cluster_id: str, data: dict) -> None:
    """클러스터 생성. DB 미설정 시 Redis 폴백."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.create_cluster_record(project_id, cluster_id, data)

    factory = get_session_factory()
    async with factory() as session:
        cluster = K3sCluster(
            id=cluster_id,
            project_id=project_id,
            name=data.get("name", ""),
            status=data.get("status", "CREATING"),
            status_reason=data.get("status_reason") or None,
            server_vm_id=data.get("server_vm_id") or None,
            server_flavor_id=data.get("server_flavor_id") or None,
            agent_flavor_id=data.get("agent_flavor_id") or None,
            server_image_id=data.get("server_image_id") or None,
            network_id=data.get("network_id") or None,
            security_group_id=data.get("security_group_id") or None,
            server_ip=data.get("server_ip") or None,
            api_address=data.get("api_address") or None,
            k3s_version=data.get("k3s_version") or None,
            key_name=data.get("key_name") or None,
            ssh_public_key=data.get("ssh_public_key") or None,
            agent_count=int(data.get("agent_count") or 0),
            occm_enabled=bool(data.get("occm_enabled", False)),
            plugins_enabled=data.get("plugins_enabled") or None,
            created_by_user_id=data.get("created_by_user_id") or None,
            created_by_username=data.get("created_by_username") or None,
            api_lb_id=data.get("api_lb_id") or None,
            api_lb_pool_id=data.get("api_lb_pool_id") or None,
            api_fip_id=data.get("api_fip_id") or None,
            api_fip_address=data.get("api_fip_address") or None,
            os_type=data.get("os_type") or "ubuntu",
            app_credential_id=data.get("app_credential_id") or None,
            template_id=data.get("template_id") or None,
            template_snapshot=data.get("template_snapshot") or None,
            resource_policy_snapshot=data.get("resource_policy_snapshot") or None,
            master_count=int(data.get("master_count") or 1),
            stampede_enabled=bool(data.get("stampede_enabled", False)),
        )
        session.add(cluster)
        from drover.services import nodegroup as _nodegroups

        await _nodegroups.create_default_nodegroups(
            session,
            cluster_id=cluster_id,
            server_flavor_id=data.get("server_flavor_id") or None,
            server_image_id=data.get("server_image_id") or None,
            agent_flavor_id=data.get("agent_flavor_id") or None,
            agent_image_id=data.get("default_agent_image_id") or data.get("server_image_id") or None,
            agent_count=int(data.get("agent_count") or 0),
        )
        await session.commit()


async def get_cluster(project_id: str, cluster_id: str) -> dict | None:
    """단일 클러스터 조회. 없으면 None."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.get_cluster(project_id, cluster_id)

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id, K3sCluster.project_id == project_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster is None:
            return None
        # agent_vms lazy load
        await session.refresh(cluster, ["agent_vms"])
        return _cluster_to_dict(cluster)


async def get_cluster_admin(cluster_id: str) -> dict | None:
    """관리자용 단일 클러스터 조회 (project_id 필터 없음)."""
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster is None:
            return None
        await session.refresh(cluster, ["agent_vms"])
        d = _cluster_to_dict(cluster)
        d["project_id"] = cluster.project_id
        return d


async def list_clusters(project_id: str, include_deleted: bool = False) -> list[dict]:
    """프로젝트의 클러스터 목록 (최신순). include_deleted=False 이면 deleted_at IS NULL 필터."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.list_clusters(project_id)

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.project_id == project_id)
        if not include_deleted:
            stmt = stmt.where(K3sCluster.deleted_at.is_(None))
        stmt = stmt.order_by(K3sCluster.created_at.desc())
        result = await session.execute(stmt)
        clusters = result.scalars().all()
        out = []
        for c in clusters:
            await session.refresh(c, ["agent_vms"])
            out.append(_cluster_to_dict(c))
        return out


async def list_all_clusters(include_deleted: bool = False) -> list[dict]:
    """전체 프로젝트의 클러스터 목록 (관리자용)."""
    from drover.services import redis_store as _redis

    if is_db_available():
        factory = get_session_factory()
        async with factory() as session:
            stmt = select(K3sCluster)
            if not include_deleted:
                stmt = stmt.where(K3sCluster.deleted_at.is_(None))
            stmt = stmt.order_by(K3sCluster.created_at.desc())
            result = await session.execute(stmt)
            clusters = result.scalars().all()
            if clusters:
                out = []
                for c in clusters:
                    await session.refresh(c, ["agent_vms"])
                    d = _cluster_to_dict(c)
                    d["project_id"] = c.project_id
                    out.append(d)
                return out

    # DB 미설정이거나 DB에 데이터 없으면 Redis fallback
    return await _redis.list_all_clusters()


async def update_cluster_status(
    project_id: str,
    cluster_id: str,
    status: str,
    status_reason: str | None = None,
    **extra_fields,
) -> None:
    """상태 업데이트 + 추가 필드 (server_ip, api_address, node_token 등)."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.update_cluster_status(project_id, cluster_id, status, status_reason, **extra_fields)

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id, K3sCluster.project_id == project_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster is None:
            _logger.warning("update_cluster_status: cluster %s not found", cluster_id)
            return

        cluster.status = status
        cluster.updated_at = datetime.now(UTC)
        if status_reason is not None:
            cluster.status_reason = status_reason

        # 허용된 컬럼 매핑
        _column_map = {
            "server_ip",
            "api_address",
            "k3s_version",
            "node_token",
            "server_vm_id",
            "network_id",
            "security_group_id",
            "server_flavor_id",
            "agent_flavor_id",
            "key_name",
            "ssh_public_key",
            "api_lb_id",
            "api_lb_pool_id",
            "api_fip_id",
            "api_fip_address",
            "plugin_status",
            "secret_cloud_config_status",
            "app_credential_id",
            "master_count",
        }
        for k, v in extra_fields.items():
            if k in _column_map:
                if k == "node_token" and v:
                    setattr(cluster, k, encrypt_node_token(v))
                elif k in ("plugin_status", "secret_cloud_config_status"):
                    # None이면 기존 값 보존
                    if v is not None:
                        setattr(cluster, k, v)
                else:
                    setattr(cluster, k, v if v else None)
            # agent_vm_ids는 add_agent_vms()로 별도 처리

        await session.commit()
async def update_cluster_reconciliation(
    cluster_id: str,
    last_reconciled_at: datetime,
    drift_status: dict,
    status: str | None = None,
    status_reason: str | None = None,
) -> None:
    """Update cluster reconciliation timestamp, drift_status, and optional status/status_reason."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        if hasattr(_redis, "update_cluster_reconciliation"):
            return await _redis.update_cluster_reconciliation(
                cluster_id, last_reconciled_at, drift_status, status, status_reason
            )
        return

    factory = get_session_factory()
    if factory is None:
        return

    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster is not None:
            cluster.last_reconciled_at = last_reconciled_at
            cluster.drift_status = drift_status
            if status is not None:
                cluster.status = status
            if status_reason is not None:
                cluster.status_reason = status_reason
            cluster.updated_at = datetime.now(UTC)
            await session.commit()


async def delete_cluster_record(
    project_id: str,
    cluster_id: str,
    *,
    user_id: str | None = None,
    reason: str | None = None,
) -> None:
    """클러스터 soft-delete — status='DELETED' + deleted_at 기록. agent_vms 는 보존."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.delete_cluster_record(project_id, cluster_id)

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id, K3sCluster.project_id == project_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster is None:
            return
        cluster.status = "DELETED"
        cluster.deleted_at = datetime.now(UTC)
        cluster.deleted_by_user_id = user_id
        cluster.deleted_reason = reason
        cluster.updated_at = datetime.now(UTC)
        await session.commit()


# ---------------------------------------------------------------------------
# 에이전트 VM
# ---------------------------------------------------------------------------


async def add_agent_vms(cluster_id: str, vm_entries: list[dict]) -> None:
    """에이전트 VM 레코드 추가. vm_entries: [{"vm_id": ..., "name": ...}, ...]"""
    if not is_db_available():
        # Redis 폴백: agent_vm_ids를 기존 방식으로 업데이트
        # cluster의 project_id를 가져와야 하므로 Redis에서 직접 처리
        return

    factory = get_session_factory()
    async with factory() as session:
        for entry in vm_entries:
            vm = K3sAgentVM(
                cluster_id=cluster_id,
                vm_id=entry["vm_id"],
                name=entry.get("name"),
                status="CREATING",
            )
            session.add(vm)
        await session.commit()


async def remove_agent_vms(cluster_id: str, vm_ids: list[str]) -> None:
    """지정된 에이전트 VM 레코드 제거."""
    if not is_db_available():
        return

    factory = get_session_factory()
    async with factory() as session:
        await session.execute(
            delete(K3sAgentVM).where(
                K3sAgentVM.cluster_id == cluster_id,
                K3sAgentVM.vm_id.in_(vm_ids),
            )
        )
        await session.commit()


async def get_agent_vm_names(cluster_id: str, vm_ids: list[str]) -> dict[str, str | None]:
    """vm_id → name 매핑 반환. DB 미설정 시 빈 dict 반환."""
    if not is_db_available():
        return {}

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sAgentVM).where(
            K3sAgentVM.cluster_id == cluster_id,
            K3sAgentVM.vm_id.in_(vm_ids),
        )
        result = await session.execute(stmt)
        return {row.vm_id: row.name for row in result.scalars()}


async def update_agent_count(project_id: str, cluster_id: str, agent_count: int) -> None:
    """클러스터의 agent_count 필드 업데이트."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.update_cluster_status(project_id, cluster_id, None, agent_count=agent_count)

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id, K3sCluster.project_id == project_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster:
            cluster.agent_count = agent_count
            cluster.updated_at = datetime.now(UTC)
            await session.commit()


async def get_cluster_node_token(project_id: str, cluster_id: str) -> str | None:
    """내부용: node_token 조회 (API 응답에는 포함하지 않음)."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        c = await _redis.get_cluster(project_id, cluster_id)
        return c.get("node_token") if c else None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster.node_token).where(K3sCluster.id == cluster_id, K3sCluster.project_id == project_id)
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if not row:
            return None
        try:
            return decrypt_node_token(row)
        except Exception:
            # 암호화 전 평문 토큰이 저장된 레거시 레코드 — 그대로 반환
            # TODO: 마이그레이션 후 이 분기를 제거할 것
            _logger.warning(
                "cluster %s: node_token 복호화 실패 — 평문 레거시 레코드로 간주합니다. 재프로비저닝을 권장합니다.",
                cluster_id,
            )
            return row


async def update_agent_vm_status(cluster_id: str, vm_id: str, status: str) -> None:
    """에이전트 VM 상태 업데이트."""
    if not is_db_available():
        return

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sAgentVM).where(
            K3sAgentVM.cluster_id == cluster_id,
            K3sAgentVM.vm_id == vm_id,
        )
        result = await session.execute(stmt)
        vm = result.scalar_one_or_none()
        if vm:
            vm.status = status
            await session.commit()


# ---------------------------------------------------------------------------
# Kubeconfig
# ---------------------------------------------------------------------------


async def store_kubeconfig(project_id: str, cluster_id: str, kubeconfig_yaml: str) -> None:
    """kubeconfig를 AES-256-GCM 암호화하여 저장."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.store_kubeconfig(project_id, cluster_id, kubeconfig_yaml)

    encrypted = encrypt_kubeconfig(kubeconfig_yaml)
    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id, K3sCluster.project_id == project_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster:
            cluster.kubeconfig_encrypted = encrypted
            cluster.updated_at = datetime.now(UTC)
            await session.commit()


async def get_kubeconfig(project_id: str, cluster_id: str) -> str | None:
    """암호화된 kubeconfig를 복호화하여 반환. 없으면 None."""
    if not is_db_available():
        from drover.services import redis_store as _redis

        return await _redis.get_kubeconfig(project_id, cluster_id)

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster.kubeconfig_encrypted).where(
            K3sCluster.id == cluster_id, K3sCluster.project_id == project_id
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if not row:
            return None
        return decrypt_kubeconfig(row)


async def get_kubeconfig_admin(cluster_id: str) -> str | None:
    """관리자용 kubeconfig 조회 (project_id 필터 없음)."""
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster.kubeconfig_encrypted).where(K3sCluster.id == cluster_id)
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if not row:
            return None
        return decrypt_kubeconfig(row)


# ---------------------------------------------------------------------------
# 콜백 토큰 — Redis 유지 (TTL 일회성, DB 불필요)
# ---------------------------------------------------------------------------


async def create_callback_token(project_id: str, cluster_id: str) -> str:
    from drover.services import redis_store as _redis

    return await _redis.create_callback_token(project_id, cluster_id)


async def consume_callback_token(token: str) -> dict | None:
    from drover.services import redis_store as _redis

    return await _redis.consume_callback_token(token)


# ---------------------------------------------------------------------------
# 스테일 클러스터 정리
# ---------------------------------------------------------------------------


async def check_stale_clusters(timeout_minutes: int = 30) -> None:
    """Scan and recover expired WAITING_CALLBACK operations and their clusters."""
    from drover.services import operations

    await operations.recover_expired_callback_operations(timeout_seconds=timeout_minutes * 60)

# ---------------------------------------------------------------------------
# Project Manager Credentials (Octavia Ingress App Cred 관리용)
# ---------------------------------------------------------------------------


async def get_manager_credentials(project_id: str) -> dict | None:
    """프로젝트 관리 사용자 자격 조회. 없으면 None."""
    if not is_db_available():
        return None

    from sqlalchemy import text

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(
            text(
                "SELECT user_id, username, encrypted_password FROM project_manager_credentials WHERE project_id = :pid"
            ),
            {"pid": project_id},
        )
        row = result.mappings().one_or_none()
        if row is None:
            return None
        return {"user_id": row["user_id"], "username": row["username"], "encrypted_password": row["encrypted_password"]}


async def save_manager_credentials(project_id: str, user_id: str, username: str, encrypted_password: str) -> None:
    """프로젝트 관리 사용자 자격 저장 (INSERT OR REPLACE)."""
    if not is_db_available():
        return

    from sqlalchemy import text

    factory = get_session_factory()
    async with factory() as session:
        await session.execute(
            text(
                "INSERT INTO project_manager_credentials "
                "(project_id, user_id, username, encrypted_password) "
                "VALUES (:pid, :uid, :uname, :epw) "
                "ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), "
                "username = VALUES(username), encrypted_password = VALUES(encrypted_password)"
            ),
            {"pid": project_id, "uid": user_id, "uname": username, "epw": encrypted_password},
        )
        await session.commit()


async def get_cluster_app_credential_id(project_id: str, cluster_id: str) -> str | None:
    """클러스터의 app_credential_id 조회."""
    if not is_db_available():
        return None

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster.app_credential_id).where(
            K3sCluster.id == cluster_id, K3sCluster.project_id == project_id
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# HA 콜백 토큰 / 조인 카운터 — Redis 전담 (k3s_cluster.py 위임)
# ---------------------------------------------------------------------------


async def create_ha_callback_token(project_id: str, cluster_id: str, server_index: int) -> str:
    from drover.services import redis_store as _redis

    return await _redis.create_ha_callback_token(project_id, cluster_id, server_index)


async def consume_ha_callback_token(token: str) -> dict | None:
    from drover.services import redis_store as _redis

    return await _redis.consume_ha_callback_token(token)


async def get_ha_join_count(cluster_id: str) -> int:
    from drover.services import redis_store as _redis

    return await _redis.get_ha_join_count(cluster_id)


async def incr_ha_join_count(cluster_id: str) -> int:
    from drover.services import redis_store as _redis

    return await _redis.incr_ha_join_count(cluster_id)


async def record_rotation(cluster_id: str, initiated_by: str) -> None:
    """인증서 회전 완료 기록 — last_rotation_at, last_rotation_initiated_by 갱신."""
    if not is_db_available():
        _logger.warning("record_rotation: DB 미설정 — 회전 기록 스킵 (cluster=%s)", cluster_id)
        return

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(K3sCluster).where(K3sCluster.id == cluster_id)
        result = await session.execute(stmt)
        cluster = result.scalar_one_or_none()
        if cluster is None:
            _logger.warning("record_rotation: cluster %s not found", cluster_id)
            return
        cluster.last_rotation_at = datetime.now(UTC)
        cluster.last_rotation_initiated_by = initiated_by
        cluster.updated_at = datetime.now(UTC)
        await session.commit()
