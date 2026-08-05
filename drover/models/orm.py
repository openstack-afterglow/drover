"""Drover SQLAlchemy ORM models."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    BOOLEAN,
    CHAR,
    INT,
    JSON,
    TEXT,
    VARCHAR,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from drover.db import Base


def _now() -> datetime:
    return datetime.now(UTC)


class K3sCluster(Base):
    __tablename__ = "k3s_clusters"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    project_id: Mapped[str] = mapped_column(VARCHAR(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(VARCHAR(63), nullable=False)
    status: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="CREATING")
    status_reason: Mapped[str | None] = mapped_column(TEXT)

    server_vm_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    server_flavor_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    agent_flavor_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    server_image_id: Mapped[str | None] = mapped_column(VARCHAR(128))
    network_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    security_group_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    api_lb_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    api_lb_pool_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    api_fip_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    api_fip_address: Mapped[str | None] = mapped_column(VARCHAR(45))

    server_ip: Mapped[str | None] = mapped_column(VARCHAR(45))
    api_address: Mapped[str | None] = mapped_column(VARCHAR(255))
    k3s_version: Mapped[str | None] = mapped_column(VARCHAR(32))
    node_token: Mapped[str | None] = mapped_column(VARCHAR(512))

    key_name: Mapped[str | None] = mapped_column(VARCHAR(255))
    ssh_public_key: Mapped[str | None] = mapped_column(TEXT)
    kubeconfig_encrypted: Mapped[str | None] = mapped_column(TEXT)

    created_by_user_id: Mapped[str | None] = mapped_column(VARCHAR(64), index=True)
    created_by_username: Mapped[str | None] = mapped_column(VARCHAR(255))

    agent_count: Mapped[int] = mapped_column(INT, nullable=False, default=0)
    occm_enabled: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, default=False)
    plugins_enabled: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    plugin_status: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    secret_cloud_config_status: Mapped[str | None] = mapped_column(VARCHAR(20), nullable=True)
    os_type: Mapped[str] = mapped_column(VARCHAR(10), nullable=False, default="ubuntu")
    app_credential_id: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)

    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    deleted_by_user_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    deleted_reason: Mapped[str | None] = mapped_column(VARCHAR(255))

    master_count: Mapped[int] = mapped_column(INT, nullable=False, default=1)
    template_id: Mapped[str | None] = mapped_column(CHAR(36), nullable=True)
    template_snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    resource_policy_snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    stampede_enabled: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, default=False)
    last_rotation_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_rotation_initiated_by: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True)

    agent_vms: Mapped[list[K3sAgentVM]] = relationship(
        "K3sAgentVM", back_populates="cluster", cascade="all, delete-orphan"
    )
    nodegroups: Mapped[list[K3sNodegroup]] = relationship(
        "K3sNodegroup", back_populates="cluster", cascade="all, delete-orphan"
    )

    __table_args__ = (Index("idx_k3s_cluster_project_created", "project_id", "created_at"),)


class K3sAgentVM(Base):
    __tablename__ = "k3s_agent_vms"

    id: Mapped[int] = mapped_column(INT, primary_key=True, autoincrement=True)
    cluster_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("k3s_clusters.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vm_id: Mapped[str] = mapped_column(VARCHAR(64), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(VARCHAR(255))
    status: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="CREATING")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)

    cluster: Mapped[K3sCluster] = relationship("K3sCluster", back_populates="agent_vms")


class K3sNodegroup(Base):
    __tablename__ = "k3s_nodegroups"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("k3s_clusters.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(VARCHAR(63), nullable=False)
    role: Mapped[str] = mapped_column(VARCHAR(10), nullable=False, default="agent")
    node_count: Mapped[int] = mapped_column(INT, nullable=False, default=0)
    flavor_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    image_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    labels: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    taints: Mapped[list | None] = mapped_column(JSON, nullable=True)
    is_default: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, default=False)

    stampede_enabled: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, default=False)
    min_size: Mapped[int] = mapped_column(INT, nullable=False, default=0)
    max_size: Mapped[int] = mapped_column(INT, nullable=False, default=5)
    stampede_state: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    cluster: Mapped[K3sCluster] = relationship("K3sCluster", back_populates="nodegroups")
    vms: Mapped[list[K3sNodegroupVM]] = relationship(
        "K3sNodegroupVM", back_populates="nodegroup", cascade="all, delete-orphan"
    )

    __table_args__ = (Index("idx_ng_cluster_role", "cluster_id", "role"),)


class K3sNodegroupVM(Base):
    __tablename__ = "k3s_nodegroup_vms"

    id: Mapped[int] = mapped_column(INT, primary_key=True, autoincrement=True)
    nodegroup_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("k3s_nodegroups.id", ondelete="CASCADE"), nullable=False, index=True
    )
    cluster_id: Mapped[str] = mapped_column(
        CHAR(36), ForeignKey("k3s_clusters.id", ondelete="CASCADE"), nullable=False, index=True
    )
    vm_id: Mapped[str] = mapped_column(VARCHAR(64), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(VARCHAR(255))
    status: Mapped[str] = mapped_column(VARCHAR(20), nullable=False, default="CREATING")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)

    nodegroup: Mapped[K3sNodegroup] = relationship("K3sNodegroup", back_populates="vms")


class K3sClusterTemplate(Base):
    __tablename__ = "k3s_cluster_templates"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    name: Mapped[str] = mapped_column(VARCHAR(63), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(TEXT)
    k3s_version: Mapped[str | None] = mapped_column(VARCHAR(32))
    default_node_count: Mapped[int] = mapped_column(INT, nullable=False, default=1)
    default_agent_flavor_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    default_image_id: Mapped[str | None] = mapped_column(VARCHAR(64))
    plugins_enabled: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    os_type: Mapped[str] = mapped_column(VARCHAR(10), nullable=False, default="ubuntu")
    public_visible: Mapped[bool] = mapped_column(BOOLEAN, nullable=False, default=True)
    created_by: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DroverJob(Base):
    __tablename__ = "drover_jobs"

    id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(CHAR(36), nullable=False, index=True)
    project_id: Mapped[str] = mapped_column(VARCHAR(64), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(VARCHAR(32), nullable=False)
    status: Mapped[str] = mapped_column(VARCHAR(16), nullable=False, default="queued")
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    attempts: Mapped[int] = mapped_column(INT, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(TEXT, nullable=True)
    user_id: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True)
    username: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)

    __table_args__ = (Index("idx_drover_jobs_claim", "status", "created_at"),)


class ResourcePolicy(Base):
    __tablename__ = "resource_policies"

    policy_key: Mapped[str] = mapped_column(VARCHAR(128), primary_key=True)
    resource_kind: Mapped[str] = mapped_column(VARCHAR(64), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    resource_name: Mapped[str | None] = mapped_column(VARCHAR(255), nullable=True)
    constraints: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    updated_by_user_id: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)


class RuntimeSetting(Base):
    __tablename__ = "runtime_settings"

    setting_key: Mapped[str] = mapped_column(VARCHAR(128), primary_key=True)
    value_json: Mapped[dict | list | str | int | float | bool] = mapped_column(JSON, nullable=False)
    updated_by_user_id: Mapped[str | None] = mapped_column(VARCHAR(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)


class GpuQuota(Base):
    """프로젝트별 GPU 타입 quota. limit=-1 은 무제한."""

    __tablename__ = "gpu_quotas"

    id: Mapped[int] = mapped_column(INT, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(VARCHAR(64), nullable=False, index=True)
    gpu_type: Mapped[str] = mapped_column(VARCHAR(64), nullable=False)
    limit: Mapped[int] = mapped_column(INT, nullable=False, default=-1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)

    __table_args__ = (Index("idx_gpu_quota_project_type", "project_id", "gpu_type", unique=True),)
