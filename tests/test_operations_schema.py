"""Tests for durable operations and managed resources ORM schema & migrations."""

from __future__ import annotations

import hashlib
import uuid

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from drover.db import Base
from drover.models.orm import (
    DroverJob,
    DroverOperation,
    DroverOperationEvent,
    K3sCluster,
    ManagedOpenStackResource,
)
from drover.scripts.migrate import MIGRATIONS, MigrationLedgerError, load_manifest


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(engine, class_=Session, expire_on_commit=False)
    session = session_factory()
    yield session
    session.close()
    engine.dispose()


def test_load_manifest_validates_checksums():
    manifest = load_manifest()
    assert len(manifest) >= 3
    logical_ids = [m.logical_id for m in manifest]
    assert "001_baseline" in logical_ids
    assert "002_operations" in logical_ids
    assert "003_cluster_reconciliation" in logical_ids

    op_mig = next(m for m in manifest if m.logical_id == "002_operations")
    actual_hash = hashlib.sha256((MIGRATIONS / op_mig.relative_path).read_bytes()).hexdigest()
    assert op_mig.sha256 == actual_hash


def test_load_manifest_detects_tampering(tmp_path):
    bad_manifest = tmp_path / "manifest.txt"
    bad_manifest.write_text("001_baseline|001_baseline.sql|0000000000000000000000000000000000000000000000000000000000000000\n")
    with pytest.raises(MigrationLedgerError, match="checksum drift"):
        load_manifest(bad_manifest)


def test_migration_statements_parsing():
    from drover.scripts.migrate import _statements

    op_sql_path = MIGRATIONS / "002_operations.sql"
    statements = _statements(op_sql_path)
    assert len(statements) >= 4

    joined = "\n".join(statements)
    assert "CREATE TABLE IF NOT EXISTS `drover_operations`" in joined
    assert "CREATE TABLE IF NOT EXISTS `drover_operation_events`" in joined
    assert "CREATE TABLE IF NOT EXISTS `managed_openstack_resources`" in joined
    assert "ALTER TABLE `drover_jobs` ADD COLUMN `operation_id`" in joined
    assert "idx_drover_op_proj_idemp" in joined
    assert "idx_drover_op_event_seq" in joined
    assert "idx_managed_res_identity" in joined

    reconciliation_sql = (MIGRATIONS / "003_cluster_reconciliation.sql").read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS `last_reconciled_at` DATETIME(6) NULL" in reconciliation_sql
    assert "ADD COLUMN IF NOT EXISTS `drift_status` JSON NULL" in reconciliation_sql


def test_orm_drover_operation_creation_and_relationships(db_session: Session):
    cluster_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    res_id = str(uuid.uuid4())

    cluster = K3sCluster(
        id=cluster_id,
        project_id="proj-123",
        name="test-cluster",
        status="CREATING",
    )
    db_session.add(cluster)
    db_session.flush()

    op = DroverOperation(
        id=op_id,
        project_id="proj-123",
        cluster_id=cluster_id,
        kind="create",
        status="RUNNING",
        request_id="req-001",
        idempotency_key="idemp-key-1",
        request_hash="hash-abc-123",
    )
    db_session.add(op)
    db_session.flush()

    event = DroverOperationEvent(
        operation_id=op_id,
        sequence=1,
        phase="initializing",
        message="Starting provisioning",
        payload_json={"step": 1},
    )
    res = ManagedOpenStackResource(
        id=res_id,
        cluster_id=cluster_id,
        operation_id=op_id,
        service="nova",
        resource_type="server",
        resource_id="vm-uuid-001",
        name="test-cluster-master-0",
        state="ACTIVE",
    )
    job = DroverJob(
        id=job_id,
        cluster_id=cluster_id,
        project_id="proj-123",
        kind="create",
        status="queued",
        operation_id=op_id,
    )
    db_session.add_all([event, res, job])
    db_session.commit()

    # Query back and verify relationships
    stmt = select(K3sCluster).where(K3sCluster.id == cluster_id)
    retrieved_cluster = db_session.scalars(stmt).one()

    assert len(retrieved_cluster.operations) == 1
    assert retrieved_cluster.operations[0].id == op_id
    assert len(retrieved_cluster.managed_resources) == 1
    assert retrieved_cluster.managed_resources[0].id == res_id

    assert len(op.events) == 1
    assert op.events[0].phase == "initializing"

    assert len(op.resources) == 1
    assert op.resources[0].resource_id == "vm-uuid-001"

    assert len(op.jobs) == 1
    assert op.jobs[0].id == job_id
    assert job.operation is not None
    assert job.operation.id == op_id


def test_idempotency_key_uniqueness_per_project(db_session: Session):
    cluster_id = str(uuid.uuid4())
    cluster = K3sCluster(id=cluster_id, project_id="proj-123", name="test-cluster")
    db_session.add(cluster)
    db_session.flush()

    op1 = DroverOperation(
        id=str(uuid.uuid4()),
        project_id="proj-123",
        cluster_id=cluster_id,
        kind="create",
        idempotency_key="key-alpha",
    )
    db_session.add(op1)
    db_session.commit()

    # Duplicate idempotency_key in SAME project should fail
    op2 = DroverOperation(
        id=str(uuid.uuid4()),
        project_id="proj-123",
        cluster_id=cluster_id,
        kind="scale",
        idempotency_key="key-alpha",
    )
    db_session.add(op2)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    # Same idempotency_key in DIFFERENT project should succeed
    op3 = DroverOperation(
        id=str(uuid.uuid4()),
        project_id="proj-456",
        cluster_id=cluster_id,
        kind="create",
        idempotency_key="key-alpha",
    )
    db_session.add(op3)
    db_session.commit()

    # Multiple NULL idempotency_keys in same project should succeed
    op4 = DroverOperation(
        id=str(uuid.uuid4()),
        project_id="proj-123",
        cluster_id=cluster_id,
        kind="delete",
        idempotency_key=None,
    )
    op5 = DroverOperation(
        id=str(uuid.uuid4()),
        project_id="proj-123",
        cluster_id=cluster_id,
        kind="reconcile",
        idempotency_key=None,
    )
    db_session.add_all([op4, op5])
    db_session.commit()


def test_operation_event_sequence_uniqueness(db_session: Session):
    cluster_id = str(uuid.uuid4())
    op_id = str(uuid.uuid4())
    cluster = K3sCluster(id=cluster_id, project_id="proj-123", name="test-cluster")
    op = DroverOperation(id=op_id, project_id="proj-123", cluster_id=cluster_id, kind="create")
    db_session.add_all([cluster, op])
    db_session.flush()

    e1 = DroverOperationEvent(operation_id=op_id, sequence=1, phase="phase-1")
    db_session.add(e1)
    db_session.commit()

    e2 = DroverOperationEvent(operation_id=op_id, sequence=1, phase="phase-2-dup")
    db_session.add(e2)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    e3 = DroverOperationEvent(operation_id=op_id, sequence=2, phase="phase-2")
    db_session.add(e3)
    db_session.commit()


def test_managed_openstack_resource_identity_uniqueness(db_session: Session):
    cluster_id = str(uuid.uuid4())
    cluster = K3sCluster(id=cluster_id, project_id="proj-123", name="test-cluster")
    db_session.add(cluster)
    db_session.flush()

    r1 = ManagedOpenStackResource(
        id=str(uuid.uuid4()),
        cluster_id=cluster_id,
        service="nova",
        resource_type="server",
        resource_id="res-uuid-1",
    )
    db_session.add(r1)
    db_session.commit()

    # Duplicate (service, resource_type, resource_id) should fail
    r2 = ManagedOpenStackResource(
        id=str(uuid.uuid4()),
        cluster_id=cluster_id,
        service="nova",
        resource_type="server",
        resource_id="res-uuid-1",
    )
    db_session.add(r2)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()

    # Different service or resource_type should succeed
    r3 = ManagedOpenStackResource(
        id=str(uuid.uuid4()),
        cluster_id=cluster_id,
        service="cinder",
        resource_type="volume",
        resource_id="res-uuid-1",
    )
    db_session.add(r3)
    db_session.commit()
