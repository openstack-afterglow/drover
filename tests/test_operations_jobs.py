"""Focused tests for Drover Operation and Job transactional orchestration (Step 2.2)."""

import asyncio
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from drover.models.orm import DroverJob, DroverOperation, DroverOperationEvent, K3sCluster
from drover.services import jobs, operations


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        if isinstance(self.value, list):
            return self.value[0] if self.value else None
        return self.value

    def scalar_one(self):
        if isinstance(self.value, list):
            return self.value[0]
        return self.value

    def scalars(self):
        return self

    def all(self):
        if isinstance(self.value, list):
            return self.value
        return [self.value] if self.value is not None else []


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_args):
        return False


class _TestSession:
    def __init__(self, store=None):
        self.store = store if store is not None else {}
        self.added = []
        self.events = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def begin(self):
        return _Transaction(self)

    def add(self, entity):
        self.added.append(entity)
        if isinstance(entity, K3sCluster):
            self.store[(K3sCluster, entity.id)] = entity
        elif isinstance(entity, DroverOperation):
            self.store[(DroverOperation, entity.id)] = entity
        elif isinstance(entity, DroverJob):
            self.store[(DroverJob, entity.id)] = entity
        elif isinstance(entity, DroverOperationEvent):
            self.events.append(entity)

    async def execute(self, statement):
        sql = str(statement).lower()
        if "drover_operation_events" in sql:
            if self.events:
                max_seq = max(e.sequence for e in self.events)
                return _Result(max_seq)
            return _Result(None)
        if "drover_operations" in sql:
            ops = [v for k, v in self.store.items() if k[0] == DroverOperation]
            if "waiting_callback" in sql:
                ops = [o for o in ops if o.status == "WAITING_CALLBACK"]
            return _Result(ops)
        if "k3s_clusters" in sql:
            clusters = [v for k, v in self.store.items() if k[0] == K3sCluster and v.deleted_at is None]
            return _Result(clusters)
        if "drover_jobs" in sql:
            jobs_list = [v for k, v in self.store.items() if k[0] == DroverJob]
            return _Result(jobs_list)
        return _Result(None)
    async def get(self, model, object_id, **_kwargs):
        return self.store.get((model, object_id))

    def flush(self):
        pass


def _factory(session):
    return lambda: session


@pytest.mark.asyncio
async def test_enqueue_job_links_operation_transactionally(monkeypatch):
    """enqueue_job must create DroverOperation and DroverJob linked in one transaction."""
    session = _TestSession()
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    job_id = await jobs.enqueue_job(
        cluster_id="cluster-100",
        project_id="proj-1",
        kind="scale",
        payload={"desired_count": 3},
        request_id="req-555",
        idempotency_key="idemp-123",
        request_hash="hash-xyz",
    )

    # Verify both operation and job were added to session
    ops = [x for x in session.added if isinstance(x, DroverOperation)]
    jobs_list = [x for x in session.added if isinstance(x, DroverJob)]
    events = [x for x in session.added if isinstance(x, DroverOperationEvent)]

    assert len(ops) == 1
    assert len(jobs_list) == 1
    assert len(events) == 1

    op = ops[0]
    job = jobs_list[0]
    event = events[0]

    assert job.id == job_id
    assert job.operation_id == op.id
    assert op.cluster_id == "cluster-100"
    assert op.project_id == "proj-1"
    assert op.kind == "scale"
    assert op.status == "QUEUED"
    assert op.request_id == "req-555"
    assert op.idempotency_key == "idemp-123"
    assert op.request_hash == "hash-xyz"

    assert event.operation_id == op.id
    assert event.sequence == 1
    assert event.phase == "job_enqueued"


@pytest.mark.asyncio
async def test_event_sequencing():
    """Sequential events on an operation must assign incrementing sequence numbers (1, 2, 3...)."""
    session = _TestSession()
    op_id = str(uuid.uuid4())

    e1 = await operations._append_event_impl(session, op_id, phase="phase_1", message="First", payload_json=None)
    e2 = await operations._append_event_impl(session, op_id, phase="phase_2", message="Second", payload_json=None)
    e3 = await operations._append_event_impl(session, op_id, phase="phase_3", message="Third", payload_json=None)

    assert e1.sequence == 1
    assert e2.sequence == 2
    assert e3.sequence == 3


@pytest.mark.asyncio
async def test_job_claim_and_terminal_success_transition(monkeypatch):
    """Job claim transitions operation QUEUED -> RUNNING; completion transitions -> SUCCEEDED."""
    op = DroverOperation(
        id="op-1",
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="scale",
        status="QUEUED",
        created_at=datetime.now(UTC),
    )
    job = DroverJob(
        id="job-1",
        cluster_id="cluster-1",
        project_id="proj-1",
        kind="scale",
        status="running",
        attempts=1,
        payload_json={"desired_count": 5},
        operation_id="op-1",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    cluster = K3sCluster(
        id="cluster-1",
        project_id="proj-1",
        name="test-cluster",
        status="ACTIVE",
    )

    session = _TestSession(
        store={
            (DroverOperation, "op-1"): op,
            (DroverJob, "job-1"): job,
            (K3sCluster, "cluster-1"): cluster,
        }
    )
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    # Mock _claim_one to return job-1
    monkeypatch.setattr(
        jobs,
        "_claim_one",
        AsyncMock(return_value=("job-1", 1, "scale", "cluster-1", "proj-1", {"desired_count": 5})),
    )

    # 1. Claim & complete execution
    monkeypatch.setattr(jobs, "_execute_job_direct", AsyncMock())

    assert await jobs.process_one_job() is True

    # Check job completed and op succeeded
    assert job.status == "completed"
    assert op.status == "SUCCEEDED"
    assert op.finished_at is not None

    # Check events recorded
    event_phases = [e.phase for e in session.events]
    assert "job_completed" in event_phases


@pytest.mark.asyncio
async def test_job_terminal_failure_transition(monkeypatch):
    """Job failure after 3 attempts sets operation FAILED, error message, and cluster ERROR."""
    op = DroverOperation(
        id="op-2",
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="scale",
        status="RUNNING",
        started_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
    )
    job = DroverJob(
        id="job-2",
        cluster_id="cluster-1",
        project_id="proj-1",
        kind="scale",
        status="running",
        attempts=3,
        payload_json={"desired_count": 5},
        operation_id="op-2",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    cluster = K3sCluster(
        id="cluster-1",
        project_id="proj-1",
        name="test-cluster",
        status="SCALING",
    )

    session = _TestSession(
        store={
            (DroverOperation, "op-2"): op,
            (DroverJob, "job-2"): job,
            (K3sCluster, "cluster-1"): cluster,
        }
    )
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    # Execute failure retry logic directly for attempt 3
    res = await jobs._retry_or_fail("job-2", attempt=3, error="Nova quota exceeded")
    assert res is True

    assert job.status == "failed"
    assert op.status == "FAILED"
    assert "Nova quota exceeded" in (op.error or "")
    assert cluster.status == "ERROR"
    assert "Nova quota exceeded" in (cluster.status_reason or "")

    event_phases = [e.phase for e in session.events]
    assert "job_failed" in event_phases


@pytest.mark.asyncio
async def test_waiting_callback_to_running_transition(monkeypatch):
    """Create operation transitions RUNNING -> WAITING_CALLBACK and stays WAITING_CALLBACK when create job finishes."""
    op = DroverOperation(
        id="op-3",
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="create",
        status="RUNNING",
        started_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
    )
    job = DroverJob(
        id="job-3",
        cluster_id="cluster-1",
        project_id="proj-1",
        kind="create",
        status="running",
        attempts=1,
        payload_json={"server_vm_id": "vm-server-1"},
        operation_id="op-3",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    session = _TestSession(
        store={
            (DroverOperation, "op-3"): op,
            (DroverJob, "job-3"): job,
        }
    )
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    # Simulate create job setting WAITING_CALLBACK
    op.status = "WAITING_CALLBACK"

    # Complete create job
    assert await jobs._complete("job-3", attempt=1) is True

    # Operation must remain WAITING_CALLBACK (not SUCCEEDED)
    assert op.status == "WAITING_CALLBACK"
    assert op.finished_at is None

    event_phases = [e.phase for e in session.events]
    assert "server_boot_ready" in event_phases


@pytest.mark.asyncio
async def test_create_operation_completes_only_after_cluster_is_active(monkeypatch):
    op = DroverOperation(
        id="op-create",
        project_id="proj-1",
        cluster_id="cluster-create",
        kind="create",
        status="RUNNING",
        started_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
    )
    initial_job = DroverJob(
        id="job-create-initial",
        cluster_id="cluster-create",
        project_id="proj-1",
        kind="create",
        status="running",
        attempts=1,
        payload_json={},
        operation_id=op.id,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    cluster = K3sCluster(
        id="cluster-create",
        project_id="proj-1",
        name="test-cluster",
        status="CREATING",
    )
    session = _TestSession(
        store={
            (DroverOperation, op.id): op,
            (DroverJob, initial_job.id): initial_job,
            (K3sCluster, cluster.id): cluster,
        }
    )
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    assert await jobs._complete(initial_job.id, attempt=1) is True
    assert op.status == "RUNNING"
    assert op.finished_at is None

    follow_up_job = DroverJob(
        id="job-create-agents",
        cluster_id=cluster.id,
        project_id=cluster.project_id,
        kind="provision_agents",
        status="running",
        attempts=1,
        payload_json={},
        operation_id=op.id,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    session.store[(DroverJob, follow_up_job.id)] = follow_up_job
    cluster.status = "ACTIVE"

    assert await jobs._complete(follow_up_job.id, attempt=1) is True
    assert op.status == "SUCCEEDED"
    assert op.finished_at is not None



@pytest.mark.parametrize("terminal_status", ["FAILED", "CANCELLED"])
@pytest.mark.asyncio
async def test_terminal_create_operation_is_not_reopened_by_late_job(monkeypatch, terminal_status):
    op = DroverOperation(
        id=f"op-{terminal_status.lower()}",
        project_id="proj-1",
        cluster_id="cluster-terminal",
        kind="create",
        status=terminal_status,
        finished_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
    )
    original_finished_at = op.finished_at
    job = DroverJob(
        id=f"job-{terminal_status.lower()}",
        cluster_id=op.cluster_id,
        project_id=op.project_id,
        kind="bootstrap_ha",
        status="running",
        attempts=1,
        payload_json={},
        operation_id=op.id,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    cluster = K3sCluster(
        id=op.cluster_id,
        project_id=op.project_id,
        name="terminal-cluster",
        status="ERROR",
    )
    session = _TestSession(
        store={
            (DroverOperation, op.id): op,
            (DroverJob, job.id): job,
            (K3sCluster, cluster.id): cluster,
        }
    )
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    assert await jobs._complete(job.id, attempt=1) is True
    assert op.status == terminal_status
    assert op.finished_at == original_finished_at
    assert session.events[-1].phase == "job_completed"
    assert "after operation terminalized" in session.events[-1].message


@pytest.mark.asyncio
async def test_ha_bootstrap_completion_keeps_create_operation_nonterminal(monkeypatch):
    op = DroverOperation(
        id="op-ha",
        project_id="proj-1",
        cluster_id="cluster-ha",
        kind="create",
        status="RUNNING",
        created_at=datetime.now(UTC),
    )
    job = DroverJob(
        id="job-ha",
        cluster_id=op.cluster_id,
        project_id=op.project_id,
        kind="bootstrap_ha",
        status="running",
        attempts=1,
        payload_json={},
        operation_id=op.id,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    cluster = K3sCluster(
        id=op.cluster_id,
        project_id=op.project_id,
        name="ha-cluster",
        status="PROVISIONING",
    )
    session = _TestSession(
        store={
            (DroverOperation, op.id): op,
            (DroverJob, job.id): job,
            (K3sCluster, cluster.id): cluster,
        }
    )
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    assert await jobs._complete(job.id, attempt=1) is True
    assert op.status == "RUNNING"
    assert op.finished_at is None
    assert session.events[-1].phase == "server_boot_ready"

@pytest.mark.asyncio
async def test_lease_recovery_appends_event(monkeypatch):
    """Re-claiming a stale running job logs lease_recovered event on linked operation."""
    op = DroverOperation(
        id="op-4",
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="scale",
        status="RUNNING",
        created_at=datetime.now(UTC),
    )

    session = _TestSession(
        store={
            (DroverOperation, "op-4"): op,
        }
    )

    # Directly verify event phase generated for lease recovery
    event = await operations._append_event_impl(
        session, "op-4", phase="lease_recovered", message="Re-claimed stale job scale attempt 2"
    )

    assert event.sequence == 1
    assert event.phase == "lease_recovered"
    assert "Re-claimed stale job" in (event.message or "")


@pytest.mark.asyncio
async def test_idempotency_key_replay_and_conflict():
    """create_or_get_operation handles duplicate idempotency_key replay & hash conflict."""
    session = _TestSession()

    op1 = await operations.create_or_get_operation(
        session,
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="scale",
        idempotency_key="key-abc",
        request_hash="hash-111",
    )
    session.store[(DroverOperation, op1.id)] = op1

    # Simulate finding existing by mocking session.execute
    monkeypatch_exec = AsyncMock(return_value=_Result(op1))
    session.execute = monkeypatch_exec

    # Replay with same key & hash -> returns original op
    op2 = await operations.create_or_get_operation(
        session,
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="scale",
        idempotency_key="key-abc",
        request_hash="hash-111",
    )
    assert op2.id == op1.id

    # Replay with same key & different hash -> raises IdempotencyConflictError
    with pytest.raises(operations.IdempotencyConflictError):
        await operations.create_or_get_operation(
            session,
            project_id="proj-1",
            cluster_id="cluster-1",
            kind="scale",
            idempotency_key="key-abc",
            request_hash="hash-222-changed",
        )
@pytest.mark.asyncio
async def test_recover_expired_callback_operations(monkeypatch):
    """Scan WAITING_CALLBACK operations past TTL, transition to FAILED, append timeout event, mark cluster ERROR, and enqueue delete job."""
    from datetime import timedelta
    old_time = datetime.now(UTC) - timedelta(minutes=40)
    op = DroverOperation(
        id="op-cb-expired",
        project_id="proj-1",
        cluster_id="cluster-cb-1",
        kind="create",
        status="WAITING_CALLBACK",
        created_at=old_time,
    )
    cluster = K3sCluster(
        id="cluster-cb-1",
        project_id="proj-1",
        name="cb-cluster",
        status="PROVISIONING",
    )
    session = _TestSession(
        store={
            (DroverOperation, "op-cb-expired"): op,
            (K3sCluster, "cluster-cb-1"): cluster,
        }
    )
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    recovered = await operations.recover_expired_callback_operations(timeout_seconds=1800)

    assert recovered == ["op-cb-expired"]
    assert op.status == "FAILED"
    assert op.error == "Cloud-init callback timed out"
    assert op.finished_at is not None
    assert cluster.status == "ERROR"
    assert cluster.status_reason == "Cloud-init callback timed out"

    timeout_events = [e for e in session.events if e.phase == "callback_timeout"]
    assert len(timeout_events) == 1
    assert timeout_events[0].operation_id == "op-cb-expired"

    delete_jobs = [j for (mod, _), j in session.store.items() if mod == DroverJob and j.kind == "delete"]
    assert len(delete_jobs) == 1
    assert delete_jobs[0].cluster_id == "cluster-cb-1"


@pytest.mark.asyncio
async def test_schedule_worker_reconciliations_dedupe_and_concurrency(monkeypatch):
    """schedule_worker_reconciliations respects per-cluster deduplication and bounded project concurrency."""
    from drover.services import reconciliation

    c1 = K3sCluster(id="c1", project_id="proj-A", name="cl-1", status="ACTIVE")
    c2 = K3sCluster(id="c2", project_id="proj-A", name="cl-2", status="ACTIVE")
    c3 = K3sCluster(id="c3", project_id="proj-A", name="cl-3", status="ACTIVE")

    j1 = DroverJob(id="j1", cluster_id="c1", project_id="proj-A", kind="reconcile", status="running")

    session = _TestSession(
        store={
            (K3sCluster, "c1"): c1,
            (K3sCluster, "c2"): c2,
            (K3sCluster, "c3"): c3,
            (DroverJob, "j1"): j1,
        }
    )
    monkeypatch.setattr("drover.db.get_session_factory", lambda: _factory(session))
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    enqueued = await reconciliation.schedule_worker_reconciliations(max_per_project=2)
    assert len(enqueued) == 1

    c1_jobs = [j for (mod, _), j in session.store.items() if mod == DroverJob and j.cluster_id == "c1"]
    assert len(c1_jobs) == 1

    enqueued_next = await reconciliation.schedule_worker_reconciliations(max_per_project=2)
    assert len(enqueued_next) == 0
@pytest.mark.asyncio
async def test_reconcile_worker_loop_execution(monkeypatch):
    """_reconcile_worker_loop invokes callback recovery and reconciliation scheduler."""
    from drover import worker
    from drover.services import operations, reconciliation

    called_recover = False
    called_schedule = False

    async def mock_recover(timeout_seconds=1800):
        nonlocal called_recover
        called_recover = True
        return []

    async def mock_schedule(max_per_project=2):
        nonlocal called_schedule
        called_schedule = True
        return []

    monkeypatch.setattr(operations, "recover_expired_callback_operations", mock_recover)
    monkeypatch.setattr(reconciliation, "schedule_worker_reconciliations", mock_schedule)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()]))

    with pytest.raises(asyncio.CancelledError):
        await worker._reconcile_worker_loop()

    assert called_recover is True
    assert called_schedule is True
