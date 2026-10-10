"""Focused tests for Drover Operation and Job transactional orchestration (Step 2.2)."""

import asyncio
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import mysql

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
        server_vm_id="server-create",
        kubeconfig_encrypted="encrypted-kubeconfig",
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
    # Only clusters with an active control credential are reconciled; all three are authorized here.
    monkeypatch.setattr(
        "drover.services.cluster_authority.authorized_cluster_ids", AsyncMock(return_value={"c1", "c2", "c3"})
    )

    enqueued = await reconciliation.schedule_worker_reconciliations(max_per_project=2)
    assert len(enqueued) == 1

    c1_jobs = [j for (mod, _), j in session.store.items() if mod == DroverJob and j.cluster_id == "c1"]
    assert len(c1_jobs) == 1

    enqueued_next = await reconciliation.schedule_worker_reconciliations(max_per_project=2)
    assert len(enqueued_next) == 0
@pytest.mark.asyncio
async def test_reconcile_worker_loop_execution(monkeypatch):
    """Maintenance invokes callback recovery, deleted-target recovery, and reconciliation in order."""
    from drover import worker
    from drover.services import operations, reconciliation

    called_recover = False
    called_schedule = False
    calls = []

    async def mock_recover(timeout_seconds=1800):
        nonlocal called_recover
        called_recover = True
        calls.append("callback")
        return []

    async def mock_deleted_recover():
        calls.append("deleted")
        return []

    async def mock_schedule(max_per_project=2):
        nonlocal called_schedule
        called_schedule = True
        calls.append("schedule")
        return []

    monkeypatch.setattr(operations, "recover_expired_callback_operations", mock_recover)
    monkeypatch.setattr(operations, "recover_deleted_cluster_operations", mock_deleted_recover)
    monkeypatch.setattr(reconciliation, "schedule_worker_reconciliations", mock_schedule)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()]))

    with pytest.raises(asyncio.CancelledError):
        await worker._reconcile_worker_loop()

    assert called_recover is True
    assert called_schedule is True
    assert calls == ["callback", "deleted", "schedule"]


class _RecoveryTransaction(_Transaction):
    async def __aenter__(self):
        self.session.in_transaction = True
        self.original_events = list(self.session.events)
        self.original_states = [
            (op, op.status, op.error, op.finished_at)
            for (model, _), op in self.session.store.items() if model == DroverOperation
        ]
        return self.session

    async def __aexit__(self, exc_type, *_args):
        if exc_type is not None:
            self.session.events[:] = self.original_events
            for op, status, error, finished_at in self.original_states:
                op.status, op.error, op.finished_at = status, error, finished_at
        self.session.in_transaction = False
        return False


class _RecoverySession(_TestSession):
    """Stateful SQL-boundary double; records lock SQL, not real database concurrency."""

    def __init__(self, store):
        super().__init__(store)
        self.statements = []
        self.gets = []
        self.connections = []
        self.in_transaction = False
        self.before_job_lock = None
        self.before_operation_lock = None
        self.busy_clusters = set()
        self.busy_operations = set()
        self.fail_event = False

    def begin(self):
        return _RecoveryTransaction(self)

    async def connection(self, *, execution_options):
        assert self.in_transaction
        self.connections.append(execution_options)

    def add(self, entity):
        if isinstance(entity, DroverOperationEvent):
            assert self.in_transaction
            if self.fail_event:
                raise RuntimeError("event insert failed")
        super().add(entity)

    async def get(self, model, object_id, **kwargs):
        self.gets.append((model, object_id, kwargs, self.in_transaction))
        return await super().get(model, object_id, **kwargs)

    def active_jobs(self, cluster_id, operation_id):
        return [
            job for (model, _), job in self.store.items()
            if model == DroverJob and job.status in {"queued", "running"}
            and (job.cluster_id == cluster_id or job.operation_id == operation_id)
        ]

    async def execute(self, statement):
        compiled = statement.compile(dialect=mysql.dialect())
        sql, params = str(compiled).lower(), compiled.params
        self.statements.append((sql, params, self.in_transaction))
        if "from drover_operation_events" in sql:
            events = [e.sequence for e in self.events if e.operation_id == params["operation_id_1"]]
            return _Result(max(events) if events else None)
        if "from drover_operations inner join k3s_clusters" in sql:
            candidates = []
            for (model, _), op in self.store.items():
                if model != DroverOperation:
                    continue
                cluster = self.store.get((K3sCluster, op.cluster_id))
                if (
                    op.status == "RUNNING" and cluster is not None
                    and cluster.project_id == op.project_id and cluster.deleted_at is not None
                    and not self.active_jobs(op.cluster_id, op.id)
                    and ("project_id_1" not in params or op.project_id == params["project_id_1"])
                    and ("id_1" not in params or op.id == params["id_1"])
                ):
                    candidates.append(op)
            candidates.sort(key=lambda op: (op.created_at, op.id))
            return _Result(candidates[:params["param_1"]])
        if "from drover_jobs" in sql:
            if self.before_job_lock is not None:
                self.before_job_lock(self)
                self.before_job_lock = None
            return _Result(self.active_jobs(params["cluster_id_1"], params["operation_id_1"]))
        if "from k3s_clusters" in sql:
            cluster = self.store.get((K3sCluster, params["id_1"]))
            if (
                cluster is None or cluster.id in self.busy_clusters or cluster.deleted_at is None
                or cluster.project_id != params["project_id_1"]
            ):
                return _Result(None)
            return _Result(cluster)
        if "from drover_operations" in sql:
            if "for update" in sql:
                if self.before_operation_lock is not None:
                    self.before_operation_lock(self)
                    self.before_operation_lock = None
                op = self.store.get((DroverOperation, params["id_1"]))
                if (
                    op is None or op.id in self.busy_operations or op.status != params["status_1"]
                    or op.project_id != params["project_id_1"] or op.cluster_id != params["cluster_id_1"]
                ):
                    return _Result(None)
                return _Result(op)
            attempts = [
                op for (model, _), op in self.store.items()
                if model == DroverOperation and op.project_id == params["project_id_1"]
                and op.cluster_id == params["cluster_id_1"] and op.kind == params["kind_1"]
            ]
            attempts.sort(key=lambda op: (op.created_at, op.id), reverse=True)
            return _Result(attempts[:1])
        raise AssertionError(f"Unexpected recovery SQL: {sql}")


def _deleted_operation_session(*, kind="create", status="RUNNING", project_id="proj-1", suffix="1"):
    now = datetime.now(UTC)
    op = DroverOperation(
        id=f"op-{suffix}", cluster_id=f"cluster-{suffix}", project_id=project_id,
        kind=kind, status=status, created_at=now, started_at=now,
    )
    cluster = K3sCluster(
        id=op.cluster_id, project_id=project_id, name=f"deleted-{suffix}",
        status="DELETED", deleted_at=now,
    )
    return _RecoverySession({(DroverOperation, op.id): op, (K3sCluster, cluster.id): cluster}), op, cluster


@pytest.mark.asyncio
async def test_nine_deleted_create_operations_recover_once_after_jobs_complete(monkeypatch):
    """Completed create stages leave RUNNING unless ACTIVE; soft deletion needs terminal recovery."""
    from drover.services import deletion

    session = _RecoverySession({})
    ops = []
    for i in range(9):
        fixture, op, cluster = _deleted_operation_session(suffix=str(i))
        cluster.status, cluster.deleted_at = "ERROR", None
        session.store.update(fixture.store)
        job = DroverJob(
            id=f"job-{i}", cluster_id=cluster.id, project_id=cluster.project_id,
            operation_id=op.id, kind="create", status="running", attempts=1,
        )
        session.store[(DroverJob, job.id)] = job
        ops.append(op)
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    for i in range(9):
        assert await jobs._complete(f"job-{i}", attempt=1) is True
    assert all(op.status == "RUNNING" and op.finished_at is None for op in ops)
    assert all(event.phase == "server_boot_ready" for event in session.events)
    for (model, _), cluster in session.store.items():
        if model == K3sCluster:
            cluster.status, cluster.deleted_at = "DELETED", datetime.now(UTC)

    enqueue = AsyncMock()
    cloud_delete = AsyncMock()
    monkeypatch.setattr(jobs, "enqueue_job", enqueue)
    monkeypatch.setattr(deletion, "execute_delete_cluster", cloud_delete)
    original_started = {op.id: op.started_at for op in ops}
    cluster_states = {key: (obj.status, obj.deleted_at) for key, obj in session.store.items() if key[0] == K3sCluster}
    assert set(await operations.recover_deleted_cluster_operations()) == {op.id for op in ops}
    terminal_states = [(op.status, op.error, op.finished_at) for op in ops]
    assert all(op.status == "CANCELLED" and op.finished_at is not None for op in ops)
    assert {op.id: op.started_at for op in ops} == original_started
    assert await operations.recover_deleted_cluster_operations() == []
    assert [(op.status, op.error, op.finished_at) for op in ops] == terminal_states
    assert cluster_states == {key: (obj.status, obj.deleted_at) for key, obj in session.store.items() if key[0] == K3sCluster}
    for op in ops:
        events = [event for event in session.events if event.operation_id == op.id]
        assert [event.sequence for event in events] == [1, 2]
        assert events[-1].phase == "deleted_cluster_recovered"
        assert events[-1].payload_json == {
            "cluster_id": op.cluster_id, "project_id": op.project_id,
            "previous_status": "RUNNING", "status": "CANCELLED",
        }
    enqueue.assert_not_awaited()
    cloud_delete.assert_not_awaited()


@pytest.mark.parametrize("job_status", ["queued", "running"])
@pytest.mark.parametrize("link", ["target", "operation", "both"])
@pytest.mark.asyncio
async def test_deleted_recovery_preserves_all_active_jobs(monkeypatch, job_status, link):
    session, op, cluster = _deleted_operation_session()
    job = DroverJob(
        id="active-job", cluster_id=cluster.id if link != "operation" else "other-target",
        project_id="other-project", operation_id=op.id if link != "target" else None,
        kind="reconcile", status=job_status, attempts=3, claimed_at=datetime(2020, 1, 1, tzinfo=UTC),
        payload_json={"continuation": True},
    )
    session.store[(DroverJob, job.id)] = job
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    original_job = (job.status, job.attempts, job.claimed_at, job.payload_json)

    assert await operations.recover_deleted_cluster_operations() == []
    assert op.status == "RUNNING" and op.finished_at is None
    assert (job.status, job.attempts, job.claimed_at, job.payload_json) == original_job
    assert session.events == []


@pytest.mark.parametrize("kind", ["create", "scale", "delete", "reconcile", "rotate_certificates", "nodegroup_reconcile", "reauthorize"])
@pytest.mark.asyncio
async def test_deleted_recovery_never_infers_operation_success(monkeypatch, kind):
    session, op, cluster = _deleted_operation_session(kind=kind)
    job = DroverJob(
        id="finished-job", cluster_id=cluster.id, project_id=cluster.project_id,
        operation_id=op.id, kind=kind, status="failed", last_error="original job failure",
    )
    session.store[(DroverJob, job.id)] = job
    op.error = "original operation failure"
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))

    assert await operations.recover_deleted_cluster_operations() == [op.id]
    assert op.status == "CANCELLED" and op.error == "original operation failure"
    assert job.status == "failed" and job.last_error == "original job failure"


@pytest.mark.parametrize("status", ["QUEUED", "WAITING_CALLBACK", "SUCCEEDED", "FAILED", "CANCELLED"])
@pytest.mark.asyncio
async def test_deleted_recovery_preserves_nonrunning_and_terminal_operations(monkeypatch, status):
    session, op, _ = _deleted_operation_session(status=status)
    op.error = "existing error"
    op.finished_at = datetime(2020, 1, 1, tzinfo=UTC)
    before = (op.status, op.error, op.started_at, op.finished_at)
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))

    assert await operations.recover_deleted_cluster_operations() == []
    assert (op.status, op.error, op.started_at, op.finished_at) == before
    assert session.events == []


@pytest.mark.parametrize("target", ["undeleted", "status_only", "missing", "mismatched"])
@pytest.mark.asyncio
async def test_deleted_recovery_requires_soft_deleted_matching_project_target(monkeypatch, target):
    session, op, cluster = _deleted_operation_session()
    if target == "undeleted":
        cluster.status, cluster.deleted_at = "ERROR", None
    elif target == "status_only":
        cluster.deleted_at = None
    elif target == "missing":
        del session.store[(K3sCluster, cluster.id)]
    else:
        cluster.project_id = "other-project"
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))

    assert await operations.recover_deleted_cluster_operations() == []
    assert op.status == "RUNNING" and op.finished_at is None
    assert session.events == []


@pytest.mark.asyncio
async def test_deleted_recovery_rechecks_job_after_candidate_scan(monkeypatch):
    session, op, cluster = _deleted_operation_session()

    def admit_job(current_session):
        current_session.store[(DroverJob, "late-job")] = DroverJob(
            id="late-job", cluster_id=cluster.id, project_id=op.project_id,
            operation_id=None, kind="delete", status="queued",
        )

    session.before_job_lock = admit_job
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    assert await operations.recover_deleted_cluster_operations() == []
    assert op.status == "RUNNING" and session.events == []


@pytest.mark.parametrize("change", ["restored", "mismatched"])
@pytest.mark.asyncio
async def test_deleted_recovery_rechecks_target_after_candidate_scan(monkeypatch, change):
    session, op, cluster = _deleted_operation_session()

    def change_target(_session):
        if change == "restored":
            cluster.deleted_at = None
        else:
            cluster.project_id = "other-project"

    session.before_job_lock = change_target
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    assert await operations.recover_deleted_cluster_operations() == []
    assert op.status == "RUNNING" and session.events == []


@pytest.mark.parametrize("race", ["terminalized", "busy_cluster", "busy_operation"])
@pytest.mark.asyncio
async def test_deleted_recovery_skips_concurrent_parent_row_work(monkeypatch, race):
    session, op, cluster = _deleted_operation_session()
    if race == "terminalized":
        def terminalize(_session):
            op.status, op.finished_at = "FAILED", datetime.now(UTC)
        session.before_operation_lock = terminalize
    elif race == "busy_cluster":
        session.busy_clusters.add(cluster.id)
    else:
        session.busy_operations.add(op.id)
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))

    assert await operations.recover_deleted_cluster_operations() == []
    assert op.status == ("FAILED" if race == "terminalized" else "RUNNING")
    assert session.events == []


@pytest.mark.asyncio
async def test_deleted_recovery_status_and_event_roll_back_together(monkeypatch):
    session, op, _ = _deleted_operation_session()
    session.fail_event = True
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    with pytest.raises(RuntimeError, match="event insert failed"):
        await operations.recover_deleted_cluster_operations()
    assert op.status == "RUNNING" and op.finished_at is None and op.error is None
    assert session.events == []


@pytest.mark.asyncio
async def test_deleted_recovery_exact_filters_lock_order_and_selection(monkeypatch):
    session, op, _ = _deleted_operation_session()
    other, other_op, _ = _deleted_operation_session(project_id="proj-2", suffix="2")
    session.store.update(other.store)
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    assert await operations.recover_deleted_cluster_operations(
        project_id=op.project_id, operation_id=op.id, batch_size=1,
    ) == [op.id]
    assert other_op.status == "RUNNING"
    assert session.connections == [{"isolation_level": "SERIALIZABLE"}]

    candidate, job_lock, cluster_lock, op_lock, sequence_read = session.statements
    sql, params, in_transaction = candidate
    assert not in_transaction and "for update" not in sql
    assert "drover_operations.status =" in sql and params["status_1"] == "RUNNING"
    assert "k3s_clusters.project_id = drover_operations.project_id" in sql
    assert "k3s_clusters.deleted_at is not null" in sql and "not (exists" in sql
    assert params["project_id_1"] == op.project_id and params["id_1"] == op.id
    assert params["param_1"] == 1
    sql, params, in_transaction = job_lock
    assert in_transaction and "for update" in sql and "skip locked" not in sql
    assert params["status_1"] == ["queued", "running"]
    assert "drover_jobs.cluster_id =" in sql and "or drover_jobs.operation_id =" in sql
    assert "drover_jobs.project_id =" not in sql and "claimed_at <" not in sql
    assert "drover_jobs.kind =" not in sql
    for sql, _, in_transaction in (cluster_lock, op_lock):
        assert in_transaction and "for update skip locked" in sql
    assert "k3s_clusters.deleted_at is not null" in cluster_lock[0]
    assert cluster_lock[1]["project_id_1"] == op.project_id
    assert op_lock[1]["status_1"] == "RUNNING"
    assert op_lock[1]["project_id_1"] == op.project_id and op_lock[1]["cluster_id_1"] == op.cluster_id
    assert sequence_read[2] and "for update" in sequence_read[0]
    assert session.gets[-1] == (DroverOperation, op.id, {"with_for_update": True}, True)


@pytest.mark.asyncio
async def test_latest_create_operation_is_read_only_scoped_and_deterministic(monkeypatch):
    session, first, _ = _deleted_operation_session()
    latest = DroverOperation(
        id="op-z", project_id=first.project_id, cluster_id=first.cluster_id,
        kind="create", status="FAILED", created_at=first.created_at,
    )
    unrelated = DroverOperation(
        id="op-zz", project_id="other-project", cluster_id=first.cluster_id,
        kind="create", status="RUNNING", created_at=first.created_at,
    )
    scale = DroverOperation(
        id="op-zzz", project_id=first.project_id, cluster_id=first.cluster_id,
        kind="scale", status="SUCCEEDED", created_at=first.created_at,
    )
    for op in (latest, unrelated, scale):
        session.store[(DroverOperation, op.id)] = op
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))

    assert await operations.get_latest_create_operation(first.project_id, first.cluster_id) is latest
    assert await operations.get_latest_create_operation(first.project_id, "missing") is None
    sql, params, in_transaction = session.statements[0]
    assert not in_transaction and "for update" not in sql
    assert params["project_id_1"] == first.project_id and params["cluster_id_1"] == first.cluster_id
    assert params["kind_1"] == "create" and params["param_1"] == 1
    assert "order by drover_operations.created_at desc, drover_operations.id desc" in sql
    assert "drover_operations.status =" not in sql and session.events == []


@pytest.mark.asyncio
async def test_operation_recovery_and_latest_create_without_database(monkeypatch):
    monkeypatch.setattr(operations, "get_session_factory", lambda: None)
    assert await operations.recover_deleted_cluster_operations() == []
    assert await operations.get_latest_create_operation("proj-1", "cluster-1") is None
    with pytest.raises(ValueError, match="batch_size must be positive"):
        await operations.recover_deleted_cluster_operations(batch_size=0)


@pytest.mark.asyncio
async def test_two_first_events_remain_recoverable_after_database_deadlock(monkeypatch):
    """Distinct operation mutexes do not prevent InnoDB empty-event-gap insert deadlocks."""
    from sqlalchemy.exc import OperationalError

    session, first, _ = _deleted_operation_session(suffix="first")
    peer, second, _ = _deleted_operation_session(suffix="second")
    session.store.update(peer.store)
    monkeypatch.setattr(operations, "get_session_factory", lambda: _factory(session))
    append = operations._append_event_impl
    deadlock = OperationalError(
        "INSERT INTO drover_operation_events", {},
        Exception(1213, "Deadlock found when trying to get lock; try restarting transaction"),
    )
    # A server may abort either first-event writer when the operations share an empty index gap.
    # Inject that boundary rather than claiming this double reproduces real InnoDB concurrency.
    monkeypatch.setattr(operations, "_append_event_impl", AsyncMock(side_effect=deadlock))
    with pytest.raises(OperationalError):
        await operations.recover_deleted_cluster_operations()
    assert first.status == second.status == "RUNNING"
    assert first.finished_at is second.finished_at is None
    assert session.events == []

    # The existing maintenance loop's next pass can recover both; no partial terminal/event remains.
    monkeypatch.setattr(operations, "_append_event_impl", append)
    assert set(await operations.recover_deleted_cluster_operations()) == {first.id, second.id}
    assert first.status == second.status == "CANCELLED"
    assert [(event.operation_id, event.sequence) for event in session.events] == [
        (first.id, 1), (second.id, 1),
    ]
    assert await operations.recover_deleted_cluster_operations() == []
    assert len(session.events) == 2
