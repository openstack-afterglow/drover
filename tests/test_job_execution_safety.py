"""Behavior-defined SQL non-replay and actual SDK physical dispatch fences."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import requests
from keystoneauth1 import session as ks_session
from requests.adapters import HTTPAdapter
from sqlalchemy.dialects import mysql
from test_jobs import _factory, _Session

from drover.models.orm import DroverJob, DroverOperation, K3sCluster
from drover.services import execution, execution_safety, jobs, worker_ownership

pytestmark = pytest.mark.asyncio


def _state(monkeypatch, *, kind="scale", status="running", attempt=1, payload=None):
    job = DroverJob(
        id="job", cluster_id="cluster", project_id="project", kind=kind,
        status=status, attempts=attempt, claimed_at=datetime.now(UTC), payload_json=payload or {},
    )
    cluster = K3sCluster(id="cluster", project_id="project", name="cluster", status="ACTIVE")
    session = _Session(objects={(DroverJob, job.id): job, (K3sCluster, cluster.id): cluster})
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))
    return job, cluster, session


def _guard(job):
    job_id, attempt, kind, cluster_id, project_id = job.id, job.attempts, job.kind, job.cluster_id, job.project_id
    return execution_safety.DispatchGuard(
        lambda: jobs._execution_checkpoint(
            job_id, attempt=attempt, kind=kind, cluster_id=cluster_id, project_id=project_id,
        ),
        ownership_checkpoint=lambda: jobs._execution_checkpoint(
            job_id, attempt=attempt, kind=kind, cluster_id=cluster_id,
            project_id=project_id, mutation=False,
        ),
        transaction_checkpoint=lambda session: jobs._execution_checkpoint(
            job_id, attempt=attempt, kind=kind, cluster_id=cluster_id,
            project_id=project_id, mutation=False, session=session,
        ),
    )


@pytest.fixture(autouse=True)
def _periodic_checkpoint(monkeypatch):
    monkeypatch.setattr(worker_ownership, "checkpoint", AsyncMock())
    monkeypatch.setattr(worker_ownership, "checkpoint_sync", lambda: None)
    yield
    execution_safety.revoke_all_local()


class _Provider(HTTPAdapter):
    def __init__(self, outcome=202):
        super().__init__(max_retries=3)
        self.outcome = outcome
        self.calls = []

    def send(self, request, **kwargs):
        self.calls.append((request.method, self.max_retries.total))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        response = requests.Response()
        response.status_code = self.outcome
        response.url = request.url
        response.request = request
        response._content = b"{}"
        return response


def _connection(guard, provider):
    session = ks_session.Session()
    session.session.mount("http://", provider)
    conn = SimpleNamespace(session=session)
    with execution_safety.bound(guard):
        execution_safety.protect_connection(conn)
    return conn


async def _request(conn, method="POST", url="http://fixture/resource", **kwargs):
    return await asyncio.to_thread(
        conn.session.request, url, method, authenticated=False, **kwargs,
    )


@pytest.mark.parametrize("outcome", [202, requests.exceptions.ConnectionError("response lost"), 503, 307])
async def test_accepted_unknown_and_redirect_outcomes_never_requeue_mutation(monkeypatch, outcome):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider(outcome)
    conn = _connection(guard, provider)
    try:
        try:
            await _request(conn, connect_retries=5, status_code_retries=5, redirect=True)
        except Exception:
            pass
        assert job.payload_json[jobs._EXECUTION_START] == {"attempt": 1}
        assert provider.calls == [("POST", 0)]
        assert await jobs._retry_or_fail(job.id, attempt=1, error="provider outcome uncertain")
        assert job.status == "failed"
        assert "automatic replay disabled" in job.last_error
        assert "inspect provider resources" in job.last_error
    finally:
        guard.close()
        conn.session.session.close()


async def test_accepted_distinct_mutations_continue_only_in_live_same_attempt(monkeypatch):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider()
    conn = _connection(guard, provider)
    try:
        await _request(conn)
        await _request(conn, url="http://fixture/other-resource")
        assert len(provider.calls) == 2
        job.attempts = 2
        with pytest.raises(execution_safety.OwnershipLost):
            await _request(conn)
        assert len(provider.calls) == 2
    finally:
        guard.close()


async def test_swallowed_unknown_send_cannot_issue_later_mutation(monkeypatch):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider(requests.exceptions.ConnectionError("unknown response"))
    conn = _connection(guard, provider)
    try:
        with pytest.raises(Exception):
            await _request(conn)
        provider.outcome = 202
        with pytest.raises(execution_safety.OwnershipLost):
            await _request(conn)
        assert len(provider.calls) == 1
    finally:
        guard.close()


@pytest.mark.parametrize("change", ["attempt", "expired", "failed", "cluster", "project", "operation"])
async def test_sql_former_owner_is_fenced_before_next_physical_mutation(monkeypatch, change):
    job, cluster, session = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider()
    conn = _connection(guard, provider)
    try:
        await _request(conn)
        if change == "attempt":
            job.attempts += 1
        elif change == "expired":
            job.claimed_at -= timedelta(seconds=jobs._LEASE_SECONDS + 1)
        elif change == "failed":
            job.status = "failed"
        elif change == "cluster":
            cluster.deleted_at = datetime.now(UTC)
        elif change == "project":
            cluster.project_id = "foreign"
        else:
            job.operation_id = "operation"
            session.objects[(DroverOperation, "operation")] = DroverOperation(
                id="operation", project_id="project", cluster_id="cluster", kind="scale", status="CANCELLED",
            )
        with pytest.raises(execution_safety.OwnershipLost):
            await _request(conn)
        assert len(provider.calls) == 1
    finally:
        guard.close()


async def test_expired_attempt_cannot_resurrect_with_heartbeat_or_complete(monkeypatch):
    job, _, _ = _state(monkeypatch)
    job.claimed_at -= timedelta(seconds=jobs._LEASE_SECONDS + 1)
    previous = job.claimed_at
    assert not await jobs._renew_lease(job.id, attempt=1)
    assert not await jobs._complete(job.id, attempt=1)
    assert job.claimed_at == previous


@pytest.mark.parametrize("heartbeat_error", [False, True])
async def test_heartbeat_loss_revokes_and_cancels_running_dispatch(monkeypatch, heartbeat_error):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def dispatch():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    task = asyncio.create_task(dispatch())
    await started.wait()
    monkeypatch.setattr(jobs, "_renew_lease", AsyncMock(side_effect=RuntimeError("SQL down")) if heartbeat_error else AsyncMock(return_value=False))
    original_wait_for = asyncio.wait_for

    async def timeout_wait(awaitable, timeout):
        return await original_wait_for(awaitable, timeout=0.001)

    monkeypatch.setattr(jobs.asyncio, "wait_for", timeout_wait)
    try:
        await jobs._heartbeat_lease(job.id, attempt=1, stop=asyncio.Event(), guard=guard, dispatch=task)
        await asyncio.gather(task, return_exceptions=True)
        assert cancelled.is_set()
        with pytest.raises(execution_safety.OwnershipLost):
            guard.check()
    finally:
        guard.close()


@pytest.mark.parametrize("payload", [{}, {jobs._EXECUTION_START: {"attempt": 1}}, {jobs._EXECUTION_START: None}])
async def test_expired_mutation_lease_is_terminal_not_reclaimed(monkeypatch, payload):
    job, _, session = _state(monkeypatch, payload=payload)
    job.claimed_at -= timedelta(seconds=jobs._LEASE_SECONDS + 1)
    session.execute_values = [job, None, None]
    assert await jobs._claim_one() is None
    assert job.status == "failed"
    assert job.attempts == 1
    assert "replay" in job.last_error


async def test_read_only_provider_retry_and_sql_recovery_remain_safe(monkeypatch):
    job, _, session = _state(monkeypatch, kind="reconcile")
    guard = _guard(job)
    provider = _Provider(200)
    conn = _connection(guard, provider)
    try:
        await _request(conn, method="GET", connect_retries=2, status_code_retries=2)
        assert provider.calls == [("GET", 3)]
        assert jobs._EXECUTION_START not in job.payload_json
        with execution_safety.bound(guard):
            await execution_safety.ownership_checkpoint()
        assert await jobs._retry_or_fail(job.id, attempt=1, error="temporary read failure")
        assert job.status == "queued"
        session.execute_values = [job, None]
        assert (await jobs._claim_one())[1] == 2
    finally:
        guard.close()


async def test_pre_dispatch_authority_unavailable_can_retry_without_marker(monkeypatch):
    job, _, _ = _state(monkeypatch)
    monkeypatch.setattr(jobs, "_claim_one", AsyncMock(return_value=(job.id, 1, job.kind, job.cluster_id, job.project_id, {})))
    monkeypatch.setattr(jobs, "_execute_job_direct", AsyncMock(side_effect=execution.ExecutionAuthorityUnavailable("directory unavailable")))
    assert await jobs.process_one_job()
    assert job.status == "queued"
    assert jobs._EXECUTION_START not in job.payload_json


async def test_token_authentication_does_not_cross_mutation_boundary(monkeypatch):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider(201)
    conn = _connection(guard, provider)
    try:
        await _request(conn, url="http://fixture/v3/auth/tokens")
        assert jobs._EXECUTION_START not in job.payload_json
    finally:
        guard.close()


async def test_read_only_job_cannot_dispatch_mutation(monkeypatch):
    job, _, _ = _state(monkeypatch, kind="reconcile")
    guard = _guard(job)
    provider = _Provider()
    conn = _connection(guard, provider)
    try:
        with pytest.raises(execution_safety.OwnershipLost):
            await _request(conn)
        assert not provider.calls
    finally:
        guard.close()


@pytest.mark.parametrize("key", [jobs._EXECUTION_START, "_delegation_id", "_operation_id"])
async def test_enqueue_rejects_caller_controlled_execution_metadata(monkeypatch, key):
    with pytest.raises(ValueError, match="owned by the worker"):
        await jobs.enqueue_job("cluster", "project", "scale", {key: "caller"})


async def test_claim_uses_durable_delegation_not_payload_identity(monkeypatch):
    job, _, session = _state(monkeypatch, status="queued", attempt=0, payload={"_delegation_id": "forged", "_operation_id": "forged"})
    session.execute_values = [job, None]
    claimed = await jobs._claim_one()
    assert "_delegation_id" not in claimed[-1]
    assert "_operation_id" not in claimed[-1]
    sql = str(session.statements[0].compile(dialect=mysql.dialect())).upper()
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "EXISTS" in sql
    assert "CLUSTER_ID" in sql


async def test_cluster_competing_claim_does_not_dispatch_second_job(monkeypatch):
    job, _, session = _state(monkeypatch, status="queued", attempt=0)
    session.execute_values = [job, "other-live-attempt"]
    assert await jobs._claim_one() is None
    assert job.status == "queued"
    assert job.attempts == 0


async def test_cancel_after_started_drains_inflight_sdk_and_prevents_later_send(monkeypatch):
    job, _, _ = _state(monkeypatch)
    entered = asyncio.Event()
    released = threading.Event()
    finished = threading.Event()
    sends = []

    class BlockingProvider(_Provider):
        def send(self, request, **kwargs):
            sends.append(request.method)
            loop.call_soon_threadsafe(entered.set)
            released.wait()
            try:
                return super().send(request, **kwargs)
            finally:
                finished.set()

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(jobs, "_claim_one", AsyncMock(return_value=(job.id, 1, job.kind, job.cluster_id, job.project_id, {})))
    saved = []

    async def dispatch(*_args):
        conn = SimpleNamespace(session=ks_session.Session())
        conn.session.session.mount("http://", BlockingProvider())
        execution_safety.protect_connection(conn)
        saved.append(conn)
        await _request(conn)

    monkeypatch.setattr(jobs, "_execute_job_direct", dispatch)
    task = asyncio.create_task(jobs.process_one_job())
    try:
        await entered.wait()
        execution_safety.revoke_all_local()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        released.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        assert job.status == "failed"
        with pytest.raises(execution_safety.OwnershipLost):
            await _request(saved[0])
        assert sends == ["POST"]
    finally:
        released.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("outcome", [503, 307, "unknown"])
async def test_kubernetes_ambiguity_revokes_later_mutations(monkeypatch, outcome):
    job, _, _ = _state(monkeypatch, kind="reauthorize")
    guard = _guard(job)
    calls = []

    async def provider(request):
        calls.append(request.method)
        if outcome == "unknown":
            raise httpx.ReadError("response lost", request=request)
        return httpx.Response(outcome, json={}, headers={"Location": "http://fixture/redirect"})

    try:
        with execution_safety.bound(guard):
            async with httpx.AsyncClient(transport=httpx.MockTransport(provider), follow_redirects=True) as client:
                execution_safety.protect_httpx_client(client)
                try:
                    await client.post("http://fixture/secret")
                except httpx.ReadError:
                    pass
                with pytest.raises(execution_safety.OwnershipLost):
                    await client.put("http://fixture/configmap")
        assert calls == ["POST"]
        assert jobs._EXECUTION_START in job.payload_json
    finally:
        guard.close()


async def test_same_loop_sync_mutation_fails_closed_without_deadlock(monkeypatch):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider()
    conn = _connection(guard, provider)
    try:
        with pytest.raises(execution_safety.OwnershipLost, match="worker thread"):
            conn.session.request("http://fixture/resource", "POST", authenticated=False)
        assert not provider.calls
    finally:
        guard.close()


async def test_periodic_cleanup_delete_checks_ownership_at_physical_send(monkeypatch):
    provider = _Provider(204)
    session = ks_session.Session()
    session.session.mount("http://", provider)
    execution_safety.install_session_guard(session)
    monkeypatch.setattr(worker_ownership, "checkpoint_sync", lambda: (_ for _ in ()).throw(execution_safety.OwnershipLost("periodic lease lost")))
    with pytest.raises(execution_safety.OwnershipLost):
        await asyncio.to_thread(session.request, "http://fixture/v3/OS-TRUST/trusts/trust", "DELETE", authenticated=False)
    assert not provider.calls


async def test_local_expiry_blocks_delayed_physical_dispatch(monkeypatch):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider()
    conn = _connection(guard, provider)
    try:
        guard.renewed(-1)
        with pytest.raises(execution_safety.OwnershipLost, match="deadline expired"):
            await _request(conn)
        assert not provider.calls
        assert jobs._EXECUTION_START not in job.payload_json
    finally:
        guard.close()


@pytest.mark.parametrize("field,value", [("kind", "delete"), ("cluster_id", "foreign"), ("project_id", "foreign")])
async def test_durable_job_source_change_does_not_replace_claimed_identity(monkeypatch, field, value):
    job, _, _ = _state(monkeypatch)
    guard = _guard(job)
    provider = _Provider()
    conn = _connection(guard, provider)
    try:
        setattr(job, field, value)
        with pytest.raises(execution_safety.OwnershipLost):
            await _request(conn)
        assert not provider.calls
    finally:
        guard.close()


@pytest.mark.parametrize("authority_failure", [False, True])
async def test_authority_failure_is_separate_from_uncertain_terminal_outcome(monkeypatch, authority_failure):
    payload = {} if authority_failure else {jobs._EXECUTION_START: {"attempt": 1}}
    job, _, _ = _state(monkeypatch, payload=payload)
    mark = AsyncMock()
    monkeypatch.setattr(jobs, "_mark_cluster_failed", mark)
    assert await jobs._retry_or_fail(
        job.id, attempt=1, error="stopped", terminal=authority_failure, authority_failure=authority_failure,
    )
    assert job.status == "failed"
    assert mark.await_args.kwargs["authority_failure"] is authority_failure


async def test_pre_dispatch_cancellation_can_retry_without_mutation_boundary(monkeypatch):
    job, _, _ = _state(monkeypatch)
    entered = asyncio.Event()

    async def dispatch(*_args):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(jobs, "_claim_one", AsyncMock(return_value=(job.id, 1, job.kind, job.cluster_id, job.project_id, {})))
    monkeypatch.setattr(jobs, "_execute_job_direct", dispatch)
    task = asyncio.create_task(jobs.process_one_job())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert job.status == "queued"
    assert jobs._EXECUTION_START not in job.payload_json


async def test_cinder_post_acceptance_then_blocked_get_poll_drains_before_close(monkeypatch):
    from drover.services import cinder

    job, _, _ = _state(monkeypatch)
    polling = asyncio.Event()
    release = threading.Event()
    get_finished = threading.Event()
    closed = asyncio.Event()
    loop = asyncio.get_running_loop()
    sessions = []

    class PollProvider(_Provider):
        def send(self, request, **kwargs):
            if request.method == "GET":
                loop.call_soon_threadsafe(polling.set)
                release.wait()
                try:
                    return super().send(request, **kwargs)
                finally:
                    get_finished.set()
            return super().send(request, **kwargs)

    async def dispatch(*_args):
        session = ks_session.Session()
        session.session.mount("http://", PollProvider())
        sessions.append(session)
        volume = SimpleNamespace(id="volume", name="boot", status="available", size=1,
                                 volume_type="standard", attachments=[])

        def create_volume(**_kwargs):
            session.request("http://fixture/volumes", "POST", authenticated=False)
            return volume

        def wait_for_status(value, **_kwargs):
            session.request("http://fixture/volumes/volume", "GET", authenticated=False)
            return value

        conn = SimpleNamespace(session=session, block_storage=SimpleNamespace(
            create_volume=create_volume, wait_for_status=wait_for_status,
        ))
        execution_safety.protect_connection(conn)
        try:
            await worker_ownership.run_sync(cinder.create_volume_from_image, conn, "boot", "image", 1)
        finally:
            assert get_finished.is_set(), "Cancelled SDK polling was abandoned before close"
            session.session.close()
            closed.set()

    monkeypatch.setattr(jobs, "_claim_one", AsyncMock(return_value=(job.id, 1, job.kind, job.cluster_id, job.project_id, {})))
    monkeypatch.setattr(jobs, "_execute_job_direct", dispatch)
    task = asyncio.create_task(jobs.process_one_job())
    try:
        await polling.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not closed.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert get_finished.is_set() and closed.is_set()
        assert job.status == "failed"
        assert jobs._EXECUTION_START in job.payload_json
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        for session in sessions:
            session.session.close()


async def test_cancelled_authentication_factory_drains_and_closes_produced_connection(monkeypatch):
    entered = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    conn = SimpleNamespace(session=ks_session.Session())
    loop = asyncio.get_running_loop()
    close = AsyncMock()

    def authenticate():
        loop.call_soon_threadsafe(entered.set)
        release.wait()
        finished.set()
        return conn

    async def opening(_project):
        return await worker_ownership.run_sync(authenticate)

    monkeypatch.setattr(execution, "_open_connection", opening)
    monkeypatch.setattr("drover.services.keystone.close_connection", close)
    task = asyncio.create_task(execution.open_connection("project"))
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        close.assert_not_awaited()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        close.assert_awaited_once_with(conn)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        conn.session.session.close()


async def test_connection_result_is_closed_when_ownership_lost_during_open(monkeypatch):
    conn = SimpleNamespace(session=ks_session.Session())
    close = AsyncMock()
    monkeypatch.setattr(execution, "_open_connection", AsyncMock(return_value=conn))
    monkeypatch.setattr(execution_safety, "ownership_checkpoint", AsyncMock(side_effect=execution_safety.OwnershipLost("lost while authenticating")))
    monkeypatch.setattr("drover.services.keystone.close_connection", close)
    with pytest.raises(execution_safety.OwnershipLost):
        await execution.open_connection("project")
    close.assert_awaited_once_with(conn)
    conn.session.session.close()


async def test_stale_claim_cannot_enter_native_dispatch(monkeypatch):
    job, cluster, _ = _state(monkeypatch, kind="provision_agents")
    guard = _guard(job)
    dispatch = AsyncMock()
    monkeypatch.setattr(jobs, "_dispatch_job", dispatch)
    job.attempts = 2
    cluster.status = "ERROR"
    try:
        with execution_safety.bound(guard), pytest.raises(execution_safety.OwnershipLost):
            await jobs._execute_job_direct("provision_agents", {"_delegation_id": "delegation"}, "cluster", "project")
        dispatch.assert_not_awaited()
        assert cluster.status == "ERROR"
    finally:
        guard.close()


async def test_zero_agent_activation_revalidates_attempt_inside_its_write_transaction(monkeypatch):
    from drover.services import provisioner, store

    job, cluster, _ = _state(monkeypatch, kind="provision_agents")
    guard = _guard(job)
    cluster.status = "ERROR"
    job.operation_id = "operation"
    operation = DroverOperation(id="operation", cluster_id="cluster", project_id="project", kind="create", status="RUNNING")

    class TakeoverSession(_Session):
        entries = 0

        async def __aenter__(self):
            self.entries += 1
            if self.entries == 2:
                # Outside validation passed; reclaim wins before the write starts.
                job.attempts = 2
                operation.status = "FAILED"
            return self

    session = TakeoverSession(execute_values=[cluster], objects={
        (DroverJob, job.id): job,
        (K3sCluster, cluster.id): cluster,
        (DroverOperation, operation.id): operation,
    })
    session.commit = AsyncMock()
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))
    monkeypatch.setattr(store, "get_session_factory", lambda: _factory(session))
    monkeypatch.setattr(store, "is_db_available", lambda: True)
    monkeypatch.setattr(store, "get_cluster", AsyncMock(return_value={
        "id": "cluster", "project_id": "project", "agent_count": 0, "status": "ERROR",
    }))
    try:
        with execution_safety.bound(guard), pytest.raises(execution_safety.OwnershipLost):
            await provisioner.provision_agents("project", "cluster", "server-ip", "node-token")
        assert session.entries == 2
        assert cluster.status == "ERROR"
        assert operation.status == "FAILED"
        session.commit.assert_not_awaited()
    finally:
        guard.close()
