"""Focused endpoint tests for validated durable cluster creation (Roadmap Step 2.3)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from drover.api.clusters import router as clusters_router
from drover.auth import get_os_conn, get_token_info
from drover.models.orm import DroverJob, DroverOperation, DroverOperationEvent


class MemoryStore:
    def __init__(self):
        self.operations = {}  # op_id -> DroverOperation
        self.idemp_index = {}  # (project_id, idemp_key) -> op_id
        self.events = {}  # op_id -> list[DroverOperationEvent]
        self.jobs = {}  # job_id -> DroverJob
        self.clusters = {}  # (project_id, cluster_id) -> dict

    def add_operation(self, op: DroverOperation):
        self.operations[op.id] = op
        if op.idempotency_key:
            self.idemp_index[(op.project_id, op.idempotency_key)] = op.id
        if op.id not in self.events:
            self.events[op.id] = []

    def add_event(self, op_id: str, phase: str, message: str, payload_json: dict | None = None):
        op_events = self.events.setdefault(op_id, [])
        seq = len(op_events) + 1
        ev = DroverOperationEvent(
            id=len(op_events) + 100,
            operation_id=op_id,
            sequence=seq,
            phase=phase,
            message=message,
            payload_json=payload_json,
        )
        op_events.append(ev)
        return ev


@pytest.fixture
def store():
    return MemoryStore()


@pytest.fixture
def test_app(store):
    app = FastAPI()

    async def mock_os_conn():
        conn = MagicMock()
        conn._afterglow_project_id = "proj-test-123"
        conn._afterglow_user_id = "user-test-123"
        return conn

    async def mock_token_info():
        return {"project_id": "proj-test-123", "user_id": "user-test-123", "username": "testuser"}

    app.dependency_overrides[get_os_conn] = mock_os_conn
    app.dependency_overrides[get_token_info] = mock_token_info
    from drover.api.clusters import limiter as clusters_limiter
    clusters_limiter.enabled = False
    app.state.limiter = clusters_limiter
    app.include_router(clusters_router, prefix="/v1/clusters")
    return app


@pytest.fixture
async def async_client(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture(autouse=True)
def mock_durable_services(store, monkeypatch):
    policy_snapshot = {
        "k3s.server_image": {"id": "img-srv-1", "name": "ubuntu"},
        "k3s.server_flavor": {"id": "flv-srv-1", "name": "m1.medium"},
        "k3s.default_agent_flavor": {"id": "flv-agt-1", "name": "m1.small"},
        "k3s.volume_availability_zone": {"id": "nova", "name": "nova"},
    }

    monkeypatch.setattr(
        "drover.services.resource_policy_store.resolve_policy_snapshot",
        AsyncMock(return_value=policy_snapshot),
    )
    monkeypatch.setattr(
        "drover.services.resource_policy_store.get_policy_snapshot",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        "drover.services.resource_policy_store.get_required_runtime_setting",
        AsyncMock(return_value="v1.28.2+k3s1"),
    )
    monkeypatch.setattr(
        "drover.services.instance_orchestration.resolve_default_network",
        AsyncMock(return_value="net-test-1"),
    )

    # Store mocks
    async def mock_create_cluster_record(project_id, cluster_id, data):
        store.clusters[(project_id, cluster_id)] = dict(data)

    monkeypatch.setattr("drover.services.store.create_cluster_record", mock_create_cluster_record)

    # Operations mocks
    async def mock_get_op_by_idemp(session_or_factory, project_id, idemp_key):
        assert session_or_factory is None
        op_id = store.idemp_index.get((project_id, idemp_key))
        if op_id:
            return store.operations.get(op_id)
        return None

    async def mock_get_active_op(session_or_factory, cluster_id, kind=None):
        for op in store.operations.values():
            if op.cluster_id == cluster_id and (kind is None or op.kind == kind):
                return op
        return None

    async def mock_get_operation(session_or_factory, operation_id):
        return store.operations.get(operation_id)

    async def mock_get_operation_events(session_or_factory, operation_id, since_sequence=0):
        evs = store.events.get(operation_id, [])
        return [e for e in evs if e.sequence > since_sequence]

    monkeypatch.setattr("drover.services.operations.get_operation_by_idempotency_key", mock_get_op_by_idemp)
    monkeypatch.setattr("drover.services.operations.get_active_operation", mock_get_active_op)
    monkeypatch.setattr("drover.services.operations.get_operation", mock_get_operation)
    monkeypatch.setattr("drover.services.operations.get_operation_events", mock_get_operation_events)

    # Jobs enqueue mock
    async def mock_enqueue_job(
        cluster_id,
        project_id,
        kind,
        payload,
        user_id=None,
        username=None,
        request_id=None,
        idempotency_key=None,
        request_hash=None,
        op_kind=None,
    ):
        op_id = store.idemp_index.get((project_id, idempotency_key)) if idempotency_key else None
        if not op_id:
            op_id = f"op-{len(store.operations) + 1}"
            op = DroverOperation(
                id=op_id,
                project_id=project_id,
                cluster_id=cluster_id,
                kind=op_kind or "create",
                status="WAITING_CALLBACK",
                request_id=request_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            store.add_operation(op)

            # Record initial job_enqueued event
            store.add_event(op_id, "job_enqueued", f"Job {kind} enqueued", {"job_id": f"job-{op_id}", "kind": kind, "step": "security_group", "progress": 5})

        job_id = f"job-{len(store.jobs) + 1}"
        store.jobs[job_id] = DroverJob(
            id=job_id,
            cluster_id=cluster_id,
            project_id=project_id,
            kind=kind,
            status="queued",
            operation_id=op_id,
        )
        return job_id

    monkeypatch.setattr("drover.services.jobs.enqueue_job", mock_enqueue_job)


async def _parse_sse(response):
    events = []
    import json

    async for line in response.aiter_lines():
        if line.startswith("data: "):
            payload = line[6:].strip()
            if payload:
                events.append(json.loads(payload))
    return events


@pytest.mark.asyncio
async def test_queue_only_behavior(async_client, store):
    """create_k3s_cluster_async queues the job without executing OpenStack calls inside the request."""
    mock_nova = patch("drover.services.nova.create_server", MagicMock())
    mock_neutron = patch("drover.services.neutron.create_security_group", MagicMock())
    mock_cinder = patch("drover.services.cinder.create_volume_from_image", MagicMock())

    with mock_nova as nova_mock, mock_neutron as neutron_mock, mock_cinder as cinder_mock:
        async with async_client.stream(
            "POST",
            "/v1/clusters/async",
            json={"name": "queue-only-cluster", "agent_count": 1},
        ) as response:
            assert response.status_code == 200
            events = await _parse_sse(response)

    # OpenStack creation calls must NOT be executed inside request handler or SSE generator
    nova_mock.assert_not_called()
    neutron_mock.assert_not_called()
    cinder_mock.assert_not_called()

    assert len(events) >= 1
    assert "operation_id" in events[0]
    assert events[0]["operation_id"] != ""


@pytest.mark.asyncio
async def test_same_key_replay(async_client, store):
    """Submitting with identical Idempotency-Key returns original operation without duplicate cluster or job."""
    headers = {"Idempotency-Key": "idemp-key-replay-100"}
    body = {"name": "replay-cluster", "agent_count": 2}

    # First request
    async with async_client.stream(
        "POST",
        "/v1/clusters/async",
        json=body,
        headers=headers,
    ) as resp1:
        assert resp1.status_code == 200
        events1 = await _parse_sse(resp1)

    op_id_1 = events1[0]["operation_id"]
    cluster_id_1 = events1[0]["cluster_id"]
    initial_clusters_count = len(store.clusters)
    initial_jobs_count = len(store.jobs)

    # Second request with SAME Idempotency-Key and SAME body
    async with async_client.stream(
        "POST",
        "/v1/clusters/async",
        json=body,
        headers=headers,
    ) as resp2:
        assert resp2.status_code == 200
        events2 = await _parse_sse(resp2)

    op_id_2 = events2[0]["operation_id"]
    cluster_id_2 = events2[0]["cluster_id"]

    assert op_id_1 == op_id_2
    assert cluster_id_1 == cluster_id_2
    # Ensure no duplicate cluster record or job was enqueued
    assert len(store.clusters) == initial_clusters_count
    assert len(store.jobs) == initial_jobs_count


@pytest.mark.asyncio
async def test_hash_conflict_409(async_client, store):
    """Submitting with same Idempotency-Key but different request payload returns HTTP 409."""
    headers = {"Idempotency-Key": "idemp-key-conflict-200"}

    # Initial request
    async with async_client.stream(
        "POST",
        "/v1/clusters/async",
        json={"name": "cluster-initial", "agent_count": 1},
        headers=headers,
    ) as resp1:
        assert resp1.status_code == 200
        await _parse_sse(resp1)

    # Conflicting request with same Idempotency-Key but DIFFERENT body (agent_count=5)
    resp2 = await async_client.post(
        "/v1/clusters/async",
        json={"name": "cluster-initial", "agent_count": 5},
        headers=headers,
    )
    assert resp2.status_code == 409
    assert "Idempotency key 'idemp-key-conflict-200' reused with a different request hash" in resp2.json()["detail"]


@pytest.mark.asyncio
async def test_sse_operation_id_included(async_client, store):
    """Every event in the SSE progress stream includes operation_id."""
    async with async_client.stream(
        "POST",
        "/v1/clusters/async",
        json={"name": "sse-op-id-cluster", "agent_count": 1},
    ) as response:
        assert response.status_code == 200
        events = await _parse_sse(response)

    assert len(events) >= 1
    for ev in events:
        assert "operation_id" in ev
        assert ev["operation_id"] is not None
        assert len(ev["operation_id"]) > 0


@pytest.mark.asyncio
async def test_disconnected_stream_does_not_cancel_job(async_client, store):
    """Browser/client disconnecting mid-stream does not cancel or delete the queued job."""
    async with async_client.stream(
        "POST",
        "/v1/clusters/async",
        json={"name": "disconnect-cluster", "agent_count": 1},
    ) as response:
        assert response.status_code == 200
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                break
        # Disconnect stream context

    # Verify job and operation remain intact in MemoryStore
    assert len(store.jobs) == 1
    job = list(store.jobs.values())[0]
    assert job.status == "queued"
    op = store.operations.get(job.operation_id)
    assert op is not None
    assert op.status in ("QUEUED", "RUNNING", "WAITING_CALLBACK", "SUCCEEDED")
