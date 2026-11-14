"""Focused endpoint tests for validated durable cluster creation (Roadmap Step 2.3)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.usefixtures("admitted_authority")

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from drover.api.clusters import router as clusters_router
from drover.auth import get_os_conn, get_token_info
from drover.models.orm import DroverClusterCredential, DroverJob, DroverOperation, DroverOperationEvent


class MemoryStore:
    def __init__(self):
        self.operations = {}  # op_id -> DroverOperation
        self.idemp_index = {}  # (project_id, idemp_key) -> op_id
        self.events = {}  # op_id -> list[DroverOperationEvent]
        self.jobs = {}  # job_id -> DroverJob
        self.clusters = {}  # (project_id, cluster_id) -> dict
        self.credentials = []

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

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def begin(self):
        return self

    def add(self, row):
        assert isinstance(row, DroverClusterCredential)
        self.credentials.append(row)


@pytest.fixture
def store():
    return MemoryStore()


@pytest.fixture
def test_app(store):
    app = FastAPI()
    conn = MagicMock()
    conn._afterglow_project_id = "proj-test-123"
    conn._afterglow_user_id = "user-test-123"
    app.state.caller_conn = conn
    networks = {
        "external-net": SimpleNamespace(id="external-net", name="public", is_router_external=True, is_shared=False),
        "private-net": SimpleNamespace(id="private-net", name="private", is_router_external=False, is_shared=False),
    }
    conn.network.get_network.side_effect = networks.get
    from keystoneauth1.exceptions.http import NotFound
    from openstack.network.v2.quota import QuotaDetails

    conn.network.get_quota.return_value = QuotaDetails(
        security_group={"limit": 10, "used": 1, "reserved": 0},
        security_group_rule={"limit": 100, "used": 4, "reserved": 0},
    )
    conn.network.get.side_effect = NotFound("legacy rule defaults")

    async def mock_os_conn():
        return conn

    async def mock_token_info():
        return {"project_id": "proj-test-123", "user_id": "user-test-123", "username": "testuser",
                "roles": ["member", "reader", "drover-inventory_reader", "drover-clusters_editor", "drover-access_admin"]}

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
    monkeypatch.setattr("drover.db.get_session_factory", lambda: lambda: store)
    policy_snapshot = {
        "k3s.server_image": {"id": "img-srv-1", "name": "ubuntu"},
        "k3s.server_flavor": {"id": "flv-srv-1", "name": "m1.medium"},
        "k3s.default_agent_flavor": {"id": "flv-agt-1", "name": "m1.small"},
        "k3s.volume_availability_zone": {"id": "nova", "name": "nova"},
    }

    async def resolve_snapshot(*, conn, keys):
        if keys == ("k3s.default_network",):
            return {"k3s.default_network": {"id": "external-net", "name": "public"}}
        return {key: dict(value) for key, value in policy_snapshot.items()}

    monkeypatch.setattr("drover.services.resource_policy_store.resolve_policy_snapshot", resolve_snapshot)
    monkeypatch.setattr(
        "drover.services.resource_policy_store.get_policy_snapshot",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        "drover.services.resource_policy_store.get_required_runtime_setting",
        AsyncMock(return_value="v1.28.2+k3s1"),
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
        *,
        session=None,
        delegation=None,
        delegation_id=None,
    ):
        assert session is store
        assert delegation.project_id == project_id and delegation.cluster_id == cluster_id
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
            payload_json=dict(payload),
            operation_id=op_id,
            delegation_id=delegation.id,
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
@pytest.mark.parametrize("stampede_enabled,expected", [(None, True), (False, False), (True, True)])
@pytest.mark.parametrize("agent_count", [0, 1])
async def test_queue_only_behavior(async_client, store, stampede_enabled, expected, agent_count):
    """create_k3s_cluster_async queues the job without executing OpenStack calls inside the request."""
    mock_nova = patch("drover.services.nova.create_server", MagicMock())
    mock_neutron = patch("drover.services.neutron.create_security_group", MagicMock())
    mock_cinder = patch("drover.services.cinder.create_volume_from_image", MagicMock())
    body = {"name": "queue-only-cluster", "agent_count": agent_count}
    if stampede_enabled is not None:
        body["stampede_enabled"] = stampede_enabled

    with mock_nova as nova_mock, mock_neutron as neutron_mock, mock_cinder as cinder_mock:
        async with async_client.stream(
            "POST",
            "/v1/clusters/async",
            json=body,
        ) as response:
            assert response.status_code == 200
            events = await _parse_sse(response)

    # OpenStack creation calls must NOT be executed inside request handler or SSE generator
    nova_mock.assert_not_called()
    neutron_mock.assert_not_called()
    cinder_mock.assert_not_called()
    cluster = next(iter(store.clusters.values()))
    job = next(iter(store.jobs.values())).payload_json
    for data in (cluster, job):
        assert data["stampede_enabled"] is expected
        assert data["agent_flavor_id"] == "flv-agt-1"
        assert data["agent_count"] == agent_count

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
    assert "drover_pre_admission_refused" not in resp2.text


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


async def test_unknown_keypair_prevents_enqueue(async_client, store, test_app):
    """A caller-visible missing keypair is rejected before durable state is created."""
    test_app.state.caller_conn.compute.find_keypair.return_value = None

    response = await async_client.post(
        "/v1/clusters/async",
        json={"name": "missing-key", "key_name": "caller-key"},
    )

    assert response.status_code == 400
    assert store.clusters == {}
    assert store.jobs == {}


async def test_public_key_snapshot_survives_durable_queue(async_client, store, test_app):
    """The first caller key snapshot survives idempotent replay without a new job."""
    headers = {"Idempotency-Key": "ssh-key-snapshot-replay"}
    body = {"name": "snapshotted-key", "key_name": "caller-key"}
    test_app.state.caller_conn.compute.find_keypair.return_value = MagicMock(
        public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 caller@example"
    )

    async with async_client.stream("POST", "/v1/clusters/async", json=body, headers=headers) as response:
        assert response.status_code == 200
        await _parse_sse(response)

    expected_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5"
    cluster = next(iter(store.clusters.values()))
    job = next(iter(store.jobs.values()))
    assert cluster["ssh_public_key"] == expected_key
    assert job.payload_json["ssh_public_key"] == expected_key

    test_app.state.caller_conn.compute.find_keypair.return_value = MagicMock(
        public_key="ssh-ed25519 changed-key-material"
    )
    async with async_client.stream("POST", "/v1/clusters/async", json=body, headers=headers) as response:
        assert response.status_code == 200
        await _parse_sse(response)

    assert len(store.clusters) == 1
    assert len(store.jobs) == 1
    assert next(iter(store.jobs.values())).payload_json["ssh_public_key"] == expected_key


async def test_multiline_keypair_prevents_enqueue(async_client, store, test_app):
    """Caller key material that could extend userdata is rejected before enqueue."""
    test_app.state.caller_conn.compute.find_keypair.return_value = MagicMock(
        public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 caller@example\nssh-rsa injected"
    )

    response = await async_client.post(
        "/v1/clusters/async",
        json={"name": "invalid-key", "key_name": "caller-key"},
    )
    assert response.status_code == 400
    assert store.clusters == {}
    assert store.jobs == {}


@pytest.mark.asyncio
async def test_explicit_private_network_rejected_before_durable_writes(async_client, store):
    response = await async_client.post(
        "/v1/clusters/async", json={"name": "private-cluster", "network_id": "private-net"}
    )

    assert response.status_code == 400
    assert store.clusters == {}
    assert store.jobs == {}
    assert store.operations == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("network_id", [None, "", "external-net"])
async def test_external_network_is_snapshotted_in_cluster_and_job(async_client, store, network_id):
    body = {"name": "external-cluster", "agent_count": 1}
    if network_id is not None:
        body["network_id"] = network_id

    response = await async_client.post("/v1/clusters/async", json=body)

    assert response.status_code == 200
    cluster = next(iter(store.clusters.values()))
    job = next(iter(store.jobs.values())).payload_json
    for data in (cluster, job):
        assert data["network_id"] == "external-net"
        assert data["resource_policy_snapshot"]["k3s.default_network"] == {
            "id": "external-net", "name": "public"
        }


@pytest.mark.asyncio
async def test_default_network_policy_failure_prevents_enqueue(async_client, store, monkeypatch):
    from drover.services import resource_policy_store
    from drover.services.resource_policies import ResourcePolicyValidationError

    original_resolve = resource_policy_store.resolve_policy_snapshot

    async def resolve_snapshot(*, conn, keys):
        if keys == ("k3s.default_network",):
            raise ResourcePolicyValidationError("default network policy is invalid")
        return await original_resolve(conn=conn, keys=keys)

    monkeypatch.setattr(resource_policy_store, "resolve_policy_snapshot", resolve_snapshot)
    response = await async_client.post("/v1/clusters/async", json={"name": "no-default"})

    assert response.status_code == 503
    assert store.clusters == {}
    assert store.jobs == {}

@pytest.mark.asyncio
async def test_default_network_lookup_unavailable_prevents_enqueue(async_client, store, monkeypatch):
    from drover.services import resource_policy_store

    original_resolve = resource_policy_store.resolve_policy_snapshot

    async def resolve_snapshot(*, conn, keys):
        if keys == ("k3s.default_network",):
            raise RuntimeError("password=not-for-response")
        return await original_resolve(conn=conn, keys=keys)

    monkeypatch.setattr(resource_policy_store, "resolve_policy_snapshot", resolve_snapshot)
    response = await async_client.post("/v1/clusters/async", json={"name": "network-outage"})

    assert response.status_code == 503
    assert "not-for-response" not in response.text
    assert store.clusters == {}
    assert store.jobs == {}


@pytest.mark.asyncio
async def test_network_lookup_failure_is_sanitized(async_client, store, test_app):
    test_app.state.caller_conn.network.get_network.side_effect = RuntimeError("secret-password=example")

    response = await async_client.post(
        "/v1/clusters/async", json={"name": "lookup-unavailable", "network_id": "external-net"}
    )

    assert response.status_code == 503
    assert "secret-password" not in response.text
    assert "drover_pre_admission_refused" not in response.text
    assert store.clusters == {}
    assert store.jobs == {}


@pytest.mark.asyncio
async def test_queued_create_without_network_never_reaches_nova():
    from drover.services.provisioner import create_cluster_job

    with (
        patch("drover.services.execution.open_connection", new=AsyncMock(return_value=MagicMock())),
        patch("drover.services.keystone.close_connection", new=AsyncMock()),
        patch("drover.services.store.update_cluster_status", new=AsyncMock()),
        patch("drover.services.neutron.create_security_group") as create_security_group,
        patch("drover.services.nova.create_server") as create_server,
    ):
        with pytest.raises(RuntimeError, match="Creation-time network snapshot is missing"):
            await create_cluster_job("project", "cluster", {"name": "legacy-job"})

    create_security_group.assert_not_called()
    create_server.assert_not_called()



@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["security_group", "security_group_rule"])
async def test_native_security_quota_shortage_never_issues_or_queues(
    async_client, store, test_app, resource,
):
    from openstack.network.v2.quota import QuotaDetails

    entries = {
        "security_group": {"limit": 10, "used": 1, "reserved": 0},
        "security_group_rule": {"limit": 100, "used": 4, "reserved": 0},
    }
    entries[resource]["reserved"] = entries[resource]["limit"] - entries[resource]["used"]
    test_app.state.caller_conn.network.get_quota.return_value = QuotaDetails(**entries)
    with patch("drover.api.delegated.issue_credentials", new=AsyncMock()) as issue:
        response = await async_client.post("/v1/clusters/async", json={"name": "quota-shortage"})

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "drover_pre_admission_refused"
    assert detail["reason"] == "security_group_quota_exceeded"
    assert resource in detail["message"]
    assert "proj-test-123" in detail["message"]
    assert "reserved=" in detail["message"]
    assert detail["quota"] == {
        "project_id": "proj-test-123",
        "resource": resource,
        "required": 1 if resource == "security_group" else 12,
        "available": 0,
        **entries[resource],
    }
    issue.assert_not_awaited()
    assert store.clusters == store.jobs == store.operations == {}
    assert store.credentials == []


@pytest.mark.asyncio
async def test_native_security_quota_unknown_never_issues_or_queues(async_client, store, test_app):
    test_app.state.caller_conn.network.get_quota.side_effect = RuntimeError("secret-token=not-for-response")
    with patch("drover.api.delegated.issue_credentials", new=AsyncMock()) as issue:
        response = await async_client.post("/v1/clusters/async", json={"name": "quota-unknown"})

    assert response.status_code == 503
    assert "not-for-response" not in response.text
    assert response.json()["detail"] == {
        "code": "drover_pre_admission_refused",
        "reason": "security_group_quota_unavailable",
        "project_id": "proj-test-123",
        "message": "Neutron security-group quota or default-rule budget is unavailable; no creation was queued",
    }
    issue.assert_not_awaited()
    assert store.clusters == store.jobs == store.operations == {}


@pytest.mark.asyncio
async def test_idempotent_receipt_does_not_readmit_security_quota(async_client, store, test_app):
    headers = {"Idempotency-Key": "quota-receipt"}
    body = {"name": "quota-receipt"}
    first = await async_client.post("/v1/clusters/async", headers=headers, json=body)
    test_app.state.caller_conn.network.get_quota.side_effect = AssertionError("receipt must not readmit")
    second = await async_client.post("/v1/clusters/async", headers=headers, json=body)

    assert first.status_code == second.status_code == 200
    assert len(store.clusters) == len(store.jobs) == len(store.operations) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,status", [("shortage", 409), ("unavailable", 503)])
async def test_native_http_pre_admission_marker_has_no_trust_queue_or_resource_write(
    async_client, store, test_app, failure, status,
):
    from test_native_security_group_quota_http import PROJECT, native_neutron

    with native_neutron() as (conn, state):
        test_app.state.caller_conn.network = conn.network
        test_app.state.caller_conn._afterglow_project_id = PROJECT
        # Existing token-info override is the same synthetic target, not an
        # administrator rescope or alternate project executor.
        async def token_info():
            return {"project_id": PROJECT, "user_id": "user-test-123", "roles": ["member"]}

        test_app.dependency_overrides[get_token_info] = token_info
        if failure == "shortage":
            state["quota"]["security_group"]["limit"] = 1
        else:
            state["quota_status"] = 503
        with (
            patch("drover.api.delegated.admitted_operation") as admit,
            patch("drover.api.delegated.issue_credentials", new=AsyncMock()) as issue,
        ):
            response = await async_client.post("/v1/clusters/async", json={"name": "no-native-write"})
        assert response.status_code == status
        detail = response.json()["detail"]
        assert detail["code"] == "drover_pre_admission_refused"
        assert detail["message"]
        assert all(method == "GET" for method, _ in state["requests"])
        assert ("GET", f"/v2.0/quotas/{PROJECT}/details") in state["requests"]
        admit.assert_not_called()
        issue.assert_not_awaited()
        assert store.clusters == store.jobs == store.operations == {}
        assert store.credentials == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,status", [("shortage", 409), ("unavailable", 503)])
async def test_overlapping_keyed_refusal_cannot_claim_no_operation_exists(
    async_client, store, test_app, monkeypatch, failure, status,
):
    from openstack.network.v2.quota import QuotaDetails

    from drover.services import jobs, operations, provisioner

    original_lookup = operations.get_operation_by_idempotency_key
    original_enqueue = jobs.enqueue_job
    original_preflight = provisioner.preflight_cluster_security_group
    both_missed = asyncio.Event()
    accepted = asyncio.Event()
    lookups = 0
    preflights = 0

    async def simultaneous_miss(*args):
        nonlocal lookups
        lookups += 1
        if lookups <= 2:
            if lookups == 2:
                both_missed.set()
            await both_missed.wait()
            return None
        return await original_lookup(*args)

    async def enqueue_then_release(*args, **kwargs):
        result = await original_enqueue(*args, **kwargs)
        accepted.set()
        return result

    async def quota_after_other_accepts(*args):
        nonlocal preflights
        preflights += 1
        if preflights == 2:
            await accepted.wait()
            if failure == "shortage":
                test_app.state.caller_conn.network.get_quota.return_value = QuotaDetails(
                    security_group={"limit": 1, "used": 1, "reserved": 0},
                    security_group_rule={"limit": 100, "used": 4, "reserved": 0},
                )
            else:
                test_app.state.caller_conn.network.get_quota.side_effect = RuntimeError("quota read unavailable")
        await original_preflight(*args)

    monkeypatch.setattr(operations, "get_operation_by_idempotency_key", simultaneous_miss)
    monkeypatch.setattr(jobs, "enqueue_job", enqueue_then_release)
    monkeypatch.setattr(provisioner, "preflight_cluster_security_group", quota_after_other_accepts)
    headers = {"Idempotency-Key": "overlapping-key"}
    body = {"name": "shared-operation"}
    first, second = await asyncio.gather(
        async_client.post("/v1/clusters/async", headers=headers, json=body),
        async_client.post("/v1/clusters/async", headers=headers, json=body),
    )
    assert sorted([first.status_code, second.status_code]) == [200, status]
    refused = first if first.status_code == status else second
    detail = refused.json()["detail"]
    assert detail["reason"] == f"security_group_quota_{'exceeded' if failure == 'shortage' else 'unavailable'}"
    assert detail["message"]
    assert "code" not in detail
    assert "pre_admission" not in refused.text
    assert "no creation was queued" not in refused.text
    assert len(store.clusters) == len(store.jobs) == len(store.operations) == 1
    receipt = await operations.get_operation_by_idempotency_key(None, "proj-test-123", headers["Idempotency-Key"])
    assert receipt is not None
    assert receipt.id == next(iter(store.operations))
    # Readback of the same key reuses the admitted operation even though quota
    # is now unavailable/exhausted; it does not queue or repeat provider writes.
    replay = await async_client.post("/v1/clusters/async", headers=headers, json=body)
    assert replay.status_code == 200
    assert len(store.clusters) == len(store.jobs) == len(store.operations) == 1


_PROFILE = {
    "network_id": "external-net",
    "ssh_cidrs": ["172.30.0.50/32"],
    "api_cidrs": ["172.30.0.11/32", "172.30.0.12/32"],
    "nodeport_cidrs": ["172.30.0.11/32"],
    "http_cidrs": [],
}


@pytest.fixture
def cluster_profile(monkeypatch):
    monkeypatch.setattr(
        "drover.services.resource_policy_store.get_cluster_profile",
        AsyncMock(side_effect=lambda name: dict(_PROFILE) if name == "sandbox" else None),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("extra", [{"allowed_cidrs": ["0.0.0.0/0"]}, {"network_id": "external-net"}])
async def test_profile_rejects_request_level_network_and_cidrs_before_durable_writes(
    async_client, store, cluster_profile, extra
):
    response = await async_client.post("/v1/clusters/async", json={"name": "p", "profile": "sandbox", **extra})

    assert response.status_code == 400
    assert store.clusters == {} and store.jobs == {} and store.operations == {}


@pytest.mark.asyncio
async def test_unknown_profile_is_rejected_before_durable_writes(async_client, store, cluster_profile):
    response = await async_client.post("/v1/clusters/async", json={"name": "p", "profile": "missing"})

    assert response.status_code == 400
    assert store.clusters == {} and store.jobs == {} and store.operations == {}


@pytest.mark.asyncio
async def test_invalid_profile_policy_fails_closed(async_client, store, monkeypatch):
    from drover.services.resource_policy_store import RuntimeSettingValidationError

    monkeypatch.setattr(
        "drover.services.resource_policy_store.get_cluster_profile",
        AsyncMock(side_effect=RuntimeSettingValidationError("bad")),
    )
    response = await async_client.post("/v1/clusters/async", json={"name": "p", "profile": "sandbox"})

    assert response.status_code == 503
    assert store.clusters == {} and store.jobs == {}


@pytest.mark.asyncio
async def test_profile_cluster_snapshots_administrator_sources_not_request_values(async_client, store, cluster_profile):
    response = await async_client.post("/v1/clusters/async", json={"name": "profiled", "profile": "sandbox"})

    assert response.status_code == 200
    cluster = next(iter(store.clusters.values()))
    job = next(iter(store.jobs.values())).payload_json
    for data in (cluster, job):
        snapshot = data["resource_policy_snapshot"]["cluster_profile"]
        assert snapshot == {"name": "sandbox", **_PROFILE}
        assert data["network_id"] == "external-net"
        assert data["allowed_cidrs"] is None
