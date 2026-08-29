"""API tests for /v1/operations and /v1/operations/{operation_id}/events."""

from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app
from drover.models.orm import DroverOperation, DroverOperationEvent


@pytest.fixture
def mock_operations_store(monkeypatch):
    """Set up in-memory store for operations and events and mock authentication."""
    ops = {}
    events = {}

    def add_op(op: DroverOperation):
        ops[op.id] = op

    def add_event(event: DroverOperationEvent):
        events.setdefault(event.operation_id, []).append(event)

    def mock_validate_token(token: str, project_id: str = "") -> dict:
        if token == "token-proj-1":
            return {
                "token": "token-proj-1",
                "project_id": "proj-1",
                "user_id": "user-1",
                "username": "user1",
                "roles": ["member"],
                "is_system_admin": False,
            }
        if token == "token-proj-2":
            return {
                "token": "token-proj-2",
                "project_id": "proj-2",
                "user_id": "user-2",
                "username": "user2",
                "roles": ["member"],
                "is_system_admin": False,
            }
        raise Exception("Invalid token")

    async def mock_get_operation(session_or_factory, operation_id):
        return ops.get(operation_id)

    async def mock_get_operation_events(session_or_factory, operation_id, since_sequence=0):
        op_events = events.get(operation_id, [])
        filtered = [e for e in op_events if e.sequence > since_sequence]
        return sorted(filtered, key=lambda e: e.sequence)

    monkeypatch.setattr("drover.auth.validate_token", mock_validate_token)
    monkeypatch.setattr("drover.services.operations.get_operation", mock_get_operation)
    monkeypatch.setattr("drover.services.operations.get_operation_events", mock_get_operation_events)

    return {"ops": ops, "events": events, "add_op": add_op, "add_event": add_event}


@pytest.mark.asyncio
async def test_operation_endpoints_auth_required():
    """Unauthenticated calls to /v1/operations endpoints return 401."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res1 = await client.get("/v1/operations/op-123")
        assert res1.status_code == 401

        res2 = await client.get("/v1/operations/op-123/events")
        assert res2.status_code == 401


@pytest.mark.asyncio
async def test_own_project_operation_success(mock_operations_store):
    """Tenant can retrieve their own operation details and events."""
    now = datetime.now(UTC)
    op = DroverOperation(
        id="op-100",
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="create",
        status="RUNNING",
        request_id="req-123",
        idempotency_key="idemp-123",
        created_at=now,
    )
    ev1 = DroverOperationEvent(
        id="ev-1",
        operation_id="op-100",
        sequence=1,
        phase="security_group",
        message="Creating SG",
        payload_json={"step": "security_group", "progress": 20},
        created_at=now,
    )
    mock_operations_store["add_op"](op)
    mock_operations_store["add_event"](ev1)

    headers = {"X-Auth-Token": "token-proj-1"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res_op = await client.get("/v1/operations/op-100", headers=headers)
        assert res_op.status_code == 200
        op_data = res_op.json()
        assert op_data["id"] == "op-100"
        assert op_data["project_id"] == "proj-1"
        assert op_data["kind"] == "create"
        assert op_data["status"] == "RUNNING"
        assert op_data["request_id"] == "req-123"

        res_events = await client.get("/v1/operations/op-100/events", headers=headers)
        assert res_events.status_code == 200
        events_data = res_events.json()
        assert len(events_data) == 1
        assert events_data[0]["sequence"] == 1
        assert events_data[0]["phase"] == "security_group"


@pytest.mark.asyncio
async def test_other_tenant_operation_404(mock_operations_store):
    """Retrieving another tenant's operation returns 404 (does not leak existence)."""
    now = datetime.now(UTC)
    op = DroverOperation(
        id="op-200",
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="create",
        status="RUNNING",
        created_at=now,
    )
    mock_operations_store["add_op"](op)

    # Token for proj-2 attempting to access proj-1's operation
    headers = {"X-Auth-Token": "token-proj-2"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res_op = await client.get("/v1/operations/op-200", headers=headers)
        assert res_op.status_code == 404
        assert res_op.json() == {"detail": "Operation not found"}

        res_events = await client.get("/v1/operations/op-200/events", headers=headers)
        assert res_events.status_code == 404
        assert res_events.json() == {"detail": "Operation not found"}


@pytest.mark.asyncio
async def test_ordered_events_and_since_sequence(mock_operations_store):
    """Events are returned strictly ordered by sequence and respect since_sequence query param."""
    now = datetime.now(UTC)
    op = DroverOperation(
        id="op-300",
        project_id="proj-1",
        cluster_id="cluster-1",
        kind="create",
        status="SUCCEEDED",
        created_at=now,
    )
    mock_operations_store["add_op"](op)

    # Add events out of order to verify sorting
    ev3 = DroverOperationEvent(id="ev-3", operation_id="op-300", sequence=3, phase="done", message="Finished", created_at=now)
    ev1 = DroverOperationEvent(id="ev-1", operation_id="op-300", sequence=1, phase="init", message="Init", created_at=now)
    ev2 = DroverOperationEvent(id="ev-2", operation_id="op-300", sequence=2, phase="boot", message="Booting", created_at=now)

    mock_operations_store["add_event"](ev3)
    mock_operations_store["add_event"](ev1)
    mock_operations_store["add_event"](ev2)

    headers = {"X-Auth-Token": "token-proj-1"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res_all = await client.get("/v1/operations/op-300/events", headers=headers)
        assert res_all.status_code == 200
        data_all = res_all.json()
        assert [e["sequence"] for e in data_all] == [1, 2, 3]

        res_since = await client.get("/v1/operations/op-300/events?since_sequence=1", headers=headers)
        assert res_since.status_code == 200
        data_since = res_since.json()
        assert [e["sequence"] for e in data_since] == [2, 3]
