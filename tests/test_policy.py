"""Tests for oslo.policy enforcement, action matrix, overrides, and router checks."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app
from drover.models.orm import DroverOperation, DroverOperationEvent
from drover.policy import (
    authorize,
    build_credentials,
    reset_enforcer,
)


@pytest.fixture(autouse=True)
def reset_policy_enforcer():
    reset_enforcer()
    yield
    reset_enforcer()


def test_build_credentials():
    member_token = {
        "user_id": "usr-1",
        "project_id": "proj-1",
        "roles": ["member"],
        "is_system_admin": False,
    }
    creds = build_credentials(member_token)
    assert creds["user_id"] == "usr-1"
    assert creds["project_id"] == "proj-1"
    assert creds["roles"] == ["member"]
    assert creds["is_admin"] is False
    assert creds["is_system_admin"] is False

    admin_token = {
        "user_id": "usr-admin",
        "project_id": "proj-service",
        "roles": ["admin"],
        "is_system_admin": True,
    }
    admin_creds = build_credentials(admin_token)
    assert admin_creds["is_admin"] is True
    assert admin_creds["is_system_admin"] is True


def test_member_allowed_denied_matrix():
    member_info = {
        "user_id": "user-member",
        "project_id": "proj-alpha",
        "roles": ["member"],
        "is_system_admin": False,
    }
    admin_info = {
        "user_id": "user-admin",
        "project_id": "proj-admin",
        "roles": ["admin"],
        "is_system_admin": True,
    }

    own_target = {"project_id": "proj-alpha"}
    other_target = {"project_id": "proj-beta"}

    # 1. drover:clusters:get
    assert authorize("drover:clusters:get", own_target, member_info, do_raise=False) is True
    assert authorize("drover:clusters:get", other_target, member_info, do_raise=False) is False
    assert authorize("drover:clusters:get", other_target, admin_info, do_raise=False) is True

    # 2. drover:clusters:create
    assert authorize("drover:clusters:create", own_target, member_info, do_raise=False) is True
    assert authorize("drover:clusters:create", other_target, member_info, do_raise=False) is False

    # 3. drover:clusters:scale
    assert authorize("drover:clusters:scale", own_target, member_info, do_raise=False) is True
    assert authorize("drover:clusters:scale", other_target, member_info, do_raise=False) is False
    assert authorize("drover:clusters:scale", other_target, admin_info, do_raise=False) is True

    # 4. drover:clusters:delete
    assert authorize("drover:clusters:delete", own_target, member_info, do_raise=False) is True
    assert authorize("drover:clusters:delete", other_target, member_info, do_raise=False) is False
    assert authorize("drover:clusters:delete", other_target, admin_info, do_raise=False) is True

    # 5. drover:templates:manage
    assert authorize("drover:templates:manage", own_target, member_info, do_raise=False) is False
    assert authorize("drover:templates:manage", own_target, admin_info, do_raise=False) is True

    # 6. drover:operations:get
    assert authorize("drover:operations:get", own_target, member_info, do_raise=False) is True
    assert authorize("drover:operations:get", other_target, member_info, do_raise=False) is False
    assert authorize("drover:operations:get", other_target, admin_info, do_raise=False) is True

    # 7. drover:admin
    assert authorize("drover:admin", own_target, member_info, do_raise=False) is False
    assert authorize("drover:admin", own_target, admin_info, do_raise=False) is True


def test_policy_file_override():
    member_info = {
        "user_id": "usr-1",
        "project_id": "proj-1",
        "roles": ["template_manager"],
        "is_system_admin": False,
    }
    plain_member = {
        "user_id": "usr-2",
        "project_id": "proj-1",
        "roles": ["member"],
        "is_system_admin": False,
    }

    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write('"drover:templates:manage": "role:template_manager or is_system_admin:True"\n')
        override_file = f.name

    try:
        # Default policy denies member managing templates
        assert authorize("drover:templates:manage", None, member_info, do_raise=False) is False

        # Override policy allows member with template_manager role
        assert (
            authorize(
                "drover:templates:manage",
                None,
                member_info,
                do_raise=False,
                policy_file=override_file,
            )
            is True
        )

        # Override policy denies plain member without template_manager role
        assert (
            authorize(
                "drover:templates:manage",
                None,
                plain_member,
                do_raise=False,
                policy_file=override_file,
            )
            is False
        )
    finally:
        Path(override_file).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_operation_access_security(monkeypatch):
    now = datetime.now(UTC)
    op = DroverOperation(
        id="op-sec-100",
        project_id="proj-owner",
        cluster_id="cls-1",
        kind="create",
        status="SUCCEEDED",
        created_at=now,
    )
    ev = DroverOperationEvent(
        id="ev-sec-1",
        operation_id="op-sec-100",
        sequence=1,
        phase="bootstrap",
        message="Bootstrapping",
        created_at=now,
    )

    async def mock_get_op(session, op_id):
        if op_id == "op-sec-100":
            return op
        return None

    async def mock_get_events(session, op_id, since_sequence=0):
        if op_id == "op-sec-100":
            return [ev]
        return []

    def mock_validate(token, project_id=""):
        if token == "tok-owner":
            return {"user_id": "u1", "project_id": "proj-owner", "roles": ["member"], "is_system_admin": False}
        if token == "tok-other":
            return {"user_id": "u2", "project_id": "proj-other", "roles": ["member"], "is_system_admin": False}
        if token == "tok-admin":
            return {"user_id": "admin", "project_id": "proj-admin", "roles": ["admin"], "is_system_admin": True}
        raise Exception("Invalid token")

    monkeypatch.setattr("drover.auth.validate_token", mock_validate)
    monkeypatch.setattr("drover.services.operations.get_operation", mock_get_op)
    monkeypatch.setattr("drover.services.operations.get_operation_events", mock_get_events)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # 1. Owner access -> 200
        res_owner = await ac.get("/v1/operations/op-sec-100", headers={"X-Auth-Token": "tok-owner"})
        assert res_owner.status_code == 200
        assert res_owner.json()["id"] == "op-sec-100"

        res_owner_events = await ac.get("/v1/operations/op-sec-100/events", headers={"X-Auth-Token": "tok-owner"})
        assert res_owner_events.status_code == 200
        assert len(res_owner_events.json()) == 1

        # 2. Other tenant member access -> 404 (does not leak existence)
        res_other = await ac.get("/v1/operations/op-sec-100", headers={"X-Auth-Token": "tok-other"})
        assert res_other.status_code == 404
        assert res_other.json() == {"detail": "Operation not found"}

        res_other_events = await ac.get("/v1/operations/op-sec-100/events", headers={"X-Auth-Token": "tok-other"})
        assert res_other_events.status_code == 404
        assert res_other_events.json() == {"detail": "Operation not found"}

        # 3. System admin access -> 200
        res_admin = await ac.get("/v1/operations/op-sec-100", headers={"X-Auth-Token": "tok-admin"})
        assert res_admin.status_code == 200
        assert res_admin.json()["id"] == "op-sec-100"


def test_no_remaining_direct_router_admin_checks():
    """Verify all admin endpoints use policy enforcement dependencies."""
    from fastapi.routing import APIRoute

    admin_routes = [
        r for r in app.routes
        if isinstance(r, APIRoute) and r.path.startswith("/v1/admin")
    ]
    assert len(admin_routes) > 0

    for route in admin_routes:
        # Check that route dependencies include policy checks
        dep_funcs = [d.dependency for d in route.dependencies]
        # None of the routes should use unadapted raw functions without policy enforcement
        # All dependencies in admin paths must go through require_policy
        names = [f.__name__ for f in dep_funcs]
        assert "require_policy" in names or "_policy_dependency" in names or any("policy" in str(d) for d in dep_funcs), (
            f"Route {route.path} lacks policy dependency: {names}"
        )
