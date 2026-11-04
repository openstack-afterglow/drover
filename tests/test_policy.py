"""Tests for oslo.policy enforcement, action matrix, overrides, and router checks."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from drover.main import app
from drover.models.orm import DroverOperation, DroverOperationEvent
from drover.policy import (
    authorize,
    build_credentials,
    capabilities,
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


@pytest.mark.parametrize("role", ["project_member", "project_admin", "project_owner", "project_manager"])
def test_project_alias_names_without_native_graph_have_no_service_entitlement(role):
    token = {"user_id": "u", "project_id": "p", "roles": [role], "is_system_admin": False}
    assert not build_credentials(token)["is_system_admin"]
    for action in ("list", "get", "create", "scale", "delete"):
        assert not authorize(f"drover:clusters:{action}", {"project_id": "p"}, token, do_raise=False)
    assert not authorize("drover:admin", None, token, do_raise=False)


@pytest.mark.parametrize(("role", "allowed"), [
    ("reader", {"list", "get"}),
    ("member", {"list", "get", "access", "create", "scale", "delete", "full", "workloads", "rotate"}),
    ("manager", {"list", "get", "access", "create", "scale", "delete", "full", "workloads", "rotate"}),
    ("admin", {"list", "get", "access", "create", "scale", "delete", "full", "workloads", "rotate"}),
    ("drover_reader", set()),
    ("drover_user", set()),
    ("drover_editor", set()),
    ("drover_admin", set()),
    ("drover-inventory_reader", set()),
    ("drover-access_user", set()),
    ("drover-clusters_editor", set()),
    ("drover-workloads_editor", set()),
    ("drover-access_admin", set()),
    ("drover-clusters_admin", set()),
    ("lumen_admin", set()),
    ("waygate_admin", set()),
    ("palimpsest_admin", set()),
])
def test_standard_project_role_and_legacy_role_matrix(role, allowed):
    token = {"user_id": "u", "project_id": "p", "roles": [role]}
    actions = {**{a: f"drover:clusters:{a}" for a in ("list", "get", "create", "scale", "delete")},
               "access": "drover:access:get", "full": "drover:access:admin",
               "workloads": "drover:workloads:write", "rotate": "drover:certificates:rotate"}
    for action, rule in actions.items():
        assert authorize(rule, {"project_id": "p"}, token, do_raise=False) == (action in allowed)
        assert not authorize(rule, {"project_id": "other"}, token, do_raise=False)
    for rule in ("drover:admin", "drover:templates:manage"):
        assert not authorize(rule, None, token, do_raise=False)


def test_project_management_roles_do_not_become_system_authority():
    for roles in (["member"], ["manager"], ["admin"], ["member", "drover-access_admin"]):
        token = {"project_id": "p", "roles": roles}
        assert authorize("drover:access:get", None, token)
        assert not authorize("drover:admin", None, token, do_raise=False)
    verified = {"project_id": "p", "roles": ["admin"], "is_system_admin": True}
    assert authorize("drover:admin", None, verified)


@pytest.mark.parametrize("roles", [[], ["reader"], ["drover_admin"]])
def test_personal_provenance_cannot_bypass_standard_project_roles(roles):
    token = {"user_id": "u", "project_id": "p", "roles": roles,
             "personal_project_owner": True}
    for action in ("drover:clusters:delete", "drover:access:admin", "drover:workloads:write"):
        assert not authorize(action, {"project_id": "p"}, token, do_raise=False)
    assert "personal_project_owner" not in build_credentials(token)


def test_project_admin_policy_override_is_not_system_authority(tmp_path):
    path = tmp_path / "policy.yaml"
    path.write_text('"drover:clusters:create": "role:admin"\n'
                    '"drover:templates:manage": "@"\n"drover:admin": "@"\n')
    token = {"user_id": "u", "project_id": "p", "roles": ["admin"]}
    assert authorize("drover:clusters:create", None, token)
    assert authorize("drover:clusters:create", None, token, policy_file=str(path))
    assert not authorize("drover:clusters:create", {"project_id": "other"}, token,
                         do_raise=False, policy_file=str(path))
    for action in ("drover:templates:manage", "drover:admin"):
        assert not authorize(action, None, token, do_raise=False, policy_file=str(path))


def test_unknown_action_denied_even_with_policy_override(tmp_path):
    path = tmp_path / "policy.yaml"
    path.write_text('"drover:unregistered": "@"\n')
    assert not authorize("drover:unregistered", None, {"project_id": "p", "is_system_admin": True},
                         do_raise=False, policy_file=str(path))


def test_packaged_sample_matches_registered_defaults():
    import yaml

    from drover.policy import list_rules

    sample = Path(__file__).parents[1] / "drover" / "policy.yaml.sample"
    assert yaml.safe_load(sample.read_text()) == {rule.name: rule.check_str for rule in list_rules()}


def test_reduced_credentials_advertise_metadata_only():
    document = capabilities({"project_id": "p", "roles": ["member"], "credential_restricted": True})
    assert set(document["actions"]) == {
        "drover:clusters:list", "drover:clusters:get", "drover:operations:get",
    }


@pytest.mark.parametrize("restricted", [False, True])
def test_allow_all_cannot_cross_project_or_credential_ceiling(tmp_path, restricted):
    from drover.policy import list_rules

    path = tmp_path / "policy.yaml"
    path.write_text("".join(f'"{rule.name}": "@"\n' for rule in list_rules()))
    token = {"user_id": "u", "project_id": "p", "roles": ["member"], "credential_restricted": restricted}
    for action in ("drover:clusters:list", "drover:clusters:get", "drover:operations:get"):
        assert authorize(action, None, token, policy_file=str(path))
        assert not authorize(action, {"project_id": "other"}, token, do_raise=False, policy_file=str(path))
    for action in ("drover:access:get", "drover:access:read", "drover:access:admin",
                   "drover:workloads:write", "drover:certificates:rotate",
                   "drover:clusters:create", "drover:clusters:scale", "drover:clusters:delete",
                   "drover:clusters:reauthorize", "drover:clusters:retire_credentials"):
        assert authorize(action, None, token, do_raise=False, policy_file=str(path)) is not restricted
        assert not authorize(action, {"project_id": "other"}, token, do_raise=False, policy_file=str(path))
    for action in ("drover:admin", "drover:templates:manage"):
        assert not authorize(action, None, token, do_raise=False, policy_file=str(path))


@pytest.mark.parametrize("roles", [
    [], ["reader"], ["drover_admin"], ["drover-access_admin"], ["deployment_operator"],
])
def test_allow_all_cannot_bypass_native_core_role_floor(tmp_path, roles):
    from drover.policy import list_rules

    path = tmp_path / "policy.yaml"
    path.write_text("".join(f'"{rule.name}": "@"\n' for rule in list_rules()))
    token = {"user_id": "u", "project_id": "p", "roles": roles,
             "personal_project_owner": True}
    metadata = {"drover:clusters:list", "drover:clusters:get", "drover:operations:get"}
    for rule in list_rules():
        if not rule.name.startswith("drover:"):
            continue
        assert authorize(rule.name, {"project_id": "p"}, token, do_raise=False,
                         policy_file=str(path)) is (roles == ["reader"] and rule.name in metadata)


@pytest.mark.asyncio
async def test_policy_reload_changes_native_api_and_capabilities(tmp_path, monkeypatch):
    import os
    from unittest.mock import AsyncMock

    from drover import policy
    from drover.config import Settings

    path = tmp_path / "policy.yaml"
    path.write_text('"drover:clusters:list": "!"\n')
    monkeypatch.setattr(policy, "get_settings", lambda: Settings(oslo_policy_policy_file=str(path)))
    token = {"user_id": "u", "project_id": "p", "roles": ["reader"]}
    monkeypatch.setattr("drover.auth.validate_token", lambda *_: token)
    monkeypatch.setattr("drover.api.clusters.k3s_cluster.list_clusters", AsyncMock(return_value=[]))
    monkeypatch.setattr("drover.api.clusters.is_db_configured", lambda: False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test",
                           headers={"X-Auth-Token": "test-token"}) as client:
        assert (await client.get("/v1/clusters?no_cache=true")).status_code == 403
        before = (await client.get("/v1/policy/capabilities")).json()
        assert "drover:clusters:list" not in before["actions"]
        assert "drover:clusters:get" in before["actions"]
        path.write_text('"drover:clusters:list": "role:reader"\n')
        os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 2))
        assert (await client.get("/v1/clusters?no_cache=true")).status_code == 200
        after = (await client.get("/v1/policy/capabilities")).json()
        assert after["service"] == "drover"
        assert "drover:clusters:list" in after["actions"]
        assert after["actions"] == sorted(after["actions"])
        assert after["capabilities"] == sorted(after["capabilities"])
        path.write_text('"drover:clusters:list": "!"\n')
        os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 4))
        assert (await client.get("/v1/clusters?no_cache=true")).status_code == 403
        assert "drover:clusters:list" not in (await client.get("/v1/policy/capabilities")).json()["actions"]


@pytest.mark.asyncio
async def test_capability_discovery_requires_auth_not_project_entitlement(monkeypatch):
    token = {"user_id": "u", "project_id": "p", "roles": []}
    monkeypatch.setattr("drover.auth.validate_token", lambda *_: token)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/v1/policy/capabilities")).status_code == 401
        response = await client.get("/v1/policy/capabilities", headers={"X-Auth-Token": "test-token"})
        assert response.status_code == 200
        assert response.json() == {"service": "drover", "capabilities": [], "actions": []}


@pytest.mark.parametrize("bad", ['[invalid yaml', '[]', '"drover:clusters:create": null\n',
                                  '"drover:clusters:create": false\n', '"drover:clusters:create": []\n',
                                  '"drover:clusters:create": "rule:missing"\n',
                                  '"drover:clusters:create": "rule:loop"\n"loop": "rule:loop"\n'])
def test_bad_policy_update_never_falls_back_to_defaults(tmp_path, bad):
    path = tmp_path / "policy.yaml"
    path.write_text(bad)
    for _ in range(2):
        with pytest.raises(HTTPException) as error:
            authorize("drover:clusters:create", None, {"project_id": "p", "roles": ["member"]},
                      policy_file=str(path))
        assert error.value.status_code == 503


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
            return {"user_id": "u1", "project_id": "proj-owner", "roles": ["reader"], "is_system_admin": False}
        if token == "tok-other":
            return {"user_id": "u2", "project_id": "proj-other", "roles": ["reader"], "is_system_admin": False}
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
    from fastapi.routing import APIRoute, iter_route_contexts

    # FastAPI >=0.137 keeps included routers as a tree in app.routes; resolve the
    # effective routes (include prefix + router-level dependencies) instead.
    admin_routes = [
        r for r in iter_route_contexts(app.routes)
        if isinstance(r.original_route, APIRoute) and r.path.startswith("/v1/admin")
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


@pytest.mark.parametrize("action", ["get", "create", "scale", "delete", "reauthorize"])
def test_verified_system_admin_needs_no_project_service_leaves(action):
    token = {"user_id": "operator", "project_id": "target", "connection_project_id": "home",
             "roles": [], "is_system_admin": True}
    assert authorize(f"drover:clusters:{action}", {"project_id": "target"}, token)
    assert build_credentials(token)["project_id"] == "home"
    token["is_system_admin"] = False
    assert not authorize(f"drover:clusters:{action}", {"project_id": "target"}, token, do_raise=False)


def test_logical_target_cannot_relabel_native_project_roles():
    token = {"user_id": "member", "project_id": "target", "connection_project_id": "home",
             "roles": ["member"], "is_system_admin": False}
    assert not authorize("drover:clusters:create", {"project_id": "target"}, token, do_raise=False)


def test_policy_directory_override_is_used_by_actions_and_projection(tmp_path, monkeypatch):
    import os

    from drover import policy
    from drover.config import Settings

    path = tmp_path / "policy.yaml"
    path.write_text("{}\n")
    directory = tmp_path / "policy.d"
    directory.mkdir()
    override = directory / "10-local.yaml"
    override.write_text('"drover:clusters:create": "role:deployment_operator"\n')
    monkeypatch.setattr(policy, "get_settings", lambda: Settings(
        oslo_policy_policy_file=str(path), oslo_policy_policy_dirs=[str(directory)]))
    token = {"project_id": "p", "roles": ["member", "deployment_operator"]}
    assert authorize("drover:clusters:create", None, token)
    assert "drover:clusters:create" in capabilities(token)["actions"]
    override.write_text('"drover:clusters:create": "!"\n')
    os.utime(override, (override.stat().st_atime, override.stat().st_mtime + 2))
    assert not authorize("drover:clusters:create", None, token, do_raise=False)
    assert "drover:clusters:create" not in capabilities(token)["actions"]


def test_workload_policy_override_does_not_remove_principal_namespace_boundary(tmp_path, monkeypatch):
    from drover import policy
    from drover.config import Settings
    from drover.services.credentials import workload_namespace

    path = tmp_path / "policy.yaml"
    path.write_text('"drover:workloads:write": "@"\n"drover:access:admin": "!"\n')
    monkeypatch.setattr(policy, "get_settings", lambda: Settings(oslo_policy_policy_file=str(path)))
    token = {"project_id": "p", "user_id": "u", "roles": ["member"]}
    policy.authorize_workload_namespace(workload_namespace("p", "u"), token)
    with pytest.raises(HTTPException) as error:
        policy.authorize_workload_namespace(workload_namespace("p", "other-user"), token)
    assert error.value.status_code == 403


def test_missing_custom_policy_file_never_uses_permissive_defaults(tmp_path):
    with pytest.raises(HTTPException) as error:
        authorize("drover:clusters:create", None, {"project_id": "p", "roles": ["member"]},
                  policy_file=str(tmp_path / "missing-operator.yaml"))
    assert error.value.status_code == 503


@pytest.mark.parametrize("expression", ["not malformed", "@ or malformed", "role:member or broken",
                                        "role:member and (", "not (broken)"])
@pytest.mark.parametrize("in_directory", [False, True])
def test_native_parser_failure_rejects_entire_load_and_second_request(tmp_path, monkeypatch, expression, in_directory):
    import logging

    from drover import policy
    from drover.config import Settings

    path = tmp_path / "policy.yaml"
    directory = tmp_path / "policy.d"
    directory.mkdir()
    path.write_text("{}\n")
    configured = directory / "10-local.yaml" if in_directory else path
    import yaml

    configured.write_text(yaml.safe_dump({"drover:clusters:create": expression}))
    monkeypatch.setattr(policy, "get_settings", lambda: Settings(
        oslo_policy_policy_file=str(path), oslo_policy_policy_dirs=[str(directory)]))
    original_logger = policy._parser.LOG
    previous_disable = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)  # Syntax rejection must not depend on logging configuration.
        for _ in range(2):
            with pytest.raises(HTTPException) as error:
                authorize("drover:clusters:create", None, {"project_id": "p", "roles": ["member"]})
            assert error.value.status_code == 503
            assert policy._parser.LOG is original_logger
    finally:
        logging.disable(previous_disable)
    configured.write_text('"drover:clusters:create": "role:member"\n')
    assert authorize("drover:clusters:create", None, {"project_id": "p", "roles": ["member"]})


@pytest.mark.parametrize("expression", ["", "@", "not !", "! or role:member",
                                        "role:member and (not role:blocked)"])
def test_valid_native_oslo_expression_forms_remain_supported(tmp_path, expression):
    import yaml

    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump({"drover:clusters:create": expression}))
    assert authorize("drover:clusters:create", None, {"project_id": "p", "roles": ["member"]},
                     policy_file=str(path))


def test_enforcer_never_parses_operator_toml_as_oslo_ini(tmp_path, monkeypatch):
    from drover import policy
    from drover.config import Settings

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drover.conf").write_text('[drover]\ndelegated_required_roles = [\n "member",\n]\n')
    path = tmp_path / "policy.yaml"
    path.write_text("{}\n")
    monkeypatch.setattr(policy, "get_settings", lambda: Settings(oslo_policy_policy_file=str(path)))
    assert authorize("drover:clusters:create", None, {"project_id": "p", "roles": ["member"]})
    assert policy.get_enforcer().conf.config_file == []


def test_explicit_missing_policy_directory_is_unavailable(tmp_path, monkeypatch):
    from drover import policy
    from drover.config import Settings

    missing = tmp_path / "not-mounted"
    monkeypatch.setattr(policy, "get_settings", lambda: Settings(
        oslo_policy_policy_dirs=[str(missing)]))
    with pytest.raises(HTTPException) as error:
        authorize("drover:clusters:create", None, {"project_id": "p", "roles": ["member"]})
    assert error.value.status_code == 503


@pytest.mark.parametrize("expression", [
    '"drover:clusters:delete": "not rule:missing"\n',
    '"drover:clusters:delete": "not rule:cycle"\n"cycle": "not rule:drover:clusters:delete"\n',
    '"drover:clusters:delete": "role:member and not (rule:missing or role:reader)"\n',
])
def test_negated_missing_or_cyclic_reference_fails_closed(tmp_path, monkeypatch, expression):
    from drover import policy
    from drover.config import Settings

    path = tmp_path / "policy.yaml"
    path.write_text(expression)
    monkeypatch.setattr(policy, "get_settings", lambda: Settings(oslo_policy_policy_file=str(path)))
    with pytest.raises(HTTPException) as error:
        authorize("drover:clusters:delete", None, {"project_id": "p", "roles": ["member"]})
    assert error.value.status_code == 503
