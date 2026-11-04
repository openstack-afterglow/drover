"""Fresh uniquely bound global assignments remain authoritative without service edges."""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from drover.auth import _current_project_roles
from drover.policy import authorize, reset_enforcer


def role(rid, name, domain_id=None):
    return SimpleNamespace(id=rid, name=name, domain_id=domain_id)


@pytest.fixture
def directory(monkeypatch):
    client = MagicMock()
    client.roles.list.return_value = [role("base", "load-balancer_member"), role("parent", "project_member"),
                                      role("leaf", "member"), role("read", "reader")]
    client.inference_rules.list_inference_roles.return_value = [
        {"prior_role": {"id": "parent"}, "implies": [{"id": "leaf"}, {"id": "read"}]},
    ]
    client.role_assignments.list.return_value = [
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": rid}}
        for rid in ("base", "parent")
    ]
    monkeypatch.setattr("drover.auth._get_admin_ks_client", lambda: client)
    def get(path):
        if path == "/roles":
            key, rows = "roles", client.roles.list()
        elif path == "/role_inferences":
            key, rows = "role_inferences", client.inference_rules.list_inference_roles()
        else:
            from urllib.parse import parse_qs, urlsplit

            query = parse_qs(urlsplit(path).query)
            key, rows = "role_assignments", client.role_assignments.list(
                user=query["user.id"][0], project=query["scope.project.id"][0], effective=True)
        if isinstance(rows, list):
            rows = [vars(row) if isinstance(row, SimpleNamespace) else row for row in rows]
        return None, {key: rows, "links": {"next": None}}
    client.get.side_effect = get
    reset_enforcer()
    yield client
    reset_enforcer()


def test_native_project_member_graph_authorizes_without_service_roles(directory):
    roles = _current_project_roles("u", "p")
    assert roles == ["load-balancer_member", "member", "project_member", "reader"]
    assert authorize("drover:access:get", None, {"user_id": "u", "project_id": "p", "roles": roles})
    directory.role_assignments.list.assert_called_with(user="u", project="p", effective=True)
    assert authorize("drover:clusters:create", None, {"project_id": "p", "roles": roles})
    directory.role_assignments.list.return_value = []
    revoked = _current_project_roles("u", "p")
    assert not authorize("drover:access:get", None, {"project_id": "p", "roles": revoked}, do_raise=False)


def test_assignment_revocation_is_immediate_for_next_request(directory):
    assert "member" in _current_project_roles("u", "p")
    directory.role_assignments.list.return_value = []
    assert _current_project_roles("u", "p") == []


def test_domain_alias_cannot_assert_global_member(directory):
    directory.roles.list.return_value.append(role("alias", "member", "domain"))
    directory.inference_rules.list_inference_roles.return_value = []
    directory.role_assignments.list.return_value[1]["role"]["id"] = "alias"
    assert _current_project_roles("u", "p") == ["load-balancer_member"]


def test_unlisted_domain_role_does_not_block_unrelated_global_member(directory):
    directory.roles.get.return_value = role("domain-only", "admin", "domain")
    directory.role_assignments.list.return_value.append(
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": "domain-only"}},
    )
    directory.inference_rules.list_inference_roles.return_value.append(
        {"prior_role": {"id": "domain-only"}, "implies": [{"id": "leaf"}]},
    )
    roles = _current_project_roles("u", "p")
    assert "member" in roles
    assert "admin" not in roles


@pytest.mark.parametrize("failure", ["catalog", "graph", "assignments", "unknown", "scope", "subject", "duplicate"])
def test_directory_failures_do_not_fall_back_to_token_roles(directory, failure):
    if failure == "catalog":
        directory.roles.list.side_effect = RuntimeError("unavailable")
    elif failure == "graph":
        directory.inference_rules.list_inference_roles.side_effect = RuntimeError("unavailable")
    elif failure == "assignments":
        directory.role_assignments.list.side_effect = RuntimeError("unavailable")
    elif failure == "unknown":
        directory.inference_rules.list_inference_roles.return_value[0]["implies"] = [{"id": "unknown"}]
    elif failure == "scope":
        directory.role_assignments.list.return_value[0]["scope"]["project"]["id"] = "other"
    elif failure == "subject":
        directory.role_assignments.list.return_value[0]["user"]["id"] = "other"
    else:
        directory.roles.list.return_value.append(role("ambiguous", "member"))
    with pytest.raises((ValueError, RuntimeError)):
        _current_project_roles("u", "p")


def test_actual_project_admin_inference_does_not_grant_system_authority(directory):
    directory.roles.list.return_value.append(role("danger", "admin"))
    directory.inference_rules.list_inference_roles.return_value.append(
        {"prior_role": {"id": "leaf"}, "implies": [{"id": "danger"}]},
    )
    roles = _current_project_roles("u", "p")
    assert authorize("drover:access:get", None, {"project_id": "p", "roles": roles})
    assert not authorize("drover:access:get", {"project_id": "other"}, {"project_id": "p", "roles": roles}, do_raise=False)
    assert not authorize("drover:admin", None, {"project_id": "p", "roles": roles}, do_raise=False)


@pytest.mark.parametrize("boundary", ["cycle", "missing_edges", "partial_catalog", "partial_domain_role"])
def test_shared_role_graph_rejects_partial_or_cyclic_authority(directory, boundary):
    if boundary == "cycle":
        directory.inference_rules.list_inference_roles.return_value.append(
            {"prior_role": {"id": "leaf"}, "implies": [{"id": "parent"}]})
    elif boundary == "missing_edges":
        directory.inference_rules.list_inference_roles.return_value[0].pop("implies")
    elif boundary == "partial_catalog":
        directory.roles.list.return_value[0] = {"id": "base"}
    else:
        directory.roles.get.return_value = {"id": "domain-only", "domain_id": "domain"}
        directory.role_assignments.list.return_value.append(
            {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": "domain-only"}})
    with pytest.raises(ValueError):
        _current_project_roles("u", "p")


@pytest.mark.parametrize("collection", ["roles", "role_inferences", "role_assignments"])
@pytest.mark.parametrize("partial", ["next", "truncated"])
def test_incomplete_envelope_cannot_establish_project_role_ceiling(directory, collection, partial):
    original_get = directory.get.side_effect
    def get(path):
        response, body = original_get(path)
        if collection in body:
            if partial == "next":
                body["links"]["next"] = "http://127.0.0.1/next"
            else:
                body["truncated"] = True
        return response, body
    directory.get.side_effect = get
    with pytest.raises(ValueError, match="Incomplete Keystone authority"):
        _current_project_roles("u", "p")


def test_casefold_global_collision_is_rejected_before_actual_engine(directory):
    directory.roles.list.return_value = [role("core", "Member")]
    directory.inference_rules.list_inference_roles.return_value = []
    directory.role_assignments.list.return_value = [
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": "core"}}]
    roles = _current_project_roles("u", "p")
    assert roles == ["Member"]  # Preserve the native name; never manufacture an exact core alias.
    assert not authorize("drover:clusters:create", None, {"project_id": "p", "roles": roles}, do_raise=False)
    directory.roles.list.return_value.append(role("ambiguous", "member"))
    with pytest.raises(ValueError, match="case-insensitive global role"):
        _current_project_roles("u", "p")


@pytest.mark.parametrize("legacy", ["drover_admin", "drover-access_user", "lumen_editor",
                                  "waygate-connect_user", "palimpsest-publish_editor"])
def test_legacy_assignment_alone_cannot_replace_native_core_membership(directory, legacy):
    directory.roles.list.return_value.append(role("retired", legacy))
    directory.role_assignments.list.return_value = [
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": "retired"}},
    ]
    roles = _current_project_roles("u", "p")
    assert roles == [legacy]
    assert not authorize("drover:clusters:get", None, {"project_id": "p", "roles": roles}, do_raise=False)
    directory.role_assignments.list.return_value.append(
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": "parent"}})
    roles = _current_project_roles("u", "p")
    assert "member" in roles and authorize("drover:clusters:create", None, {"project_id": "p", "roles": roles})


def test_reader_downgrade_removes_management_without_service_role_fallback(directory):
    assert authorize("drover:clusters:delete", None, {"project_id": "p", "roles": _current_project_roles("u", "p")})
    directory.roles.list.return_value.append(role("retired", "drover_admin"))
    directory.role_assignments.list.return_value = [
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": rid}}
        for rid in ("read", "retired")
    ]
    roles = _current_project_roles("u", "p")
    assert roles == ["drover_admin", "reader"]
    assert authorize("drover:clusters:get", None, {"project_id": "p", "roles": roles})
    assert not authorize("drover:clusters:delete", None, {"project_id": "p", "roles": roles}, do_raise=False)
