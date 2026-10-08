"""Current Keystone assignments and real role-ID edges are native authority."""
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
    client.roles.list.return_value = [role("base", "member"), role("parent", "drover_user"),
                                      role("leaf", "drover-access_user"), role("read", "drover-inventory_reader")]
    client.inference_rules.list_inference_roles.return_value = [
        {"prior_role": {"id": "parent"}, "implies": [{"id": "leaf"}, {"id": "read"}]},
    ]
    client.role_assignments.list.return_value = [
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": rid}}
        for rid in ("base", "parent")
    ]
    monkeypatch.setattr("drover.auth._get_admin_ks_client", lambda: client)
    reset_enforcer()
    yield client
    reset_enforcer()


def test_actual_parent_edges_grant_and_removed_edge_revokes(directory):
    roles = _current_project_roles("u", "p")
    assert authorize("drover:access:get", None, {"user_id": "u", "project_id": "p", "roles": roles})
    directory.role_assignments.list.assert_called_with(user="u", project="p", effective=True)
    directory.inference_rules.list_inference_roles.return_value[0]["implies"] = [{"id": "read"}]
    roles = _current_project_roles("u", "p")
    assert "drover_user" in roles
    assert "drover-access_user" not in roles
    assert not authorize("drover:access:get", None, {"project_id": "p", "roles": roles}, do_raise=False)
    assert authorize("drover:clusters:get", None, {"project_id": "p", "roles": roles})


def test_assignment_revocation_is_immediate_for_next_request(directory):
    assert "drover-access_user" in _current_project_roles("u", "p")
    directory.role_assignments.list.return_value = []
    assert _current_project_roles("u", "p") == []


def test_domain_alias_cannot_assert_global_leaf(directory):
    directory.roles.list.return_value.append(role("alias", "drover-access_user", "domain"))
    directory.inference_rules.list_inference_roles.return_value = []
    directory.role_assignments.list.return_value[1]["role"]["id"] = "alias"
    assert _current_project_roles("u", "p") == ["member"]


def test_unlisted_domain_role_does_not_block_unrelated_global_service(directory):
    directory.roles.get.return_value = role("domain-only", "drover-access_admin", "domain")
    directory.role_assignments.list.return_value.append(
        {"user": {"id": "u"}, "scope": {"project": {"id": "p"}}, "role": {"id": "domain-only"}},
    )
    directory.inference_rules.list_inference_roles.return_value.append(
        {"prior_role": {"id": "domain-only"}, "implies": [{"id": "leaf"}]},
    )
    roles = _current_project_roles("u", "p")
    assert "drover-access_user" in roles
    assert "drover-access_admin" not in roles


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
        directory.roles.list.return_value.append(role("ambiguous", "drover-access_user"))
    with pytest.raises((ValueError, RuntimeError)):
        _current_project_roles("u", "p")


def test_actual_graph_reaching_openstack_admin_is_not_tenant_authority(directory):
    directory.roles.list.return_value.append(role("danger", "admin"))
    directory.inference_rules.list_inference_roles.return_value.append(
        {"prior_role": {"id": "leaf"}, "implies": [{"id": "danger"}]},
    )
    roles = _current_project_roles("u", "p")
    assert not authorize("drover:access:get", None, {"project_id": "p", "roles": roles}, do_raise=False)
    assert not authorize("drover:admin", None, {"project_id": "p", "roles": roles}, do_raise=False)
