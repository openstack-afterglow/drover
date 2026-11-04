"""Drover request-scoped OpenStack authorization contracts."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from keystoneauth1.exceptions.http import Unauthorized
from requests import Response
from starlette.requests import Request

from drover.auth import _get_admin_ks_client, get_os_conn, require_token, validate_token

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def clear_internal_keystone_endpoint_cache():
    with patch("drover.auth._internal_keystone_endpoint_cache", None):
        yield


@pytest.fixture(autouse=True)
def synthetic_project_role_directory():
    # This module tests token transport; current role graph behavior has its own tests.
    from drover import auth

    def current_authority(user, project):
        return {"enabled": auth.current_principal_state(user, project),
                "role_map": {name: name for name in auth._current_project_roles(user, project)},
                "is_system_admin": auth._is_system_admin(user)}

    with (patch("drover.auth._current_project_roles", return_value=["member"]),
          patch("drover.auth.current_principal_state", return_value=(True, True)),
          patch("drover.auth.current_project_authority", side_effect=current_authority)):
        yield


_TOKEN_INFO = {
    "token": "caller-scoped-token",
    "project_id": "project-1",
    "user_id": "user-1",
}


async def test_get_os_conn_uses_caller_token_and_closes_connection():
    conn = MagicMock()
    generator = get_os_conn(_TOKEN_INFO)
    with patch("openstack.connect", return_value=conn) as connect:
        yielded = await anext(generator)
        assert yielded is conn
        await generator.aclose()

    kwargs = connect.call_args.kwargs
    assert kwargs["auth_type"] == "token"
    assert kwargs["token"] == "caller-scoped-token"
    assert kwargs["project_id"] == "project-1"
    assert "username" not in kwargs
    assert "password" not in kwargs
    assert conn._afterglow_token == "caller-scoped-token"
    assert conn._afterglow_project_id == "project-1"
    assert conn._afterglow_user_id == "user-1"
    conn.close.assert_called_once_with()


async def test_get_os_conn_fails_closed_when_scoped_connection_fails():
    generator = get_os_conn(_TOKEN_INFO)
    with patch("openstack.connect", side_effect=RuntimeError("Keystone unavailable")):
        with pytest.raises(HTTPException) as exc_info:
            await anext(generator)

    assert exc_info.value.status_code == 401


def _keystone_response(project_id):
    body = {
        "token": {
            "methods": ["password"],
            "expires_at": "2099-01-01T00:00:00Z",
            "issued_at": "2026-01-01T00:00:00Z",
            "user": {"id": "user-1", "name": "member", "domain": {"id": "default"}},
            "roles": [{"id": "role-member", "name": "member"}],
        }
    }
    if project_id:
        body["token"]["project"] = {
            "id": project_id,
            "name": project_id,
            "domain": {"id": "default"},
        }
    response = Response()
    response.status_code = 200
    response._content = json.dumps(body).encode()
    response.headers["X-Subject-Token"] = "validated-token"
    return response


@pytest.mark.parametrize("methods", [["openid"], ["openid", "token"], ["mapped", "token"], ["saml2", "token"]])
async def test_federated_human_uses_fresh_native_project_and_system_authority(methods):
    from drover.policy import authorize

    response = _keystone_response("project-1")
    body = response.json()
    body["token"]["methods"] = methods
    body["token"]["roles"] = [{"id": "stale-reader", "name": "reader"}]
    response._content = json.dumps(body).encode()
    with (
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://127.0.0.1/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=response),
        patch("drover.auth._current_project_roles", return_value=["member"]),
        patch("drover.auth._is_system_admin", return_value=True),
    ):
        info = validate_token("federated-human")
    assert info["is_human"] is True and info["credential_restricted"] is False
    assert info["roles"] == ["member"] and info["is_system_admin"] is True
    assert authorize("drover:clusters:create", None, info)
    assert authorize("drover:admin", None, info)


@pytest.mark.parametrize("methods", [["openid", "unknown"], ["unknown"], ["token"]])
async def test_unknown_or_token_only_methods_cannot_expand_issued_reader_to_current_member(methods):
    from drover.policy import authorize

    response = _keystone_response("project-1")
    body = response.json()
    body["token"]["methods"] = methods
    body["token"]["roles"] = [{"id": "reader-id", "name": "reader"}]
    response._content = json.dumps(body).encode()
    with (
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://127.0.0.1/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=response),
        patch("drover.auth.current_project_role_map", return_value={"reader": "reader-id", "member": "member-id"}),
        patch("drover.auth._is_system_admin", return_value=True) as system,
    ):
        info = validate_token("reduced-reader")
    assert info["roles"] == ["reader"] and info["credential_restricted"] is True
    assert info["is_system_admin"] is False
    assert authorize("drover:clusters:get", None, info)
    for action in ("drover:clusters:create", "drover:workloads:write", "drover:access:get", "drover:access:admin"):
        assert not authorize(action, None, info, do_raise=False)
    system.assert_not_called()


@pytest.mark.parametrize(
    ("default_project", "auth_url"),
    [(None, "http://keystone"), ("other-default-project", "http://keystone/v3/")],
)
async def test_token_without_project_header_preserves_issued_scope(default_project, auth_url):
    def keystone_request(session, url, method, **kwargs):
        # Reauthentication selects the user's default, not the submitted token's scope.
        if method.upper() == "POST":
            return _keystone_response(default_project)
        assert method.upper() == "GET"
        assert url == "http://keystone/v3/auth/tokens"
        assert kwargs["headers"]["X-Auth-Token"] == "caller-scoped-token"
        assert kwargs["headers"]["X-Subject-Token"] == "caller-scoped-token"
        return _keystone_response("project-1")

    request = Request({"type": "http", "headers": []})
    with (
        patch("drover.auth.get_settings", return_value=SimpleNamespace(os_auth_url=auth_url, ssl_verify=True)),
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://keystone/v3"),
        patch("drover.auth.ks_session.Session.request", keystone_request),
        patch("drover.auth._is_system_admin", return_value=False),
    ):
        principal = await require_token(request, "caller-scoped-token", None)

    assert principal["project_id"] == "project-1"
    assert principal["token"] == "caller-scoped-token"
    assert principal["roles"] == ["member"]


async def test_token_introspection_failure_is_denied():
    request = Request({"type": "http", "headers": []})
    with (
        patch(
            "drover.auth.get_settings", return_value=SimpleNamespace(os_auth_url="http://keystone/v3", ssl_verify=True)
        ),
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://keystone/v3"),
        patch("drover.auth.ks_session.Session.request", side_effect=Unauthorized("revoked token")),
        pytest.raises(HTTPException) as error,
    ):
        await require_token(request, "revoked-token", None)

    assert error.value.status_code == 401


async def test_valid_but_unscoped_token_is_denied():
    request = Request({"type": "http", "headers": []})
    with (
        patch(
            "drover.auth.get_settings", return_value=SimpleNamespace(os_auth_url="http://keystone/v3", ssl_verify=True)
        ),
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://keystone/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=_keystone_response(None)),
        patch("drover.auth._is_system_admin", return_value=False),
        pytest.raises(HTTPException) as error,
    ):
        await require_token(request, "unscoped-token", None)

    assert error.value.status_code == 401


def _service_settings(auth_url="https://keystone.public.example/v3"):
    return SimpleNamespace(
        os_auth_url=auth_url,
        os_username="drover",
        os_password="service-secret",
        os_project_name="drover-service",
        os_user_domain_name="Default",
        os_project_domain_name="Default",
        os_region_name="RegionOne",
        ssl_verify=True,
    )


@pytest.mark.parametrize(
    "catalog_endpoint",
    ["https://keystone.internal.example", "https://keystone.internal.example/v3/"],
)
async def test_validate_token_uses_internal_identity_endpoint(catalog_endpoint):
    session = MagicMock()
    session.get_endpoint.return_value = catalog_endpoint
    session.get.return_value = _keystone_response("project-1")

    with (
        patch("drover.auth.get_settings", return_value=_service_settings()),
        patch("drover.auth.ks_session.Session", return_value=session),
        patch("drover.auth._is_system_admin", return_value=False),
    ):
        principal = validate_token("caller-scoped-token")

    session.get_endpoint.assert_called_once_with(
        service_type="identity",
        interface="internal",
        region_name="RegionOne",
    )
    session.get.assert_called_once_with(
        "https://keystone.internal.example/v3/auth/tokens",
        headers={"X-Auth-Token": "caller-scoped-token", "X-Subject-Token": "caller-scoped-token"},
        authenticated=False,
    )
    assert principal["project_id"] == "project-1"


async def test_explicit_project_rescope_uses_internal_identity_endpoint():
    def keystone_request(session, url, method, **kwargs):
        assert url == "https://keystone.internal.example/v3/auth/tokens"
        if method.upper() == "GET":
            return _keystone_response("project-1")
        assert method.upper() == "POST"
        scope = kwargs["json"]["auth"]["scope"]["project"]
        assert scope == {"id": "project-2"}
        return _keystone_response("project-2")

    request = Request({"type": "http", "headers": []})
    with (
        patch("drover.auth.get_settings", return_value=SimpleNamespace(ssl_verify=True)),
        patch(
            "drover.auth._resolve_internal_keystone_endpoint",
            return_value="https://keystone.internal.example/v3",
        ),
        patch("drover.auth.ks_session.Session.request", keystone_request),
        patch("drover.auth._is_system_admin", return_value=False),
    ):
        principal = await require_token(request, "caller-scoped-token", "project-2")

    assert principal["project_id"] == "project-2"


async def test_validate_token_does_not_fall_back_without_internal_identity_endpoint():
    session = MagicMock()
    session.get_endpoint.return_value = None

    with (
        patch("drover.auth.get_settings", return_value=_service_settings()),
        patch("drover.auth.ks_session.Session", return_value=session),
        pytest.raises(RuntimeError, match="internal endpoint"),
    ):
        validate_token("caller-scoped-token")

    session.get.assert_not_called()


@pytest.mark.parametrize(
    "catalog_endpoint",
    ["https://keystone.internal.example", "https://keystone.internal.example/v3/"],
)
async def test_admin_keystone_client_uses_internal_identity_endpoint(catalog_endpoint):
    session = MagicMock()
    session.get_endpoint.return_value = catalog_endpoint
    client = MagicMock()

    with (
        patch("drover.auth.get_settings", return_value=_service_settings()),
        patch("drover.auth.ks_session.Session", return_value=session),
        patch("keystoneclient.v3.client.Client", return_value=client) as client_factory,
    ):
        assert _get_admin_ks_client() is client

    client_factory.assert_called_once_with(
        session=session,
        endpoint_override="https://keystone.internal.example/v3",
    )


@pytest.fixture
def system_directory(monkeypatch):
    client = MagicMock()
    client.roles.list.return_value = [SimpleNamespace(id=rid, name=name, domain_id=None)
                                      for rid, name in (("admin-id", "admin"), ("parent-id", "operator"))]
    client.inference_rules.list_inference_roles.return_value = [
        {"prior_role": {"id": "parent-id"}, "implies": [{"id": "admin-id"}]},
    ]
    client.groups.list.return_value = []
    client.role_assignments.list.return_value = []
    monkeypatch.setattr("drover.auth._get_admin_ks_client", lambda: client)
    def get(path):
        from urllib.parse import parse_qs, urlsplit

        parsed = urlsplit(path)
        if parsed.path == "/roles":
            key, rows = "roles", client.roles.list()
        elif parsed.path == "/role_inferences":
            key, rows = "role_inferences", client.inference_rules.list_inference_roles()
        elif parsed.path.startswith("/users/"):
            key, rows = "groups", client.groups.list(user=parsed.path.split("/")[2])
        else:
            query = parse_qs(parsed.query)
            kwargs = {key.removesuffix(".id"): values[0] for key, values in query.items() if key in {"user.id", "group.id"}}
            key, rows = "role_assignments", client.role_assignments.list(**kwargs, system="all", effective=False)
        if isinstance(rows, list):
            rows = [vars(row) if isinstance(row, SimpleNamespace) else row for row in rows]
        return None, {key: rows, "links": {"next": None}}
    client.get.side_effect = get
    return client


@pytest.mark.parametrize("kind", ["direct", "implied", "group", "group-implied"])
async def test_raw_system_admin_ignores_empty_effective_system_expansion(system_directory, kind):
    from drover.auth import _is_system_admin

    group = kind.startswith("group")
    subject, identity = ("group", "operators") if group else ("user", "operator")
    role_id = "parent-id" if "implied" in kind else "admin-id"
    system_directory.groups.list.return_value = [{"id": "operators"}] if group else []
    def assignments(**kwargs):
        if kwargs.get("effective") is True:
            return []  # Faithful Keystone behavior: never expand effective system assignments.
        assert kwargs["system"] == "all" and "role" not in kwargs
        if kwargs.get(subject) == identity:
            return [{subject: {"id": identity}, "role": {"id": role_id}, "scope": {"system": {"all": True}}}]
        return []
    system_directory.role_assignments.list.side_effect = assignments
    assert _is_system_admin("operator") is True
    system_directory.roles.list.assert_called_once_with()
    system_directory.groups.list.assert_called_once_with(user="operator")
    assert all(call.kwargs["effective"] is False for call in system_directory.role_assignments.list.call_args_list)
    if "implied" in kind:
        system_directory.inference_rules.list_inference_roles.return_value = []
    elif group:
        system_directory.groups.list.return_value = []
    else:
        system_directory.role_assignments.list.side_effect = None
        system_directory.role_assignments.list.return_value = []
    assert _is_system_admin("operator") is False


@pytest.mark.parametrize("boundary", ["catalog", "inferences", "groups", "assignments", "ambiguous", "cycle",
                                        "partial_role", "partial_inference", "partial_group", "unknown_role",
                                        "foreign_user", "foreign_group", "project_scope", "mixed_scope", "partial_scope"])
async def test_raw_system_authority_unavailable_is_not_confirmed_revocation(system_directory, boundary):
    from keystoneauth1 import exceptions as ks_exc

    from drover.auth import _is_system_admin
    from drover.services.execution import ExecutionAuthorityUnavailable

    row = {"user": {"id": "operator"}, "role": {"id": "admin-id"}, "scope": {"system": {"all": True}}}
    system_directory.role_assignments.list.return_value = [row]
    lookups = {"catalog": system_directory.roles.list,
               "inferences": system_directory.inference_rules.list_inference_roles,
               "groups": system_directory.groups.list, "assignments": system_directory.role_assignments.list}
    if boundary in lookups:
        lookups[boundary].side_effect = ks_exc.ConnectFailure("synthetic directory outage")
    elif boundary == "ambiguous":
        system_directory.roles.list.return_value.append(SimpleNamespace(id="duplicate", name="admin", domain_id=None))
    elif boundary == "cycle":
        system_directory.inference_rules.list_inference_roles.return_value.append(
            {"prior_role": {"id": "admin-id"}, "implies": [{"id": "parent-id"}]})
    elif boundary == "partial_role":
        system_directory.roles.list.return_value[0] = {"id": "admin-id"}
    elif boundary == "partial_inference":
        system_directory.inference_rules.list_inference_roles.return_value[0].pop("implies")
    elif boundary == "partial_group":
        system_directory.groups.list.return_value = [{}]
    elif boundary == "unknown_role":
        row["role"]["id"] = "missing-id"
    elif boundary == "foreign_user":
        row["user"]["id"] = "foreign"
    elif boundary == "foreign_group":
        row.pop("user")
        row["group"] = {"id": "foreign"}
    elif boundary == "project_scope":
        row["scope"] = {"project": {"id": "target"}}
    elif boundary == "mixed_scope":
        row["scope"]["project"] = {"id": "target"}
    else:
        row["scope"] = {"system": {}}
    with pytest.raises(ExecutionAuthorityUnavailable):
        _is_system_admin("operator")


async def test_native_http_unavailable_system_authority_returns_503(system_directory):
    from drover.services.execution import ExecutionAuthorityUnavailable

    with patch("drover.auth.validate_token", side_effect=ExecutionAuthorityUnavailable("unavailable")):
        with pytest.raises(HTTPException) as error:
            await require_token(Request({"type": "http", "headers": []}), "human-token", None)
    assert error.value.status_code == 503


@pytest.mark.parametrize("kind", ["application_credential", "trust", "token_only"])
async def test_admin_owned_reduced_token_keeps_original_scope_and_role_id_ceiling(kind):
    from drover.policy import authorize

    response = _keystone_response("project-1")
    body = response.json()
    if kind == "application_credential":
        body["token"]["methods"] = ["application_credential"]
        body["token"]["application_credential"] = {"id": "reduced-id", "restricted": True}
    elif kind == "trust":
        body["token"]["OS-TRUST:trust"] = {
            "id": "trust-id", "impersonation": True,
            "trustor_user": {"id": "user-1"}, "trustee_user": {"id": "other"},
        }
    else:
        body["token"]["methods"] = ["token"]
    response._content = json.dumps(body).encode()
    with (
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://127.0.0.1/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=response) as request,
        patch("drover.auth.current_project_role_map", return_value={"member": "role-member", "admin": "extra-id"}),
        patch("drover.auth._is_system_admin", return_value=True) as system,
    ):
        info = validate_token("reduced-token", "project-1")
        assert info["roles"] == ["member"]
        assert info["credential_restricted"] is True and info["is_system_admin"] is False
        assert info["token"] == "reduced-token" and info["project_id"] == "project-1"
        assert not authorize("drover:admin", None, info, do_raise=False)
        assert not authorize("drover:access:admin", None, info, do_raise=False)
        assert request.call_args.args[1].upper() == "GET"
        system.assert_not_called()
        with pytest.raises(PermissionError, match="issued project scope"):
            validate_token("reduced-token", "foreign")
        assert all(call.args[1].upper() == "GET" for call in request.call_args_list)


async def test_original_access_rule_credentials_are_rejected_before_native_authority_expansion():
    response = _keystone_response("project-1")
    body = response.json()
    body["token"]["methods"] = ["application_credential"]
    body["token"]["application_credential"] = {"id": "reduced", "access_rules": [{"method": "GET", "path": "/servers"}]}
    response._content = json.dumps(body).encode()
    with (
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://127.0.0.1/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=response),
        patch("drover.auth.current_project_role_map") as roles,
        patch("drover.auth._is_system_admin") as system,
        pytest.raises(PermissionError, match="access-rule"),
    ):
        validate_token("access-rule-token")
    roles.assert_not_called()
    system.assert_not_called()


async def test_reduced_credential_cannot_publish_stored_admin_credential_even_with_issued_member():
    from drover.policy import authorize

    info = {"project_id": "project", "roles": ["member"],
            "is_system_admin": True, "credential_restricted": True}
    assert not authorize("drover:access:admin", None, info, do_raise=False)




@pytest.mark.parametrize("body", [
    {"role_assignments": [], "links": {"next": "http://127.0.0.1/next"}},
    {"groups": [], "truncated": True},
    {"roles": [{}]},
    {"role_inferences": None},
    {"groups": [], "links": None},
])
async def test_sdk_collection_guard_rejects_partial_metadata_before_materialization(body):
    from drover.auth import _identity_collection

    client = MagicMock()
    client.get.return_value = (None, body)
    key = next(iter(body))
    with pytest.raises(ValueError, match="Keystone authority"):
        _identity_collection(client, f"/{key}", key)
    client.get.assert_called_once_with(f"/{key}")


@pytest.mark.parametrize("kind", ["absent", "domain-local"])
async def test_confirmed_non_system_principal_cannot_assert_global_admin(system_directory, kind):
    from drover.auth import _is_system_admin

    if kind == "domain-local":
        system_directory.roles.list.return_value.append(
            SimpleNamespace(id="domain-admin", name="admin", domain_id="other-domain"))
        system_directory.role_assignments.list.return_value = [
            {"user": {"id": "operator"}, "role": {"id": "domain-admin"}, "scope": {"system": {"all": True}}}]
    assert _is_system_admin("operator") is False


async def test_human_raw_admin_alternative_needs_no_target_project_grants(system_directory):
    from drover.policy import authorize

    system_directory.role_assignments.list.return_value = [
        {"user": {"id": "user-1"}, "role": {"id": "admin-id"}, "scope": {"system": {"all": True}}}]
    with (
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://127.0.0.1/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=_keystone_response("project-1")),
        patch("drover.auth.current_principal_state", return_value=(True, True)),
        patch("drover.auth._current_project_roles", return_value=[]),
    ):
        info = await require_token(Request({"type": "http", "headers": []}), "human-token", None)
    assert info["project_id"] == "project-1"
    assert info["roles"] == [] and info["is_system_admin"] is True
    assert authorize("drover:clusters:create", {"project_id": "target-project"}, info)


@pytest.mark.parametrize("lookup", ["project", "system"])
async def test_human_directory_unavailability_stays_503_through_native_auth(system_directory, lookup):
    with (
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://127.0.0.1/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=_keystone_response("project-1")),
        patch("drover.auth.current_principal_state", return_value=(True, True)),
        patch("drover.auth._current_project_roles", side_effect=ValueError("partial catalog") if lookup == "project" else None,
              return_value=[]),
    ):
        if lookup == "system":
            system_directory.inference_rules.list_inference_roles.return_value = None
        with pytest.raises(HTTPException) as error:
            await require_token(Request({"type": "http", "headers": []}), "human-token", None)
    assert error.value.status_code == 503


@pytest.mark.parametrize("collection", ["roles", "role_inferences", "groups", "role_assignments"])
async def test_native_auth_collection_404_is_503_not_invalid_token(system_directory, collection):
    from keystoneauth1 import exceptions as ks_exc

    original_get = system_directory.get.side_effect
    def get(path):
        if path.split("?", 1)[0].rsplit("/", 1)[-1] == collection:
            raise ks_exc.NotFound("synthetic collection endpoint unavailable")
        return original_get(path)
    system_directory.get.side_effect = get
    with (
        patch("drover.auth._resolve_internal_keystone_endpoint", return_value="http://127.0.0.1/v3"),
        patch("drover.auth.ks_session.Session.request", return_value=_keystone_response("project-1")),
        patch("drover.auth.current_principal_state", return_value=(True, True)),
        pytest.raises(HTTPException) as error,
    ):
        await require_token(Request({"type": "http", "headers": []}), "human-token", None)
    assert error.value.status_code == 503


@pytest.mark.parametrize("marker", [False, 0, 0.0, "", [], {}])
async def test_collection_next_rejects_all_non_null_falsy_markers(marker):
    from drover.auth import _identity_collection

    client = MagicMock()
    client.get.return_value = (None, {"roles": [], "links": {"next": marker}})
    with pytest.raises(ValueError, match="Incomplete Keystone authority"):
        _identity_collection(client, "/roles", "roles")


@pytest.mark.parametrize("marker", [0, 0.0, "", [], {}])
async def test_collection_truncated_rejects_falsy_non_boolean_markers(marker):
    from drover.auth import _identity_collection

    client = MagicMock()
    client.get.return_value = (None, {"roles": [], "links": {"next": None}, "truncated": marker})
    with pytest.raises(ValueError, match="Incomplete Keystone authority"):
        _identity_collection(client, "/roles", "roles")


@pytest.mark.parametrize("metadata", [{}, {"links": {}}, {"links": {"next": None}},
                                      {"truncated": None}, {"truncated": False}])
async def test_complete_collection_allows_only_absent_null_or_false_markers(metadata):
    from drover.auth import _identity_collection

    client = MagicMock()
    client.get.return_value = (None, {"roles": [], **metadata})
    assert _identity_collection(client, "/roles", "roles") == []
