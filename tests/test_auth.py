"""Drover request-scoped OpenStack authorization contracts."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import HTTPException
from keystoneauth1 import session as ks_session
from keystoneauth1 import token_endpoint
from keystoneauth1.exceptions.http import BadRequest, Unauthorized
from keystoneclient.v3.role_assignments import RoleAssignmentManager
from keystoneclient.v3.roles import RoleManager
from requests import ConnectionError as RequestsConnectionError
from requests import Response
from requests import Session as RequestsSession
from starlette.requests import Request

from drover.auth import _get_admin_ks_client, _is_system_admin, get_os_conn, require_token, validate_token

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def clear_internal_keystone_endpoint_cache():
    with patch("drover.auth._internal_keystone_endpoint_cache", None):
        yield


@pytest.fixture(autouse=True)
def synthetic_project_role_directory():
    # Token transport tests bypass the project-role graph, never the system-admin directory.
    with patch("drover.auth._current_project_roles", return_value=["member"]):
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
            "methods": ["token"],
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
        assert method.upper() == "POST"
        assert url == "https://keystone.internal.example/v3/auth/tokens"
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


_SYSTEM_USER = "verified-system-user"
_ADMIN_ROLE = {"id": "real-admin-id", "name": "admin", "domain_id": None}
_DIRECT_SYSTEM_QUERY = {
    "user.id": [_SYSTEM_USER], "role.id": [_ADMIN_ROLE["id"]], "scope.system": ["all"],
}


def _direct_system_assignment(**changes):
    return {
        "user": {"id": _SYSTEM_USER}, "role": {"id": _ADMIN_ROLE["id"]},
        "scope": {"system": {"all": True}},
    } | changes


@pytest.fixture
def native_system_directory(monkeypatch):
    """Installed managers/adapter/session over real HTTP; only Keystone is synthetic."""
    state = SimpleNamespace(
        catalog={"roles": [dict(_ADMIN_ROLE)]},
        assignments={"role_assignments": [_direct_system_assignment()]},
        calls=[], failure_path=None, failure_status=500,
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_GET(self):
            url = urlsplit(self.path)
            query = parse_qs(url.query)
            state.calls.append((url.path, query))
            status, body = 200, None
            if self.headers.get("X-Auth-Token") != "synthetic-directory-token":
                status = 401
            elif url.path == state.failure_path:
                status = state.failure_status
            elif url.path == "/v3/roles" and query == {"name": ["admin"]}:
                body = state.catalog
            elif url.path == "/v3/role_assignments" and query == _DIRECT_SYSTEM_QUERY:
                body = state.assignments
            else:
                # Reproduce the effective-system rejection at the HTTP boundary,
                # not an invented unsupported system kwarg in the native manager.
                status = 400
            if body is None:
                body = {"error": {"code": status, "message": "Synthetic Keystone rejection"}}
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    endpoint = f"http://127.0.0.1:{server.server_address[1]}/v3"
    transport = RequestsSession()
    transport.trust_env = False
    session = ks_session.Session(
        auth=token_endpoint.Token(endpoint, "synthetic-directory-token"), session=transport, timeout=2,
    )
    monkeypatch.setattr("drover.auth._get_admin_ks_session", lambda: session)
    monkeypatch.setattr("drover.auth._resolve_internal_keystone_endpoint", lambda session=None: endpoint)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        transport.close()


async def test_system_admin_requires_direct_native_system_assignment(native_system_directory):
    client = _get_admin_ks_client()
    assert isinstance(client.role_assignments, RoleAssignmentManager)
    assert isinstance(client.roles, RoleManager)
    # The installed manager accepts system='all' and actually sends effective=True.
    with pytest.raises(BadRequest):
        client.role_assignments.list(user=_SYSTEM_USER, role=_ADMIN_ROLE["id"], system="all", effective=True)
    assert native_system_directory.calls == [
        ("/v3/role_assignments", _DIRECT_SYSTEM_QUERY | {"effective": ["True"]}),
    ]
    native_system_directory.calls.clear()

    assert _is_system_admin(_SYSTEM_USER) is True
    assert native_system_directory.calls == [
        ("/v3/roles", {"name": ["admin"]}), ("/v3/role_assignments", _DIRECT_SYSTEM_QUERY),
    ]


@pytest.mark.parametrize("user_id", [None, "", 123])
async def test_system_admin_missing_user_never_queries_directory(native_system_directory, user_id):
    assert _is_system_admin(user_id) is False
    assert native_system_directory.calls == []


@pytest.mark.parametrize("assignments", [
    [],
    [{}],
    [_direct_system_assignment(user={})],
    [_direct_system_assignment(user=None)],
    [_direct_system_assignment(role={})],
    [_direct_system_assignment(role=None)],
    [_direct_system_assignment(user={"id": "other-user"})],
    [_direct_system_assignment(role={"id": "domain-admin-id"})],
    [_direct_system_assignment(scope={"project": {"id": "tenant"}})],
    [_direct_system_assignment(scope={"domain": {"id": "default"}})],
    [_direct_system_assignment(user={}, group={"id": "admins"})],
    [_direct_system_assignment(group={"id": "admins"})],
    [_direct_system_assignment(scope={})],
    [_direct_system_assignment(scope={"system": {"all": False}})],
    [_direct_system_assignment(scope={"system": {"all": "all"}})],
    [_direct_system_assignment(scope={"system": {"all": 1}})],
    [_direct_system_assignment(scope={"system": {"id": "all"}})],
    [_direct_system_assignment(scope={"system": {"all": True}, "project": {"id": "tenant"}})],
    [_direct_system_assignment(scope={"system": {"all": True}, "domain": {"id": "default"}})],
    [_direct_system_assignment(scope={"system": {"all": True}, "OS-INHERIT:inherited_to": "projects"})],
    [_direct_system_assignment(), _direct_system_assignment()],
    [_direct_system_assignment(), _direct_system_assignment(user={"id": "other-user"})],
    [{"role": {"id": _ADMIN_ROLE["id"]}, "scope": {"system": {"all": True}}}],
    [{"user": {"id": _SYSTEM_USER}, "scope": {"system": {"all": True}}}],
    [{"user": {"id": _SYSTEM_USER}, "role": {"id": _ADMIN_ROLE["id"]}}],
    [_direct_system_assignment(user={"id": ""})],
    [_direct_system_assignment(role={"id": ""})],
    [{"group": {"id": "admins"}, "role": {"id": _ADMIN_ROLE["id"]}, "scope": {"system": {"all": True}}}],
    [_direct_system_assignment(scope={"system": {"all": True, "id": "other-system"}})],
], ids=[
    "empty", "empty-row", "missing-user-id", "null-user", "missing-role-id", "null-role", "wrong-user",
    "wrong-role", "project", "domain", "group", "user-and-group", "missing-scope", "wrong-system",
    "nonboolean-system", "integer-system", "system-id-not-all", "mixed-project", "mixed-domain",
    "inherited", "duplicate", "mixed-subjects",
    "missing-user", "missing-role", "absent-scope", "empty-user-id", "empty-role-id", "group-only",
    "extra-system-selector",
])
async def test_native_system_assignment_rejects_wrong_or_ambiguous_rows(native_system_directory, assignments):
    native_system_directory.assignments = {"role_assignments": assignments}
    assert _is_system_admin(_SYSTEM_USER) is False
    assert native_system_directory.calls[-1] == ("/v3/role_assignments", _DIRECT_SYSTEM_QUERY)


@pytest.mark.parametrize("catalog", [
    [],
    [{"name": "admin"}],
    [_ADMIN_ROLE | {"id": ""}],
    [_ADMIN_ROLE | {"id": None}],
    [_ADMIN_ROLE | {"id": 123}],
    [_ADMIN_ROLE | {"name": "manager"}],
    [_ADMIN_ROLE | {"domain_id": "default"}],
    [_ADMIN_ROLE | {"domain": {"id": "default"}}],
    [_ADMIN_ROLE, _ADMIN_ROLE | {"id": "other-admin-id"}],
    [_ADMIN_ROLE, _ADMIN_ROLE],
], ids=[
    "empty", "missing-id", "empty-id", "null-id", "nonstring-id", "wrong-name", "domain-only",
    "domain-object", "ambiguous", "duplicate",
])
async def test_native_admin_role_catalog_fails_closed(native_system_directory, catalog):
    native_system_directory.catalog = {"roles": catalog}
    assert _is_system_admin(_SYSTEM_USER) is False
    assert native_system_directory.calls == [("/v3/roles", {"name": ["admin"]})]


async def test_native_admin_role_catalog_uses_unique_global_not_domain_alias(native_system_directory):
    native_system_directory.catalog = {"roles": [
        _ADMIN_ROLE | {"id": "domain-admin-id", "domain_id": "default"}, _ADMIN_ROLE,
    ]}
    assert _is_system_admin(_SYSTEM_USER) is True
    assert native_system_directory.calls[-1] == ("/v3/role_assignments", _DIRECT_SYSTEM_QUERY)


@pytest.mark.parametrize("path", ["/v3/roles", "/v3/role_assignments"])
@pytest.mark.parametrize("status", [400, 401, 403, 500])
async def test_native_system_directory_http_failures_are_denied(native_system_directory, path, status):
    native_system_directory.failure_path = path
    native_system_directory.failure_status = status
    assert _is_system_admin(_SYSTEM_USER) is False
    assert native_system_directory.calls[-1][0] == path


@pytest.mark.parametrize("path", ["/v3/roles", "/v3/role_assignments"])
async def test_native_system_directory_malformed_response_is_denied(native_system_directory, path):
    # HTTP succeeds, but native manager decoding raises for its missing collection key.
    if path == "/v3/roles":
        native_system_directory.catalog = {}
    else:
        native_system_directory.assignments = {}
    assert _is_system_admin(_SYSTEM_USER) is False
    assert native_system_directory.calls[-1][0] == path


@pytest.mark.parametrize("path", ["/v3/roles", "/v3/role_assignments"])
async def test_native_system_directory_transport_exception_is_denied(native_system_directory, monkeypatch, path):
    original_send = RequestsSession.send
    attempted = []

    def failing_send(session, request, **kwargs):
        if urlsplit(request.url).path == path:
            attempted.append(path)
            raise RequestsConnectionError("Synthetic directory connection failure")
        return original_send(session, request, **kwargs)

    monkeypatch.setattr(RequestsSession, "send", failing_send)
    assert _is_system_admin(_SYSTEM_USER) is False
    assert attempted == [path]


async def test_system_admin_client_construction_exception_is_denied(native_system_directory, monkeypatch):
    def unavailable(session=None):
        raise RuntimeError("Synthetic internal endpoint unavailable")

    monkeypatch.setattr("drover.auth._resolve_internal_keystone_endpoint", unavailable)
    assert _is_system_admin(_SYSTEM_USER) is False
    assert native_system_directory.calls == []
