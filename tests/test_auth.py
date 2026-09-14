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
