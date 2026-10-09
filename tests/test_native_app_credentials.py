"""Installed SDK creation/cleanup and current directory graph against synthetic HTTP only."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from keystoneauth1 import exceptions as ks_exc
from keystoneauth1 import session, token_endpoint
from keystoneclient.v3 import client
from openstack.connection import Connection
from openstack.identity.v3._proxy import Proxy

from drover import auth
from drover.services import cluster_authority, delegation

OWNER = "synthetic-owner"
PROJECT = "synthetic-project"
CREDENTIAL = "synthetic-credential"


@pytest.fixture
def native_credentials(monkeypatch):
    state = SimpleNamespace(
        catalog=[{"id": "member-id", "name": "member"}, {"id": "reader-id", "name": "reader"},
                 {"id": "admin-id", "name": "admin"}, {"id": "manager-id", "name": "manager"},
                 {"id": "unrelated-id", "name": "unrelated"}],
        edges=[{"prior_role": {"id": "member-id"}, "implies": [{"id": "reader-id"}]}],
        held=["member-id", "admin-id", "unrelated-id"],
        returned_roles=[{"id": "member-id", "name": "member"}, {"id": "reader-id", "name": "reader"}],
        requests=[], created=[], deleted=[], overrides={},
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def send(self, status, body=None):
            payload = json.dumps(body).encode() if body is not None else b""
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            path = urlsplit(self.path)
            state.requests.append(("GET", path.path))
            if path.path == "/v3/roles":
                return self.send(200, {"roles": state.catalog, "links": {"next": None}})
            if path.path == "/v3/role_inferences":
                return self.send(200, {"role_inferences": state.edges, "links": {"next": None}})
            if path.path == "/v3/role_assignments":
                query = parse_qs(path.query)
                if query.get("user.id") != [OWNER] or query.get("scope.project.id") != [PROJECT]:
                    return self.send(400, {"error": {"message": "wrong principal"}})
                assignments = [{"user": {"id": OWNER}, "scope": {"project": {"id": PROJECT}},
                                "role": {"id": rid}} for rid in state.held]
                return self.send(200, {"role_assignments": assignments, "links": {"next": None}})
            return self.send(404, {"error": {"message": "unexpected synthetic endpoint"}})

        def do_POST(self):
            state.requests.append(("POST", self.path))
            if self.path != f"/v3/users/{OWNER}/application_credentials":
                return self.send(404)
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requested = body["application_credential"]
            # Independent provider contract: reject widening the request itself.
            if requested.get("roles") != [{"id": "member-id"}] or requested.get("unrestricted") is not False:
                return self.send(403, {"error": {"message": "unsafe synthetic request"}})
            state.created.append(CREDENTIAL)
            return self.send(201, {"application_credential": {
                "id": CREDENTIAL, "secret": "synthetic-only-never-exported", "project_id": PROJECT,
                "unrestricted": False, "roles": state.returned_roles, **state.overrides,
            }})

        def do_DELETE(self):
            state.requests.append(("DELETE", self.path))
            if self.path != f"/v3/users/{OWNER}/application_credentials/{CREDENTIAL}":
                return self.send(404)
            state.deleted.append(CREDENTIAL)
            return self.send(204)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/v3"
    transport = requests.Session()
    transport.trust_env = False
    ks_session = session.Session(auth=token_endpoint.Token(url, "synthetic-caller-token"),
                                 session=transport, timeout=5)
    directory = client.Client(session=ks_session, endpoint_override=url)
    connection = Connection(session=ks_session)
    identity = Proxy(session=ks_session, endpoint_override=url, service_type="identity", version="3")
    identity._connection = connection
    monkeypatch.setattr(auth, "_get_admin_ks_client", lambda: directory)
    monkeypatch.setattr(delegation, "get_settings", lambda: SimpleNamespace(
        drover_delegated_required_roles=["member"], drover_delegated_optional_roles=[],
    ))
    try:
        yield state, SimpleNamespace(identity=identity)
    finally:
        connection.close()
        transport.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


async def _issue(connection):
    return await cluster_authority.issue(
        connection, {"user_id": OWNER, "project_id": PROJECT}, project_id=PROJECT,
        cluster_id="synthetic-cluster", generation=1, purposes=[cluster_authority.CONTROL],
    )


@pytest.mark.asyncio
async def test_native_credential_accepts_current_implied_roles_and_snapshots_them(native_credentials):
    state, connection = native_credentials
    issued = await _issue(connection)
    assert len(issued) == 1
    assert issued[0].owner_user_id == OWNER
    assert issued[0].app_credential_id == CREDENTIAL
    assert issued[0].role_names == ["member", "reader"]
    assert issued[0].role_ids == ["member-id", "reader-id"]
    assert "synthetic-only-never-exported" not in repr(issued)
    assert state.created == [CREDENTIAL]
    assert state.deleted == []


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [
    [{"id": "reader-id"}],  # Required member root is missing.
    [{"id": "member-id"}, {"id": "unrelated-id"}],  # Held is not sufficient.
    [{"id": "member-id"}, {"id": "admin-id"}],
    [{"id": "member-id"}, {"id": "manager-id"}],
    [{"id": "member-id"}, {"id": "unknown-id"}],
    [{"id": "member-id"}, {"id": "domain-id"}],
    [],
])
async def test_native_credential_rejects_roles_outside_closure_and_cleans_up(native_credentials, roles):
    state, connection = native_credentials
    state.returned_roles = roles
    with pytest.raises(cluster_authority.CredentialIssueError):
        await _issue(connection)
    assert state.created == [CREDENTIAL]
    assert state.deleted == [CREDENTIAL]


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{"project_id": "other-project"}, {"unrestricted": True}, {"secret": ""}])
async def test_native_credential_rejects_postcreate_authority_mismatch(native_credentials, overrides):
    state, connection = native_credentials
    state.overrides = overrides
    with pytest.raises(cluster_authority.CredentialIssueError):
        await _issue(connection)
    assert state.deleted == [CREDENTIAL]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["admin", "manager", "unknown", "domain", "ambiguous", "unheld"])
async def test_native_directory_rejects_unsafe_or_invalid_closure_before_creation(native_credentials, failure):
    state, connection = native_credentials
    if failure in {"admin", "manager"}:
        state.edges[0]["implies"] = [{"id": f"{failure}-id"}]
    elif failure == "unknown":
        state.edges[0]["implies"] = [{"id": "unknown-id"}]
    elif failure == "domain":
        state.catalog.append({"id": "domain-id", "name": "domain-role", "domain_id": "domain"})
        state.edges[0]["implies"] = [{"id": "domain-id"}]
    elif failure == "ambiguous":
        state.catalog.append({"id": "duplicate-id", "name": "reader"})
    else:
        state.held = ["admin-id", "unrelated-id"]
    with pytest.raises((cluster_authority.CredentialIssueError, ValueError, delegation.DelegationDenied, ks_exc.NotFound)):
        await _issue(connection)
    assert state.created == []
    assert state.deleted == []


@pytest.mark.asyncio
async def test_native_credential_expansion_requires_the_current_graph(native_credentials):
    state, connection = native_credentials
    state.edges = []
    with pytest.raises(cluster_authority.CredentialIssueError):
        await _issue(connection)
    assert state.deleted == [CREDENTIAL]


@pytest.mark.asyncio
async def test_native_credential_closure_is_transitive_and_cycle_safe(native_credentials):
    state, connection = native_credentials
    state.catalog.append({"id": "observer-id", "name": "observer"})
    state.edges.extend([
        {"prior_role": {"id": "reader-id"}, "implies": [{"id": "observer-id"}]},
        {"prior_role": {"id": "observer-id"}, "implies": [{"id": "member-id"}]},
    ])
    state.returned_roles.append({"id": "observer-id", "name": "observer"})
    issued = await _issue(connection)
    assert issued[0].role_names == ["member", "observer", "reader"]
    assert issued[0].role_ids == ["member-id", "observer-id", "reader-id"]
    assert state.deleted == []
