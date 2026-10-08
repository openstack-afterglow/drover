"""Native keystoneauth1/python-keystoneclient trust lifecycle against a synthetic loopback Keystone.

Real SDK objects issue real HTTP: caller-token trust creation, trust-scoped password authentication without project
selectors (validated OS-TRUST fields), deletion through the impersonating trust token, and revoked-role denial.
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from drover import auth
from drover.config import Settings
from drover.services import delegation, execution

CALLER_TOKEN = "caller-token"
TRUSTOR = "operator-id"
TRUSTEE = "drover-service-id"
PROJECT = "tenant-project"
SERVICE_PASSWORD = "service-secret"
ROLES = {"member-id": "member", "editor-id": "drover-clusters_editor", "reader-id": "reader"}


class _Keystone:
    def __init__(self):
        self.trusts: dict[str, dict] = {}
        self.trust_tokens: dict[str, str] = {}
        self.revoked = False
        self.requests: list[tuple[str, str, dict]] = []


def _handler(state: _Keystone):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def _send(self, status: int, body: dict | None = None, headers: dict | None = None):
            payload = json.dumps(body).encode() if body is not None else b""
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(length) or b"{}")

        def do_POST(self):
            body = self._body()
            state.requests.append(("POST", self.path, body))
            if self.path == "/v3/OS-TRUST/trusts":
                if self.headers.get("X-Auth-Token") != CALLER_TOKEN:
                    return self._send(401, {"error": {"code": 401}})
                trust = dict(body["trust"])
                trust["id"] = uuid.uuid4().hex
                trust["roles"] = [{"id": role["id"], "name": ROLES[role["id"]]} for role in trust["roles"]]
                state.trusts[trust["id"]] = trust
                return self._send(201, {"trust": trust})
            if self.path == "/v3/auth/tokens":
                identity = body["auth"]["identity"]
                scope = body["auth"].get("scope", {})
                user = identity["password"]["user"]
                trust = state.trusts.get((scope.get("OS-TRUST:trust") or {}).get("id"))
                if "project" in scope or user.get("id") != TRUSTEE or user.get("password") != SERVICE_PASSWORD:
                    return self._send(401, {"error": {"code": 401}})
                if trust is None:
                    return self._send(404, {"error": {"code": 404}})
                if state.revoked:
                    return self._send(401, {"error": {"code": 401, "message": "trustor lost a delegated role"}})
                token = uuid.uuid4().hex
                state.trust_tokens[token] = trust["id"]
                expires = trust["expires_at"]
                roles = [*trust["roles"], {"id": "reader-id", "name": "reader"}]  # Keystone adds implied roles.
                return self._send(201, {"token": {
                    "methods": ["password"],
                    "expires_at": expires,
                    "user": {"id": TRUSTOR, "name": "operator", "domain": {"id": "default", "name": "Default"}},
                    "project": {"id": PROJECT, "name": "tenant", "domain": {"id": "default", "name": "Default"}},
                    "roles": roles,
                    "OS-TRUST:trust": {"id": trust["id"], "impersonation": True,
                                       "trustee_user": {"id": TRUSTEE}, "trustor_user": {"id": TRUSTOR}},
                    "catalog": [],
                }}, {"X-Subject-Token": token})
            return self._send(404, {"error": {"code": 404}})

        def do_DELETE(self):
            state.requests.append(("DELETE", self.path, {}))
            trust_id = self.path.rsplit("/", 1)[-1]
            token_trust = state.trust_tokens.get(self.headers.get("X-Auth-Token") or "")
            if token_trust is None and self.headers.get("X-Auth-Token") != CALLER_TOKEN:
                return self._send(401, {"error": {"code": 401}})
            if trust_id not in state.trusts:
                return self._send(404, {"error": {"code": 404}})
            del state.trusts[trust_id]
            return self._send(204)

    return Handler


@pytest.fixture
def keystone(monkeypatch):
    state = _Keystone()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/v3"
    settings = Settings(os_auth_url=url, os_password=SERVICE_PASSWORD, drover_delegated_required_roles=["member"],
                        drover_delegated_optional_roles=["load-balancer_member"])
    monkeypatch.setattr(delegation, "get_settings", lambda: settings)
    monkeypatch.setattr(auth, "_resolve_internal_keystone_endpoint", lambda session=None: url)
    monkeypatch.setattr(auth, "service_user_id", lambda: TRUSTEE)
    monkeypatch.setattr(auth, "current_principal_state", lambda user, project: (True, True))
    monkeypatch.setattr(auth, "_is_system_admin", lambda user: False)
    monkeypatch.setattr(
        auth, "current_project_role_map",
        lambda user, project: {"member": "member-id", "drover-clusters_editor": "editor-id"},
    )
    yield state
    server.shutdown()


def _token_info():
    return {"user_id": TRUSTOR, "project_id": PROJECT, "token": CALLER_TOKEN}


def _snapshot(admitted):
    return delegation._DelegationSnapshot(
        id=admitted.id, project_id=PROJECT, cluster_id="cluster", operation_id="op", action=admitted.action,
        trust_id=admitted.trust_id, trustor_user_id=TRUSTOR, trustee_user_id=TRUSTEE,
        role_ids=tuple(admitted.role_ids), expires_at=admitted.expires_at,
    )


def test_native_trust_create_authenticate_and_delete(keystone):
    admitted = delegation._admit_sync(_token_info(), PROJECT, "cluster", delegation.ACTION_CREATE)
    created = keystone.requests[0][2]["trust"]
    assert created["trustor_user_id"] == TRUSTOR and created["trustee_user_id"] == TRUSTEE
    assert created["project_id"] == PROJECT and created["impersonation"] is True
    assert created["roles"] == [{"id": "member-id"}]
    assert admitted.expires_at <= datetime.now(UTC) + timedelta(seconds=14400 + 60)

    conn = delegation._connect_sync(_snapshot(admitted))
    auth_body = next(body for method, path, body in keystone.requests if path == "/v3/auth/tokens")
    assert "project" not in auth_body["auth"]["scope"]
    assert auth_body["auth"]["scope"]["OS-TRUST:trust"]["id"] == admitted.trust_id
    conn.close()

    assert delegation._delete_trust_with_trust_token_sync(admitted.trust_id, TRUSTEE) == "deleted"
    assert admitted.trust_id not in keystone.trusts
    assert delegation._delete_trust_with_trust_token_sync(admitted.trust_id, TRUSTEE) == "gone"


def test_native_trust_revoked_role_is_terminal(keystone, monkeypatch):
    admitted = delegation._admit_sync(_token_info(), PROJECT, "cluster", delegation.ACTION_CREATE)
    keystone.revoked = True
    with pytest.raises(execution.AuthorityRevoked):
        delegation._connect_sync(_snapshot(admitted))
    assert delegation._delete_trust_with_trust_token_sync(admitted.trust_id, TRUSTEE) == "inert"

    monkeypatch.setattr(auth, "current_project_role_map", lambda user, project: {"member": "member-id"})
    keystone.revoked = False
    with pytest.raises(execution.AuthorityRevoked, match="capability"):
        delegation._connect_sync(_snapshot(admitted))
