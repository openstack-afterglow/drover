"""Real Uvicorn/Keystone-client loopback role smoke; synthetic domain stores only.

No external cloud, IAM writes, shared DB/Redis or live credentials. Lifespan is off.
Run from repository root with PYTHONPATH=. python scripts/standard_role_http_smoke.py.
"""
from __future__ import annotations

import json
import socket
import tempfile
import threading
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import httpx
import uvicorn
from keystoneauth1 import session, token_endpoint
from keystoneclient.v3 import client as keystone_client

from drover import auth, policy
from drover.config import Settings
from drover.main import app
from drover.services import delegation

ROLES = [{"id": name, "name": name} for name in ("manager", "member", "reader", "custom", "drover_admin", "admin")]
ASSIGNMENTS = {"manager": ["manager"], "member": ["member"], "reader": ["reader"],
               "custom": ["custom"], "legacy": ["drover_admin"], "legacy-implied": ["drover_admin"],
               "reduced": ["member"], "foreign": ["member"]}
CALLS = []


class Identity(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        path = urlsplit(self.path).path.removeprefix("/v3")
        assert path == "/OS-TRUST/trusts", path
        document = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["trust"]
        assert document["trustor_user_id"] in {"manager", "member"}
        assert document["roles"] == [{"id": "member"}]
        assert document["trustee_user_id"] == "synthetic-service"
        CALLS.append({"method": "POST", "path": path})
        document["id"] = "trust-" + document["trustor_user_id"]
        data = json.dumps({"trust": document}).encode()
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_DELETE(self):
        path = urlsplit(self.path).path.removeprefix("/v3")
        assert path.startswith("/OS-TRUST/trusts/trust-")
        CALLS.append({"method": "DELETE", "path": path})
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path.removeprefix("/v3")
        CALLS.append({"method": "GET", "path": path})
        if path == "/auth/tokens":
            name = self.headers.get("X-Subject-Token", "reader")
            project = "other" if name == "foreign" else "project"
            reduced = name == "reduced"
            body = {"token": {"methods": ["application_credential" if reduced else "password"],
                "expires_at": "2099-01-01T00:00:00Z", "issued_at": "2026-01-01T00:00:00Z",
                "user": {"id": name, "name": name, "domain": {"id": "domain"}},
                "project": {"id": project, "name": project, "domain": {"id": "domain"}},
                "roles": [{"id": "reader", "name": "reader"}] if reduced else
                         [{"id": role, "name": role} for role in ASSIGNMENTS[name]]}}
            if reduced:
                body["token"]["application_credential"] = {"id": "synthetic-app", "restricted": True}
        elif path == "/roles":
            body = {"roles": ROLES, "links": {"next": None}}
        elif path == "/role_inferences":
            body = {"role_inferences": [
                {"prior_role": {"id": "manager"}, "implies": [{"id": "member"}]},
                {"prior_role": {"id": "member"}, "implies": [{"id": "reader"}]},
                {"prior_role": {"id": "custom"}, "implies": [{"id": "member"}]},
            ], "links": {"next": None}}
            # Legacy native implications remain valid when explicitly present.
            if getattr(self.server, "legacy_implication", False):
                body["role_inferences"].append({"prior_role": {"id": "drover_admin"}, "implies": [{"id": "member"}]})
        elif path == "/role_assignments":
            user = query.get("user.id", [""])[0]
            project = query.get("scope.project.id", [""])[0]
            rows = [] if not project else [{"user": {"id": user}, "scope": {"project": {"id": project}},
                    "role": {"id": role}} for role in ASSIGNMENTS[user]]
            body = {"role_assignments": rows, "links": {"next": None}}
        elif path.endswith("/groups"):
            body = {"groups": [], "links": {"next": None}}
        elif path.startswith("/users/"):
            body = {"user": {"id": path.split("/")[-1], "domain_id": "domain", "enabled": True}}
        elif path.startswith("/projects/"):
            body = {"project": {"id": path.split("/")[-1], "domain_id": "domain", "enabled": True}}
        elif path.startswith("/domains/"):
            body = {"domain": {"id": "domain", "enabled": True}}
        else:
            self.send_error(404)
            return
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    receipts = []
    identity = ThreadingHTTPServer(("127.0.0.1", 0), Identity)
    identity_thread = threading.Thread(target=identity.serve_forever, daemon=True)
    identity_thread.start()
    endpoint = f"http://127.0.0.1:{identity.server_port}/v3"
    native_session = session.Session(auth=token_endpoint.Token(endpoint, "directory-token"))
    directory = keystone_client.Client(session=native_session, endpoint_override=endpoint)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
    settings = Settings(_env_file=None, os_auth_url=endpoint, oslo_policy_policy_file="")

    async def cluster(project, cluster_id):
        if project != "project" or cluster_id != "owned":
            return None
        return {"id": "owned", "name": "owned", "project_id": project, "status": "ACTIVE"}

    async def configmap(*args, **kwargs):
        return {"name": args[2], "namespace": args[1], "data": args[3]}

    # Only synthetic domain storage/provider writes are replaced. Auth, SDK HTTP,
    # graph, action policy, dependencies and real listening app remain native.
    with ExitStack() as stack, tempfile.TemporaryDirectory() as scratch:
        stack.enter_context(patch("drover.auth.get_settings", return_value=settings))
        stack.enter_context(patch("drover.policy.get_settings", return_value=settings))
        stack.enter_context(patch("drover.auth._resolve_internal_keystone_endpoint", return_value=endpoint))
        stack.enter_context(patch("drover.auth._get_admin_ks_client", return_value=directory))
        stack.enter_context(patch("drover.services.delegation.get_settings", return_value=settings))
        stack.enter_context(patch("drover.auth.service_user_id", return_value="synthetic-service"))
        stack.enter_context(patch("drover.services.store.list_clusters", new=AsyncMock(return_value=[])))
        stack.enter_context(patch("drover.services.store.get_cluster", new=cluster))
        stack.enter_context(patch("drover.api.clusters.is_db_configured", return_value=False))
        stack.enter_context(patch("drover.services.kube.create_configmap", new=configmap))
        stack.enter_context(patch("drover.api.configmaps.invalidate", new=AsyncMock()))
        stack.enter_context(patch("drover.api.configmaps.rec", new=AsyncMock()))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        with httpx.Client(base_url=url, timeout=10, trust_env=False) as client:
            import time
            deadline = time.monotonic() + 10
            while not server.started and time.monotonic() < deadline:
                time.sleep(0.01)
            assert server.started
            try:
                for role in ("manager", "member"):
                    principal = auth.validate_token(role)
                    admitted = delegation._admit_sync(principal, "project", "owned", "drover:clusters:create")
                    assert admitted.role_ids == ["member"]
                    delegation._delete_trust_with_caller_token(role, admitted.trust_id)
                for role in ("manager", "member", "reader", "custom", "legacy", "reduced", "foreign"):
                    headers = {"X-Auth-Token": role}
                    cap = client.get("/v1/policy/capabilities", headers=headers)
                    assert cap.status_code == 200, cap.text
                    actions = cap.json()["actions"]
                    manage = role in {"manager", "member", "custom", "foreign"}
                    assert ("drover:clusters:delete" in actions) is manage
                    inventory = client.get("/v1/clusters?no_cache=true", headers=headers)
                    assert inventory.status_code == (403 if role == "legacy" else 200), inventory.text
                    write = client.post("/v1/clusters/owned/namespaces/default/configmaps", headers=headers,
                                        json={"name": "smoke", "data": {"safe": "synthetic"}})
                    expected = 404 if role == "foreign" else 201 if manage else 403
                    assert write.status_code == expected, (role, write.status_code, write.text)
                    if not manage:
                        for path in ("/v1/clusters/owned/kubeconfig?grade=admin", "/v1/clusters/owned/secrets"):
                            assert client.get(path, headers=headers).status_code == 403
                    assert client.get("/v1/admin/resource-policies", headers=headers).status_code == 403
                    receipts.append({"role": role, "capabilities": cap.status_code,
                                     "inventory": inventory.status_code, "configmap_post": write.status_code,
                                     "manage_actions": manage})
                identity.legacy_implication = True
                implied = client.get("/v1/policy/capabilities", headers={"X-Auth-Token": "legacy-implied"})
                assert "drover:clusters:delete" in implied.json()["actions"]
                receipts.append({"role": "legacy-native-implied-member", "manage_actions": True})
                # Real file reload cannot bypass reduced or reader mutation ceilings.
                path = Path(scratch) / "policy.yaml"
                path.write_text('"drover:clusters:delete": "@"\n"drover:workloads:write": "@"\n')
                settings.oslo_policy_policy_file = str(path)
                for role in ("reader", "reduced", "legacy"):
                    if role == "legacy":
                        identity.legacy_implication = False
                    result = client.get("/v1/policy/capabilities", headers={"X-Auth-Token": role})
                    assert "drover:clusters:delete" not in result.json()["actions"]
            finally:
                server.should_exit = True
                thread.join(timeout=10)
                assert not thread.is_alive()
    identity.shutdown()
    identity.server_close()
    native_session.session.close()
    policy.reset_enforcer()
    print(json.dumps({"scope": "native-uvicorn-keystone-sdk-loopback-synthetic-domain-no-live-cloud",
                      "roles": receipts, "keystone_http_requests": len(CALLS), "iam_writes": 0,
                      "provider_configmap_writes": 3, "native_trust_creates": 2, "native_trust_deletes": 2,
                      "foreign_write_denied": True,
                      "operator_allow_all_ceiling": True}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
