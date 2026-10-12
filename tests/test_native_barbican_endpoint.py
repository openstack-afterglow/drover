"""Real SDK/HTTP regression for configured Barbican interface, region and resource refs."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import openstack
from keystoneauth1 import session
from keystoneauth1.identity import v3

from drover.services import barbican


def test_project_kek_and_payload_preserve_configured_catalog_scope():
    state = {"active": False}
    marker = b"synthetic-kek-payload"
    secret_id = "77777777-1111-2222-3333-444444444444"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def send_body(self, status, body, *, headers=None, content_type="application/json"):
            payload = json.dumps(body).encode() if isinstance(body, dict) else body
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            path = urlsplit(self.path).path.rstrip("/")
            if path == "/selected/v1":
                return self.send_body(200, {"version": {
                    "id": "v1", "status": "CURRENT",
                    "links": [{"rel": "self", "href": selected + "/"}],
                }})
            if path == "/selected/v1/secrets":
                secrets = [{"status": "ACTIVE", "secret_ref": public + "/secrets/" + secret_id}] if state["active"] else []
                return self.send_body(200, {"secrets": secrets})
            if path == "/selected/v1/orders/key-order":
                return self.send_body(200, {"status": "ACTIVE", "secret_ref": public + "/secrets/" + secret_id})
            if path == "/selected/v1/secrets/" + secret_id + "/payload":
                return self.send_body(200, marker, content_type="application/octet-stream")
            return self.send_body(503, {"error": "unselected catalog endpoint"})

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            if self.path == "/v3/auth/tokens":
                catalog = [{"type": "key-manager", "name": "barbican", "id": "barbican", "endpoints": [
                    {"id": "other-internal", "interface": "internal", "region": "other-region", "url": public},
                    {"id": "selected-public", "interface": "public", "region": "selected-region", "url": public},
                    {"id": "selected-internal", "interface": "internal", "region": "selected-region", "url": selected},
                ]}]
                return self.send_body(201, {"token": {
                    "methods": ["password"],
                    "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "user": {"id": "caller", "name": "caller", "domain": {"id": "default", "name": "Default"}},
                    "project": {"id": "owner-project", "name": "owner", "domain": {"id": "default", "name": "Default"}},
                    "roles": [{"id": "creator", "name": "creator"}], "catalog": catalog,
                }}, headers={"X-Subject-Token": "synthetic-token"})
            if self.path == "/selected/v1/orders":
                state["active"] = True
                return self.send_body(202, {"order_ref": public + "/orders/key-order"})
            return self.send_body(503, {"error": "unselected catalog endpoint"})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    origin = f"http://127.0.0.1:{server.server_address[1]}"
    selected = origin + "/selected/v1"
    public = origin + "/unselected/v1"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    auth = v3.Password(auth_url=origin + "/v3", username="caller", password="synthetic-password",
                       project_id="owner-project", user_domain_name="Default")
    sess = session.Session(auth=auth, timeout=2)
    conn = None
    try:
        conn = openstack.connection.Connection(session=sess, region_name="selected-region", interface="internal")
        assert barbican._ensure_project_kek_sync(conn) == secret_id
        assert barbican._ensure_project_kek_sync(conn) == secret_id
        assert barbican.get_secret_payload(conn, secret_id) == marker
    finally:
        if conn is not None:
            conn.close()
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
