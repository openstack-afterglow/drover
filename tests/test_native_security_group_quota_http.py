"""Actual installed SDK + synthetic loopback Neutron; no real cloud resources.

Can also run directly for the read-only budget/native rejection smoke receipt.
"""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlsplit

import pytest
from keystoneauth1 import session
from keystoneauth1.token_endpoint import Token
from openstack.connection import Connection
from openstack.exceptions import ConflictException

from drover.services import neutron, provisioner

PROJECT = "quota-project"


@contextmanager
def native_neutron():
    state = {
        "quota": {
            "security_group": {"limit": 10, "used": 1, "reserved": 0},
            "security_group_rule": {"limit": 100, "used": 4, "reserved": 0},
        },
        "extension": False,
        "templates": [],
        "quota_status": 200,
        "reject_create": False,
        "requests": [],
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def respond(self, value, status=200):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            state["requests"].append(("GET", self.path))
            path = urlsplit(self.path).path.rstrip("/")
            if path in ("", "/v2.0"):
                return self.respond({"versions": [{"id": "v2.0", "status": "CURRENT", "links": [
                    {"rel": "self", "href": endpoint + "/v2.0/"},
                ]}]})
            if path == f"/v2.0/quotas/{PROJECT}/details":
                return self.respond({"quota": state["quota"]}, state["quota_status"])
            if path == "/v2.0/extensions/security-group-default-rules":
                if state["extension"]:
                    return self.respond({"extension": {"alias": "security-group-default-rules"}})
                return self.respond({"NeutronError": {"type": "NotFound", "message": "not supported"}}, 404)
            if path == "/v2.0/default-security-group-rules":
                return self.respond(state.get("template_payload", {"default_security_group_rules": state["templates"]}))
            return self.respond({"NeutronError": {"type": "NotFound", "message": "unexpected read"}}, 404)

        def do_POST(self):
            state["requests"].append(("POST", self.path))
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            if self.path == "/v2.0/security-groups" and state["reject_create"]:
                return self.respond({"NeutronError": {
                    "type": "OverQuota", "message": "Quota exceeded for resources: ['security_group'].",
                }}, 409)
            return self.respond({"NeutronError": {"type": "BadRequest", "message": "unexpected write"}}, 400)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    endpoint = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    conn = Connection(
        session=session.Session(auth=Token(endpoint, "synthetic-project-quota-token")),
        network_endpoint_override=endpoint + "/v2.0",
        network_api_version="2",
        connect_retries=0,
    )
    try:
        yield conn, state
    finally:
        conn.close()
        server.shutdown()
        server.server_close()
        thread.join()


def _check(conn, cidrs=None):
    plan = provisioner._cluster_security_group_rules(cidrs, "pending-group")
    neutron.check_security_group_quota(conn, PROJECT, len(plan))
    return len(plan)


def test_native_quota_reads_project_details_and_includes_reserved():
    with native_neutron() as (conn, state):
        state["quota"]["security_group"].update(limit=3, used=1, reserved=2)
        with pytest.raises(neutron.SecurityGroupQuotaExceeded, match="required=1, available=0"):
            _check(conn)
        assert ("GET", f"/v2.0/quotas/{PROJECT}/details") in state["requests"]
        assert all(method == "GET" for method, _ in state["requests"])


@pytest.mark.parametrize("resource", ["security_group", "security_group_rule"])
@pytest.mark.parametrize("field,value", [
    ("limit", None), ("used", None), ("reserved", None),
    ("limit", True), ("limit", -2), ("used", -1), ("used", 1.5), ("reserved", "0"),
])
def test_native_quota_missing_or_malformed_is_not_unlimited(resource, field, value):
    with native_neutron() as (conn, state):
        state["quota"][resource][field] = value
        with pytest.raises(ValueError, match=f"{resource} quota {field}"):
            _check(conn)
        assert all(method == "GET" for method, _ in state["requests"])


@pytest.mark.parametrize("status", [403, 503])
def test_native_quota_error_does_not_fall_back(status):
    with native_neutron() as (conn, state):
        state["quota_status"] = status
        with pytest.raises(Exception):
            _check(conn)
        assert all(path.endswith("/details") or path.rstrip("/") == "/v2.0"
                   for _, path in state["requests"])


def test_native_rule_budget_includes_legacy_generated_rules_and_exact_cidr_plan():
    with native_neutron() as (conn, state):
        # Two CIDRs yield eleven authored ingress rules plus two native egress.
        state["quota"]["security_group_rule"].update(limit=17)
        assert _check(conn, ["192.0.2.0/24", "198.51.100.0/24"]) == 11
        state["quota"]["security_group_rule"]["reserved"] = 1
        with pytest.raises(neutron.SecurityGroupQuotaExceeded, match="required=13, available=12"):
            _check(conn, ["192.0.2.0/24", "198.51.100.0/24"])
        assert all(method == "GET" for method, _ in state["requests"])


def test_native_first_group_includes_implicit_default_group_and_rules():
    with native_neutron() as (conn, state):
        state["quota"]["security_group"].update(limit=1, used=0)
        state["quota"]["security_group_rule"].update(limit=15, used=0)
        with pytest.raises(neutron.SecurityGroupQuotaExceeded, match="required=2, available=1"):
            _check(conn)
        state["quota"]["security_group"]["limit"] = 2
        _check(conn)  # 9 authored + 2 custom egress + 4 implicit default rules.
        state["quota"]["security_group_rule"]["limit"] = 14
        with pytest.raises(neutron.SecurityGroupQuotaExceeded, match="required=15, available=14"):
            _check(conn)
        assert not any("/security-groups" in path for _, path in state["requests"])


def test_native_custom_rule_templates_replace_legacy_assumption():
    with native_neutron() as (conn, state):
        state["extension"] = True
        state["templates"] = [
            {"id": "custom-1", "used_in_default_sg": False, "used_in_non_default_sg": True},
            {"id": "custom-2", "used_in_default_sg": True, "used_in_non_default_sg": True},
            {"id": "default-only", "used_in_default_sg": True, "used_in_non_default_sg": False},
            {"id": "custom-3", "used_in_default_sg": False, "used_in_non_default_sg": True},
        ]
        state["quota"]["security_group_rule"].update(limit=15)  # 11 free, but 12 needed.
        with pytest.raises(neutron.SecurityGroupQuotaExceeded, match="required=12, available=11"):
            _check(conn)
        state["quota"]["security_group"].update(used=0)
        state["quota"]["security_group_rule"].update(limit=14, used=0)
        _check(conn)  # 9 authored + 3 custom + 2 default.


def test_native_empty_templates_and_unlimited_quota_are_valid():
    with native_neutron() as (conn, state):
        state["extension"] = True
        for entry in state["quota"].values():
            entry.update(limit=-1, used=500, reserved=99)
        _check(conn)
        assert all(method == "GET" for method, _ in state["requests"])


@pytest.mark.parametrize("payload", [
    {},
    {"default_security_group_rules": None},
    {"default_security_group_rules": [], "default_security_group_rules_links": [{"rel": "next", "href": "/next"}]},
    {"default_security_group_rules": [{"id": "rule", "used_in_default_sg": False}]},
    {"default_security_group_rules": [
        {"id": "rule", "used_in_default_sg": False, "used_in_non_default_sg": "true"},
    ]},
    {"default_security_group_rules": [
        {"id": "rule", "used_in_default_sg": False, "used_in_non_default_sg": True},
        {"id": "rule", "used_in_default_sg": False, "used_in_non_default_sg": True},
    ]},
])
def test_native_incomplete_templates_are_not_zero_cost(payload):
    with native_neutron() as (conn, state):
        state["extension"] = True
        state["template_payload"] = payload
        with pytest.raises(ValueError):
            _check(conn)
        assert all(method == "GET" for method, _ in state["requests"])


@pytest.mark.asyncio
async def test_worker_rechecks_after_admission_before_any_resource_write():
    with native_neutron() as (conn, state):
        _check(conn)
        state["quota"]["security_group"].update(limit=1)
        with (
            patch("drover.services.execution.open_connection", new=AsyncMock(return_value=conn)),
            patch("drover.services.keystone.close_connection", new=AsyncMock()) as close,
            patch("drover.services.provisioner._guest_plugin_settings", new=AsyncMock()),
            patch("drover.config.get_settings", return_value=SimpleNamespace(drover_boot_volume_size_gb=30)),
            patch("drover.services.store.update_cluster_status", new=AsyncMock()) as status,
            patch("drover.services.operations.append_operation_event", new=AsyncMock()),
            patch("drover.services.operations.update_operation_status", new=AsyncMock()) as operation,
            patch("drover.services.neutron.create_security_group") as group,
            patch("drover.services.cinder.create_volume_from_image") as volume,
            patch("drover.services.nova.create_server") as server,
        ):
            with pytest.raises(neutron.SecurityGroupQuotaExceeded):
                await provisioner.create_cluster_job(
                    PROJECT, "cluster", {"name": "cluster", "network_id": "network"}, "operation",
                )
        group.assert_not_called()
        volume.assert_not_called()
        server.assert_not_called()
        assert status.await_args.args[2] == "ERROR"
        assert operation.await_args.args[2] == "FAILED"
        close.assert_awaited_once_with(conn)
        assert sum(path.endswith("/details") for _, path in state["requests"]) == 2


def run_native_smoke():
    with native_neutron() as (conn, state):
        rules = _check(conn)
        assert rules == 9
        assert all(method == "GET" for method, _ in state["requests"])
        state["reject_create"] = True  # Another native writer consumes capacity after preflight.
        try:
            neutron.create_security_group(conn, "synthetic-race")
        except ConflictException:
            pass
        else:
            raise AssertionError("Native quota conflict must propagate")
        assert sum(method == "POST" for method, _ in state["requests"]) == 1
        return {
            "boundary": "synthetic loopback, actual installed openstacksdk",
            "project": PROJECT,
            "authored_rules": rules,
            "preflight_resource_writes": 0,
            "native_race_conflict_propagated": True,
            "native_create_attempts": 1,
            "requests": state["requests"],
            "live_cloud_writes": 0,
        }


def test_native_conflict_after_preflight_is_not_success_or_replayed():
    assert run_native_smoke()["native_create_attempts"] == 1


if __name__ == "__main__":
    print(json.dumps(run_native_smoke(), indent=2))
