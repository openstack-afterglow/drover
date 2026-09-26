"""K3s Pod reply-route rule: every node type installs it before K3s starts."""

from __future__ import annotations

import base64
import configparser
import gzip
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

from drover.services.cloudinit import generate_agent_userdata, generate_server_userdata

_SCRIPT = "/usr/local/sbin/afterglow-k3s-pod-route.sh"
_UNIT = "afterglow-k3s-pod-route.service"


def _render(role: str, os_type: str) -> dict[str, tuple[str, int]]:
    """Return {path: (content, mode)} for files the node bootstrap writes."""
    if role == "server":
        result = generate_server_userdata(
            cluster_name="c",
            k3s_version="v1.34.1+k3s1",
            callback_url="https://drover.example",
            callback_token="tok",
            primary_network_id="net-primary",
            os_type=os_type,
        )
    else:
        result = generate_agent_userdata(
            cluster_name="c",
            k3s_version="v1.34.1+k3s1",
            server_ip="192.0.2.10",
            node_token="node-token",
            primary_network_id="net-primary",
            os_type=os_type,
        )
    if os_type == "fcos":
        files = {}
        for entry in json.loads(base64.b64decode(result.data))["storage"]["files"]:
            raw = base64.b64decode(entry["contents"]["source"].split(",", 1)[1])
            files[entry["path"]] = (gzip.decompress(raw).decode(), entry["mode"])
        return files
    config = yaml.safe_load(gzip.decompress(base64.b64decode(result.data)))
    return {wf["path"]: (wf["content"], int(wf["permissions"], 8)) for wf in config["write_files"]}


def _unit(text: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    parser.read_string(text)
    return parser


@pytest.mark.parametrize("os_type", ["ubuntu", "fcos"])
@pytest.mark.parametrize(("role", "k3s_unit"), [("server", "k3s.service"), ("agent", "k3s-agent.service")])
def test_k3s_unit_cannot_start_without_pod_route_rule(role: str, k3s_unit: str, os_type: str) -> None:
    files = _render(role, os_type)

    dropin = _unit(files[f"/etc/systemd/system/{k3s_unit}.d/10-afterglow-pod-route.conf"][0])
    assert dropin["Unit"]["Wants"] == _UNIT
    assert dropin["Unit"]["After"] == _UNIT
    assert dropin["Service"]["ExecStartPre"] == f"{_SCRIPT} ensure"

    watcher = _unit(files[f"/etc/systemd/system/{_UNIT}"][0])
    assert watcher["Service"]["ExecStart"] == f"{_SCRIPT} watch"
    assert watcher["Service"]["Restart"] == "always"

    assert files[_SCRIPT][1] & 0o111


_FAKE_IP = """#!/bin/bash
# Stateful `ip -4 rule show|add SELECTOR` emulation backed by $RULES.
[ "$1" = -4 ] && [ "$2" = rule ] || exit 2
op="$3"; shift 3
case "$op" in
  show) grep -Fx -- "$*" "$RULES" || true ;;
  add)
    [ -z "${FAIL_ADD:-}" ] || exit 1
    grep -Fxq -- "$*" "$RULES" && exit 2
    printf '%s\\n' "$*" >> "$RULES" ;;
  *) exit 2 ;;
esac
"""


def test_pod_route_ensure_is_idempotent_and_fails_closed(tmp_path: Path) -> None:
    script = tmp_path / "pod-route.sh"
    script.write_text(_render("server", "ubuntu")[_SCRIPT][0])
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "ip").write_text(_FAKE_IP)
    (fake_bin / "ip").chmod(0o755)
    rules = tmp_path / "rules"
    rules.write_text("")
    env = os.environ | {"PATH": f"{fake_bin}:/usr/bin:/bin", "RULES": str(rules)}

    def ensure(**extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["bash", str(script), "ensure"], env=env | extra, capture_output=True, text=True)

    assert ensure().returncode == 0
    assert ensure().returncode == 0
    assert rules.read_text().splitlines() == ["priority 29999 to 10.42.0.0/16 table main"]

    rules.write_text("")
    failed = ensure(FAIL_ADD="1")
    assert failed.returncode != 0
    assert "Pod route rule is missing" in failed.stderr
