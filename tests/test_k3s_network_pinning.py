"""First-boot K3s primary-network pin executable contract tests."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

from drover.services.cloudinit import _build_k3s_network_pin_script


def _umask_022() -> None:
    os.umask(0o022)


_METADATA = """{
  "networks": [
    {"network_id": "net-primary", "link": "link-primary", "type": "ipv4"},
    {"network_id": "net-primary", "link": "link-primary", "type": "ipv6"},
    {"network_id": "net-secondary", "link": "link-secondary", "type": "ipv4"}
  ],
  "links": [
    {"id": "link-primary", "ethernet_mac_address": "AA:BB:CC:DD:EE:01"},
    {"id": "link-secondary", "ethernet_mac_address": "AA:BB:CC:DD:EE:02"}
  ]
}
"""


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(0o755)


def _prepare_guest(tmp_path: Path, *, extra_iface: bool = False) -> tuple[Path, Path, Path]:
    sys_class_net = tmp_path / "sys" / "class" / "net"
    sys_class_net.mkdir(parents=True, exist_ok=True)
    interfaces = {
        "eth-primary": "aa:bb:cc:dd:ee:01",
        "eth-secondary": "aa:bb:cc:dd:ee:02",
    }
    if extra_iface:
        interfaces["eth-third"] = "aa:bb:cc:dd:ee:03"
    for name, mac in interfaces.items():
        iface = sys_class_net / name
        iface.mkdir(exist_ok=True)
        (iface / "device").mkdir(exist_ok=True)
        (iface / "address").write_text(mac + "\n")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(exist_ok=True)
    metadata_file = tmp_path / "network_data.json"
    metadata_file.write_text(_METADATA)

    _write_executable(
        fake_bin / "curl",
        """#!/bin/bash
printf '%s\n' "$*" >> "$CURL_LOG"
cat "$METADATA_FILE"
""",
    )
    _write_executable(
        fake_bin / "ip",
        """#!/bin/bash
case "$*" in
  *"eth-primary"*) echo '2: eth-primary inet 192.0.2.10/24 scope global eth-primary' ;;
  *"eth-secondary"*) echo '3: eth-secondary inet 198.51.100.20/24 scope global eth-secondary' ;;
  *"eth-third"*) echo '4: eth-third inet 203.0.113.30/24 scope global eth-third' ;;
  *) exit 1 ;;
esac
""",
    )
    _write_executable(fake_bin / "systemd-cat", "#!/bin/bash\ncat >&2\n")
    _write_executable(fake_bin / "flock", "#!/bin/bash\nexit 0\n")
    _write_executable(
        fake_bin / "afterglow-nic-up.sh",
        """#!/bin/bash
printf '%s\n' "$1" >> "$HANDLER_LOG"
""",
    )
    return sys_class_net, fake_bin, metadata_file


def _run_pin(
    tmp_path: Path,
    *,
    server: bool = True,
    extra_iface: bool = False,
    metadata_url: str = "http://metadata.test/network_data.json",
) -> tuple[subprocess.CompletedProcess[str], Path, Path, Path]:
    sys_class_net, fake_bin, metadata_file = _prepare_guest(tmp_path, extra_iface=extra_iface)
    rancher_dir = tmp_path / "nested" / "etc" / "rancher" / "k3s"
    script = _build_k3s_network_pin_script(
        primary_network_id="net-primary",
        server=server,
        rancher_dir=str(rancher_dir),
        sys_class_net_dir=str(sys_class_net),
        metadata_url=metadata_url,
        retry_count=2,
        retry_delay_seconds=0,
    )
    script_path = tmp_path / "pin.sh"
    script_path.write_text(script)
    script_path.chmod(0o750)
    handler_path = tmp_path / "handler.sh"
    _write_executable(handler_path, '#!/bin/bash\nprintf \'%s\\n\' "$1" >> "$HANDLER_LOG"\n')
    env = os.environ | {
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "CURL_LOG": str(tmp_path / "curl.log"),
        "HANDLER_LOG": str(tmp_path / "handler.log"),
        "METADATA_FILE": str(metadata_file),
        "AFTERGLOW_NIC_HANDLER": str(handler_path),
    }
    result = subprocess.run(
        ["bash", str(script_path)],
        env=env,
        capture_output=True,
        text=True,
        preexec_fn=_umask_022,
    )
    return result, rancher_dir, tmp_path / "curl.log", tmp_path / "handler.log"


def test_server_pin_resolves_metadata_and_publishes_secure_files(tmp_path):
    result, rancher_dir, curl_log, handler_log = _run_pin(tmp_path)
    assert result.returncode == 0, result.stderr
    env_file = rancher_dir / "afterglow-primary-network.env"
    yaml_file = rancher_dir / "config.yaml.d" / "10-afterglow-primary-network.yaml"
    assert env_file.read_text() == ("AFTERGLOW_K3S_NODE_IP=192.0.2.10\nAFTERGLOW_K3S_PRIMARY_IFACE=eth-primary\n")
    assert yaml_file.read_text() == (
        'node-ip: "192.0.2.10"\nflannel-iface: "eth-primary"\nadvertise-address: "192.0.2.10"\n'
    )
    assert stat.S_IMODE((rancher_dir / "config.yaml.d").stat().st_mode) == 0o700
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(yaml_file.stat().st_mode) == 0o600
    assert not list(rancher_dir.rglob("*.tmp.*"))
    assert len(handler_log.read_text().splitlines()) == 2
    assert len(curl_log.read_text().splitlines()) == 1


def test_agent_retry_reuses_byte_stable_pin_without_metadata_recapture(tmp_path):
    result, rancher_dir, curl_log, _handler_log = _run_pin(tmp_path, server=False)
    assert result.returncode == 0, result.stderr
    env_file = rancher_dir / "afterglow-primary-network.env"
    yaml_file = rancher_dir / "config.yaml.d" / "10-afterglow-primary-network.yaml"
    first_env = env_file.read_bytes()
    first_curl_count = len(curl_log.read_text().splitlines())

    result, _, second_curl_log, _ = _run_pin(tmp_path, server=False, extra_iface=True)
    assert result.returncode == 0, result.stderr
    assert env_file.read_bytes() == first_env
    assert len(second_curl_log.read_text().splitlines()) == first_curl_count
    assert "advertise-address" not in yaml_file.read_text()


def test_pin_fails_closed_for_ambiguous_fallback(tmp_path):
    sys_class_net, fake_bin, metadata_file = _prepare_guest(tmp_path)
    metadata_file.write_text("not-json")
    rancher_dir = tmp_path / "rancher" / "k3s"
    script_path = tmp_path / "pin.sh"
    script_path.write_text(
        _build_k3s_network_pin_script(
            primary_network_id="net-primary",
            server=True,
            rancher_dir=str(rancher_dir),
            sys_class_net_dir=str(sys_class_net),
            metadata_url="http://metadata.test/network_data.json",
            retry_count=1,
            retry_delay_seconds=0,
        )
    )
    script_path.chmod(0o750)
    env = os.environ | {
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "CURL_LOG": str(tmp_path / "curl.log"),
        "METADATA_FILE": str(metadata_file),
    }
    result = subprocess.run(
        ["bash", str(script_path)],
        env=env,
        capture_output=True,
        text=True,
        preexec_fn=_umask_022,
    )
    assert result.returncode != 0
    assert not (rancher_dir / "afterglow-primary-network.env").exists()
    assert not (rancher_dir / "config.yaml.d" / "10-afterglow-primary-network.yaml").exists()


@pytest.mark.parametrize("value", ["", "net-primary\nforged", "net-primary\x7f"])
def test_pin_rejects_invalid_network_identity(value):
    with pytest.raises(ValueError):
        _build_k3s_network_pin_script(primary_network_id=value, server=True)
