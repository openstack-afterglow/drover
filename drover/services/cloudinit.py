"""k3s cloud-init / Ignition 템플릿 렌더링."""

import base64
import gzip
import json
import shlex
from pathlib import Path
from typing import NamedTuple

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from drover.utils.ssh_keys import validate_ssh_public_key

TEMPLATE_DIR = Path(__file__).parent.parent / "templates"
_jinja = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    undefined=StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
    # YAML/Bash 출력 — HTML autoescape 비활성. 사용자 입력은 반드시 `| shlex_quote`.
    autoescape=False,
)
_jinja.filters["shlex_quote"] = shlex.quote

OS_TYPE_UBUNTU = "ubuntu"
OS_TYPE_FCOS = "fcos"
VALID_OS_TYPES = {OS_TYPE_UBUNTU, OS_TYPE_FCOS}


class UserdataResult(NamedTuple):
    """cloud-init 또는 Ignition userdata 렌더링 결과."""

    data: str  # Nova user_data로 전달할 값
    config_drive: bool  # FCOS는 config_drive=True 필요


def _b64(text: str) -> str:
    """문자열을 base64로 인코딩하여 반환."""
    return base64.b64encode(text.encode()).decode()


def _validate_pin_input(name: str, value: str) -> str:
    if not isinstance(value, str) or not value or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError(f"{name}이 비어 있거나 제어 문자를 포함합니다")
    return value


def _build_k3s_network_pin_script(
    *,
    primary_network_id: str,
    server: bool,
    rancher_dir: str = "/etc/rancher/k3s",
    sys_class_net_dir: str = "/sys/class/net",
    metadata_url: str = "http://169.254.169.254/openstack/latest/network_data.json",
    retry_count: int = 15,
    retry_delay_seconds: int = 2,
) -> str:
    """Build the first-boot script that pins K3s to the creation network."""
    primary_network_id = _validate_pin_input("primary_network_id", primary_network_id)
    rancher_dir = _validate_pin_input("rancher_dir", rancher_dir)
    sys_class_net_dir = _validate_pin_input("sys_class_net_dir", sys_class_net_dir)
    metadata_url = _validate_pin_input("metadata_url", metadata_url)
    if retry_count < 1:
        raise ValueError("retry_count는 1 이상이어야 합니다")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds는 0 이상이어야 합니다")

    primary_network_id_q = shlex.quote(primary_network_id)
    rancher_dir_q = shlex.quote(rancher_dir)
    sys_class_net_dir_q = shlex.quote(sys_class_net_dir)
    metadata_url_q = shlex.quote(metadata_url)
    server_yaml = 'advertise-address: "${AFTERGLOW_K3S_NODE_IP}"\n' if server else ""
    return (
        r"""#!/bin/bash
set -euo pipefail

PRIMARY_NETWORK_ID=__PRIMARY_NETWORK_ID__
RANCHER_DIR=__RANCHER_DIR__
SYS_CLASS_NET_DIR=__SYS_CLASS_NET_DIR__
METADATA_URL=__METADATA_URL__
RETRY_COUNT=__RETRY_COUNT__
RETRY_DELAY_SECONDS=__RETRY_DELAY_SECONDS__
ENV_FILE="${RANCHER_DIR}/afterglow-primary-network.env"
YAML_FILE="${RANCHER_DIR}/config.yaml.d/10-afterglow-primary-network.yaml"
PIN_LOCK_FILE="${RANCHER_DIR}/afterglow-primary-network.lock"
NIC_HANDLER="${AFTERGLOW_NIC_HANDLER:-/usr/local/sbin/afterglow-nic-up.sh}"

log() { echo "[afterglow-k3s-pin] $*" | systemd-cat -t afterglow-k3s-pin 2>/dev/null || echo "[afterglow-k3s-pin] $*" >&2; }
fail() { log "ERROR: primary network pin missing: $*"; exit 1; }

is_ipv4() {
  local value="$1" part
  local -a octets
  IFS=. read -r -a octets <<< "${value}"
  [ "${#octets[@]}" -eq 4 ] || return 1
  for part in "${octets[@]}"; do
    [[ "${part}" =~ ^[0-9]{1,3}$ ]] || return 1
    [ "${part}" -le 255 ] || return 1
  done
}

is_iface() {
  [[ "$1" =~ ^[A-Za-z0-9_.:-]{1,15}$ ]]
}

physical_ifaces() {
  local entry iface
  for entry in "${SYS_CLASS_NET_DIR}"/*; do
    [ -d "${entry}" ] || continue
    [ -e "${entry}/device" ] || continue
    iface="${entry##*/}"
    is_iface "${iface}" || continue
    printf '%s\n' "${iface}"
  done
}

global_ipv4() {
  local iface="$1" addr
  local -a addresses=()
  while read -r addr; do
    [ -n "${addr}" ] && addresses+=("${addr}")
  done < <(ip -4 -o addr show dev "${iface}" scope global 2>/dev/null | awk '{split($4, a, "/"); print a[1]}')
  [ "${#addresses[@]}" -eq 1 ] || return 1
  is_ipv4 "${addresses[0]}" || return 1
  printf '%s\n' "${addresses[0]}"
}

command -v flock >/dev/null 2>&1 || fail "flock is unavailable"
mkdir -p "${RANCHER_DIR}/config.yaml.d"
chmod 700 "${RANCHER_DIR}/config.yaml.d"
exec 9>"${PIN_LOCK_FILE}"
chmod 600 "${PIN_LOCK_FILE}"
flock -x 9

AFTERGLOW_K3S_NODE_IP=""
AFTERGLOW_K3S_PRIMARY_IFACE=""
if [ -e "${ENV_FILE}" ]; then
  _pin_lines=()
  while IFS= read -r _line || [ -n "${_line}" ]; do
    _pin_lines+=("${_line}")
  done < "${ENV_FILE}" || fail "cannot read ${ENV_FILE}"
  [ "${#_pin_lines[@]}" -eq 2 ] || fail "invalid environment file"
  [[ "${_pin_lines[0]}" =~ ^AFTERGLOW_K3S_NODE_IP=([^[:space:]]+)$ ]] || fail "invalid node IP state"
  AFTERGLOW_K3S_NODE_IP="${BASH_REMATCH[1]}"
  [[ "${_pin_lines[1]}" =~ ^AFTERGLOW_K3S_PRIMARY_IFACE=([^[:space:]]+)$ ]] || fail "invalid interface state"
  AFTERGLOW_K3S_PRIMARY_IFACE="${BASH_REMATCH[1]}"
  is_ipv4 "${AFTERGLOW_K3S_NODE_IP}" || fail "invalid pinned node IP"
  is_iface "${AFTERGLOW_K3S_PRIMARY_IFACE}" || fail "invalid pinned interface"
else
  [ -e "${YAML_FILE}" ] && fail "YAML exists without a valid environment file"
  command -v curl >/dev/null 2>&1 || fail "curl is unavailable"
  command -v jq >/dev/null 2>&1 || fail "jq is unavailable"

  METADATA_JSON=""
  METADATA_OK=0
  while IFS= read -r _iface; do
    for _attempt in $(seq 1 "${RETRY_COUNT}"); do
      _candidate="$(curl --interface "${_iface}" -fsS --connect-timeout 2 --max-time 4 "${METADATA_URL}" 2>/dev/null || true)"
      if [ -n "${_candidate}" ] && jq -e 'type == "object"' >/dev/null 2>&1 <<< "${_candidate}"; then
        METADATA_JSON="${_candidate}"
        METADATA_OK=1
        break 2
      fi
      [ "${_attempt}" -lt "${RETRY_COUNT}" ] && sleep "${RETRY_DELAY_SECONDS}"
    done
  done < <(physical_ifaces)

  if [ "${METADATA_OK}" -eq 1 ]; then
    _matching_links=()
    while IFS= read -r _line || [ -n "${_line}" ]; do
      _matching_links+=("${_line}")
    done < <(jq -r --arg network_id "${PRIMARY_NETWORK_ID}" \
      '[.networks[]? | select(.network_id == $network_id) | .link] | unique | .[]' <<< "${METADATA_JSON}")
    [ "${#_matching_links[@]}" -eq 1 ] || fail "network_id does not resolve to exactly one link"
    [ -n "${_matching_links[0]}" ] && [ "${_matching_links[0]}" != "null" ] || fail "primary link is empty"
    _matching_macs=()
    while IFS= read -r _line || [ -n "${_line}" ]; do
      _matching_macs+=("${_line}")
    done < <(jq -r --arg link "${_matching_links[0]}" \
      '[.links[]? | select(.id == $link) | .ethernet_mac_address] | map(select(type == "string" and length > 0) | ascii_downcase) | unique | .[]' <<< "${METADATA_JSON}")
    [ "${#_matching_macs[@]}" -eq 1 ] || fail "primary link does not resolve to exactly one MAC"

    _mac_matches=()
    while IFS= read -r _iface; do
      _iface_mac="$(tr '[:upper:]' '[:lower:]' < "${SYS_CLASS_NET_DIR}/${_iface}/address")"
      [ "${_iface_mac}" = "${_matching_macs[0]}" ] && _mac_matches+=("${_iface}")
    done < <(physical_ifaces)
    [ "${#_mac_matches[@]}" -eq 1 ] || fail "primary MAC does not resolve to exactly one interface"
    AFTERGLOW_K3S_PRIMARY_IFACE="${_mac_matches[0]}"
    is_iface "${AFTERGLOW_K3S_PRIMARY_IFACE}" || fail "primary interface name is invalid"

    for _second in $(seq 0 60); do
      AFTERGLOW_K3S_NODE_IP="$(global_ipv4 "${AFTERGLOW_K3S_PRIMARY_IFACE}" || true)"
      [ -n "${AFTERGLOW_K3S_NODE_IP}" ] && break
      [ "${_second}" -lt 60 ] && sleep 1
    done
    is_ipv4 "${AFTERGLOW_K3S_NODE_IP}" || fail "primary interface has no unique global IPv4"
  else
    _fallback_candidates=()
    while IFS= read -r _iface; do
      _fallback_ip="$(global_ipv4 "${_iface}" || true)"
      [ -n "${_fallback_ip}" ] && _fallback_candidates+=("${_iface}|${_fallback_ip}")
    done < <(physical_ifaces)
    [ "${#_fallback_candidates[@]}" -eq 1 ] || fail "metadata unavailable and IPv4 fallback is ambiguous"
    AFTERGLOW_K3S_PRIMARY_IFACE="${_fallback_candidates[0]%%|*}"
    AFTERGLOW_K3S_NODE_IP="${_fallback_candidates[0]#*|}"
    is_iface "${AFTERGLOW_K3S_PRIMARY_IFACE}" || fail "fallback interface name is invalid"
    is_ipv4 "${AFTERGLOW_K3S_NODE_IP}" || fail "fallback IP is invalid"
  fi

  _env_tmp="${ENV_FILE}.tmp.$$"
  umask 077
  : > "${_env_tmp}"
  chmod 600 "${_env_tmp}"
  printf 'AFTERGLOW_K3S_NODE_IP=%s\nAFTERGLOW_K3S_PRIMARY_IFACE=%s\n' \
    "${AFTERGLOW_K3S_NODE_IP}" "${AFTERGLOW_K3S_PRIMARY_IFACE}" > "${_env_tmp}"
  chmod 600 "${_env_tmp}"
  mv -f -- "${_env_tmp}" "${ENV_FILE}"
fi

_yaml_tmp="${YAML_FILE}.tmp.$$"
umask 077
: > "${_yaml_tmp}"
chmod 600 "${_yaml_tmp}"
cat > "${_yaml_tmp}" <<EOF
node-ip: "${AFTERGLOW_K3S_NODE_IP}"
flannel-iface: "${AFTERGLOW_K3S_PRIMARY_IFACE}"
__SERVER_YAML__EOF
chmod 600 "${_yaml_tmp}"
mv -f -- "${_yaml_tmp}" "${YAML_FILE}"
flock -u 9

if [ -x "${NIC_HANDLER}" ]; then
  while IFS= read -r _iface; do
    "${NIC_HANDLER}" "${_iface}" || log "NIC replay failed for ${_iface}"
  done < <(physical_ifaces)
fi
""".replace("__PRIMARY_NETWORK_ID__", primary_network_id_q)
        .replace("__RANCHER_DIR__", rancher_dir_q)
        .replace("__SYS_CLASS_NET_DIR__", sys_class_net_dir_q)
        .replace("__METADATA_URL__", metadata_url_q)
        .replace("__RETRY_COUNT__", str(retry_count))
        .replace("__RETRY_DELAY_SECONDS__", str(retry_delay_seconds))
        .replace("__SERVER_YAML__", server_yaml)
    )


def _build_fcos_nic_script() -> str:
    return """#!/bin/bash
set -euo pipefail
IFACE="${1:-}"
[ -n "${IFACE}" ] || exit 0
case "${IFACE}" in
  lo|lo:*|cni*|flannel*|veth*|kube*|dummy*|tunl*) exit 0 ;;
esac
PIN_ENV="/etc/rancher/k3s/afterglow-primary-network.env"
for _second in $(seq 0 300); do
  if [ -r "${PIN_ENV}" ]; then
    . "${PIN_ENV}"
    if [[ "${AFTERGLOW_K3S_PRIMARY_IFACE:-}" =~ ^[A-Za-z0-9_.:-]{1,15}$ ]]; then
      break
    fi
  fi
  [ "${_second}" -lt 300 ] && sleep 1
done
[ -n "${AFTERGLOW_K3S_PRIMARY_IFACE:-}" ] || exit 1
[ "${IFACE}" = "${AFTERGLOW_K3S_PRIMARY_IFACE}" ] && exit 0
LOCK="/run/afterglow-nic.lock"
TARGET="afterglow-${IFACE}"
exec 9>"${LOCK}"
flock -x 9
ACTIVE_PROFILE="$(nmcli -g GENERAL.CONNECTION device show "${IFACE}" 2>/dev/null || true)"
if [ -n "${ACTIVE_PROFILE}" ] && [ "${ACTIVE_PROFILE}" != "--" ] && [ "${ACTIVE_PROFILE}" != "${TARGET}" ]; then
  nmcli connection down "${ACTIVE_PROFILE}" >/dev/null 2>&1 || true
  nmcli connection delete "${ACTIVE_PROFILE}" >/dev/null 2>&1 || true
fi
if ! nmcli connection show "${TARGET}" >/dev/null 2>&1; then
  nmcli connection add type ethernet ifname "${IFACE}" con-name "${TARGET}" >/dev/null
fi
nmcli connection modify "${TARGET}" \
  connection.interface-name "${IFACE}" \
  connection.autoconnect yes \
  connection.autoconnect-priority 100 \
  ipv4.method auto \
  ipv4.never-default yes \
  ipv4.ignore-auto-routes yes \
  ipv4.ignore-auto-dns yes \
  ipv6.method disabled >/dev/null
nmcli connection up "${TARGET}" >/dev/null
"""


def _fcos_nic_unit() -> str:
    return """[Unit]
Description=Afterglow NIC up handler for %i
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/afterglow-nic-up.sh %i
TimeoutStartSec=330s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""


def _fcos_nic_rule() -> str:
    return """SUBSYSTEM=="net", ACTION=="add", KERNEL!="lo", KERNEL!="cni*", KERNEL!="flannel*", KERNEL!="veth*", KERNEL!="kube*", KERNEL!="dummy*", KERNEL!="tunl*", RUN+="/bin/systemctl --no-block start afterglow-nic-up@%k.service"
"""


def _ignition_file(path: str, content: str, mode: int = 0o644) -> dict:
    """Ignition storage.files 항목 생성 (gzip 압축으로 user_data 크기 절감)."""
    compressed = gzip.compress(content.encode())
    b64 = base64.b64encode(compressed).decode()
    return {
        "path": path,
        "mode": mode,
        "contents": {
            "compression": "gzip",
            "source": f"data:;base64,{b64}",
        },
    }


def _build_server_ignition(
    cluster_name: str,
    k3s_version: str,
    callback_url: str,
    callback_token: str,
    cloud_conf: str,
    plugins: list[dict],
    extra_server_args: list[str],
    extra_write_files: list[dict],
    extra_tls_sans: list[str],
    needs_external_cloud_provider: bool,
    primary_network_id: str,
    server_node_name: str = "",
    cluster_init: bool = False,
    join_url: str = "",
    ha_node_token: str = "",
) -> str:
    """FCOS k3s 서버 노드 Ignition JSON 생성."""
    pin_script = _build_k3s_network_pin_script(primary_network_id=primary_network_id, server=True)
    template_vars = dict(
        cluster_name=cluster_name,
        k3s_version=k3s_version,
        callback_url=callback_url,
        callback_token=callback_token,
        cloud_conf=cloud_conf,
        plugins=plugins,
        extra_server_args=extra_server_args,
        extra_tls_sans=extra_tls_sans,
        needs_external_cloud_provider=needs_external_cloud_provider,
        pin_script=pin_script,
    )

    callback_sh = _jinja.get_template("k3s_server_fcos_callback.sh.j2").render(**template_vars)

    # k3s 설치 스크립트 (systemd ExecStart용)
    tls_sans_args = " ".join(f"--tls-san {shlex.quote(san)}" for san in extra_tls_sans)
    cloud_controller_args = ""
    if needs_external_cloud_provider:
        cloud_controller_args = '--disable-cloud-controller --kubelet-arg="cloud-provider=external"'
    extra_args_str = " ".join(shlex.quote(a) for a in extra_server_args) if extra_server_args else ""

    # HA 분기 인자
    if cluster_init:
        ha_args = "--cluster-init"
    elif join_url and ha_node_token:
        ha_args = f"--server {shlex.quote(join_url)} --token {shlex.quote(ha_node_token)}"
    else:
        ha_args = ""

    node_name = server_node_name or f"{cluster_name}-server"
    install_script = f"""#!/bin/bash
set -euo pipefail
if ! /bin/bash /opt/k3s/pin-primary-network.sh; then
  curl -sf -X POST {shlex.quote(callback_url + "/v1/callback")} \
    -H "Content-Type: application/json" \
    -d '{{"token": "{callback_token}", "success": false, "error": "primary network pin missing"}}' || true
  exit 1
fi
. /etc/rancher/k3s/afterglow-primary-network.env
SERVER_IP="${{AFTERGLOW_K3S_NODE_IP}}"
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_VERSION={shlex.quote(k3s_version)} \
  INSTALL_K3S_SKIP_SELINUX_RPM=true \
  sh -s - server \
    --tls-san "${{SERVER_IP}}" \
    {tls_sans_args} \
    --write-kubeconfig-mode 644 \
    {ha_args} \
    {cloud_controller_args} \
    {extra_args_str} \
    --node-name={shlex.quote(node_name)}
nohup /bin/bash /opt/k3s/callback.sh > /var/log/k3s-callback.log 2>&1 &
"""

    install_unit = f"""[Unit]
Description=K3s Server Install - {cluster_name}
After=network-online.target
Wants=network-online.target
ConditionPathExists=!/etc/rancher/k3s/k3s.yaml

[Service]
Type=oneshot
TimeoutStartSec=15min
RemainAfterExit=yes
ExecStart=/bin/bash /opt/k3s/install.sh
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""

    files: list[dict] = []

    if cloud_conf:
        files.append(_ignition_file("/etc/kubernetes/cloud.conf", cloud_conf, mode=0o600))

    for plugin in plugins:
        files.append(_ignition_file(f"/opt/k3s/{plugin['name']}-manifests.yaml", plugin["content"], mode=0o644))

    for wf in extra_write_files:
        mode = int(wf.get("permissions", "0644"), 8)
        files.append(_ignition_file(wf["path"], wf["content"], mode=mode))

    files.extend(
        [
            _ignition_file("/opt/k3s/pin-primary-network.sh", pin_script, mode=0o750),
            _ignition_file("/usr/local/sbin/afterglow-nic-up.sh", _build_fcos_nic_script(), mode=0o750),
            _ignition_file(
                "/etc/systemd/system/afterglow-nic-up@.service",
                _fcos_nic_unit(),
                mode=0o644,
            ),
            _ignition_file("/etc/udev/rules.d/99-afterglow-nic.rules", _fcos_nic_rule(), mode=0o644),
        ]
    )

    files.append(_ignition_file("/opt/k3s/callback.sh", callback_sh, mode=0o750))
    files.append(_ignition_file("/opt/k3s/install.sh", install_script, mode=0o750))
    files.append(_ignition_file("/etc/systemd/system/k3s-install.service", install_unit, mode=0o644))

    ignition = {
        "ignition": {"version": "3.4.0"},
        "storage": {"files": files},
        "systemd": {
            "units": [
                {"name": "k3s-install.service", "enabled": True},
            ]
        },
    }
    return json.dumps(ignition)


def _build_agent_ignition(
    cluster_name: str,
    k3s_version: str,
    server_ip: str,
    node_token: str,
    ssh_public_key: str,
    extra_agent_args: list[str],
    primary_network_id: str,
) -> str:
    """FCOS k3s 에이전트 노드 Ignition JSON 생성."""
    template_vars = dict(
        cluster_name=cluster_name,
        k3s_version=k3s_version,
        server_ip=server_ip,
        node_token=node_token,
        extra_agent_args=extra_agent_args,
    )
    join_sh = _jinja.get_template("k3s_agent_fcos_join.sh.j2").render(**template_vars)

    join_unit = f"""[Unit]
Description=K3s Agent Join - {cluster_name}
After=network-online.target
Wants=network-online.target
ConditionPathExists=!/etc/systemd/system/k3s-agent.service

[Service]
Type=oneshot
TimeoutStartSec=35min
ExecStart=/bin/bash /opt/k3s/agent-join.sh
Restart=on-failure
RestartSec=120
StartLimitIntervalSec=14400
StartLimitBurst=20
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""
    pin_script = _build_k3s_network_pin_script(primary_network_id=primary_network_id, server=False)
    files: list[dict] = [
        _ignition_file("/opt/k3s/pin-primary-network.sh", pin_script, mode=0o750),
        _ignition_file("/opt/k3s/agent-join.sh", join_sh, mode=0o750),
        _ignition_file("/usr/local/sbin/afterglow-nic-up.sh", _build_fcos_nic_script(), mode=0o750),
        _ignition_file(
            "/etc/systemd/system/afterglow-nic-up@.service",
            _fcos_nic_unit(),
            mode=0o644,
        ),
        _ignition_file("/etc/udev/rules.d/99-afterglow-nic.rules", _fcos_nic_rule(), mode=0o644),
        _ignition_file("/etc/systemd/system/k3s-agent-join.service", join_unit, mode=0o644),
    ]

    passwd: dict = {}
    if ssh_public_key:
        passwd = {"users": [{"name": "core", "sshAuthorizedKeys": [ssh_public_key]}]}

    ignition: dict = {
        "ignition": {"version": "3.4.0"},
        "storage": {"files": files},
        "systemd": {
            "units": [
                {"name": "k3s-agent-join.service", "enabled": True},
            ]
        },
    }
    if passwd:
        ignition["passwd"] = passwd

    return json.dumps(ignition)


def generate_server_userdata(
    cluster_name: str,
    k3s_version: str,
    callback_url: str,
    callback_token: str,
    *,
    primary_network_id: str,
    cloud_conf: str | None = None,
    plugin_manifests: list[dict] | None = None,  # [{"name": "occm", "content": "..."}]
    extra_server_args: list[str] | None = None,
    extra_write_files: list[dict] | None = None,
    extra_tls_sans: list[str] | None = None,
    needs_external_cloud_provider: bool = False,
    os_type: str = OS_TYPE_UBUNTU,
    server_node_name: str | None = None,
    barbican_kms_enabled: bool = False,
    # HA 멀티 마스터 파라미터
    cluster_init: bool = False,  # server#1: --cluster-init (embedded etcd)
    join_url: str | None = None,  # server#2/3: --server <url>
    ha_node_token: str | None = None,  # server#2/3 조인 토큰
    # 하위호환 파라미터 (deprecated — 레지스트리 우회 시에만 사용)
    occm_enabled: bool = False,
    occm_manifests: str | None = None,
) -> UserdataResult:
    """k3s 서버 노드 userdata를 렌더링하여 UserdataResult 반환.

    Ubuntu: cloud-init YAML → gzip+base64 인코딩, config_drive=False
    FCOS  : Ignition JSON → raw (gzip 안 함), config_drive=True

    신규 호출자는 plugin_manifests / extra_server_args / cloud_conf를 직접 전달할 것.
    occm_enabled + occm_manifests는 하위호환용으로만 유지.
    """
    # 하위호환: 구 occm_enabled 파라미터 지원
    if occm_enabled and occm_manifests and not plugin_manifests:
        plugin_manifests = [{"name": "occm", "content": occm_manifests}]
        needs_external_cloud_provider = True
    primary_network_id = _validate_pin_input("primary_network_id", primary_network_id)
    pin_script = _build_k3s_network_pin_script(primary_network_id=primary_network_id, server=True)

    if os_type == OS_TYPE_FCOS:
        ign_str = _build_server_ignition(
            cluster_name=cluster_name,
            k3s_version=k3s_version,
            callback_url=callback_url,
            callback_token=callback_token,
            cloud_conf=cloud_conf or "",
            plugins=plugin_manifests or [],
            extra_server_args=extra_server_args or [],
            extra_write_files=extra_write_files or [],
            extra_tls_sans=extra_tls_sans or [],
            needs_external_cloud_provider=needs_external_cloud_provider,
            primary_network_id=primary_network_id,
            server_node_name=server_node_name or "",
            cluster_init=cluster_init,
            join_url=join_url or "",
            ha_node_token=ha_node_token or "",
        )
        # Ignition JSON 유효성 간단 확인
        json.loads(ign_str)
        # Nova API는 user_data를 base64 디코딩 후 config drive에 기록.
        # raw JSON을 그대로 보내면 base64 디코딩 시 바이너리 garbage가 되므로
        # 반드시 base64 인코딩하여 전달해야 한다.
        encoded = base64.b64encode(ign_str.encode()).decode()
        return UserdataResult(data=encoded, config_drive=True)

    # Ubuntu (기본)
    template_vars = dict(
        cluster_name=cluster_name,
        k3s_version=k3s_version,
        callback_url=callback_url,
        pin_script=pin_script,
        callback_token=callback_token,
        cloud_conf=cloud_conf or "",
        plugins=plugin_manifests or [],
        extra_server_args=extra_server_args or [],
        extra_write_files=extra_write_files or [],
        extra_tls_sans=extra_tls_sans or [],
        needs_external_cloud_provider=needs_external_cloud_provider,
        server_node_name=server_node_name or f"{cluster_name}-server",
        barbican_kms_enabled=barbican_kms_enabled,
        cluster_init=cluster_init,
        join_url=join_url or "",
        ha_node_token=ha_node_token or "",
    )
    yaml_str = _jinja.get_template("k3s_server.yaml.j2").render(**template_vars)
    encoded = base64.b64encode(gzip.compress(yaml_str.encode())).decode()
    return UserdataResult(data=encoded, config_drive=False)


def generate_agent_userdata(
    cluster_name: str,
    k3s_version: str,
    server_ip: str,
    node_token: str,
    ssh_public_key: str | None = None,
    *,
    primary_network_id: str,
    extra_agent_args: list[str] | None = None,
    os_type: str = OS_TYPE_UBUNTU,
    # 하위호환 파라미터 (deprecated)
    occm_enabled: bool = False,
) -> UserdataResult:
    """k3s 에이전트 노드 userdata를 렌더링하여 UserdataResult 반환."""
    if not node_token:
        raise ValueError("node_token이 비어있습니다. 서버 콜백에서 토큰이 전달되지 않았습니다.")
    primary_network_id = _validate_pin_input("primary_network_id", primary_network_id)
    pin_script = _build_k3s_network_pin_script(primary_network_id=primary_network_id, server=False)

    # SSH 공개키 형식 검증 (YAML injection 방지)
    if ssh_public_key:
        validate_ssh_public_key(ssh_public_key)

    # 하위호환: occm_enabled → cloud-provider=external
    agent_args = list(extra_agent_args or [])
    if occm_enabled and "--kubelet-arg=cloud-provider=external" not in agent_args:
        agent_args.append("--kubelet-arg=cloud-provider=external")

    if os_type == OS_TYPE_FCOS:
        ign_str = _build_agent_ignition(
            cluster_name=cluster_name,
            k3s_version=k3s_version,
            server_ip=server_ip,
            primary_network_id=primary_network_id,
            node_token=node_token,
            ssh_public_key=ssh_public_key or "",
            extra_agent_args=agent_args,
        )
        json.loads(ign_str)
        encoded = base64.b64encode(ign_str.encode()).decode()
        return UserdataResult(data=encoded, config_drive=True)

    # Ubuntu (기본)
    template_vars = dict(
        cluster_name=cluster_name,
        k3s_version=k3s_version,
        pin_script=pin_script,
        server_ip=server_ip,
        node_token=node_token,
        ssh_public_key=ssh_public_key or "",
        extra_agent_args=agent_args,
    )
    yaml_str = _jinja.get_template("k3s_agent.yaml.j2").render(**template_vars)
    return UserdataResult(data=base64.b64encode(gzip.compress(yaml_str.encode())).decode(), config_drive=False)
