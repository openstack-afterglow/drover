"""PR3 회귀 테스트 — Barbican KMS plugin 외부 systemd service 패턴.

이전 host static pod 패턴(§ 7f1de4c)이 K3s 1.34 의 kubelet startup sequence 와
양립 불가하여 cluster 영구 데드락 발생. 본 테스트는 새 systemd service 패턴이
다음을 보장하는지 검증:
- static pod manifest 가 더 이상 작성되지 않음
- systemd unit 파일 + install script + cloud.conf + encryption-config 4개 파일 작성
- k3s server install args 에서 pod-manifest-path 제거 (encryption-provider-config 만 유지)
- cloud-init runcmd 가 KMS install → start → sock wait → k3s start 순서를 보장
"""

from unittest.mock import MagicMock

from drover.services.plugins.barbican_kms import BarbicanKmsPlugin


def _settings_with_kms() -> MagicMock:
    s = MagicMock()
    s.drover_barbican_kms_enabled = True
    s.drover_barbican_kms_kek_id = "kek-uuid-test"
    s.drover_barbican_kms_image = "registry.k8s.io/provider-os/barbican-kms-plugin:v1.34.1"
    s.os_auth_url = "https://keystone.example.com:5000/v3"
    s.os_username = "admin"
    s.os_password = "secret"
    s.os_user_domain_name = "Default"
    s.os_project_name = "admin"
    s.os_project_domain_name = "Default"
    s.os_region_name = "RegionOne"
    s.os_insecure = True
    s.os_cacert = ""
    return s


# ---------------------------------------------------------------------------
# extra_write_files — 4개 파일 (systemd unit + install script + cloud.conf + enc-config)
# ---------------------------------------------------------------------------


def test_extra_write_files_does_not_include_static_pod_manifest():
    """PR3 핵심: host static pod manifest 가 더 이상 작성되지 않아야 한다."""
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", _settings_with_kms())
    paths = [f["path"] for f in files]
    assert not any("/var/lib/rancher/k3s/agent/pod-manifests" in p for p in paths), (
        f"static pod manifest 가 여전히 작성됨: {paths}"
    )
    assert not any("barbican-kms.yaml" in p and "pod-manifests" in p for p in paths)


def test_extra_write_files_includes_systemd_unit():
    """barbican-kms.service systemd unit 파일이 작성되어야 한다."""
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", _settings_with_kms())
    unit_files = [f for f in files if f["path"] == "/etc/systemd/system/barbican-kms.service"]
    assert len(unit_files) == 1
    content = unit_files[0]["content"]
    assert "[Unit]" in content
    assert "Before=k3s.service" in content, "k3s.service 보다 먼저 시작되어야 함 (데드락 방지)"
    assert "ExecStart=/usr/local/bin/barbican-kms-plugin" in content
    assert "--socketpath=/var/lib/kms/kms.sock" in content
    assert "--cloud-config=/etc/kubernetes/barbican-cloud.conf" in content
    # KEK ID 는 CLI flag 가 아닌 cloud.conf [KeyManager] key-id 로 전달 (v1.34.1 검증).
    # ExecStart 라인 (주석 아닌) 에 --key-id 가 없어야 함 — barbican-kms-plugin v1.34.1 미지원
    exec_lines = [ln for ln in content.split("\n") if not ln.strip().startswith("#")]
    assert not any("--key-id" in ln for ln in exec_lines), (
        "barbican-kms-plugin 은 --key-id flag 를 지원하지 않음 — cloud.conf 로 전달"
    )
    # cloud.conf 에 key-id 가 들어있는지 별도 확인
    cc = next(f for f in files if f["path"] == "/etc/kubernetes/barbican-cloud.conf")
    assert "key-id=kek-uuid-test" in cc["content"]


def test_extra_write_files_includes_install_script():
    """install_kms.sh 가 0750 권한으로 작성되어야 한다 (podman 기반 — k3s containerd 의존 X)."""
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", _settings_with_kms())
    install_files = [f for f in files if f["path"] == "/opt/k3s/install_kms.sh"]
    assert len(install_files) == 1
    assert install_files[0]["permissions"] == "0750"
    content = install_files[0]["content"]
    assert "podman pull" in content, "podman 으로 image pull 해야 함 (k3s containerd 의존성 제거)"
    assert "k3s ctr" not in content, "k3s ctr 사용 금지 — chicken-and-egg 데드락 원인"
    assert "registry.k8s.io/provider-os/barbican-kms-plugin:v1.34.1" in content
    assert "/usr/local/bin/barbican-kms-plugin" in content
    assert 'file "$KMS_BIN_DEST"' in content, "binary ELF 검증 (entrypoint wrapper fail-fast)"


def test_extra_write_files_includes_encryption_config():
    """encryption-config.yaml 이 0600 권한으로 작성되어야 한다 (apiserver 가 부팅 시 읽음)."""
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", _settings_with_kms())
    enc_files = [f for f in files if f["path"] == "/etc/kubernetes/encryption-config.yaml"]
    assert len(enc_files) == 1
    assert enc_files[0]["permissions"] == "0600"
    assert "kms" in enc_files[0]["content"].lower()
    assert "/var/lib/kms/kms.sock" in enc_files[0]["content"]


def test_extra_write_files_includes_cloud_conf():
    """barbican-cloud.conf 가 0600 권한으로 작성되어야 한다 (KMS plugin 이 인증에 사용)."""
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", _settings_with_kms())
    cc_files = [f for f in files if f["path"] == "/etc/kubernetes/barbican-cloud.conf"]
    assert len(cc_files) == 1
    assert cc_files[0]["permissions"] == "0600"
    assert "[Global]" in cc_files[0]["content"]


def test_extra_write_files_returns_exactly_four_files():
    """PR3 후 정확히 4개 파일: systemd unit + install script + encryption-config + cloud.conf."""
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", _settings_with_kms())
    assert len(files) == 4


# ---------------------------------------------------------------------------
# server_install_args — pod-manifest-path 제거 + encryption-provider-config 유지
# ---------------------------------------------------------------------------


def test_server_install_args_drops_pod_manifest_path():
    """PR3 핵심: --kubelet-arg=pod-manifest-path 가 더 이상 필요 없어야 한다 (host static pod 폐기)."""
    plugin = BarbicanKmsPlugin()
    args = plugin.server_install_args(_settings_with_kms())
    assert not any("pod-manifest-path" in a for a in args), f"pod-manifest-path 인자가 여전히 존재: {args}"


def test_server_install_args_keeps_encryption_provider_config():
    """encryption-provider-config 인자는 여전히 필요 (apiserver 가 부팅 시 읽음)."""
    plugin = BarbicanKmsPlugin()
    args = plugin.server_install_args(_settings_with_kms())
    assert any("encryption-provider-config=/etc/kubernetes/encryption-config.yaml" in a for a in args)


# ---------------------------------------------------------------------------
# drover_server.yaml.j2 cloud-init flow — barbican_kms_enabled 분기
# ---------------------------------------------------------------------------


def test_drover_server_userdata_kms_enabled_orders_kms_before_drover_install():
    """PR3 hotfix: KMS install/start/sock-wait 가 k3s install 보다 먼저 위치해야 한다.

    이전 흐름은 k3s install (SKIP_START 의도) → KMS install → sock wait → k3s start.
    SKIP_START 가 env var 위치 잘못으로 효과 없어 chicken-and-egg 데드락.

    새 흐름: KMS install → KMS start → sock wait → k3s install (자동 enable+start).
    KMS sock 이 이미 존재하므로 k3s 자동 start 가 정상 부팅.
    """
    import base64
    import gzip

    from drover.services import cloudinit as drover_cloudinit

    result = drover_cloudinit.generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.34.6+k3s1",
        callback_url="https://callback.example.com",
        callback_token="tok",
        cloud_conf="",
        plugin_manifests=[],
        extra_server_args=[],
        extra_write_files=[],
        extra_tls_sans=[],
        needs_external_cloud_provider=False,
        barbican_kms_enabled=True,
    )
    yaml_str = gzip.decompress(base64.b64decode(result.data)).decode()

    # SKIP_START / SKIP_ENABLE 모두 부재 (새 순서로 불필요)
    assert "INSTALL_K3S_SKIP_START" not in yaml_str
    assert "INSTALL_K3S_SKIP_ENABLE" not in yaml_str

    # KMS 흐름 항목들 모두 존재
    assert "/opt/k3s/install_kms.sh" in yaml_str
    assert "systemctl enable --now barbican-kms.service" in yaml_str
    assert "/var/lib/kms/kms.sock" in yaml_str

    # 순서: install_kms.sh → barbican-kms.service start → sock wait → k3s curl install
    install_kms_pos = yaml_str.find("/opt/k3s/install_kms.sh")
    kms_start_pos = yaml_str.find("systemctl enable --now barbican-kms.service")
    sock_wait_pos = yaml_str.find("waiting for /var/lib/kms/kms.sock")
    drover_install_pos = yaml_str.find("https://get.k3s.io")
    assert install_kms_pos < kms_start_pos < sock_wait_pos < drover_install_pos, (
        f"잘못된 순서: install_kms={install_kms_pos}, kms_start={kms_start_pos}, "
        f"sock_wait={sock_wait_pos}, drover_install={drover_install_pos}"
    )

    # podman 이 packages 에 포함되어 cloud-init 가 미리 install
    assert "  - podman" in yaml_str, "podman 이 packages 에 포함되어야 함 (k3s containerd 의존성 제거)"


def test_drover_server_userdata_kms_disabled_uses_default_flow():
    """barbican_kms_enabled=False (기본) 시 기존 흐름 (k3s install 이 자동 start)."""
    import base64
    import gzip

    from drover.services import cloudinit as drover_cloudinit

    result = drover_cloudinit.generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.34.6+k3s1",
        callback_url="https://callback.example.com",
        callback_token="tok",
        cloud_conf="",
        plugin_manifests=[],
        extra_server_args=[],
        extra_write_files=[],
        extra_tls_sans=[],
        needs_external_cloud_provider=False,
        barbican_kms_enabled=False,
    )
    yaml_str = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "INSTALL_K3S_SKIP_START" not in yaml_str
    assert "install_kms.sh" not in yaml_str
    assert "barbican-kms.service" not in yaml_str
    # KMS 비활성화 시 podman 도 packages 에 포함되지 않음
    assert "  - podman" not in yaml_str


# ---------------------------------------------------------------------------
# PR1 — cloud.conf 인증을 admin password → application credential 로 전환
# ---------------------------------------------------------------------------


def test_cloud_conf_uses_app_credential_when_provided():
    """PR1: app_credential 전달 시 cloud.conf 가 application-credential-id/secret 사용,
    admin password 미노출."""
    plugin = BarbicanKmsPlugin()
    app_cred = {"id": "app-cred-id-test", "secret": "app-cred-secret-test", "user_id": "u1"}
    files = plugin.extra_write_files("proj-1", "test", _settings_with_kms(), app_credential=app_cred)
    cc = next(f for f in files if f["path"] == "/etc/kubernetes/barbican-cloud.conf")
    content = cc["content"]
    assert "application-credential-id=app-cred-id-test" in content
    assert "application-credential-secret=app-cred-secret-test" in content
    # admin password 가 cloud.conf 에 노출되지 않아야 한다
    assert "password=secret" not in content
    assert "username=admin" not in content


def test_cloud_conf_falls_back_to_admin_password_when_no_app_cred():
    """PR1 fallback: app_credential=None 시 admin password 사용 (deprecated, dev 전용)."""
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test", _settings_with_kms(), app_credential=None)
    cc = next(f for f in files if f["path"] == "/etc/kubernetes/barbican-cloud.conf")
    content = cc["content"]
    assert "application-credential-id" not in content
    assert "username=admin" in content
    assert "password=secret" in content


def test_aggregate_extra_write_files_passes_app_credential_to_kms():
    """aggregate_extra_write_files(app_credential=...) 가 barbican_kms plugin 에 전달되어야 한다."""
    from drover.services import plugins as drover_plugins

    s = _settings_with_kms()
    app_cred = {"id": "app-cred-id-test", "secret": "app-cred-secret-test", "user_id": "u1"}
    files = drover_plugins.aggregate_extra_write_files("proj-1", "test", s, app_credential=app_cred)
    cc = next((f for f in files if f["path"] == "/etc/kubernetes/barbican-cloud.conf"), None)
    assert cc is not None
    assert "application-credential-id=app-cred-id-test" in cc["content"]


def test_extra_write_files_signature_backward_compatible():
    """app_credential 인자 미전달 시에도 동작 (PR1 land 전 caller 호환)."""
    plugin = BarbicanKmsPlugin()
    # app_credential kwarg 미전달
    files = plugin.extra_write_files("proj-1", "test", _settings_with_kms())
    assert len(files) == 4  # systemd unit + install + cloud.conf + enc-config


# ---------------------------------------------------------------------------
# Integration test — 실제 binary --help 와 template ExecStart flag 일치 검증
# (--key-id 같은 잘못된 flag 가 들어가는 회귀 방지)
# ---------------------------------------------------------------------------


import re
import shutil
import subprocess

import pytest

_BARBICAN_KMS_IMAGE = "registry.k8s.io/provider-os/barbican-kms-plugin:v1.34.1"


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available locally")
def test_systemd_exec_flags_match_real_binary_help():
    """systemd unit 의 ExecStart flag 가 실제 binary 의 --help 출력과 일치하는지 검증.

    이전 PR3 hotfix 에서 --key-id flag 가 미지원인데도 template 에 들어간 회귀 방지.
    docker 가 local 에 있을 때만 실행 (CI/dev 머신 의존).
    """
    # 1. 실제 binary --help 호출 (Mac 에서 amd64 image 실행 시 --platform 필수)
    try:
        out = subprocess.check_output(
            ["docker", "run", "--platform=linux/amd64", "--rm", _BARBICAN_KMS_IMAGE, "--help"],
            stderr=subprocess.STDOUT,
            text=True,
            timeout=120,
        )
    except subprocess.CalledProcessError as e:
        out = e.output
    except subprocess.TimeoutExpired:
        pytest.skip("docker pull/run timeout — network 환경 이슈")

    # 2. supported flags 추출. 출력에 'Flags:' 섹션이 없으면 binary 가 정상 실행 안 된 것 (cross-arch 등) — skip.
    if "Flags:" not in out:
        pytest.skip(f"barbican-kms-plugin --help 출력에 'Flags:' 섹션 없음 (cross-arch?): {out[:300]}")
    flags_section = out.split("Flags:", 1)[1]
    supported = set(re.findall(r"--([a-z][a-z0-9-]*)", flags_section))
    assert supported, f"Flags 섹션에서 flag 못 찾음: {flags_section[:300]}"

    # 3. template 의 ExecStart flag 추출
    plugin = BarbicanKmsPlugin()
    files = plugin.extra_write_files("proj-1", "test", _settings_with_kms())
    unit_content = next(f["content"] for f in files if f["path"].endswith("barbican-kms.service"))
    exec_lines = [ln for ln in unit_content.split("\n") if "ExecStart=" in ln or ln.strip().startswith("--")]
    used_flags = set()
    for ln in exec_lines:
        used_flags.update(re.findall(r"--([a-z][a-z0-9-]*)", ln))

    # 4. 모든 used flags 가 supported 에 포함되어야 함
    unsupported = used_flags - supported
    assert not unsupported, (
        f"barbican-kms-plugin {_BARBICAN_KMS_IMAGE} 가 지원하지 않는 flags 가 ExecStart 에 사용됨: {unsupported}\n"
        f"supported: {sorted(supported)}\n"
        f"used: {sorted(used_flags)}"
    )
