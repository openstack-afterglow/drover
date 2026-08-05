"""drover_plugins 레지스트리 및 각 플러그인 단위 테스트."""

import base64
import gzip
from unittest.mock import MagicMock

import yaml


def _base_settings(**kwargs) -> MagicMock:
    """테스트용 Settings MagicMock 생성."""
    defaults = {
        "os_auth_url": "https://keystone.example.com:5000/v3",
        "os_username": "admin",
        "os_password": "secret",
        "os_project_name": "admin",
        "os_user_domain_name": "Default",
        "os_project_domain_name": "Default",
        "os_region_name": "RegionOne",
        "os_insecure": False,
        "os_cacert": "",
        # OCCM
        "drover_occm_enabled": False,
        "drover_occm_image": "registry.k8s.io/provider-os/openstack-cloud-controller-manager:v1.35.0",
        # Cinder CSI
        "drover_cinder_csi_enabled": False,
        "drover_cinder_csi_image": "registry.k8s.io/provider-os/cinder-csi-plugin:v1.31.0",
        # Immutable resource-policy values supplied through the plugin context.
        # Manila CSI
        "drover_manila_csi_enabled": False,
        "drover_manila_csi_image": "registry.k8s.io/provider-os/manila-csi-plugin:v1.31.0",
        "drover_manila_csi_nfs_image": "registry.k8s.io/sig-storage/nfsplugin:v4.9.0",
        "drover_manila_csi_share_protocol": "NFS",
        # Keystone Auth
        "drover_keystone_auth_enabled": False,
        "drover_keystone_auth_image": "registry.k8s.io/provider-os/k8s-keystone-auth:v1.31.0",
        "drover_keystone_auth_policy": "",
        # Immutable Octavia resource-policy values are supplied below.
        # Octavia Ingress
        "drover_octavia_ingress_enabled": False,
        "drover_octavia_ingress_image": "registry.k8s.io/provider-os/octavia-ingress-controller:v1.31.0",
        # Barbican KMS
        "drover_barbican_kms_enabled": False,
        "drover_barbican_kms_image": "registry.k8s.io/provider-os/barbican-kms-plugin:v1.31.0",
        "drover_barbican_kms_kek_id": "",
    }
    defaults.update(kwargs)
    mock = MagicMock()
    for k, v in defaults.items():
        setattr(mock, k, v)
    resource_ids = {
        "k3s.occm_floating_network": "ext-net-id",
        "k3s.lb_subnet": "lb-subnet-id",
        "k3s.volume_availability_zone": "nova",
        "k3s.octavia_ingress_floating_network": "ingress-ext-net-id",
    }
    resource_names = {"k3s.occm_public_network": "external"}
    mock.resource_id.side_effect = lambda key: resource_ids.get(key, "")
    mock.resource_name.side_effect = lambda key: resource_names.get(key, "")
    return mock


# ---------------------------------------------------------------------------
# OCCM 플러그인
# ---------------------------------------------------------------------------


def test_occm_should_deploy_false_when_disabled():
    from drover.services.plugins.occm import OccmPlugin

    s = _base_settings(drover_occm_enabled=False)
    assert OccmPlugin().should_deploy(s) is False


def test_occm_should_deploy_false_when_no_auth_url():
    from drover.services.plugins.occm import OccmPlugin

    s = _base_settings(drover_occm_enabled=True, os_auth_url="")
    assert OccmPlugin().should_deploy(s) is False


def test_occm_should_deploy_true():
    from drover.services.plugins.occm import OccmPlugin

    s = _base_settings(drover_occm_enabled=True)
    assert OccmPlugin().should_deploy(s) is True


def test_occm_cloud_conf_sections_contains_global():
    from drover.services.plugins.occm import OccmPlugin

    s = _base_settings(drover_occm_enabled=True)
    result = OccmPlugin().cloud_conf_sections("proj-1", s)
    assert "[Global]" in result
    assert "auth-url=https://keystone.example.com:5000/v3" in result
    assert "tenant-id=proj-1" in result


def test_occm_manifests_valid_yaml():
    from drover.services.plugins.occm import OccmPlugin

    s = _base_settings(drover_occm_enabled=True)
    manifests = OccmPlugin().generate_manifests("test-cluster", "proj-1", s)
    docs = list(yaml.safe_load_all(manifests))
    assert len(docs) > 0
    kinds = {d["kind"] for d in docs if d}
    assert "DaemonSet" in kinds
    assert "ClusterRole" in kinds


def test_occm_needs_external_cloud_provider():
    from drover.services.plugins.occm import OccmPlugin

    s = _base_settings(drover_occm_enabled=True)
    assert OccmPlugin().needs_external_cloud_provider(s) is True


# ---------------------------------------------------------------------------
# Cinder CSI 플러그인
# ---------------------------------------------------------------------------


def test_cinder_csi_disabled_by_default():
    from drover.services.plugins.cinder_csi import CinderCsiPlugin

    s = _base_settings()
    assert CinderCsiPlugin().should_deploy(s) is False


def test_cinder_csi_should_deploy_true():
    from drover.services.plugins.cinder_csi import CinderCsiPlugin

    s = _base_settings(drover_cinder_csi_enabled=True)
    assert CinderCsiPlugin().should_deploy(s) is True


def test_cinder_csi_manifests_valid_yaml():
    from drover.services.plugins.cinder_csi import CinderCsiPlugin

    s = _base_settings(drover_cinder_csi_enabled=True)
    manifests = CinderCsiPlugin().generate_manifests("test-cluster", "proj-1", s)
    docs = list(yaml.safe_load_all(manifests))
    kinds = {d["kind"] for d in docs if d}
    assert "CSIDriver" in kinds
    assert "StatefulSet" in kinds
    assert "DaemonSet" in kinds
    assert "StorageClass" in kinds


def test_cinder_csi_cloud_conf_section():
    from drover.services.plugins.cinder_csi import CinderCsiPlugin

    s = _base_settings(drover_cinder_csi_enabled=True)
    section = CinderCsiPlugin().cloud_conf_sections("proj-1", s)
    assert "[BlockStorage]" in section


def test_cinder_csi_no_external_cloud_provider():
    from drover.services.plugins.cinder_csi import CinderCsiPlugin

    s = _base_settings(drover_cinder_csi_enabled=True)
    assert CinderCsiPlugin().needs_external_cloud_provider(s) is False


# ---------------------------------------------------------------------------
# Manila CSI 플러그인
# ---------------------------------------------------------------------------


def test_manila_csi_disabled_by_default():
    from drover.services.plugins.manila_csi import ManilaCsiPlugin

    s = _base_settings()
    assert ManilaCsiPlugin().should_deploy(s) is False


def test_manila_csi_should_deploy_true():
    from drover.services.plugins.manila_csi import ManilaCsiPlugin

    s = _base_settings(drover_manila_csi_enabled=True)
    assert ManilaCsiPlugin().should_deploy(s) is True


def test_manila_csi_manifests_valid_yaml():
    from drover.services.plugins.manila_csi import ManilaCsiPlugin

    s = _base_settings(drover_manila_csi_enabled=True)
    manifests = ManilaCsiPlugin().generate_manifests("test-cluster", "proj-1", s)
    docs = [d for d in yaml.safe_load_all(manifests) if d]
    kinds = {d["kind"] for d in docs}
    assert "CSIDriver" in kinds
    assert "StatefulSet" in kinds
    assert "StorageClass" in kinds
    # NFS CSI 드라이버도 포함되어야 함
    names = {d.get("metadata", {}).get("name") for d in docs}
    assert "nfs.csi.k8s.io" in names


def test_manila_csi_no_cloud_conf_sections():
    from drover.services.plugins.manila_csi import ManilaCsiPlugin

    s = _base_settings(drover_manila_csi_enabled=True)
    assert ManilaCsiPlugin().cloud_conf_sections("proj-1", s) == ""


# ---------------------------------------------------------------------------
# Octavia Ingress 플러그인
# ---------------------------------------------------------------------------

_FAKE_APP_CRED = {"id": "appcred-id-123", "secret": "appcred-secret-xyz", "user_id": "user-abc"}
_FAKE_SUBNET_ID = "subnet-abc-123"


def test_octavia_ingress_disabled_by_default():
    from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin

    s = _base_settings()
    assert OctaviaIngressPlugin().should_deploy(s) is False


def test_octavia_ingress_deploys_without_subnet_id():
    """subnet_id 미설정이어도 enabled+auth이면 should_deploy는 True (subnet은 런타임 도출)."""
    from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin

    s = _base_settings(drover_octavia_ingress_enabled=True, drover_octavia_ingress_subnet_id="")
    assert OctaviaIngressPlugin().should_deploy(s) is True


def test_octavia_ingress_should_deploy_true():
    from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin

    s = _base_settings(drover_octavia_ingress_enabled=True)
    assert OctaviaIngressPlugin().should_deploy(s) is True


def test_octavia_ingress_manifests_valid_yaml():
    from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin

    s = _base_settings(drover_octavia_ingress_enabled=True)
    manifests = OctaviaIngressPlugin().generate_manifests(
        "test-cluster", "proj-1", s, subnet_id=_FAKE_SUBNET_ID, app_credential=_FAKE_APP_CRED
    )
    docs = [d for d in yaml.safe_load_all(manifests) if d]
    kinds = {d["kind"] for d in docs}
    assert "StatefulSet" in kinds
    assert "IngressClass" in kinds


def test_octavia_ingress_manifests_uses_app_credential():
    """ConfigMap에 application-credential-id/secret이 있고, username/password는 없어야 한다."""
    from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin

    s = _base_settings(drover_octavia_ingress_enabled=True)
    manifests = OctaviaIngressPlugin().generate_manifests(
        "test-cluster", "proj-1", s, subnet_id=_FAKE_SUBNET_ID, app_credential=_FAKE_APP_CRED
    )
    assert "application-credential-id: appcred-id-123" in manifests
    assert "application-credential-secret: appcred-secret-xyz" in manifests
    assert "username:" not in manifests
    assert "password:" not in manifests


def test_octavia_ingress_manifests_missing_subnet_raises():
    """subnet_id 미전달 시 ValueError."""
    import pytest

    from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin

    s = _base_settings(drover_octavia_ingress_enabled=True)
    with pytest.raises(ValueError, match="subnet_id"):
        OctaviaIngressPlugin().generate_manifests(
            "test-cluster", "proj-1", s, subnet_id="", app_credential=_FAKE_APP_CRED
        )


def test_octavia_ingress_manifests_missing_app_cred_raises():
    """app_credential 미전달 시 ValueError."""
    import pytest

    from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin

    s = _base_settings(drover_octavia_ingress_enabled=True)
    with pytest.raises(ValueError, match="app_credential"):
        OctaviaIngressPlugin().generate_manifests(
            "test-cluster", "proj-1", s, subnet_id=_FAKE_SUBNET_ID, app_credential={}
        )


# ---------------------------------------------------------------------------
# Keystone Auth 플러그인
# ---------------------------------------------------------------------------


def test_keystone_auth_disabled_by_default():
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings()
    assert KeystoneAuthPlugin().should_deploy(s) is False


def test_keystone_auth_should_deploy_when_enabled():
    """8.14 데드락 해소 후: 설정 활성화 + image + os_auth_url 모두 충족 시 True."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings(drover_keystone_auth_enabled=True)
    assert KeystoneAuthPlugin().should_deploy(s) is True


def test_keystone_auth_should_not_deploy_without_image():
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings(drover_keystone_auth_enabled=True, drover_keystone_auth_image="")
    assert KeystoneAuthPlugin().should_deploy(s) is False


def test_keystone_auth_server_install_args_includes_kubelet_arg():
    """webhook config + kubelet pod-manifest-path 모두 포함."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings(drover_keystone_auth_enabled=True)
    args = KeystoneAuthPlugin().server_install_args(s)
    assert any("authentication-token-webhook-config-file" in a for a in args)
    assert any("pod-manifest-path=/var/lib/rancher/k3s/agent/pod-manifests" in a for a in args)


def test_keystone_auth_extra_write_files_paths():
    """static pod 운영에 필요한 host file 5건이 모두 포함된다."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings(drover_keystone_auth_enabled=True)
    plugin = KeystoneAuthPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", s)
    paths = {f["path"] for f in files}
    assert paths == {
        "/etc/kubernetes/keystone-webhook.yaml",
        "/var/lib/rancher/k3s/agent/pod-manifests/k8s-keystone-auth.yaml",
        "/etc/kubernetes/keystone-auth/tls.crt",
        "/etc/kubernetes/keystone-auth/tls.key",
        "/etc/kubernetes/keystone-auth/policy.json",
    }
    # tls.key는 0600
    key_file = next(f for f in files if f["path"].endswith("tls.key"))
    assert key_file["permissions"] == "0600"


def test_keystone_auth_webhook_endpoint_is_localhost():
    """webhook config의 server endpoint는 https://127.0.0.1:8443/webhook."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings(drover_keystone_auth_enabled=True)
    plugin = KeystoneAuthPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", s)
    webhook_yaml = next(f["content"] for f in files if f["path"].endswith("keystone-webhook.yaml"))
    config = yaml.safe_load(webhook_yaml)
    assert config["clusters"][0]["cluster"]["server"] == "https://127.0.0.1:8443/webhook"


def test_keystone_auth_static_pod_listens_on_localhost():
    """static pod manifest의 컨테이너 args에 --listen=127.0.0.1:8443 포함."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings(drover_keystone_auth_enabled=True)
    plugin = KeystoneAuthPlugin()
    files = plugin.extra_write_files("proj-1", "test-cluster", s)
    pod_yaml = next(f["content"] for f in files if f["path"].endswith("k8s-keystone-auth.yaml"))
    pod = yaml.safe_load(pod_yaml)
    assert pod["kind"] == "Pod"
    assert pod["spec"]["hostNetwork"] is True
    args = pod["spec"]["containers"][0]["args"]
    assert "--listen=127.0.0.1:8443" in args
    assert "--keystone-policy-file=/etc/policy/policy.json" in args


def test_keystone_auth_generate_manifests_empty():
    """static pod로 전환되어 K8s 매니페스트는 빈 문자열."""
    from drover.services.plugins.keystone_auth import KeystoneAuthPlugin

    s = _base_settings(drover_keystone_auth_enabled=True)
    assert KeystoneAuthPlugin().generate_manifests("test-cluster", "proj-1", s) == ""


# ---------------------------------------------------------------------------
# Barbican KMS 플러그인
# ---------------------------------------------------------------------------


def test_barbican_kms_disabled_by_default():
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings()
    assert BarbicanKmsPlugin().should_deploy(s) is False


def test_barbican_kms_no_longer_requires_global_kek_id_after_pr2():
    """PR2 후: 글로벌 KEK ID 비어있어도 should_deploy=True (caller 가 ensure_project_kek 으로 동적 발급)."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="")
    assert BarbicanKmsPlugin().should_deploy(s) is True


def test_barbican_kms_should_deploy_when_enabled_and_kek_set():
    """8.14 데드락 해소 후: 설정 활성화 + KEK ID + OpenStack 자격증명 모두 충족 시 True."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="kek-uuid-123")
    assert BarbicanKmsPlugin().should_deploy(s) is True


def test_barbican_kms_server_install_args_only_encryption_provider():
    """PR3 후: encryption-provider-config 만 포함. pod-manifest-path 는 host static pod 폐기로 제거."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="kek-uuid-123")
    args = BarbicanKmsPlugin().server_install_args(s)
    assert any("encryption-provider-config" in a for a in args)
    assert not any("pod-manifest-path" in a for a in args), (
        "PR3 후 host static pod 폐기 — pod-manifest-path 인자가 더 이상 필요 없음"
    )


def test_barbican_kms_extra_write_files_systemd_pattern():
    """PR3 후: 4개 host file (encryption-config + systemd unit + install script + cloud.conf)."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="kek-uuid-123")
    files = BarbicanKmsPlugin().extra_write_files("proj-1", "test-cluster", s)
    paths = {f["path"] for f in files}
    assert paths == {
        "/etc/kubernetes/encryption-config.yaml",
        "/etc/systemd/system/barbican-kms.service",
        "/opt/k3s/install_kms.sh",
        "/etc/kubernetes/barbican-cloud.conf",
    }
    # host static pod manifest 는 더 이상 작성 안 함
    assert not any("pod-manifests" in p for p in paths)


def test_barbican_kms_systemd_unit_runs_before_drover_with_correct_args():
    """PR3 후: systemd unit 이 k3s.service 보다 먼저 시작 + KEK ID + sock path 인자 포함."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="kek-uuid-123")
    files = BarbicanKmsPlugin().extra_write_files("proj-1", "test-cluster", s)
    unit = next(f["content"] for f in files if f["path"].endswith("barbican-kms.service"))
    assert "Before=k3s.service" in unit, "데드락 방지 — k3s.service 보다 먼저 시작되어야 함"
    assert "ExecStart=/usr/local/bin/barbican-kms-plugin" in unit
    assert "--socketpath=/var/lib/kms/kms.sock" in unit
    assert "--cloud-config=/etc/kubernetes/barbican-cloud.conf" in unit
    # KEK ID 는 cloud.conf [KeyManager] key-id 로 전달 (CLI flag 아님). 주석 라인 제외.
    exec_lines = [ln for ln in unit.split("\n") if not ln.strip().startswith("#")]
    assert not any("--key-id" in ln for ln in exec_lines), "barbican-kms-plugin 은 --key-id flag 미지원 (v1.34.1 검증)"
    cc = next(f["content"] for f in files if f["path"] == "/etc/kubernetes/barbican-cloud.conf")
    assert "key-id=kek-uuid-123" in cc
    assert "Restart=always" in unit


def test_barbican_kms_generate_manifests_empty():
    """static pod로 전환되어 K8s 매니페스트는 빈 문자열."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="kek-uuid-123")
    assert BarbicanKmsPlugin().generate_manifests("test-cluster", "proj-1", s) == ""


def test_barbican_kms_arg_path_matches_write_file():
    """--encryption-provider-config 인자의 path가 extra_write_files의 encryption-config.yaml path와 일치."""
    from drover.services.plugins.barbican_kms import BarbicanKmsPlugin

    s = _base_settings(drover_barbican_kms_enabled=True, drover_barbican_kms_kek_id="kek-uuid-123")
    plugin = BarbicanKmsPlugin()
    args = plugin.server_install_args(s)
    arg_path = next(a.split("=", 2)[2] for a in args if "encryption-provider-config" in a)
    files = plugin.extra_write_files("proj-1", "test-cluster", s)
    file_paths = {f["path"] for f in files}
    assert arg_path in file_paths


# ---------------------------------------------------------------------------
# 레지스트리 집계 함수
# ---------------------------------------------------------------------------


def test_registry_no_plugins_active():
    from drover.services import plugins as drover_plugins

    s = _base_settings()
    assert drover_plugins.get_active_plugins(s) == []
    assert drover_plugins.needs_external_cloud_provider(s) is False
    assert drover_plugins.aggregate_cloud_conf("proj-1", s) is None
    manifests, failures = drover_plugins.aggregate_manifests("test-cluster", "proj-1", s)
    assert manifests == [] and failures == []


def test_registry_occm_only():
    from drover.services import plugins as drover_plugins

    s = _base_settings(drover_occm_enabled=True)
    active = drover_plugins.get_active_plugins(s)
    assert len(active) == 1
    assert active[0].name == "occm"
    assert drover_plugins.needs_external_cloud_provider(s) is True
    cloud_conf = drover_plugins.aggregate_cloud_conf("proj-1", s)
    assert cloud_conf is not None
    assert "[Global]" in cloud_conf


def test_registry_occm_plus_cinder():
    from drover.services import plugins as drover_plugins

    s = _base_settings(drover_occm_enabled=True, drover_cinder_csi_enabled=True)
    active = drover_plugins.get_active_plugins(s)
    assert len(active) == 2
    cloud_conf = drover_plugins.aggregate_cloud_conf("proj-1", s)
    assert "[Global]" in cloud_conf
    assert "[BlockStorage]" in cloud_conf
    manifests, failures = drover_plugins.aggregate_manifests("test-cluster", "proj-1", s)
    assert not failures
    assert len(manifests) == 2
    names = [m["name"] for m in manifests]
    assert "occm" in names
    assert "cinder_csi" in names


def test_registry_server_args_dedup():
    """동일 인자가 여러 플러그인에서 반환되어도 중복 없어야 함."""
    from drover.services import plugins as drover_plugins

    class FakePluginA:
        name = "fake_a"

        def should_deploy(self, s):
            return True

        def cloud_conf_sections(self, pid, s):
            return ""

        def generate_manifests(self, cn, pid, s):
            return ""

        def extra_write_files(self, pid, cn, s):
            return []

        def server_install_args(self, s):
            return ["--foo=bar", "--baz=1"]

        def agent_install_args(self, s):
            return []

        def needs_external_cloud_provider(self, s):
            return False

    class FakePluginB:
        name = "fake_b"

        def should_deploy(self, s):
            return True

        def cloud_conf_sections(self, pid, s):
            return ""

        def generate_manifests(self, cn, pid, s):
            return ""

        def extra_write_files(self, pid, cn, s):
            return []

        def server_install_args(self, s):
            return ["--foo=bar", "--other=x"]  # --foo=bar 중복

        def agent_install_args(self, s):
            return []

        def needs_external_cloud_provider(self, s):
            return False

    original = drover_plugins.ALL_PLUGINS
    drover_plugins.ALL_PLUGINS = [FakePluginA(), FakePluginB()]
    try:
        s = _base_settings()
        args = drover_plugins.aggregate_server_args(s)
        assert args.count("--foo=bar") == 1
        assert "--baz=1" in args
        assert "--other=x" in args
    finally:
        drover_plugins.ALL_PLUGINS = original


def test_aggregate_manifests_partial_failure():
    """generate_manifests 예외 발생 시 나머지는 처리되고 failures에 이름 기록."""
    from drover.services import plugins as drover_plugins

    class GoodPlugin:
        name = "good"

        def should_deploy(self, s):
            return True

        def cloud_conf_sections(self, pid, s):
            return ""

        def generate_manifests(self, cn, pid, s):
            return "---\nkind: List"

        def extra_write_files(self, pid, cn, s):
            return []

        def server_install_args(self, s):
            return []

        def agent_install_args(self, s):
            return []

        def needs_external_cloud_provider(self, s):
            return False

    class BadPlugin:
        name = "bad"

        def should_deploy(self, s):
            return True

        def cloud_conf_sections(self, pid, s):
            return ""

        def generate_manifests(self, cn, pid, s):
            raise RuntimeError("template error")

        def extra_write_files(self, pid, cn, s):
            return []

        def server_install_args(self, s):
            return []

        def agent_install_args(self, s):
            return []

        def needs_external_cloud_provider(self, s):
            return False

    original = drover_plugins.ALL_PLUGINS
    drover_plugins.ALL_PLUGINS = [GoodPlugin(), BadPlugin()]
    try:
        s = _base_settings()
        manifests, failures = drover_plugins.aggregate_manifests("cluster", "proj", s)
        assert len(manifests) == 1
        assert manifests[0]["name"] == "good"
        assert failures == ["bad"]
    finally:
        drover_plugins.ALL_PLUGINS = original


def test_registry_get_active_plugin_names():
    from drover.services import plugins as drover_plugins

    s = _base_settings(drover_occm_enabled=True, drover_cinder_csi_enabled=True)
    names = drover_plugins.get_active_plugin_names(s)
    assert names == {"occm": True, "cinder_csi": True}


# ---------------------------------------------------------------------------
# cloud-init 통합 렌더링 테스트
# ---------------------------------------------------------------------------


def test_cloudinit_server_no_plugins():
    """플러그인 없을 때 cloud-init 렌더링이 정상 동작해야 한다."""
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.31.4+k3s1",
        callback_url="http://callback.example.com",
        callback_token="token123",
    )
    assert result.config_drive is False
    decoded = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "#cloud-config" in decoded
    assert "k3s_version" not in decoded  # Jinja 변수 미치환 없어야 함
    assert "--disable-cloud-controller" not in decoded


def test_cloudinit_server_with_occm_plugin():
    """OCCM 플러그인 활성 시 cloud-init에 external cloud provider 인자가 포함되어야 한다."""
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.31.4+k3s1",
        callback_url="http://callback.example.com",
        callback_token="token123",
        cloud_conf="[Global]\nauth-url=https://example.com\n",
        plugin_manifests=[{"name": "occm", "content": "apiVersion: v1\nkind: List\nitems: []\n"}],
        needs_external_cloud_provider=True,
    )
    assert result.config_drive is False
    decoded = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "--disable-cloud-controller" in decoded
    assert "cloud-provider=external" in decoded
    assert "occm-manifests.yaml" in decoded
    assert "cloud.conf" in decoded


def test_cloudinit_server_multi_plugins():
    """복수 플러그인 시 각 매니페스트 파일이 write_files에 포함되어야 한다."""
    from drover.services.cloudinit import generate_server_userdata

    result = generate_server_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.31.4+k3s1",
        callback_url="http://callback.example.com",
        callback_token="token123",
        plugin_manifests=[
            {"name": "occm", "content": "---\n# occm manifest\n"},
            {"name": "cinder_csi", "content": "---\n# cinder manifest\n"},
        ],
        needs_external_cloud_provider=True,
    )
    assert result.config_drive is False
    decoded = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "occm-manifests.yaml" in decoded
    assert "cinder_csi-manifests.yaml" in decoded


def test_cloudinit_agent_no_extra_args():
    """에이전트: INSTALL_K3S_EXEC에 agent만 포함하고 node-ip CLI는 없어야 한다."""
    from drover.services.cloudinit import generate_agent_userdata

    result = generate_agent_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.31.4+k3s1",
        server_ip="10.0.0.1",
        node_token="tok",
    )
    assert result.config_drive is False
    decoded = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "#cloud-config" in decoded
    assert 'INSTALL_K3S_EXEC="${_EXEC_ARGS}"' in decoded
    assert '_EXEC_ARGS="agent"' in decoded
    assert "--node-ip" not in decoded


def test_cloudinit_agent_with_cloud_provider():
    """에이전트: extra_agent_args에 cloud-provider=external이 포함되어야 한다."""
    from drover.services.cloudinit import generate_agent_userdata

    result = generate_agent_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.31.4+k3s1",
        server_ip="10.0.0.1",
        node_token="tok",
        extra_agent_args=["--kubelet-arg=cloud-provider=external"],
    )
    assert result.config_drive is False
    decoded = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "cloud-provider=external" in decoded
    assert "INSTALL_K3S_EXEC" in decoded


def test_cloudinit_agent_backward_compat_occm_enabled():
    """하위호환: occm_enabled=True 시 cloud-provider=external이 포함되어야 한다."""
    from drover.services.cloudinit import generate_agent_userdata

    result = generate_agent_userdata(
        primary_network_id="net-primary",
        cluster_name="test",
        k3s_version="v1.31.4+k3s1",
        server_ip="10.0.0.1",
        node_token="tok",
        occm_enabled=True,
    )
    assert result.config_drive is False
    decoded = gzip.decompress(base64.b64decode(result.data)).decode()
    assert "cloud-provider=external" in decoded
