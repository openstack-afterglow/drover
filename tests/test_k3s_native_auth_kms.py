"""Generated native webhook/KMS configuration contracts (no live endpoints)."""

from configparser import ConfigParser
from types import SimpleNamespace

import pytest
import yaml

from drover.services.plugins.barbican_kms import BarbicanKmsPlugin
from drover.services.plugins.keystone_auth import KeystoneAuthPlugin


def _settings(**overrides):
    values = {
        "os_auth_url": "https://keystone.example.com:5000/v3",
        "os_insecure": False,
        "os_cacert": "",
        "os_region_name": "RegionOne",
        "drover_keystone_auth_image": "registry.k8s.io/provider-os/k8s-keystone-auth:v1.34.1",
        "drover_keystone_auth_policy": "",
        "drover_barbican_kms_image": "registry.k8s.io/provider-os/barbican-kms-plugin:v1.34.1",
        "drover_barbican_kms_kek_id": "project-kek",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _webhook_files(settings):
    return {f["path"]: f for f in KeystoneAuthPlugin().extra_write_files("project", "native-auth-test", settings)}


def _pod_args(files):
    pod = yaml.safe_load(files["/var/lib/rancher/k3s/agent/pod-manifests/k8s-keystone-auth.yaml"]["content"])
    return pod["spec"]["containers"][0]["args"]


def _kms_files(settings):
    files = BarbicanKmsPlugin().extra_write_files(
        "project", "native-kms-test", settings,
        app_credential={"id": "test-app-id", "secret": "test-app-secret"},
    )
    return {f["path"]: f for f in files}


def _kms_config(files):
    config = ConfigParser()
    config.read_string(files["/etc/kubernetes/barbican-cloud.conf"]["content"])
    return config


@pytest.mark.parametrize("endpoint, expected", [
    ("https://keystone.example.com:5000", "https://keystone.example.com:5000/v3"),
    ("https://keystone.example.com:5000/", "https://keystone.example.com:5000/v3"),
    ("https://keystone.example.com:5000/v3", "https://keystone.example.com:5000/v3"),
    ("https://keystone.example.com:5000/v3/", "https://keystone.example.com:5000/v3"),
    ("https://keystone.example.com/identity", "https://keystone.example.com/identity/v3"),
    ("https://keystone.example.com/identity/v3/", "https://keystone.example.com/identity/v3"),
    ("https://keystone.example.com/identity/v3/v3/", "https://keystone.example.com/identity/v3"),
])
def test_native_webhook_normalizes_v3_endpoint(endpoint, expected):
    assert f"--keystone-url={expected}" in _pod_args(_webhook_files(_settings(os_auth_url=endpoint)))


def test_webhook_custom_ca_is_copied_into_existing_readonly_mount(tmp_path):
    ca = tmp_path / "backend-ca.pem"
    ca.write_text("test CA bundle\n")
    files = _webhook_files(_settings(os_cacert=str(ca)))
    ca_file = files["/etc/kubernetes/keystone-auth/keystone-ca.crt"]
    assert ca_file["content"] == ca.read_text()
    assert ca_file["permissions"] == "0644"
    assert "--keystone-ca-file=/etc/pki/keystone-ca.crt" in _pod_args(files)
    pod = yaml.safe_load(files["/var/lib/rancher/k3s/agent/pod-manifests/k8s-keystone-auth.yaml"]["content"])
    assert any(v["hostPath"]["path"] == "/etc/kubernetes/keystone-auth" for v in pod["spec"]["volumes"])
    assert any(m["mountPath"] == "/etc/pki" and m["readOnly"] for m in pod["spec"]["containers"][0]["volumeMounts"])


def test_webhook_default_uses_system_tls_trust():
    files = _webhook_files(_settings())
    assert not any(arg.startswith("--keystone-ca-file") for arg in _pod_args(files))
    assert "/etc/kubernetes/keystone-auth/keystone-ca.crt" not in files


def test_webhook_rejects_unsupported_insecure_https():
    with pytest.raises(ValueError, match="does not support insecure HTTPS"):
        _webhook_files(_settings(os_insecure=True))


def test_webhook_http_does_not_invent_tls_flags():
    files = _webhook_files(_settings(os_auth_url="http://keystone.example.com:5000", os_insecure=True))
    assert "--keystone-url=http://keystone.example.com:5000/v3" in _pod_args(files)
    assert not any(arg.startswith("--keystone-ca-file") for arg in _pod_args(files))


@pytest.mark.parametrize("plugin", [KeystoneAuthPlugin(), BarbicanKmsPlugin()])
def test_configured_unreadable_ca_fails_without_trust_downgrade(plugin, tmp_path):
    settings = _settings(os_cacert=str(tmp_path / "missing.pem"))
    kwargs = {}
    if isinstance(plugin, BarbicanKmsPlugin):
        kwargs["app_credential"] = {"id": "test-app-id", "secret": "test-app-secret"}
    with pytest.raises(FileNotFoundError):
        plugin.extra_write_files("project", "missing-ca-test", settings, **kwargs)


def test_empty_configured_ca_is_not_silently_replaced_by_system_trust(tmp_path):
    ca = tmp_path / "empty.pem"
    ca.write_text("")
    settings = _settings(os_cacert=str(ca))
    webhook = _webhook_files(settings)
    assert "--keystone-ca-file=/etc/pki/keystone-ca.crt" in _pod_args(webhook)
    assert webhook["/etc/kubernetes/keystone-auth/keystone-ca.crt"]["content"] == ""
    kms = _kms_files(settings)
    assert _kms_config(kms)["Global"]["ca-file"] == "/etc/kubernetes/barbican-ca.crt"
    assert kms["/etc/kubernetes/barbican-ca.crt"]["content"] == ""


def test_kms_custom_ca_is_copied_to_guest_host(tmp_path):
    ca = tmp_path / "backend-ca.pem"
    ca.write_text("test CA bundle\n")
    files = _kms_files(_settings(os_cacert=str(ca)))
    config = _kms_config(files)
    assert config["Global"]["ca-file"] == "/etc/kubernetes/barbican-ca.crt"
    assert not config.getboolean("Global", "tls-insecure")
    assert files["/etc/kubernetes/barbican-ca.crt"]["content"] == ca.read_text()
    assert files["/etc/kubernetes/barbican-ca.crt"]["permissions"] == "0644"


@pytest.mark.parametrize("insecure", [False, True])
def test_kms_tls_mode_is_explicit(insecure, tmp_path):
    # Insecure mode must not attempt to load a configured CA that it will not use.
    settings = _settings(os_insecure=insecure, os_cacert=str(tmp_path / "missing.pem") if insecure else "")
    files = _kms_files(settings)
    config = _kms_config(files)
    assert config.getboolean("Global", "tls-insecure") is insecure
    assert "ca-file" not in config["Global"]
    assert "/etc/kubernetes/barbican-ca.crt" not in files


def test_encryption_writes_kms_v2_and_reads_legacy_identity_only():
    files = _kms_files(_settings())
    encryption = yaml.safe_load(files["/etc/kubernetes/encryption-config.yaml"]["content"])
    assert encryption["apiVersion"] == "apiserver.config.k8s.io/v1"
    assert encryption["kind"] == "EncryptionConfiguration"
    assert len(encryption["resources"]) == 1
    resource = encryption["resources"][0]
    assert resource["resources"] == ["secrets"]
    assert resource["providers"] == [
        {"kms": {"apiVersion": "v2", "name": "barbican-kms", "endpoint": "unix:///var/lib/kms/kms.sock", "timeout": "3s"}},
        {"identity": {}},
    ]
    assert files["/etc/kubernetes/encryption-config.yaml"]["permissions"] == "0600"
    assert _kms_config(files)["KeyManager"]["key-id"] == "project-kek"
