"""k3s_cloud_shell 서비스 단위 테스트."""

from __future__ import annotations

from drover.services.cloud_shell import (
    _user_hash,
    build_shell_kubeconfig,
    k8s_user_name,
    pod_name,
    pvc_name,
)


def test_user_hash_deterministic():
    assert _user_hash("user-123") == _user_hash("user-123")
    assert _user_hash("user-123") != _user_hash("user-456")
    h = _user_hash("user-abc")
    assert len(h) == 12
    assert all(c in "0123456789abcdef" for c in h)


def test_naming_conventions():
    uid = "test-user-id-xyz"
    assert pod_name(uid).startswith("cloud-shell-")
    assert pvc_name(uid).startswith("cloud-shell-home-")
    assert k8s_user_name(uid).startswith("afterglow-user-")
    assert len(pod_name(uid)) < 63
    assert len(pvc_name(uid)) < 63


def test_naming_is_deterministic():
    uid = "stable-user-id"
    assert pod_name(uid) == pod_name(uid)
    assert pvc_name(uid) == pvc_name(uid)
    assert k8s_user_name(uid) == k8s_user_name(uid)


def test_build_shell_kubeconfig_adds_impersonation():
    admin_yaml = """
clusters:
- cluster:
    server: https://1.2.3.4:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
users:
- name: default
  user:
    client-certificate-data: dGVzdA==
    client-key-data: dGVzdA==
"""
    import yaml

    result = build_shell_kubeconfig(admin_yaml, "my-user-id")
    kc = yaml.safe_load(result)
    assert kc["users"][0]["user"]["as"] == k8s_user_name("my-user-id")
    assert kc["users"][0]["user"]["client-certificate-data"] == "dGVzdA=="
    assert kc["clusters"][0]["cluster"]["server"] == "https://1.2.3.4:6443"
