"""Tests for file-backed credential loading, Kolla secret path rendering, and cloud-init secret exclusions."""

from pathlib import Path
from unittest.mock import patch

import jinja2
import pytest

from drover import config
from drover.services import cloudinit
from drover.services.plugins import barbican_kms, manila_csi, occm, octavia_ingress


def test_os_password_file_loading(tmp_path):
    secret_file = tmp_path / "os_password"
    secret_file.write_text("file-secret-password-123\n")

    settings = config.Settings(os_password_file=str(secret_file))
    assert settings.os_password == "file-secret-password-123"


def test_encryption_key_file_loading(tmp_path):
    secret_file = tmp_path / "key"
    key_hex = "a" * 64
    secret_file.write_text(f"{key_hex}\n")

    settings = config.Settings(drover_kubeconfig_encryption_key_file=str(secret_file))
    assert settings.drover_kubeconfig_encryption_key == key_hex


def test_database_password_file_injection(tmp_path):
    secret_file = tmp_path / "db_password"
    secret_file.write_text("secret_db_pass\n")

    settings = config.Settings(
        database_url="mysql+aiomysql://drover@127.0.0.1:3306/drover",
        database_password_file=str(secret_file),
    )
    assert settings.database_url == "mysql+aiomysql://drover:secret_db_pass@127.0.0.1:3306/drover"


def test_redis_password_file_injection(tmp_path):
    secret_file = tmp_path / "redis_password"
    secret_file.write_text("secret_redis_pass\n")

    settings = config.Settings(
        redis_url="redis://localhost:6379/7",
        redis_password_file=str(secret_file),
    )
    assert settings.redis_url == "redis://:secret_redis_pass@localhost:6379/7"


def test_validate_config_with_file_backed_credentials(tmp_path):
    os_pass_file = tmp_path / "os_password"
    os_pass_file.write_text("os_secret_123")
    enc_file = tmp_path / "enc_key"
    enc_file.write_text("c" * 64)

    settings = config.Settings(
        database_url="mysql+aiomysql://drover:pass@127.0.0.1:3306/drover",
        drover_callback_base_url="http://127.0.0.1:8011",
        drover_kubeconfig_encryption_key_file=str(enc_file),
        os_auth_url="http://127.0.0.1:5000/v3",
        os_username="drover",
        os_password_file=str(os_pass_file),
    )

    validated = config.validate_config(settings)
    assert validated.os_password == "os_secret_123"
    assert validated.drover_kubeconfig_encryption_key == "c" * 64


def test_kolla_drover_conf_renders_only_secret_paths():
    kolla_template_path = Path(__file__).parents[1] / "deploy" / "kolla" / "ansible" / "roles" / "drover" / "templates" / "drover.conf.j2"
    template_text = kolla_template_path.read_text()

    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    env.filters["bool"] = bool
    template = env.from_string(template_text)

    rendered = template.render(
        drover_keystone_auth_url="http://127.0.0.1:5000/v3",
        drover_keystone_user="drover",
        drover_keystone_password_file="/etc/drover/secrets/os_password",
        drover_service_project_name="drover-service",
        drover_service_project_id="test-proj-id-123",
        drover_keystone_project_domain_name="Default",
        drover_keystone_user_domain_name="Default",
        drover_keystone_region_name="RegionOne",
        drover_keystone_interface="internal",
        openstack_insecure=False,
        openstack_cacert="",
        drover_database_url="mysql+aiomysql://drover@127.0.0.1:3306/drover",
        drover_database_password_file="/etc/drover/secrets/database_password",
        drover_database_pool_size=5,
        drover_database_max_overflow=10,
        drover_database_connect_timeout=10,
        drover_database_pool_timeout=10,
        drover_redis_url="redis://127.0.0.1:6379/7",
        drover_redis_password_file="/etc/drover/secrets/redis_password",
        drover_callback_base_url="http://127.0.0.1:8011",
        drover_kubeconfig_encryption_key_file="/etc/drover/secrets/kubeconfig_encryption_key",
        drover_boot_volume_size_gb=30,
        drover_occm_enabled=True,
        drover_occm_image="registry.k8s.io/provider-os/openstack-cloud-controller-manager:v1.34.1",
        drover_cinder_csi_enabled=True,
        drover_cinder_csi_image="registry.k8s.io/provider-os/cinder-csi-plugin:v1.34.1",
        drover_manila_csi_enabled=False,
        drover_manila_csi_image="registry.k8s.io/provider-os/manila-csi-plugin:v1.34.1",
        drover_manila_csi_nfs_image="registry.k8s.io/sig-storage/nfsplugin:v4.9.0",
        drover_manila_csi_share_protocol="NFS",
        drover_keystone_auth_enabled=False,
        drover_keystone_auth_image="registry.k8s.io/provider-os/k8s-keystone-auth:v1.34.1",
        drover_keystone_auth_policy="",
        drover_octavia_ingress_enabled=False,
        drover_octavia_ingress_image="registry.k8s.io/provider-os/octavia-ingress-controller:v1.34.1",
        drover_barbican_kms_enabled=False,
        drover_barbican_kms_image="registry.k8s.io/provider-os/barbican-kms-plugin:v1.34.1",
        drover_barbican_kms_kek_id="",
        drover_cert_rotation_node_timeout_sec=300,
        drover_cert_rotation_job_image="registry.k8s.io/util-linux/util-linux:latest",
        drover_stampede_enabled=False,
        drover_stampede_interval=60,
        drover_stampede_scale_down_threshold=0.5,
        drover_stampede_scale_down_window=600,
        drover_stampede_scale_up_cooldown=120,
        drover_stampede_scale_down_cooldown=300,
        drover_stampede_resource_headroom_factor=0.3,
    )

    assert 'password_file = "/etc/drover/secrets/os_password"' in rendered
    assert 'password_file = "/etc/drover/secrets/database_password"' in rendered
    assert 'password_file = "/etc/drover/secrets/redis_password"' in rendered
    assert 'kubeconfig_encryption_key_file = "/etc/drover/secrets/kubeconfig_encryption_key"' in rendered
    assert "password =" not in rendered
    assert "kubeconfig_encryption_key =" not in rendered


def test_kolla_secret_files_are_root_group_readable_for_non_root_container():
    role_root = Path(__file__).parents[1] / "deploy" / "kolla" / "ansible" / "roles" / "drover"
    tasks_text = (role_root / "tasks" / "config.yml").read_text()
    dockerfile_text = (Path(__file__).parents[1] / "Dockerfile").read_text()

    assert 'mode: "0750"' in tasks_text
    assert tasks_text.count('mode: "0640"') >= 4
    assert "owner: root" in tasks_text and "group: root" in tasks_text
    assert "no_log: true" in tasks_text
    assert "adduser appuser root" in dockerfile_text


def test_cloudinit_cloud_conf_file_mode_0600():
    cloud_conf_text = "[Global]\nauth-url=http://127.0.0.1:5000/v3\n"
    res = cloudinit.generate_server_userdata(
        cluster_name="test-cluster",
        k3s_version="v1.28.4+k3s2",
        callback_url="http://127.0.0.1:8011/v1/callback",
        callback_token="testtoken",
        primary_network_id="net-123",
        cloud_conf=cloud_conf_text,
        os_type=cloudinit.OS_TYPE_UBUNTU,
    )
    # Ubuntu user_data is base64(gzip(cloud-config))
    import base64
    import gzip
    decompressed = gzip.decompress(base64.b64decode(res.data)).decode()
    assert "- path: /etc/kubernetes/cloud.conf" in decompressed
    assert "permissions: \"0600\"" in decompressed


def test_fcos_ignition_cloud_conf_mode_0600():
    cloud_conf_text = "[Global]\nauth-url=http://127.0.0.1:5000/v3\n"
    res = cloudinit.generate_server_userdata(
        cluster_name="test-cluster",
        k3s_version="v1.28.4+k3s2",
        callback_url="http://127.0.0.1:8011/v1/callback",
        callback_token="testtoken",
        primary_network_id="net-123",
        cloud_conf=cloud_conf_text,
        os_type=cloudinit.OS_TYPE_FCOS,
    )

    import base64
    import json
    ign_dict = json.loads(base64.b64decode(res.data).decode())
    files = ign_dict.get("storage", {}).get("files", [])
    cloud_conf_entry = next((f for f in files if f["path"] == "/etc/kubernetes/cloud.conf"), None)
    assert cloud_conf_entry is not None
    assert cloud_conf_entry["mode"] == 0o600


def test_service_password_guard_triggers():
    mock_settings = config.Settings(os_password="SUPER_SECRET_SERVICE_PASS_987")
    with patch("drover.config.get_settings", return_value=mock_settings):
        with pytest.raises(ValueError, match="Rendered output contains service os_password"):
            cloudinit.verify_no_service_password(
                "some content including SUPER_SECRET_SERVICE_PASS_987 in text",
                settings=mock_settings,
            )


def test_plugin_templates_do_not_inject_service_password():
    app_cred = {"id": "cred-id-123", "secret": "cred-secret-456", "user_id": "user-789"}
    mock_settings = config.Settings(
        os_auth_url="http://127.0.0.1:5000/v3",
        os_username="drover",
        os_password="SERVICE_PASSWORD_MUST_NOT_BE_HERE",
        os_region_name="RegionOne",
    )
    object.__setattr__(mock_settings, "resource_id", lambda k: "res-id-123")
    object.__setattr__(mock_settings, "resource_name", lambda k: "res-name-123")

    occm_content = occm.OccmPlugin().cloud_conf_sections(
        project_id="proj-123",
        settings=mock_settings,
        app_credential=app_cred,
    )
    assert "SERVICE_PASSWORD_MUST_NOT_BE_HERE" not in occm_content
    assert "cred-id-123" in occm_content
    assert "cred-secret-456" in occm_content

    kms_files = barbican_kms.BarbicanKmsPlugin().extra_write_files(
        project_id="proj-123",
        cluster_name="cl-1",
        settings=mock_settings,
        app_credential=app_cred,
        kek_id="kek-123",
    )
    for f in kms_files:
        assert "SERVICE_PASSWORD_MUST_NOT_BE_HERE" not in f["content"]

    manila_content = manila_csi.ManilaCsiPlugin().generate_manifests(
        project_id="proj-123",
        cluster_name="cl-1",
        settings=mock_settings,
        app_credential=app_cred,
    )
    assert "SERVICE_PASSWORD_MUST_NOT_BE_HERE" not in manila_content
    assert "cred-id-123" in manila_content

    octavia_content = octavia_ingress.OctaviaIngressPlugin().generate_manifests(
        cluster_name="cl-1",
        project_id="proj-123",
        settings=mock_settings,
        subnet_id="subnet-123",
        app_credential=app_cred,
    )
    assert "SERVICE_PASSWORD_MUST_NOT_BE_HERE" not in octavia_content
    assert "cred-id-123" in octavia_content
