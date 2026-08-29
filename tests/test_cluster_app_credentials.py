"""Tests for roadmap step 4.2: Cluster Application Credentials lifecycle."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from drover.services import deletion, provisioner
from drover.services.plugins.barbican_kms import BarbicanKmsPlugin
from drover.services.plugins.manila_csi import ManilaCsiPlugin
from drover.services.plugins.occm import OccmPlugin
from drover.services.plugins.octavia_ingress import OctaviaIngressPlugin


def _mock_settings(**kwargs) -> MagicMock:
    m = MagicMock()
    m.os_auth_url = "https://keystone.example.com:5000/v3"
    m.os_region_name = "RegionOne"
    m.os_insecure = False
    m.os_cacert = ""
    m.os_username = "admin"
    m.os_password = "secret-password"
    m.os_user_domain_name = "Default"
    m.drover_occm_enabled = True
    m.drover_occm_image = "occm-image:v1.28.0"
    m.drover_manila_csi_enabled = True
    m.drover_manila_csi_image = "manila-image:v1.28.0"
    m.drover_manila_csi_nfs_image = "nfs-image:v4.4.0"
    m.drover_manila_csi_share_protocol = "NFS"
    m.drover_barbican_kms_enabled = True
    m.drover_barbican_kms_image = "kms-image:v1.28.0"
    m.drover_barbican_kms_kek_id = "kek-uuid-123"
    m.drover_octavia_ingress_enabled = True
    m.drover_octavia_ingress_image = "octavia-ingress-image:v1.28.0"
    m.resource_id.return_value = "res-id-123"
    m.resource_name.return_value = "res-name-123"
    for k, v in kwargs.items():
        setattr(m, k, v)
    return m


_FAKE_APP_CRED = {"id": "appcred-id-999", "secret": "appcred-secret-888", "user_id": "user-111"}


def test_1_no_password_fallback():
    """Verify plugins raise ValueError when app_credential is missing and render no passwords."""
    s = _mock_settings()

    # OCCM
    with pytest.raises(ValueError, match="app_credential"):
        OccmPlugin().cloud_conf_sections("proj-1", s, app_credential=None)
    occm_conf = OccmPlugin().cloud_conf_sections("proj-1", s, app_credential=_FAKE_APP_CRED)
    assert "application-credential-id=appcred-id-999" in occm_conf
    assert "application-credential-secret=appcred-secret-888" in occm_conf
    assert "username" not in occm_conf
    assert "password" not in occm_conf

    # Manila CSI
    with pytest.raises(ValueError, match="app_credential"):
        ManilaCsiPlugin().generate_manifests("cluster-1", "proj-1", s, app_credential=None)
    manila_manifests = ManilaCsiPlugin().generate_manifests("cluster-1", "proj-1", s, app_credential=_FAKE_APP_CRED)
    assert "os-applicationCredentialID: appcred-id-999" in manila_manifests
    assert "os-applicationCredentialSecret: appcred-secret-888" in manila_manifests
    assert "os-userName" not in manila_manifests
    assert "os-password" not in manila_manifests

    # Barbican KMS
    with pytest.raises(ValueError, match="app_credential"):
        BarbicanKmsPlugin().extra_write_files("proj-1", "cluster-1", s, app_credential=None)
    kms_files = BarbicanKmsPlugin().extra_write_files("proj-1", "cluster-1", s, app_credential=_FAKE_APP_CRED)
    barbican_conf = next(f["content"] for f in kms_files if f["path"] == "/etc/kubernetes/barbican-cloud.conf")
    assert "application-credential-id=appcred-id-999" in barbican_conf
    assert "application-credential-secret=appcred-secret-888" in barbican_conf
    assert "username" not in barbican_conf
    assert "password" not in barbican_conf

    # Octavia Ingress
    with pytest.raises(ValueError, match="app_credential"):
        OctaviaIngressPlugin().generate_manifests("cluster-1", "proj-1", s, subnet_id="sub-1", app_credential=None)


def test_2_sql_stores_id_only():
    """Verify that only app_credential_id (and not secret) is stored in SQL / inventory models."""
    from drover.models.orm import K3sCluster

    cluster = K3sCluster(
        id="c-123",
        project_id="proj-1",
        name="test-cluster",
        app_credential_id="appcred-id-999",
    )
    assert cluster.app_credential_id == "appcred-id-999"
    assert not hasattr(cluster, "app_credential_secret")
    assert not hasattr(cluster, "app_cred_secret")


@pytest.mark.asyncio
async def test_3_create_failure_before_provision_call():
    """Verify cluster create fails before Nova VM boot call if app credential acquisition fails."""
    conn_mock = MagicMock()
    conn_mock.compute.create_server = MagicMock()
    conn_mock.network.subnets = MagicMock(return_value=[MagicMock(id="sub-1")])

    mock_s = _mock_settings()
    payload = {
        "name": "test-cluster",
        "master_count": 1,
        "agent_count": 0,
        "server_flavor_id": "fl-1",
        "agent_flavor_id": "fl-1",
        "server_image_id": "img-1",
        "default_agent_image_id": "img-1",
        "network_id": "net-1",
        "key_name": "",
        "k3s_version": "v1.28.4+k3s2",
        "os_type": "ubuntu",
        "plugin_settings": mock_s,
        "policy_snapshot": {"k3s.octavia_ingress_floating_network": {"id": "fip-net-1"}},
    }

    with (
        patch("drover.config.get_settings", return_value=mock_s),
        patch("drover.services.keystone.get_admin_connection_for_project", MagicMock(return_value=conn_mock)),
        patch("drover.services.keystone.create_app_credential_for_cluster", side_effect=RuntimeError("Keystone auth failed")),
        patch("drover.services.inventory.record_resource", AsyncMock()),
        patch("drover.services.operations.append_operation_event", AsyncMock()),
        patch("drover.services.operations.update_operation_status", AsyncMock()),
        patch("drover.services.store.update_cluster_status", AsyncMock()),
        patch("drover.services.cinder.create_volume_from_image", MagicMock(return_value=MagicMock(id="vol-1"))),
    ):
        with pytest.raises(RuntimeError, match="Keystone auth failed"):
            await provisioner.create_cluster_job(
                project_id="proj-1",
                cluster_id="cluster-123",
                payload=payload,
                operation_id="op-123",
            )

        # Confirm Nova create_server was NEVER called
        conn_mock.compute.create_server.assert_not_called()


def test_4_kubernetes_secret_rendered_from_app_credential():
    """Verify Kubernetes Secret/ConfigMap and host configs render using app credential ID and secret."""
    s = _mock_settings()

    # Manila CSI K8s Secret
    manila_yaml = ManilaCsiPlugin().generate_manifests("cluster-1", "proj-1", s, app_credential=_FAKE_APP_CRED)
    parsed = list(yaml.safe_load_all(manila_yaml))
    secret_doc = next(d for d in parsed if d and d.get("kind") == "Secret" and d.get("metadata", {}).get("name") == "manila-cloud-secret")
    assert secret_doc["stringData"]["os-applicationCredentialID"] == "appcred-id-999"
    assert secret_doc["stringData"]["os-applicationCredentialSecret"] == "appcred-secret-888"

    # Octavia Ingress ConfigMap
    octavia_yaml = OctaviaIngressPlugin().generate_manifests("cluster-1", "proj-1", s, subnet_id="sub-1", app_credential=_FAKE_APP_CRED)
    parsed_octavia = list(yaml.safe_load_all(octavia_yaml))
    cm_doc = next(d for d in parsed_octavia if d and d.get("kind") == "ConfigMap")
    cm_config = cm_doc["data"]["config.yaml"]
    assert "application-credential-id: appcred-id-999" in cm_config
    assert "application-credential-secret: appcred-secret-888" in cm_config


@pytest.mark.asyncio
async def test_5_delete_revokes_app_credential():
    """Verify deleting a cluster revokes its recorded Keystone application credential."""
    cluster_dict = {
        "id": "c-123",
        "project_id": "proj-1",
        "name": "test-cluster",
        "app_credential_id": "appcred-id-999",
    }

    mock_ks_delete = AsyncMock()

    with (
        patch("drover.services.inventory.list_managed_resources", AsyncMock(return_value=[])),
        patch("drover.services.inventory.mark_resource_deleted", AsyncMock()),
        patch("drover.services.keystone.delete_app_credential", mock_ks_delete),
        patch("drover.services.operations.append_operation_event", AsyncMock()),
        patch("drover.services.store.update_cluster_status", AsyncMock()),
        patch("drover.services.store.delete_cluster_record", AsyncMock()),
        patch("drover.services.activity.rec", MagicMock()),
    ):
        [s async for s in deletion.delete_cluster_progress(MagicMock(), "proj-1", cluster_dict, token_info=None, operation_id="op-1")]
        mock_ks_delete.assert_called_once_with("proj-1", "appcred-id-999")
