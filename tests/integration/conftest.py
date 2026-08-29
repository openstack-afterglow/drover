"""Integration test configuration and fixtures for live OpenStack / Drover staging tests.

Required Staging Environment Contract
-------------------------------------
This package implements end-to-end integration tests for Drover running against a live
OpenStack staging cloud (Kolla-Ansible deployment).

To run these tests:
1. Environment Gate:
   - DROVER_INTEGRATION_CLOUD: Must be set to '1', 'true', or 'yes'.
     If unset or false, all tests in tests/integration/ are deterministically skipped during pytest collection.

2. OpenStack Credentials (for openstacksdk connection):
   - OS_AUTH_URL: Keystone v3 authentication endpoint URL (e.g. "http://keystone.staging.local:5000/v3")
   - OS_USERNAME: OpenStack user name
   - OS_PASSWORD: OpenStack user password
   - OS_PROJECT_NAME or OS_PROJECT_ID: Project name/ID to scope credentials
   - OS_USER_DOMAIN_NAME: User domain (default: "Default")
   - OS_PROJECT_DOMAIN_NAME: Project domain (default: "Default")
   - OS_REGION_NAME: Region name (optional)

3. Target Disposable Infrastructure Resources:
   - DROVER_INTEGRATION_NETWORK_ID (or DROVER_INTEGRATION_NETWORK_NAME): Disposable Neutron network ID/name
   - DROVER_INTEGRATION_SUBNET_ID: Disposable Neutron subnet ID (optional)
   - DROVER_INTEGRATION_IMAGE_ID (or DROVER_INTEGRATION_IMAGE_NAME): K3s node image ID/name (Ubuntu/FCOS)
   - DROVER_INTEGRATION_FLAVOR_ID (or DROVER_INTEGRATION_FLAVOR_NAME): Nova flavor ID/name (e.g. m1.small)
   - DROVER_INTEGRATION_EXTERNAL_NET_ID: External network ID for Octavia LB / Floating IP (optional)

4. Drover Service API Endpoint & Access Token:
   - SERVICE_DROVER_INTERNAL_URL or DROVER_API_URL: Base URL of live Drover API service (e.g. "http://127.0.0.1:8011")
   - DROVER_AUTH_TOKEN: Valid project-scoped Keystone token for X-Auth-Token API header

Safety & Isolation:
------------------
- Tests use disposable test project and disposable infrastructure resources.
- Fixture finalizers track all created cluster resources and safely perform cleanup if test fails.
- Native /v1 endpoint operation polling is used to monitor cluster lifecycle operations.
"""

import os
import sys
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

# Ensure sdk directory is in sys.path when running from repo root
_SDK_DIR = Path(__file__).resolve().parents[2] / "sdk"
if _SDK_DIR.exists() and str(_SDK_DIR) not in sys.path:
    sys.path.insert(0, str(_SDK_DIR))

_INTEGRATION_DIR = Path(__file__).resolve().parent

import openstack
from drover_sdk import register as register_drover_sdk


def is_cloud_integration_enabled() -> bool:
    """Check if live cloud integration tests are enabled via DROVER_INTEGRATION_CLOUD env var."""
    val = os.environ.get("DROVER_INTEGRATION_CLOUD", "").strip().lower()
    return val in ("1", "true", "yes")


def get_staging_config() -> dict[str, str]:
    """Retrieve and validate the staging environment configuration contract."""
    config = {
        "auth_url": os.environ.get("OS_AUTH_URL", "").strip(),
        "username": os.environ.get("OS_USERNAME", "").strip(),
        "password": os.environ.get("OS_PASSWORD", "").strip(),
        "project_name": os.environ.get("OS_PROJECT_NAME", "").strip(),
        "project_id": os.environ.get("OS_PROJECT_ID", "").strip(),
        "user_domain_name": os.environ.get("OS_USER_DOMAIN_NAME", "Default").strip(),
        "project_domain_name": os.environ.get("OS_PROJECT_DOMAIN_NAME", "Default").strip(),
        "region_name": os.environ.get("OS_REGION_NAME", "").strip(),
        "network_id": (
            os.environ.get("DROVER_INTEGRATION_NETWORK_ID", "").strip()
            or os.environ.get("DROVER_INTEGRATION_NETWORK_NAME", "").strip()
        ),
        "subnet_id": os.environ.get("DROVER_INTEGRATION_SUBNET_ID", "").strip(),
        "image_id": (
            os.environ.get("DROVER_INTEGRATION_IMAGE_ID", "").strip()
            or os.environ.get("DROVER_INTEGRATION_IMAGE_NAME", "").strip()
        ),
        "flavor_id": (
            os.environ.get("DROVER_INTEGRATION_FLAVOR_ID", "").strip()
            or os.environ.get("DROVER_INTEGRATION_FLAVOR_NAME", "").strip()
        ),
        "external_net_id": os.environ.get("DROVER_INTEGRATION_EXTERNAL_NET_ID", "").strip(),
        "drover_url": (
            os.environ.get("SERVICE_DROVER_INTERNAL_URL", "").strip()
            or os.environ.get("DROVER_API_URL", "").strip()
        ),
        "drover_token": os.environ.get("DROVER_AUTH_TOKEN", "").strip(),
    }

    if is_cloud_integration_enabled():
        missing = []
        if not config["auth_url"]:
            missing.append("OS_AUTH_URL")
        if not config["username"]:
            missing.append("OS_USERNAME")
        if not config["password"]:
            missing.append("OS_PASSWORD")
        if not config["project_name"] and not config["project_id"]:
            missing.append("OS_PROJECT_NAME / OS_PROJECT_ID")
        if not config["network_id"]:
            missing.append("DROVER_INTEGRATION_NETWORK_ID / DROVER_INTEGRATION_NETWORK_NAME")
        if not config["image_id"]:
            missing.append("DROVER_INTEGRATION_IMAGE_ID / DROVER_INTEGRATION_IMAGE_NAME")
        if not config["flavor_id"]:
            missing.append("DROVER_INTEGRATION_FLAVOR_ID / DROVER_INTEGRATION_FLAVOR_NAME")

        if missing:
            raise ValueError(
                f"DROVER_INTEGRATION_CLOUD is enabled, but required staging environment variables are missing: "
                f"{', '.join(missing)}"
            )

    return config


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Hook to dynamically mark all integration tests and enforce deterministic collection skips without cloud."""
    enabled = is_cloud_integration_enabled()
    skip_reason = (
        "Skipping live cloud integration test: DROVER_INTEGRATION_CLOUD environment variable is not set to 1/true/yes."
    )
    skip_marker = pytest.mark.skip(reason=skip_reason)

    for item in items:
        if not Path(str(item.path)).resolve().is_relative_to(_INTEGRATION_DIR):
            continue
        item.add_marker(pytest.mark.integration)
        if not enabled:
            # Deterministically skip if DROVER_INTEGRATION_CLOUD is not enabled.
            item.add_marker(skip_marker)


@pytest.fixture(scope="session")
def staging_env(request: pytest.FixtureRequest) -> dict[str, str]:
    """Session fixture returning validated staging environment configuration."""
    if not is_cloud_integration_enabled():
        pytest.skip("DROVER_INTEGRATION_CLOUD is not enabled (set DROVER_INTEGRATION_CLOUD=1 to run)")
    return get_staging_config()


@pytest.fixture(scope="session")
def openstack_conn(staging_env: dict[str, str]) -> Any:
    """Session fixture returning openstacksdk Connection registered with Drover SDK proxy."""
    if not is_cloud_integration_enabled():
        pytest.skip("DROVER_INTEGRATION_CLOUD is not enabled")

    conn_args: dict[str, Any] = {
        "auth_url": staging_env["auth_url"],
        "username": staging_env["username"],
        "password": staging_env["password"],
        "user_domain_name": staging_env["user_domain_name"],
        "project_domain_name": staging_env["project_domain_name"],
    }
    if staging_env["project_id"]:
        conn_args["project_id"] = staging_env["project_id"]
    elif staging_env["project_name"]:
        conn_args["project_name"] = staging_env["project_name"]
    if staging_env["region_name"]:
        conn_args["region_name"] = staging_env["region_name"]

    conn = openstack.connect(**conn_args)
    register_drover_sdk(conn)
    if staging_env["drover_url"]:
        conn.drover.endpoint_override = staging_env["drover_url"]

    return conn


@pytest.fixture
def disposable_cluster_tracker(openstack_conn: Any) -> Generator[list[str], None, None]:
    """Fixture tracking created cluster IDs and running finalizer cleanup for residual disposable resources."""
    created_cluster_ids: list[str] = []

    yield created_cluster_ids

    # Fixture finalizer: safely delete residual clusters if test failed before explicit deletion
    for cluster_id in created_cluster_ids:
        try:
            cluster = openstack_conn.drover.get_cluster(cluster_id)
            if cluster and cluster.get("status") not in ("DELETED", "DELETING"):
                openstack_conn.drover.delete_cluster_async(cluster_id)
                # Poll deletion completion up to 60s
                for _ in range(30):
                    time.sleep(2)
                    c = openstack_conn.drover.get_cluster(cluster_id)
                    if not c or c.get("status") == "DELETED":
                        break
        except Exception:
            pass
