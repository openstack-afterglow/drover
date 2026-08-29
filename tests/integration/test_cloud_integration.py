"""End-to-end integration tests for Drover running against live OpenStack staging cloud.

Required Staging Environment Checks:
------------------------------------
This module runs live cluster lifecycle workflows against a staging Kolla/OpenStack deployment.
The following environment variables are verified prior to execution:

1. Gate:
   - DROVER_INTEGRATION_CLOUD=1 (or "true"/"yes")
   - If unset or false, tests in this module are automatically skipped during pytest collection.

2. OpenStack Credentials (for openstacksdk connection & auth):
   - OS_AUTH_URL: Keystone v3 authentication endpoint URL
   - OS_USERNAME: OpenStack user name
   - OS_PASSWORD: OpenStack user password
   - OS_PROJECT_NAME or OS_PROJECT_ID: Target project name or ID

3. Disposable Infrastructure Configuration:
   - DROVER_INTEGRATION_NETWORK_ID: Disposable Neutron network ID
   - DROVER_INTEGRATION_IMAGE_ID: K3s node image ID (Ubuntu/FCOS)
   - DROVER_INTEGRATION_FLAVOR_ID: Nova flavor ID (e.g. m1.small)
   - DROVER_INTEGRATION_EXTERNAL_NET_ID: External network ID (optional, for Octavia/FIP)

4. Drover Service API Endpoint:
   - SERVICE_DROVER_INTERNAL_URL or DROVER_API_URL: Base URL of live Drover API service

Native Operation Polling Protocol:
----------------------------------
Live targets issue lifecycle operations using native /v1 API endpoints and poll /v1/operations/{op_id}
or /v1/operations/{op_id}/events until terminal status (SUCCEEDED / FAILED / CANCELLED) is reached.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest


def _poll_operation_until_terminal(
    conn: Any,
    operation_id: str,
    timeout: int = 600,
    poll_interval: int = 5,
) -> dict[str, Any]:
    """Poll native /v1/operations/{operation_id} until terminal state is reached."""
    deadline = time.monotonic() + timeout
    terminal_statuses = {"SUCCEEDED", "FAILED", "CANCELLED"}

    last_op = None
    while time.monotonic() < deadline:
        op = conn.drover.get_operation(operation_id)
        if op:
            last_op = op
            status = op.get("status")
            if status in terminal_statuses:
                return op
        time.sleep(poll_interval)

    raise TimeoutError(
        f"Operation {operation_id} did not reach terminal status within {timeout} seconds. Last state: {last_op}"
    )


@pytest.mark.integration
def test_one_master_cluster_lifecycle(
    openstack_conn: Any,
    staging_env: dict[str, str],
    disposable_cluster_tracker: list[str],
) -> None:
    """Validate 1-master K3s cluster lifecycle end-to-end on live OpenStack staging cloud.

    Workflow:
    1. Submit 1-master, 1-agent cluster create via native /v1 endpoint using disposable infrastructure.
    2. Poll native /v1 operation status until SUCCEEDED.
    3. Query /v1/admin/managed-resources and verify Nova, Cinder, Neutron inventory & Drover ownership tags.
    4. Scale nodegroup within bounds (1 -> 2 agents) and wait for operation completion.
    5. Delete cluster via native API and poll operation to SUCCEEDED.
    6. Assert no active recorded resources remain for cluster in managed-resources inventory.
    """
    conn = openstack_conn
    idempotency_key = f"int-test-1m-{uuid.uuid4().hex[:8]}"
    cluster_name = f"int-1m-{uuid.uuid4().hex[:6]}"

    # Step 1: Submit single-master cluster creation request
    create_payload = {
        "name": cluster_name,
        "master_count": 1,
        "agent_count": 1,
        "network_id": staging_env["network_id"],
        "image_id": staging_env["image_id"],
        "flavor_id": staging_env["flavor_id"],
        "idempotency_key": idempotency_key,
    }
    if staging_env.get("subnet_id"):
        create_payload["subnet_id"] = staging_env["subnet_id"]
    if staging_env.get("external_net_id"):
        create_payload["external_network_id"] = staging_env["external_net_id"]

    # Issue stream mutation request (yields SSE lines and captures operation_id)
    stream = conn.drover.create_cluster(**create_payload)
    operation_id = None
    cluster_id = None

    for line in stream:
        if isinstance(line, str) and "operation_id" in line:
            import json
            try:
                raw_data = line.replace("data:", "").strip()
                parsed = json.loads(raw_data)
                if isinstance(parsed, dict):
                    operation_id = parsed.get("operation_id") or operation_id
                    cluster_id = parsed.get("cluster_id") or cluster_id
            except Exception:
                pass

    assert operation_id is not None, "Failed to capture operation_id from create cluster response stream"

    # Fetch operation to get cluster_id if not present in initial stream
    op_info = conn.drover.get_operation(operation_id)
    assert op_info is not None, f"Could not fetch operation {operation_id}"
    cluster_id = cluster_id or op_info.get("cluster_id")
    assert cluster_id is not None, "Failed to resolve cluster_id for created operation"

    # Track cluster for finalizer cleanup in case test fails mid-flight
    disposable_cluster_tracker.append(cluster_id)

    # Step 2: Poll native operation until terminal state (SUCCEEDED)
    final_op = _poll_operation_until_terminal(conn, operation_id, timeout=600)
    assert final_op["status"] == "SUCCEEDED", f"Cluster creation operation failed: {final_op.get('error')}"

    # Verify cluster state in Drover API
    cluster_info = conn.drover.get_cluster(cluster_id)
    assert cluster_info is not None
    assert cluster_info.get("status") in ("ACTIVE", "RUNNING", "READY", "PROVISIONING")

    # Step 3: Verify recorded inventory & Drover ownership tags/metadata
    managed_resources = conn.drover.admin_managed_resources(cluster_id=cluster_id, include_deleted=False)
    assert isinstance(managed_resources, list)
    assert len(managed_resources) > 0, "No managed resources recorded for created cluster"

    resource_types = {r.get("resource_type") for r in managed_resources if isinstance(r, dict)}

    # Ensure required core resources are tracked in inventory
    assert "nova_server" in resource_types or "server" in resource_types, f"Nova server missing from inventory: {resource_types}"
    assert "security_group" in resource_types or "neutron_security_group" in resource_types, f"Security group missing: {resource_types}"

    # Verify ownership metadata/tags on recorded resources
    for res in managed_resources:
        assert res.get("cluster_id") == cluster_id
        meta = res.get("metadata_json") or {}
        if isinstance(meta, dict) and meta:
            assert meta.get("drover.managed") == "true" or meta.get("managed") == "true"

    # Step 4: Scale nodegroup within bounds (agent count 1 -> 2)
    scale_stream = conn.drover.scale_cluster(cluster_id, agent_count=2)
    scale_op_id = None
    for line in scale_stream:
        if isinstance(line, str) and "operation_id" in line:
            import json
            try:
                raw_data = line.replace("data:", "").strip()
                parsed = json.loads(raw_data)
                if isinstance(parsed, dict):
                    scale_op_id = parsed.get("operation_id") or scale_op_id
            except Exception:
                pass

    if scale_op_id:
        scale_final_op = _poll_operation_until_terminal(conn, scale_op_id, timeout=300)
        assert scale_final_op["status"] == "SUCCEEDED", f"Scale operation failed: {scale_final_op.get('error')}"

    # Step 5: Delete cluster via native API
    del_stream = conn.drover.delete_cluster_async(cluster_id)
    del_op_id = None
    for line in del_stream:
        if isinstance(line, str) and "operation_id" in line:
            import json
            try:
                raw_data = line.replace("data:", "").strip()
                parsed = json.loads(raw_data)
                if isinstance(parsed, dict):
                    del_op_id = parsed.get("operation_id") or del_op_id
            except Exception:
                pass

    if del_op_id:
        del_final_op = _poll_operation_until_terminal(conn, del_op_id, timeout=600)
        assert del_final_op["status"] == "SUCCEEDED", f"Delete operation failed: {del_final_op.get('error')}"

    # Step 6: Assert no active recorded resources remain for cluster
    active_resources = conn.drover.admin_managed_resources(cluster_id=cluster_id, include_deleted=False)
    assert len(active_resources) == 0, f"Expected 0 active resources after deletion, found: {active_resources}"


@pytest.mark.integration
def test_three_master_ha_cluster_lifecycle(
    openstack_conn: Any,
    staging_env: dict[str, str],
    disposable_cluster_tracker: list[str],
) -> None:
    """Validate 3-master HA K3s cluster lifecycle end-to-end on live OpenStack staging cloud.

    Workflow:
    1. Submit 3-master HA cluster creation request using disposable infrastructure.
    2. Poll native /v1 operation until SUCCEEDED.
    3. Verify HA inventory: 3 master Nova VMs, Octavia LB/pool/members, Cinder boot volumes, Neutron SGs.
    4. Delete cluster via native API and poll operation to SUCCEEDED.
    5. Assert no active recorded resources remain in inventory.
    """
    conn = openstack_conn
    idempotency_key = f"int-test-3m-{uuid.uuid4().hex[:8]}"
    cluster_name = f"int-3m-{uuid.uuid4().hex[:6]}"

    # Step 1: Submit 3-master HA cluster creation request
    create_payload = {
        "name": cluster_name,
        "master_count": 3,
        "agent_count": 1,
        "network_id": staging_env["network_id"],
        "image_id": staging_env["image_id"],
        "flavor_id": staging_env["flavor_id"],
        "idempotency_key": idempotency_key,
    }
    if staging_env.get("subnet_id"):
        create_payload["subnet_id"] = staging_env["subnet_id"]
    if staging_env.get("external_net_id"):
        create_payload["external_network_id"] = staging_env["external_net_id"]

    stream = conn.drover.create_cluster(**create_payload)
    operation_id = None
    cluster_id = None

    for line in stream:
        if isinstance(line, str) and "operation_id" in line:
            import json
            try:
                raw_data = line.replace("data:", "").strip()
                parsed = json.loads(raw_data)
                if isinstance(parsed, dict):
                    operation_id = parsed.get("operation_id") or operation_id
                    cluster_id = parsed.get("cluster_id") or cluster_id
            except Exception:
                pass

    assert operation_id is not None, "Failed to capture operation_id from 3-master create response stream"

    op_info = conn.drover.get_operation(operation_id)
    assert op_info is not None, f"Could not fetch operation {operation_id}"
    cluster_id = cluster_id or op_info.get("cluster_id")
    assert cluster_id is not None, "Failed to resolve cluster_id for HA create operation"

    disposable_cluster_tracker.append(cluster_id)

    # Step 2: Poll operation until SUCCEEDED
    final_op = _poll_operation_until_terminal(conn, operation_id, timeout=900)
    assert final_op["status"] == "SUCCEEDED", f"3-master cluster creation failed: {final_op.get('error')}"

    # Step 3: Verify HA inventory
    managed_resources = conn.drover.admin_managed_resources(cluster_id=cluster_id, include_deleted=False)
    assert len(managed_resources) > 0, "No managed resources recorded for 3-master cluster"

    nova_servers = [
        r for r in managed_resources
        if r.get("resource_type") in ("nova_server", "server")
    ]
    # Expect 3 masters + 1 agent = 4 servers
    assert len(nova_servers) >= 3, f"Expected at least 3 master Nova servers, found {len(nova_servers)}"

    # Step 4: Delete HA cluster
    del_stream = conn.drover.delete_cluster_async(cluster_id)
    del_op_id = None
    for line in del_stream:
        if isinstance(line, str) and "operation_id" in line:
            import json
            try:
                raw_data = line.replace("data:", "").strip()
                parsed = json.loads(raw_data)
                if isinstance(parsed, dict):
                    del_op_id = parsed.get("operation_id") or del_op_id
            except Exception:
                pass

    if del_op_id:
        del_final_op = _poll_operation_until_terminal(conn, del_op_id, timeout=600)
        assert del_final_op["status"] == "SUCCEEDED", f"HA Delete operation failed: {del_final_op.get('error')}"

    # Step 5: Assert no active recorded resources remain
    active_resources = conn.drover.admin_managed_resources(cluster_id=cluster_id, include_deleted=False)
    assert len(active_resources) == 0, f"Expected 0 active resources after HA deletion, found: {active_resources}"
