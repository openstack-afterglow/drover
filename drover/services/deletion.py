"""Cluster deletion orchestration service."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

import openstack.connection

from drover.models.schemas import K3sProgressMessage, K3sProgressStep
from drover.services import cinder, inventory, keystone, neutron, nova, octavia, operations
from drover.services import kube as k3s_kube
from drover.services import store as k3s_cluster
from drover.services.activity import rec

_logger = logging.getLogger("drover.deletion")


async def delete_cluster_progress(
    conn: openstack.connection.Connection,
    project_id: str,
    cluster: dict,
    token_info: dict | None,
    operation_id: str | None = None,
) -> AsyncGenerator[K3sProgressMessage, None]:
    """k3s 클러스터 삭제 단계별 진행. 각 단계 진입 시 K3sProgressMessage 를 yield."""
    cluster_id: str = cluster["id"]
    cluster_name: str = cluster.get("name") or ""

    msg = K3sProgressMessage(step=K3sProgressStep.DELETE_INIT, progress=5, message="클러스터 삭제 준비 중...")
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    await k3s_cluster.update_cluster_status(project_id, cluster_id, "DELETING")

    # Load all active recorded managed resources for this cluster
    managed_res_list = await inventory.list_managed_resources(None, cluster_id=cluster_id, active_only=True)

    # Group recorded resources by service and resource_type
    nova_servers = [r for r in managed_res_list if r.service == "nova" and r.resource_type == "server"]
    octavia_members = [r for r in managed_res_list if r.service == "octavia" and r.resource_type == "member"]
    octavia_pools = [r for r in managed_res_list if r.service == "octavia" and r.resource_type == "pool"]
    octavia_listeners = [r for r in managed_res_list if r.service == "octavia" and r.resource_type == "listener"]
    octavia_lbs = [r for r in managed_res_list if r.service == "octavia" and r.resource_type == "load_balancer"]
    fips = [r for r in managed_res_list if r.service == "neutron" and r.resource_type == "floating_ip"]
    cinder_volumes = [r for r in managed_res_list if r.service == "cinder" and r.resource_type == "volume"]
    neutron_ports = [r for r in managed_res_list if r.service == "neutron" and r.resource_type == "port"]
    neutron_sg_rules = [r for r in managed_res_list if r.service == "neutron" and r.resource_type == "security_group_rule"]
    neutron_sgs = [r for r in managed_res_list if r.service == "neutron" and r.resource_type == "security_group"]
    app_creds = [r for r in managed_res_list if r.service == "keystone" and r.resource_type == "app_credential"]

    # Also include IDs recorded on the cluster record
    all_server_ids = {r.resource_id for r in nova_servers}
    if cluster.get("server_vm_id"):
        all_server_ids.add(cluster["server_vm_id"])
    agent_vm_ids = cluster.get("agent_vm_ids") or []
    if isinstance(agent_vm_ids, str):
        try:
            agent_vm_ids = json.loads(agent_vm_ids)
        except Exception:
            agent_vm_ids = []
    all_server_ids.update(agent_vm_ids)

    all_lb_ids = {r.resource_id for r in octavia_lbs}
    if cluster.get("api_lb_id"):
        all_lb_ids.add(cluster["api_lb_id"])

    all_fip_ids = {r.resource_id for r in fips}
    if cluster.get("api_fip_id"):
        all_fip_ids.add(cluster["api_fip_id"])

    all_sg_ids = {r.resource_id for r in neutron_sgs}
    if cluster.get("security_group_id"):
        all_sg_ids.add(cluster["security_group_id"])

    all_app_cred_ids = {r.resource_id for r in app_creds}
    if cluster.get("app_credential_id"):
        all_app_cred_ids.add(cluster["app_credential_id"])

    # Step A: K8s Node cleanup
    msg = K3sProgressMessage(step=K3sProgressStep.DELETE_K8S_NODES, progress=15, message="Kubernetes 노드 정리 중...")
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    all_node_names: list[str] = []
    if agent_vm_ids:
        vm_name_map = await k3s_cluster.get_agent_vm_names(cluster_id, agent_vm_ids)
        all_node_names.extend([name for name in vm_name_map.values() if name])
    server_node_name = cluster.get("server_vm_name") or ""
    if server_node_name:
        all_node_names.append(server_node_name)
    for r in nova_servers:
        if r.name and r.name not in all_node_names:
            all_node_names.append(r.name)
    if all_node_names:
        try:
            await k3s_kube.delete_k8s_nodes(cluster_id, all_node_names)
        except Exception as e:
            _logger.warning("k3s delete: K8s 노드 삭제 중 오류 (무시): %s", e)

    # Step 1: Nova server VMs and Octavia members
    msg = K3sProgressMessage(
        step=K3sProgressStep.DELETE_AGENT_VMS,
        progress=35,
        message="Nova server VMs 및 Octavia pool members 삭제 중...",
    )
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    for vid in all_server_ids:
        try:
            await asyncio.to_thread(nova.delete_server_safe, conn, vid, project_id, cluster_id)
            await inventory.mark_resource_deleted(None, service="nova", resource_type="server", resource_id=vid)
            _logger.info("k3s delete: Nova server %s fully deleted", vid)
        except Exception as e:
            _logger.warning("k3s delete: Nova server %s delete failed: %s", vid, e)

    for r in octavia_members:
        try:
            pool_id = (r.metadata_json or {}).get("pool_id") if isinstance(r.metadata_json, dict) else None
            if pool_id:
                await asyncio.to_thread(octavia.remove_member, conn, pool_id, r.resource_id)
            await inventory.mark_resource_deleted(None, service="octavia", resource_type="member", resource_id=r.resource_id)
            _logger.info("k3s delete: Octavia member %s deleted", r.resource_id)
        except Exception as e:
            _logger.warning("k3s delete: Octavia member %s delete failed: %s", r.resource_id, e)

    # Step 2: Octavia pools & listeners
    msg = K3sProgressMessage(
        step=K3sProgressStep.DELETE_LB_CLEANUP,
        progress=50,
        message="Octavia pools 및 listeners 정리 중...",
    )
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    for r in octavia_pools:
        try:
            await asyncio.to_thread(octavia.delete_pool, conn, r.resource_id)
            await inventory.mark_resource_deleted(None, service="octavia", resource_type="pool", resource_id=r.resource_id)
            _logger.info("k3s delete: Octavia pool %s deleted", r.resource_id)
        except Exception as e:
            _logger.warning("k3s delete: Octavia pool %s delete failed: %s", r.resource_id, e)

    for r in octavia_listeners:
        try:
            await asyncio.to_thread(octavia.delete_listener, conn, r.resource_id)
            await inventory.mark_resource_deleted(None, service="octavia", resource_type="listener", resource_id=r.resource_id)
            _logger.info("k3s delete: Octavia listener %s deleted", r.resource_id)
        except Exception as e:
            _logger.warning("k3s delete: Octavia listener %s delete failed: %s", r.resource_id, e)

    # Step 3: Octavia load balancers
    for lb_id in all_lb_ids:
        try:
            await asyncio.to_thread(octavia.delete_load_balancer_safe, conn, lb_id, project_id, cluster_id, cascade=True)
            await inventory.mark_resource_deleted(None, service="octavia", resource_type="load_balancer", resource_id=lb_id)
            _logger.info("k3s delete: Octavia LB %s fully deleted", lb_id)
        except Exception as e:
            _logger.warning("k3s delete: Octavia LB %s delete failed: %s", lb_id, e)

    # Step 4: Floating IPs
    msg = K3sProgressMessage(
        step=K3sProgressStep.DELETE_SERVER_VM,
        progress=70,
        message="Floating IPs 및 Boot volumes 정리 중...",
    )
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    for fip_id in all_fip_ids:
        try:
            await asyncio.to_thread(neutron.delete_floating_ip_safe, conn, fip_id, project_id, cluster_id)
            await inventory.mark_resource_deleted(None, service="neutron", resource_type="floating_ip", resource_id=fip_id)
            _logger.info("k3s delete: FIP %s deleted", fip_id)
        except Exception as e:
            _logger.warning("k3s delete: FIP %s delete failed: %s", fip_id, e)

    # Step 5: Cinder boot volumes
    for r in cinder_volumes:
        try:
            await asyncio.to_thread(cinder.delete_volume_safe, conn, r.resource_id, project_id, cluster_id)
            await inventory.mark_resource_deleted(None, service="cinder", resource_type="volume", resource_id=r.resource_id)
            _logger.info("k3s delete: Cinder volume %s deleted", r.resource_id)
        except Exception as e:
            _logger.warning("k3s delete: Cinder volume %s delete failed: %s", r.resource_id, e)

    # Step 6: Neutron ports & security groups
    msg = K3sProgressMessage(step=K3sProgressStep.DELETE_SECURITY_GROUP, progress=85, message="Ports 및 보안 그룹 삭제 중...")
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    for r in neutron_ports:
        try:
            await asyncio.to_thread(conn.network.delete_port, r.resource_id, ignore_missing=True)
            await asyncio.to_thread(neutron.wait_port_deleted, conn, r.resource_id)
            await inventory.mark_resource_deleted(None, service="neutron", resource_type="port", resource_id=r.resource_id)
            _logger.info("k3s delete: Neutron port %s deleted", r.resource_id)
        except Exception as e:
            _logger.warning("k3s delete: Neutron port %s delete failed: %s", r.resource_id, e)

    for r in neutron_sg_rules:
        try:
            await asyncio.to_thread(neutron.delete_security_group_rule, conn, r.resource_id)
            await inventory.mark_resource_deleted(None, service="neutron", resource_type="security_group_rule", resource_id=r.resource_id)
        except Exception as e:
            _logger.warning("k3s delete: SG rule %s delete failed: %s", r.resource_id, e)

    for sg_id in all_sg_ids:
        try:
            await asyncio.to_thread(neutron.delete_security_group_safe, conn, sg_id, project_id, cluster_id)
            await inventory.mark_resource_deleted(None, service="neutron", resource_type="security_group", resource_id=sg_id)
            _logger.info("k3s delete: SG %s deleted", sg_id)
        except Exception as e:
            _logger.warning("k3s delete: SG %s delete failed: %s", sg_id, e)

    # Step 7: Keystone app credentials
    msg = K3sProgressMessage(step=K3sProgressStep.DELETE_APP_CREDENTIAL, progress=95, message="App Credential 회수 중...")
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    for app_cred_id in all_app_cred_ids:
        try:
            await keystone.delete_app_credential(project_id, app_cred_id)
            await inventory.mark_resource_deleted(None, service="keystone", resource_type="app_credential", resource_id=app_cred_id)
            _logger.info("k3s delete: App credential %s deleted", app_cred_id)
        except Exception as e:
            _logger.warning("k3s delete: App credential %s delete failed: %s", app_cred_id, e)

    # Step 8: Record soft deletion
    msg = K3sProgressMessage(step=K3sProgressStep.DELETE_RECORD, progress=98, message="삭제 이력 기록 중...")
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=msg.step.value, message=msg.message
        )
    yield msg

    user_id = token_info.get("user_id") if isinstance(token_info, dict) else None
    await k3s_cluster.delete_cluster_record(project_id, cluster_id, user_id=user_id, reason="사용자 삭제 요청")
    if token_info is not None:
        await rec(token_info, conn, resource_type="k3s_cluster", action="delete", resource_id=cluster_id)

    final_msg = K3sProgressMessage(
        step=K3sProgressStep.COMPLETED,
        progress=100,
        message=f'클러스터 "{cluster_name}" 삭제 완료',
    )
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase=final_msg.step.value, message=final_msg.message
        )
    yield final_msg

async def execute_delete_cluster(
    project_id: str,
    cluster_id: str,
    payload: dict,
    operation_id: str | None = None,
) -> None:
    """Service-level cluster deletion runner used by durable job workers."""
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster or cluster.get("deleted_at"):
        return

    conn = await asyncio.to_thread(keystone.get_admin_connection_for_project, project_id)
    token_info = {
        "project_id": project_id,
        "user_id": payload.get("user_id", ""),
        "username": payload.get("username", ""),
    }
    try:
        async for _ in delete_cluster_progress(
            conn, project_id, cluster, token_info, operation_id=operation_id
        ):
            pass
    finally:
        await asyncio.to_thread(conn.close)
