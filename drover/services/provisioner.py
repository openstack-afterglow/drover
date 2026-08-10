"""k3s 에이전트/HA 서버 프로비저닝 — callback.py에서 호출하는 백그라운드 태스크."""

from __future__ import annotations

import asyncio
import logging
import random
import string

from drover.services import store as k3s_cluster

_logger = logging.getLogger(__name__)


def _rand_suffix(length: int = 5) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


# ---------------------------------------------------------------------------
# 에이전트 VM 프로비저닝 (단일 마스터 / HA 모두 공통)
# ---------------------------------------------------------------------------


async def provision_agents(project_id: str, cluster_id: str, server_ip: str, node_token: str) -> None:
    """에이전트 VM을 모두 생성하고 클러스터 상태를 ACTIVE로 전환한다."""
    from drover.config import get_settings
    from drover.services import cinder, keystone, nova
    from drover.services import cloudinit as k3s_cloudinit

    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        _logger.error("k3s agent provision: cluster %s not found", cluster_id)
        return

    agent_count = int(cluster.get("agent_count") or 0)
    if agent_count == 0:
        await k3s_cluster.update_cluster_status(project_id, cluster_id, "ACTIVE", "")
        return

    s = get_settings()
    resource_snapshot = cluster.get("resource_policy_snapshot") or {}
    from drover.services import plugins as k3s_plugins

    plugin_settings = k3s_plugins.with_resource_policy_snapshot(s, resource_snapshot)
    agent_flavor_id = cluster.get("agent_flavor_id") or ""
    network_id = cluster.get("network_id") or ""
    ssh_public_key = cluster.get("ssh_public_key") or None
    cluster_name = cluster.get("name") or cluster_id
    k3s_version = cluster.get("k3s_version") or ""
    os_type = cluster.get("os_type") or "ubuntu"
    image_id = (resource_snapshot.get("effective_agent_image") or {}).get("id") or cluster.get("server_image_id") or ""
    volume_availability_zone = (resource_snapshot.get("k3s.volume_availability_zone") or {}).get("id") or ""
    boot_volume_size = s.drover_boot_volume_size_gb
    sg_id = cluster.get("security_group_id") or None
    if not all((agent_flavor_id, network_id, k3s_version, image_id, volume_availability_zone)):
        _logger.error("k3s agent provision: creation-time resource snapshot is incomplete")
        await k3s_cluster.update_cluster_status(
            project_id, cluster_id, "ERROR", "생성 시점 리소스 스냅샷이 불완전합니다"
        )
        return

    try:
        conn = keystone.get_admin_connection_for_project(project_id)
    except Exception as e:
        _logger.error("k3s agent provision: cannot get OpenStack connection: %s", e)
        await k3s_cluster.update_cluster_status(project_id, cluster_id, "ERROR", f"OpenStack 연결 실패: {e}")
        return

    agent_vm_ids: list[str] = []
    failed_count = 0
    new_agent_entries: list[dict] = []

    for _i in range(agent_count):
        agent_name = f"{cluster_name}-{_rand_suffix()}"
        try:
            vol = await asyncio.to_thread(
                cinder.create_volume_from_image,
                conn,
                f"{agent_name}-boot",
                image_id,
                boot_volume_size,
                volume_availability_zone,
            )
            _agent_args = k3s_plugins.aggregate_agent_args(plugin_settings)

            if not _agent_args and cluster.get("occm_enabled"):
                _agent_args = ["--kubelet-arg=cloud-provider=external"]
            agent_userdata = k3s_cloudinit.generate_agent_userdata(
                cluster_name=cluster_name,
                k3s_version=k3s_version,
                server_ip=server_ip,
                node_token=node_token,
                ssh_public_key=ssh_public_key,
                primary_network_id=network_id,
                extra_agent_args=_agent_args,
                os_type=os_type,
            )
            vm = await asyncio.to_thread(
                nova.create_server,
                conn,
                agent_name,
                agent_flavor_id,
                network_id,
                vol.id,
                userdata=agent_userdata.data,
                metadata={"k3s_horse_generator_role": "k3s_agent", "k3s_horse_generator_cluster_id": cluster_id},
                delete_boot_volume_on_termination=True,
                security_groups=[sg_id] if sg_id else None,
                config_drive=agent_userdata.config_drive,
            )
            agent_vm_ids.append(vm.id)
            new_agent_entries.append({"vm_id": vm.id, "name": agent_name})
            _logger.info("k3s agent %s created: %s", agent_name, vm.id)
        except Exception as e:
            _logger.error("k3s agent %s creation failed: %s", agent_name, e)
            failed_count += 1

    if new_agent_entries:
        await k3s_cluster.add_agent_vms(cluster_id, new_agent_entries)
        from drover.services import nodegroup as k3s_nodegroup

        default_agent_id = await k3s_nodegroup.get_default_agent_nodegroup_id(cluster_id)
        if default_agent_id:
            await k3s_nodegroup.add_nodegroup_vms(default_agent_id, cluster_id, new_agent_entries)
    reason = f"에이전트 {failed_count}개 생성 실패" if failed_count else ""
    await k3s_cluster.update_cluster_status(project_id, cluster_id, "ACTIVE", reason, agent_vm_ids=agent_vm_ids)
    _logger.info(
        "k3s cluster %s ACTIVE: %d agents created, %d failed",
        cluster_id,
        len(agent_vm_ids),
        failed_count,
    )


# ---------------------------------------------------------------------------
# HA 서버#2/#3 부트스트랩
# ---------------------------------------------------------------------------


async def bootstrap_ha_servers(
    project_id: str,
    cluster_id: str,
    server_ip: str,  # server#1 내부 IP
    node_token: str,  # k3s node-token (server#1에서 수신)
    master_count: int,  # 3
    lb_pool_id: str,
    lb_fip_address: str,
) -> None:
    """server#2, server#3을 LB에 추가하고 HA join cloud-init으로 부트스트랩한다.

    The durable Drover worker executes this operation after claiming a bootstrap job.
    Each joining server reports through its one-time callback token.
    """
    from drover.config import get_settings
    from drover.services import (
        cinder,
        keystone,
        nova,
        octavia,
    )
    from drover.services import (
        cloudinit as k3s_cloudinit,
    )
    from drover.services import (
        plugins as k3s_plugins,
    )

    s = get_settings()
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        _logger.error("HA bootstrap: cluster %s not found", cluster_id)
        return

    resource_snapshot = cluster.get("resource_policy_snapshot") or {}
    plugin_settings = k3s_plugins.with_resource_policy_snapshot(s, resource_snapshot)
    cluster_name = cluster.get("name") or cluster_id
    k3s_version = cluster.get("k3s_version") or ""
    os_type = cluster.get("os_type") or "ubuntu"
    image_id = cluster.get("server_image_id") or ""
    boot_volume_size = s.drover_boot_volume_size_gb
    server_flavor_id = cluster.get("server_flavor_id") or ""
    network_id = cluster.get("network_id") or ""
    volume_availability_zone = (resource_snapshot.get("k3s.volume_availability_zone") or {}).get("id") or ""
    sg_id = cluster.get("security_group_id") or None
    key_name = cluster.get("key_name") or None
    callback_url = s.drover_callback_base_url.rstrip("/")
    if not all((k3s_version, image_id, server_flavor_id, network_id, volume_availability_zone)):
        _logger.error("HA bootstrap: creation-time resource snapshot is incomplete")
        return

    # HA 조인 URL: LB FIP 우선, 없으면 server#1 IP
    join_url = f"https://{lb_fip_address or server_ip}:6443"

    try:
        conn = keystone.get_admin_connection_for_project(project_id)
    except Exception as e:
        _logger.error("HA bootstrap: cannot get OpenStack connection: %s", e)
        return

    # server#1을 LB pool member로 추가 (server#1 IP는 이미 알려져 있음)
    try:
        subnets = await asyncio.to_thread(lambda: list(conn.network.subnets(network_id=network_id)))
        subnet_id = subnets[0].id if subnets else None
        if lb_pool_id and server_ip:
            await asyncio.to_thread(
                octavia.add_member,
                conn,
                lb_pool_id,
                server_ip,
                6443,
                subnet_id=subnet_id,
                name=f"{cluster_name}-server1",
            )
            _logger.info("HA: server#1 %s added to LB pool %s", server_ip, lb_pool_id)
    except Exception as e:
        _logger.warning("HA: failed to add server#1 to LB pool: %s", e)

    # server#2, server#3 생성
    cloud_conf = k3s_plugins.aggregate_cloud_conf(project_id, plugin_settings)
    extra_server_args = k3s_plugins.aggregate_server_args(plugin_settings)
    extra_write_files = k3s_plugins.aggregate_extra_write_files(project_id, cluster_name, plugin_settings)

    for idx in range(2, master_count + 1):
        server_suffix = _rand_suffix()
        server_vm_name = f"{cluster_name}-server{idx}-{server_suffix}"

        try:
            # HA 콜백 토큰 (server_index 포함 — callback.py가 분기 처리)
            ha_token = await k3s_cluster.create_ha_callback_token(project_id, cluster_id, idx)

            boot_vol = await asyncio.to_thread(
                cinder.create_volume_from_image,
                conn,
                f"{server_vm_name}-boot",
                image_id,
                boot_volume_size,
                volume_availability_zone,
            )

            userdata_result = k3s_cloudinit.generate_server_userdata(
                cluster_name=cluster_name,
                k3s_version=k3s_version,
                callback_url=callback_url,
                callback_token=ha_token,
                primary_network_id=network_id,
                cloud_conf=cloud_conf,
                extra_server_args=extra_server_args,
                extra_write_files=extra_write_files,
                extra_tls_sans=[lb_fip_address] if lb_fip_address else [],
                needs_external_cloud_provider=k3s_plugins.needs_external_cloud_provider(s),
                os_type=os_type,
                server_node_name=server_vm_name,
                cluster_init=False,
                join_url=join_url,
                ha_node_token=node_token,
            )

            vm = await asyncio.to_thread(
                nova.create_server,
                conn,
                server_vm_name,
                server_flavor_id,
                network_id,
                boot_vol.id,
                userdata=userdata_result.data,
                key_name=key_name,
                metadata={
                    "k3s_horse_generator_role": "k3s_server",
                    "k3s_horse_generator_cluster_id": cluster_id,
                    "k3s_horse_generator_cluster_name": cluster_name,
                },
                delete_boot_volume_on_termination=True,
                security_groups=[sg_id] if sg_id else None,
                config_drive=userdata_result.config_drive,
            )
            _logger.info("HA: server#%d VM %s created: %s", idx, server_vm_name, vm.id)

        except Exception as e:
            _logger.error("HA: server#%d creation failed: %s", idx, e)
            # 부분 실패 시 계속 진행 (embedded etcd는 quorum 없이도 단독 동작 가능)

    _logger.info(
        "HA bootstrap done for cluster %s — waiting for servers to join via callback",
        cluster_id,
    )
