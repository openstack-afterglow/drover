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


async def _resolve_ha_join_endpoint(
    conn,
    lb_id: str,
    lb_fip_address: str,
    server_ip: str,
) -> tuple[str, list[str]]:
    from drover.services import octavia

    lb_vip_address = ""
    if lb_id:
        try:
            lb = await asyncio.to_thread(octavia.get_load_balancer, conn, lb_id)
            lb_vip_address = lb.get("vip_address") or ""
        except Exception as e:
            _logger.warning("HA bootstrap: failed to resolve private LB VIP: %s", e)
    join_address = lb_vip_address or lb_fip_address or server_ip
    tls_sans = []
    for address in (lb_vip_address, lb_fip_address):
        if address and address not in tls_sans:
            tls_sans.append(address)
    return f"https://{join_address}:6443", tls_sans


# ---------------------------------------------------------------------------
# 에이전트 VM 프로비저닝 (단일 마스터 / HA 모두 공통)
# ---------------------------------------------------------------------------


async def provision_agents(project_id: str, cluster_id: str, server_ip: str, node_token: str) -> None:
    """에이전트 VM을 모두 생성하고 클러스터 상태를 ACTIVE로 전환한다."""
    from drover.config import get_settings
    from drover.services import cinder, inventory, keystone, nova
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
            vol_metadata = inventory.build_drover_metadata(cluster_id, None, "volume")
            vol = await asyncio.to_thread(
                cinder.create_volume_from_image,
                conn,
                f"{agent_name}-boot",
                image_id,
                boot_volume_size,
                volume_availability_zone,
                metadata=vol_metadata,
            )
            await inventory.record_resource(
                None, cluster_id=cluster_id, service="cinder", resource_type="volume", resource_id=vol.id, name=f"{agent_name}-boot"
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
            agent_vm_metadata = inventory.build_drover_metadata(cluster_id, None, "server")
            agent_vm_metadata.update({"k3s_horse_generator_role": "k3s_agent", "k3s_horse_generator_cluster_id": cluster_id})
            vm = await asyncio.to_thread(
                nova.create_server,
                conn,
                agent_name,
                agent_flavor_id,
                network_id,
                vol.id,
                userdata=agent_userdata.data,
                metadata=agent_vm_metadata,
                delete_boot_volume_on_termination=True,
                security_groups=[sg_id] if sg_id else None,
                config_drive=agent_userdata.config_drive,
            )
            await inventory.record_resource(
                None, cluster_id=cluster_id, service="nova", resource_type="server", resource_id=vm.id, name=agent_name
            )
            agent_vm_ids.append(vm.id)
            new_agent_entries.append({"vm_id": vm.id, "name": agent_name})
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
    operation_id: str | None = None,
) -> None:
    """server#2, server#3을 LB에 추가하고 HA join cloud-init으로 부트스트랩한다.

    The durable Drover worker executes this operation after claiming a bootstrap job.
    Each joining server reports through its one-time callback token.
    """
    from drover.config import get_settings
    from drover.services import (
        cinder,
        inventory,
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

    try:
        conn = keystone.get_admin_connection_for_project(project_id)
    except Exception as e:
        _logger.error("HA bootstrap: cannot get OpenStack connection: %s", e)
        return

    join_url, ha_extra_tls_sans = await _resolve_ha_join_endpoint(
        conn,
        cluster.get("api_lb_id") or "",
        lb_fip_address,
        server_ip,
    )

    try:
        subnets = await asyncio.to_thread(lambda: list(conn.network.subnets(network_id=network_id)))
        subnet_id = subnets[0].id if subnets else None
        member = await asyncio.to_thread(
            octavia.add_member,
            conn,
            lb_pool_id,
            server_ip,
            6443,
            subnet_id=subnet_id,
            name=f"{cluster_name}-server-1",
        )
        await inventory.record_resource(
            None,
            cluster_id=cluster_id,
            service="octavia",
            resource_type="member",
            resource_id=member["id"],
            operation_id=operation_id,
            name=f"{cluster_name}-server-1",
            metadata={"pool_id": lb_pool_id, "role": "ha_server_member", "server_index": 1},
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
            await inventory.record_resource(
                None,
                cluster_id=cluster_id,
                service="cinder",
                resource_type="volume",
                resource_id=boot_vol.id,
                operation_id=operation_id,
                name=f"{server_vm_name}-boot",
                metadata={"role": "ha_server_boot_volume", "server_index": idx},
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
                extra_tls_sans=ha_extra_tls_sans,
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
            await inventory.record_resource(
                None,
                cluster_id=cluster_id,
                service="nova",
                resource_type="server",
                resource_id=vm.id,
                operation_id=operation_id,
                name=server_vm_name,
                metadata={"role": "ha_server", "server_index": idx},
            )

        except Exception as e:
            _logger.error("HA: server#%d creation failed: %s", idx, e)
            # 부분 실패 시 계속 진행 (embedded etcd는 quorum 없이도 단독 동작 가능)

    _logger.info(
        "HA bootstrap done for cluster %s — waiting for servers to join via callback",
        cluster_id,
    )
async def create_cluster_job(
    project_id: str,
    cluster_id: str,
    payload: dict,
    operation_id: str | None = None,
) -> None:
    """Execute OpenStack resource creation for cluster create job."""
    from drover.config import get_settings
    from drover.models.schemas import K3sProgressStep
    from drover.services import cinder, inventory, keystone, neutron, nova, octavia, operations
    from drover.services import cloudinit as k3s_cloudinit
    from drover.services import plugins as k3s_plugins

    s = get_settings()
    try:
        conn = keystone.get_admin_connection_for_project(project_id)
    except Exception as e:
        _logger.error("k3s cluster create job: cannot get OpenStack connection for project %s: %s", project_id, e)
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.FAILED.value,
                message=f"OpenStack 연결 실패: {e}",
                payload_json={"step": K3sProgressStep.FAILED.value, "progress": 0, "error": str(e)},
            )
            await operations.update_operation_status(None, operation_id, "FAILED", error=str(e))
        await k3s_cluster.update_cluster_status(project_id, cluster_id, "ERROR", f"OpenStack 연결 실패: {e}")
        raise

    name = payload.get("name", "")
    master_count = int(payload.get("master_count") or 1)
    allowed_cidrs = payload.get("allowed_cidrs") or ["0.0.0.0/0"]
    policy_snapshot = payload.get("resource_policy_snapshot") or {}
    server_image_id = payload.get("server_image_id") or ""
    server_flavor_id = payload.get("server_flavor_id") or ""
    boot_volume_size = s.drover_boot_volume_size_gb
    volume_availability_zone = (policy_snapshot.get("k3s.volume_availability_zone") or {}).get("id") or ""
    key_name = payload.get("key_name") or None
    network_id = payload.get("network_id") or ""
    k3s_version = payload.get("k3s_version") or ""
    os_type = payload.get("os_type") or "ubuntu"
    plugin_settings = k3s_plugins.with_resource_policy_snapshot(s, policy_snapshot)

    sg_id: str | None = None
    boot_volume_id: str | None = None
    server_vm_id: str | None = None
    app_credential_id: str | None = None
    ha_lb_id: str | None = None
    ha_lb_pool_id: str | None = None
    ha_fip_id: str | None = None
    ha_fip_address: str | None = None

    try:
        # Step 1: Security Group
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.SECURITY_GROUP.value,
                message="k3s 보안 그룹 생성 중...",
                payload_json={"step": K3sProgressStep.SECURITY_GROUP.value, "progress": 5},
            )

        sg_name = f"k3s-{name}-{cluster_id[:8]}"
        sg_tags = inventory.build_drover_tags(cluster_id, operation_id, "security_group")
        sg = await asyncio.to_thread(
            neutron.create_security_group, conn, sg_name, f"k3s cluster {name} security group", tags=sg_tags
        )
        sg_id = sg["id"]
        await inventory.record_resource(
            None, cluster_id=cluster_id, service="neutron", resource_type="security_group", resource_id=sg_id, operation_id=operation_id, name=sg_name
        )

        mgmt_cidrs = allowed_cidrs or ["0.0.0.0/0"]
        rules = []
        for cidr in mgmt_cidrs:
            rules.append(dict(direction="ingress", protocol="tcp", port_range_min=22, port_range_max=22, remote_ip_prefix=cidr))
            rules.append(dict(direction="ingress", protocol="tcp", port_range_min=6443, port_range_max=6443, remote_ip_prefix=cidr))
        rules += [
            dict(direction="ingress", protocol="tcp", port_range_min=10250, port_range_max=10250, remote_group_id=sg_id),
            dict(direction="ingress", protocol="udp", port_range_min=8472, port_range_max=8472, remote_group_id=sg_id),
            dict(direction="ingress", protocol="udp", port_range_min=51820, port_range_max=51820, remote_group_id=sg_id),
            dict(direction="ingress", protocol="tcp", port_range_min=80, port_range_max=80, remote_ip_prefix="0.0.0.0/0"),
            dict(direction="ingress", protocol="tcp", port_range_min=443, port_range_max=443, remote_ip_prefix="0.0.0.0/0"),
            dict(direction="ingress", protocol="tcp", port_range_min=30000, port_range_max=32767, remote_ip_prefix="0.0.0.0/0"),
        ]
        for rule_kwargs in rules:
            rule = await asyncio.to_thread(neutron.create_security_group_rule, conn, sg_id, **rule_kwargs)
            rule_id = rule.get("id") if isinstance(rule, dict) else getattr(rule, "id", None)
            if rule_id:
                await inventory.record_resource(
                    None, cluster_id=cluster_id, service="neutron", resource_type="security_group_rule", resource_id=rule_id, operation_id=operation_id
                )

        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.SECURITY_GROUP.value,
                message="보안 그룹 생성 완료",
                payload_json={"step": K3sProgressStep.SECURITY_GROUP.value, "progress": 10},
            )

        extra_tls_sans: list[str] = []

        # Step 1-B: HA LB + FIP if master_count >= 3
        if master_count >= 3:
            lb_subnet_id = (policy_snapshot.get("k3s.lb_subnet") or {}).get("id")
            if not lb_subnet_id:
                raise RuntimeError("K3s API load-balancer subnet policy is required for HA clusters")
            lb_tags = inventory.build_drover_tags(cluster_id, operation_id, "load_balancer")
            ha_lb = await asyncio.to_thread(
                octavia.create_load_balancer,
                conn,
                f"k3s-ha-{name}-{cluster_id[:8]}",
                lb_subnet_id,
                vip_network_id=(policy_snapshot.get("k3s.api_lb_vip_network") or {}).get("id"),
                tags=lb_tags,
            )
            ha_lb_id = ha_lb["id"]
            await inventory.record_resource(
                None, cluster_id=cluster_id, service="octavia", resource_type="load_balancer", resource_id=ha_lb_id, operation_id=operation_id, name=ha_lb.get("name")
            )
            ha_lb_vip_address = ha_lb.get("vip_address") or ""
            if ha_lb_vip_address:
                extra_tls_sans.append(ha_lb_vip_address)
            await asyncio.to_thread(octavia.wait_for_load_balancer, conn, ha_lb_id)
            listener = await asyncio.to_thread(
                octavia.create_listener, conn, ha_lb_id, "TCP", 6443, name=f"k3s-ha-{name}-6443"
            )
            await inventory.record_resource(
                None, cluster_id=cluster_id, service="octavia", resource_type="listener", resource_id=listener["id"], operation_id=operation_id, name=listener.get("name")
            )
            ha_lb_pool_id_raw = await asyncio.to_thread(
                octavia.create_pool,
                conn,
                ha_lb_id,
                "TCP",
                name=f"k3s-ha-{name}-pool",
                listener_id=listener["id"],
            )
            ha_lb_pool_id = ha_lb_pool_id_raw["id"]
            await inventory.record_resource(
                None, cluster_id=cluster_id, service="octavia", resource_type="pool", resource_id=ha_lb_pool_id, operation_id=operation_id, name=ha_lb_pool_id_raw.get("name")
            )

            _fip_net = (policy_snapshot.get("k3s.api_lb_floating_network") or {}).get("id", "")
            if _fip_net:
                _lb_vip_port = ha_lb.get("vip_port_id")
                fip_tags = inventory.build_drover_tags(cluster_id, operation_id, "floating_ip")
                _fip = await asyncio.to_thread(
                    neutron.create_floating_ip,
                    conn,
                    floating_network_id=_fip_net,
                    port_id=_lb_vip_port,
                    tags=fip_tags,
                )
                ha_fip_id = _fip.id if hasattr(_fip, "id") else _fip["id"]
                ha_fip_address = (
                    _fip.floating_ip_address
                    if hasattr(_fip, "floating_ip_address")
                    else _fip["floating_ip_address"]
                )
                await inventory.record_resource(
                    None, cluster_id=cluster_id, service="neutron", resource_type="floating_ip", resource_id=ha_fip_id, operation_id=operation_id, name=ha_fip_address
                )
                extra_tls_sans.append(ha_fip_address)

            if operation_id:
                await operations.append_operation_event(
                    None,
                    operation_id,
                    phase=K3sProgressStep.SERVER_HA_BOOTSTRAP.value,
                    message=f"HA API LB 준비 완료 (FIP: {ha_fip_address})" if ha_fip_address else "HA API LB 준비 완료",
                    payload_json={"step": K3sProgressStep.SERVER_HA_BOOTSTRAP.value, "progress": 18},
                )

        # Step 2: Boot volume
        server_suffix = _rand_suffix()
        server_vm_name = f"{name}-{server_suffix}"
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.SERVER_VOLUME.value,
                message="서버 노드 부트 볼륨 생성 중...",
                payload_json={"step": K3sProgressStep.SERVER_VOLUME.value, "progress": 28},
            )

        vol_metadata = inventory.build_drover_metadata(cluster_id, operation_id, "volume")
        boot_vol = await asyncio.to_thread(
            cinder.create_volume_from_image,
            conn,
            f"{server_vm_name}-boot",
            server_image_id,
            boot_volume_size,
            volume_availability_zone,
            metadata=vol_metadata,
        )
        boot_volume_id = boot_vol.id
        await inventory.record_resource(
            None, cluster_id=cluster_id, service="cinder", resource_type="volume", resource_id=boot_volume_id, operation_id=operation_id, name=f"{server_vm_name}-boot"
        )
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.SERVER_VOLUME.value,
                message="서버 부트 볼륨 생성 완료",
                payload_json={"step": K3sProgressStep.SERVER_VOLUME.value, "progress": 35},
            )

        # Step 3: Cloud-init & Userdata
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.SERVER_CREATING.value,
                message="서버 VM cloud-init 생성 중...",
                payload_json={"step": K3sProgressStep.SERVER_CREATING.value, "progress": 40},
            )

        ssh_public_key = ""
        if key_name:
            try:
                kp = await asyncio.to_thread(conn.compute.find_keypair, key_name)
                if kp:
                    ssh_public_key = kp.public_key or ""
            except Exception:
                pass

        callback_token = await k3s_cluster.create_callback_token(project_id, cluster_id)
        callback_url = s.drover_callback_base_url.rstrip("/")

        _internal_network_name = ""
        try:
            _net_obj = await asyncio.to_thread(lambda: conn.network.get_network(network_id))
            _internal_network_name = _net_obj.name or ""
        except Exception:
            pass

        active_plugins = k3s_plugins.get_active_plugin_names(plugin_settings)
        active_plugins.get("occm", False)

        from drover.services import keystone as _keystone
        app_cred: dict | None = None
        needs_app_cred = (
            active_plugins.get("occm", False)
            or active_plugins.get("manila_csi", False)
            or active_plugins.get("octavia_ingress", False)
            or active_plugins.get("barbican_kms", False)
        )
        if needs_app_cred:
            if operation_id:
                await operations.append_operation_event(
                    None,
                    operation_id,
                    phase=K3sProgressStep.SERVER_CREATING.value,
                    message="App Credential 발급 중...",
                    payload_json={"step": K3sProgressStep.SERVER_CREATING.value, "progress": 38},
                )
            app_cred = await _keystone.create_app_credential_for_cluster(project_id, name)
            app_credential_id = app_cred["id"]
            await inventory.record_resource(
                None, cluster_id=cluster_id, service="keystone", resource_type="app_credential", resource_id=app_credential_id, operation_id=operation_id, name=name
            )

        cloud_conf = k3s_plugins.aggregate_cloud_conf(
            project_id, plugin_settings, internal_network_name=_internal_network_name, app_credential=app_cred
        )

        kek_id: str | None = None
        if active_plugins.get("barbican_kms", False):
            if operation_id:
                await operations.append_operation_event(
                    None,
                    operation_id,
                    phase=K3sProgressStep.SERVER_CREATING.value,
                    message="KEK (Barbican) 조회/발급 중...",
                    payload_json={"step": K3sProgressStep.SERVER_CREATING.value, "progress": 39},
                )
            from drover.services import barbican as _barbican
            kek_id = await _barbican.ensure_project_kek(project_id)

        manifest_kwargs: dict = {"app_credential": app_cred}
        if active_plugins.get("octavia_ingress", False):
            subnets = await asyncio.to_thread(lambda: list(conn.network.subnets(network_id=network_id)))
            if not subnets:
                raise RuntimeError(
                    f"네트워크 {network_id}에 subnet이 없습니다. Octavia Ingress를 위한 subnet 도출 실패."
                )
            floating_network_id = (policy_snapshot.get("k3s.octavia_ingress_floating_network") or {}).get("id")
            if not floating_network_id:
                raise RuntimeError("Octavia ingress floating network policy is required when the plugin is enabled")
            manifest_kwargs["subnet_id"] = subnets[0].id
            manifest_kwargs["floating_network_id"] = floating_network_id
        plugin_manifests, manifest_failures = k3s_plugins.aggregate_manifests(
            name, project_id, plugin_settings, **manifest_kwargs
        )
        if manifest_failures:
            err_msg = f"플러그인 매니페스트 생성 실패: {', '.join(manifest_failures)}"
            raise RuntimeError(err_msg)

        extra_server_args = k3s_plugins.aggregate_server_args(plugin_settings)
        extra_write_files = k3s_plugins.aggregate_extra_write_files(
            project_id, name, plugin_settings, app_credential=app_cred, kek_id=kek_id
        )

        userdata_result = k3s_cloudinit.generate_server_userdata(
            cluster_name=name,
            k3s_version=k3s_version,
            callback_url=callback_url,
            callback_token=callback_token,
            cloud_conf=cloud_conf,
            primary_network_id=network_id,
            plugin_manifests=plugin_manifests,
            extra_server_args=extra_server_args,
            extra_write_files=extra_write_files,
            extra_tls_sans=extra_tls_sans,
            needs_external_cloud_provider=k3s_plugins.needs_external_cloud_provider(plugin_settings),
            os_type=os_type,
            server_node_name=server_vm_name,
            barbican_kms_enabled=any(p.name == "barbican_kms" for p in k3s_plugins.get_active_plugins(s)),
            cluster_init=master_count >= 3,
        )

        # Step 4: Nova server VM creation
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.SERVER_CREATING.value,
                message="서버 VM 생성 중 (완료까지 수 분 소요)...",
                payload_json={"step": K3sProgressStep.SERVER_CREATING.value, "progress": 48},
            )

        server_vm_metadata = inventory.build_drover_metadata(cluster_id, operation_id, "server")
        server_vm_metadata.update({
            "k3s_horse_generator_role": "k3s_server",
            "k3s_horse_generator_cluster_id": cluster_id,
            "k3s_horse_generator_cluster_name": name,
        })
        server_vm = await asyncio.to_thread(
            nova.create_server,
            conn,
            server_vm_name,
            server_flavor_id,
            network_id,
            boot_volume_id,
            userdata=userdata_result.data,
            key_name=key_name,
            metadata=server_vm_metadata,
            delete_boot_volume_on_termination=True,
            security_groups=[sg_id],
            config_drive=userdata_result.config_drive,
        )
        server_vm_id = server_vm.id
        await inventory.record_resource(
            None, cluster_id=cluster_id, service="nova", resource_type="server", resource_id=server_vm_id, operation_id=operation_id, name=server_vm_name
        )

        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.SERVER_CREATING.value,
                message=f"서버 VM 생성 완료: {server_vm_id}",
                payload_json={"step": K3sProgressStep.SERVER_CREATING.value, "progress": 60},
            )

        # Update cluster record in DB with created details
        await k3s_cluster.update_cluster_status(
            project_id,
            cluster_id,
            "CREATING",
            server_vm_id=server_vm_id,
            security_group_id=sg_id,
            ssh_public_key=ssh_public_key,
            api_lb_id=ha_lb_id or "",
            api_lb_pool_id=ha_lb_pool_id or "",
            api_fip_id=ha_fip_id or "",
            api_fip_address=ha_fip_address or "",
            app_credential_id=app_credential_id or "",
        )

        # Step 5: Transition operation to WAITING_CALLBACK
        if operation_id:
            await operations.set_operation_waiting_callback(
                operation_id,
                message="k3s 초기화 대기 중 (서버 VM에서 k3s 설치 중)...",
            )

    except Exception as e:
        _logger.error("k3s cluster creation job for cluster %s failed: %s", cluster_id, e, exc_info=True)
        if operation_id:
            await operations.append_operation_event(
                None,
                operation_id,
                phase=K3sProgressStep.FAILED.value,
                message=f"클러스터 생성 실패: {e}",
                payload_json={"step": K3sProgressStep.FAILED.value, "progress": 0, "error": str(e)},
            )
            await operations.update_operation_status(None, operation_id, "FAILED", error=str(e))
        await k3s_cluster.update_cluster_status(project_id, cluster_id, "ERROR", f"클러스터 생성 실패: {e}")
        raise
