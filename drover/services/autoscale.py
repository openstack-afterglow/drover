"""k3s Stampede 노드그룹 VM 프로비저닝 서비스.

clusters.py의 _scale_agents 로직을 노드그룹 단위로 일반화·추출.
Stampede Reconciler(k3s_stampede.py)와 수동 스케일 핸들러(clusters.py) 양쪽에서 호출.
"""

import asyncio
import hashlib
import logging
import random
import string

from drover.config import get_settings
from drover.utils.ssh_keys import normalize_ssh_public_key

_logger = logging.getLogger(__name__)


class ProvisioningInProgress(RuntimeError):
    """A claimed Afterglow intent must be retried with its existing key."""


def _rand_suffix(n: int = 5) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def _stampede_node_key(key_prefix: str, index: int) -> str:
    return f"{key_prefix}-node-{index}"


def _stampede_node_name(cluster_name: str, provisioning_key: str) -> str:
    digest = hashlib.sha256(provisioning_key.encode("utf-8")).hexdigest()[:12]
    return f"{cluster_name}-stampede-{digest}"


async def _find_native_resources(conn, cluster_id: str, nodegroup_id: str, name: str, key: str):
    """Recover a prior attempt, including resources created before DB persistence."""
    from drover.services import inventory, nodegroup

    group = await nodegroup.get_nodegroup(cluster_id, nodegroup_id)
    recorded = [v for v in (group or {}).get("vms", []) if v.get("name") == name]
    resources = await inventory.list_managed_resources(cluster_id=cluster_id)
    server_ids = {v["vm_id"] for v in recorded}
    volume_ids = set()
    for resource in resources:
        if resource.service == "nova" and resource.resource_type == "server" and resource.name == name:
            server_ids.add(resource.resource_id)
        if resource.service == "cinder" and resource.resource_type == "volume" and resource.name == f"{name}-boot":
            volume_ids.add(resource.resource_id)

    def recover():
        servers = {}
        volumes = {}
        for resource_id in server_ids:
            server = conn.compute.find_server(resource_id, ignore_missing=True)
            if server is None:
                raise RuntimeError(f"Recorded provisioning server {resource_id} is missing")
            servers[server.id] = server
        for server in conn.compute.servers(details=True, name=name):
            meta = server.metadata or {}
            if server.name == name and meta.get("drover.provisioning_idempotency_key") == key:
                if meta.get("drover.cluster_id") != cluster_id:
                    raise RuntimeError("Provisioning server ownership mismatch")
                servers[server.id] = server
        for resource_id in volume_ids:
            volume = conn.block_storage.find_volume(resource_id, ignore_missing=True)
            if volume is None:
                raise RuntimeError(f"Recorded provisioning volume {resource_id} is missing")
            volumes[volume.id] = volume
        for volume in conn.block_storage.volumes(details=True, name=f"{name}-boot"):
            meta = volume.metadata or {}
            if volume.name == f"{name}-boot" and meta.get("drover.provisioning_idempotency_key") == key:
                if meta.get("drover.cluster_id") != cluster_id:
                    raise RuntimeError("Provisioning volume ownership mismatch")
                volumes[volume.id] = volume
        if len(servers) > 1 or len(volumes) > 1:
            raise RuntimeError("Ambiguous provisioning resources; refusing to create duplicates")
        return next(iter(servers.values()), None), next(iter(volumes.values()), None)

    server, volume = await asyncio.to_thread(recover)
    return server, volume, {v["vm_id"] for v in recorded}


async def provision_nodegroup_vms(
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
    add_count: int,
    *,
    flavor_id: str,
    image_id: str | None = None,
    labels: dict | None = None,
    taints: list | None = None,
    provisioning_key_prefix: str | None = None,
    gpu_required: bool = False,
    inspect_flavor_gpu: bool = False,
) -> list[dict]:
    """노드그룹에 agent VM을 add_count개 생성해 k3s 클러스터에 join시킨다.

    labels/taints는 cloud-init extra_agent_args로 주입.
    생성된 VM 목록 {vm_id, name}을 반환한다 (실패한 것은 포함하지 않음).
    """
    from drover.services import cinder, inventory, keystone, nova
    from drover.services import cloudinit as k3s_cloudinit
    from drover.services import nodegroup as k3s_nodegroup
    from drover.services import plugins as k3s_plugins
    from drover.services import store as k3s_db

    s = get_settings()

    # 클러스터 기본 정보 조회 (admin: project_id 필터 없음)
    cluster = await k3s_db.get_cluster_admin(cluster_id)
    if not cluster:
        _logger.error("provision_nodegroup_vms: cluster %s 없음", cluster_id)
        return []

    node_token = await k3s_db.get_cluster_node_token(project_id, cluster_id)
    server_ip = cluster.get("server_ip") or ""
    cluster_name = cluster.get("name") or cluster_id
    resource_snapshot = cluster.get("resource_policy_snapshot") or {}
    k3s_version = cluster.get("k3s_version") or ""
    os_type = cluster.get("os_type") or "ubuntu"
    network_id = cluster.get("network_id") or ""
    ssh_public_key = cluster.get("ssh_public_key") or None
    if cluster.get("key_name") and not ssh_public_key:
        raise RuntimeError("SSH public key snapshot is missing for the requested keypair")
    if ssh_public_key:
        ssh_public_key = normalize_ssh_public_key(ssh_public_key)
    sg_id = cluster.get("security_group_id") or None
    boot_volume_size = s.drover_boot_volume_size_gb
    volume_availability_zone = (resource_snapshot.get("k3s.volume_availability_zone") or {}).get("id") or ""

    # Explicit nodegroup image wins; otherwise use the cluster's immutable
    # effective image instead of current global policy/settings.
    if not image_id:
        image_id = (
            (resource_snapshot.get("effective_agent_image") or {}).get("id") or cluster.get("server_image_id") or ""
        )
    if not all((k3s_version, network_id, image_id, volume_availability_zone)):
        _logger.error("provision_nodegroup_vms: creation-time resource snapshot is incomplete")
        return []

    remote_provisioning = provisioning_key_prefix is not None and bool(
        getattr(s, "drover_afterglow_provisioning_url", "")
    )
    conn = None
    if not remote_provisioning:
        conn = await keystone.get_project_manager_connection(project_id)
    try:
        if (provisioning_key_prefix is None or inspect_flavor_gpu) and add_count > 0 and not gpu_required:
            # Manual nodegroups use the same explicitly selected Nova flavor.
            from drover.services import gpu
            from drover.services.stampede import _flavor_gpu_count

            if conn is None:
                conn = await keystone.get_project_manager_connection(project_id)
            flavors = await asyncio.to_thread(nova.list_flavors, conn)
            selected = next((flavor for flavor in flavors if flavor.id == flavor_id), None)
            if selected is None:
                raise RuntimeError("Selected nodegroup flavor is unavailable")
            gpu_required = _flavor_gpu_count(selected.extra_specs or {}) > 0
            if gpu_required:
                await gpu.ensure_device_plugin(cluster_id)
        # extra_agent_args 구성 (플러그인 + labels/taints + nodegroup 식별 라벨)
        _agent_args = k3s_plugins.aggregate_agent_args(s)
        if not _agent_args and cluster.get("occm_enabled"):
            _agent_args = ["--kubelet-arg=cloud-provider=external"]

        # 노드그룹 labels → --node-label
        for k, v in (labels or {}).items():
            _agent_args.append(f"--node-label={k}={v}")

        # nodegroup 식별 라벨 (Stampede 내부 추적용)
        _agent_args.append(f"--node-label=afterglow.io/nodegroup={nodegroup_id}")
        _agent_args.append("--node-label=afterglow.io/stampede=true")

        # 노드그룹 taints → --node-taint
        for taint in taints or []:
            # taint 형식: {"key": "k", "value": "v", "effect": "NoSchedule"}
            # 또는 단순 문자열 "k=v:Effect"
            if isinstance(taint, dict):
                key = taint.get("key", "")
                value = taint.get("value", "")
                effect = taint.get("effect", "NoSchedule")
                if value:
                    _agent_args.append(f"--node-taint={key}={value}:{effect}")
                else:
                    _agent_args.append(f"--node-taint={key}:{effect}")
            elif isinstance(taint, str):
                _agent_args.append(f"--node-taint={taint}")
        new_entries: list[dict] = []
        for _i in range(add_count):
            provisioning_key = (
                _stampede_node_key(provisioning_key_prefix, _i) if provisioning_key_prefix is not None else None
            )
            agent_name = (
                _stampede_node_name(cluster_name, provisioning_key)
                if provisioning_key is not None
                else f"{cluster_name}-{_rand_suffix()}"
            )
            try:
                agent_metadata = inventory.build_drover_metadata(cluster_id, None, "server")
                agent_metadata.update(
                    {
                        "k3s_horse_generator_role": "k3s_agent",
                        "k3s_horse_generator_cluster_id": cluster_id,
                        "k3s_horse_generator_nodegroup_id": nodegroup_id,
                    }
                )
                if provisioning_key is not None:
                    agent_metadata["drover.provisioning_idempotency_key"] = provisioning_key
                if remote_provisioning:
                    from drover.services import afterglow as afterglow_service

                    intent = await afterglow_service.create_provisioning_intent(
                        idempotency_key=provisioning_key,
                        project_id=project_id,
                        cluster_id=cluster_id,
                        nodegroup_id=nodegroup_id,
                        name=agent_name,
                        flavor_id=flavor_id,
                        image_id=image_id,
                        network_id=network_id,
                        boot_volume_size_gb=boot_volume_size,
                        volume_availability_zone=volume_availability_zone,
                        security_group_id=sg_id,
                        metadata=agent_metadata,
                        config_drive=os_type == "fcos",
                        settings=s,
                    )
                    state = intent.get("state")
                    if state == "succeeded":
                        result = intent
                    elif state in {"pending", "submitting"}:
                        userdata = k3s_cloudinit.generate_agent_userdata(
                            cluster_name=cluster_name,
                            k3s_version=k3s_version,
                            server_ip=server_ip,
                            node_token=node_token or "",
                            primary_network_id=network_id,
                            ssh_public_key=ssh_public_key,
                            extra_agent_args=_agent_args,
                            os_type=os_type,
                            gpu_required=gpu_required,
                        )
                        try:
                            result = await afterglow_service.submit_provisioning_intent(
                                provisioning_key,
                                userdata.data,
                                settings=s,
                            )
                        except afterglow_service.ProvisioningRemoteError as exc:
                            if exc.status_code == 409 and exc.state == "submitting" and exc.no_duplicate:
                                raise ProvisioningInProgress(provisioning_key) from exc
                            raise
                    else:
                        _logger.warning(
                            "stampede: nodegroup %s — provisioning intent %s is %s; no local VM",
                            nodegroup_id,
                            provisioning_key,
                            state or "unknown",
                        )
                        continue
                    if result.get("state") != "succeeded" or not result.get("server_id") or not result.get("volume_id"):
                        _logger.warning(
                            "stampede: nodegroup %s — provisioning intent %s did not succeed; no local VM",
                            nodegroup_id,
                            provisioning_key,
                        )
                        continue
                    await inventory.record_resource(
                        None,
                        cluster_id=cluster_id,
                        service="cinder",
                        resource_type="volume",
                        resource_id=str(result["volume_id"]),
                        name=f"{agent_name}-boot",
                    )
                    await inventory.record_resource(
                        None,
                        cluster_id=cluster_id,
                        service="nova",
                        resource_type="server",
                        resource_id=str(result["server_id"]),
                        name=agent_name,
                    )
                    entry = {"vm_id": str(result["server_id"]), "name": agent_name}
                    group = await k3s_nodegroup.get_nodegroup(cluster_id, nodegroup_id)
                    if entry["vm_id"] not in {v["vm_id"] for v in (group or {}).get("vms", [])}:
                        await k3s_nodegroup.add_nodegroup_vms(nodegroup_id, cluster_id, [entry])
                    new_entries.append(entry)
                    _logger.info("stampede: nodegroup %s — agent %s 생성됨", nodegroup_id, agent_name)
                    continue

                existing_vm = None
                vol = None
                recorded_ids = set()
                if provisioning_key is not None:
                    existing_vm, vol, recorded_ids = await _find_native_resources(
                        conn, cluster_id, nodegroup_id, agent_name, provisioning_key
                    )
                if existing_vm is not None:
                    entry = {"vm_id": existing_vm.id, "name": agent_name}
                    if existing_vm.id not in recorded_ids:
                        await k3s_nodegroup.add_nodegroup_vms(nodegroup_id, cluster_id, [entry])
                    await inventory.record_resource(
                        None, cluster_id=cluster_id, service="nova", resource_type="server",
                        resource_id=existing_vm.id, name=agent_name, metadata=agent_metadata,
                    )
                    if vol is not None:
                        await inventory.record_resource(
                            None, cluster_id=cluster_id, service="cinder", resource_type="volume",
                            resource_id=vol.id, name=f"{agent_name}-boot", metadata=vol.metadata or {},
                        )
                    if str(existing_vm.status).upper() != "ACTIVE":
                        await asyncio.to_thread(conn.compute.wait_for_server, existing_vm, status="ACTIVE", wait=600)
                    new_entries.append(entry)
                    continue
                vol_metadata = inventory.build_drover_metadata(cluster_id, None, "volume")
                vol_metadata["k3s_horse_generator_nodegroup_id"] = nodegroup_id
                if provisioning_key is not None:
                    vol_metadata["drover.provisioning_idempotency_key"] = provisioning_key
                if vol is None:
                    vol = await asyncio.to_thread(
                        cinder.create_volume_from_image,
                        conn,
                        f"{agent_name}-boot",
                        image_id,
                        boot_volume_size,
                        volume_availability_zone,
                        metadata=vol_metadata,
                    )
                elif str(vol.status).lower() != "available":
                    vol = await asyncio.to_thread(cinder.wait_volume_available, conn, vol.id, timeout=300)
                await inventory.record_resource(
                    None,
                    cluster_id=cluster_id,
                    service="cinder",
                    resource_type="volume",
                    resource_id=vol.id,
                    name=f"{agent_name}-boot",
                    metadata=vol_metadata,
                )
                userdata = k3s_cloudinit.generate_agent_userdata(
                    cluster_name=cluster_name,
                    k3s_version=k3s_version,
                    server_ip=server_ip,
                    node_token=node_token or "",
                    primary_network_id=network_id,
                    ssh_public_key=ssh_public_key,
                    extra_agent_args=_agent_args,
                    os_type=os_type,
                    gpu_required=gpu_required,
                )
                vm = await asyncio.to_thread(
                    nova.create_server,
                    conn,
                    agent_name,
                    flavor_id,
                    network_id,
                    vol.id,
                    userdata=userdata.data,
                    metadata=agent_metadata,
                    delete_boot_volume_on_termination=True,
                    security_groups=[sg_id] if sg_id else None,
                    config_drive=userdata.config_drive,
                    wait=provisioning_key is None,
                )
                await inventory.record_resource(
                    None,
                    cluster_id=cluster_id,
                    service="nova",
                    resource_type="server",
                    resource_id=vm.id,
                    name=agent_name,
                    metadata=agent_metadata,
                )
                new_entries.append({"vm_id": vm.id, "name": agent_name})
                await k3s_nodegroup.add_nodegroup_vms(nodegroup_id, cluster_id, [new_entries[-1]])
                if provisioning_key is not None:
                    server = await asyncio.to_thread(conn.compute.get_server, vm.id)
                    await asyncio.to_thread(conn.compute.wait_for_server, server, status="ACTIVE", wait=600)
                _logger.info("stampede: nodegroup %s — agent %s (%s) 생성됨", nodegroup_id, agent_name, vm.id)
            except ProvisioningInProgress:
                raise
            except Exception as e:
                _logger.error("stampede: nodegroup %s — agent %s 생성 실패: %s", nodegroup_id, agent_name, e)
                if provisioning_key is not None:
                    raise

        return new_entries
    finally:
        if conn is not None:
            await keystone.close_connection(conn)


class NodegroupDeletionError(RuntimeError):
    """Deletion stopped without forgetting resources that may still exist."""

    def __init__(self, reason: str, *, uncordon_failed: list[str] | None = None):
        self.reason = reason
        self.uncordon_failed = uncordon_failed or []
        super().__init__(reason + (f"; uncordon failed: {self.uncordon_failed}" if self.uncordon_failed else ""))


def _observe_owned_worker(conn, vm_id: str, project_id: str, cluster_id: str):
    """ID-only Nova observation; an ownership mismatch blocks every mutation."""
    from drover.services import inventory, nova

    server = nova.observe_server(conn, vm_id)
    if server is not None and not inventory.validate_resource_ownership(server, project_id, cluster_id, "server"):
        raise ValueError("Worker server ownership validation failed")
    return server


async def delete_nodegroup_vms(
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
    vm_entries: list[dict],
) -> None:
    """Drain every live node before deleting any VM; retain failed records for retry."""
    from drover.services import cinder, inventory, keystone, nova
    from drover.services import kube as k3s_kube
    from drover.services import nodegroup as k3s_nodegroup

    if not vm_entries:
        return
    conn = await keystone.get_project_manager_connection(project_id)
    cordoned: list[str] = []
    phase = "server_lookup"
    try:
        # Authoritatively absent/deleting servers resume cleanup without another drain.
        requires_drain = {}
        boot_volumes = {}
        resources = await inventory.list_managed_resources(cluster_id=cluster_id)
        for entry in vm_entries:
            vm_id = entry.get("vm_id")
            if not vm_id:
                raise ValueError("Deletion entry is missing vm_id")
            server = await asyncio.to_thread(_observe_owned_worker, conn, vm_id, project_id, cluster_id)
            requires_drain[vm_id] = server is not None and not nova.is_server_deleting(server)
            volumes = {
                r.resource_id for r in resources
                if r.service == "cinder" and r.resource_type == "volume" and r.name == f"{entry.get('name')}-boot"
            }
            boot_volumes[vm_id] = volumes

        live_vm_by_node = {entry.get("name"): entry["vm_id"] for entry in vm_entries if requires_drain[entry["vm_id"]]}
        inherited: set[str] = set()
        observed_names: set[str] = set()
        if live_vm_by_node:
            # A matching marker may predate an earlier DELETE attempt: such a cordon is
            # never reversible, so it stays out of this attempt's rollback.
            phase = "node_observation_failed"
            nodes = await k3s_kube.get_node_capacity(cluster_id)
            observed_names = {node["name"] for node in nodes}
            inherited = {
                node["name"] for node in nodes
                if node.get("unschedulable") and node.get("removal_vm_id")
                and node.get("removal_vm_id") == live_vm_by_node.get(node.get("name"))
            }

        for entry in vm_entries:
            if not requires_drain[entry["vm_id"]]:
                continue
            node_name = entry.get("name")
            if not node_name:
                raise ValueError("Live VM is missing Kubernetes node name")
            phase = f"cordon_failed:{node_name}"
            if node_name not in observed_names:
                # A new reservation (including an ambiguous POST) remains fenced on failure.
                # A conflict may be a late join or someone else's Node: never patch/uncordon it.
                if not await k3s_kube.cordon_node(cluster_id, node_name, removal_vm_id=entry["vm_id"], create_missing=True):
                    raise RuntimeError(phase)
            else:
                # The PATCH may reach the apiserver before a transport error.
                if node_name not in inherited:
                    cordoned.append(node_name)
                if not await k3s_kube.cordon_node(cluster_id, node_name, removal_vm_id=entry["vm_id"]):
                    raise RuntimeError(phase)
            phase = f"drain_failed:{node_name}"
            if not await k3s_kube.drain_node(cluster_id, node_name):
                raise RuntimeError(phase)

        for entry in vm_entries:
            vm_id = entry["vm_id"]
            node_name = entry.get("name")
            phase = f"server_delete_failed:{vm_id}"
            server = await asyncio.to_thread(_observe_owned_worker, conn, vm_id, project_id, cluster_id)
            # Past the final ownership check Nova may accept DELETE even when the call or wait
            # fails, so Pods must never be rescheduled onto this node again.
            if node_name in cordoned:
                cordoned.remove(node_name)
            if server is not None:
                if not nova.is_server_deleting(server):
                    await asyncio.to_thread(conn.compute.delete_server, vm_id, ignore_missing=True)
                await asyncio.to_thread(nova.wait_server_deleted, conn, vm_id)
            await inventory.mark_resource_deleted(service="nova", resource_type="server", resource_id=vm_id)
            for volume_id in boot_volumes[vm_id]:
                phase = f"boot_volume_delete_unverified:{volume_id}"
                # Only after Nova confirmed the server is gone; intent-provisioned workers keep it.
                await asyncio.to_thread(cinder.delete_detached_boot_volume, conn, volume_id, project_id, cluster_id)
                await inventory.mark_resource_deleted(service="cinder", resource_type="volume", resource_id=volume_id)
            phase = f"node_delete_failed:{node_name}"
            if node_name and not await k3s_kube.delete_k8s_node(cluster_id, node_name):
                raise RuntimeError(phase)
            await k3s_nodegroup.remove_nodegroup_vms(nodegroup_id, [vm_id])

        if project_id:
            try:
                from drover.services.activity import record

                await record(
                    project_id=project_id,
                    user_id="stampede-system",
                    username="Stampede",
                    resource_type="k3s_stampede",
                    resource_id=cluster_id,
                    resource_name=nodegroup_id,
                    action="scale_down",
                    status="success",
                    extra={"removed_count": len(vm_entries), "node_names": [e.get("name") for e in vm_entries]},
                )
            except Exception:
                pass
    except Exception as exc:
        uncordon_failed = []
        for node_name in cordoned:
            try:
                if not await k3s_kube.uncordon_node(cluster_id, node_name):
                    uncordon_failed.append(node_name)
            except Exception:
                uncordon_failed.append(node_name)
        raise NodegroupDeletionError(phase, uncordon_failed=uncordon_failed) from exc
    finally:
        await keystone.close_connection(conn)




async def reconcile_nodegroup_vms(
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
) -> list[dict]:
    """Reconcile recorded nodegroup VM rows against OpenStack Nova server tags/metadata."""
    from drover.services import keystone, nova
    from drover.services import nodegroup as nodegroup_store

    ng = await nodegroup_store.get_nodegroup(cluster_id, nodegroup_id)
    if not ng:
        return []

    vms = ng.get("vms") or []
    if not vms:
        if ng.get("node_count", 0) != 0:
            await nodegroup_store.set_nodegroup_count(cluster_id, nodegroup_id, 0)
        return []

    verified_vms: list[dict] = []
    conn = await keystone.get_project_manager_connection(project_id)
    try:
        for vm_entry in vms:
            vm_id = vm_entry.get("vm_id")
            if not vm_id:
                continue
            # Only a genuine Nova 404 is absence; the absent row stays tracked for its own
            # volume/Node cleanup. Other observation errors propagate.
            s = await asyncio.to_thread(nova.observe_server, conn, vm_id)
            # ERROR/SHUTOFF workers still occupy quota and have tracked resources.
            if s is not None and str(s.status or "").upper() not in ("DELETED", "SOFT_DELETED"):
                verified_vms.append(vm_entry)

        actual_count = len(verified_vms)
        if ng.get("node_count") != actual_count:
            await nodegroup_store.set_nodegroup_count(cluster_id, nodegroup_id, actual_count)
        return verified_vms
    finally:
        if conn is not None:
            await keystone.close_connection(conn)


async def provision_nodegroup_and_reconcile(
    project_id: str,
    cluster_id: str,
    nodegroup: dict,
    add_count: int,
    *,
    operation_id: str | None = None,
    triggering_metric: str | dict | None = None,
) -> None:
    """Provision a nodegroup and reconcile its desired count to tracked VMs."""
    from drover.services import operations

    created = await provision_nodegroup_vms(
        project_id=project_id,
        cluster_id=cluster_id,
        nodegroup_id=nodegroup["id"],
        add_count=add_count,
        flavor_id=nodegroup["flavor_id"],
        image_id=nodegroup.get("image_id"),
        labels=nodegroup.get("labels"),
        taints=nodegroup.get("taints"),
        provisioning_key_prefix=(
            f"nodegroup-{cluster_id}-{nodegroup['id']}-{operation_id}" if operation_id else None
        ),
        inspect_flavor_gpu=True,
    )
    verified = await reconcile_nodegroup_vms(project_id, cluster_id, nodegroup["id"])
    result_vm_ids = [v["vm_id"] for v in created]

    if operation_id:
        await operations.append_operation_event(
            None,
            operation_id,
            phase="nodegroup_reconciled",
            message=f"Nodegroup {nodegroup['id']} provisioned ({len(created)} created, {len(verified)} active)",
            payload_json={
                "nodegroup_id": nodegroup["id"],
                "requested_add_count": add_count,
                "created_count": len(created),
                "actual_count": len(verified),
                "vm_ids": result_vm_ids,
                "triggering_metric": triggering_metric or "manual",
            },
        )

    if len(created) != add_count:
        raise RuntimeError(f"nodegroup {nodegroup['id']} provisioned {len(created)}/{add_count} requested nodes")


async def delete_nodegroup_and_reconcile(
    project_id: str,
    cluster_id: str,
    nodegroup: dict,
    remove_entries: list[dict],
    *,
    delete_group: bool = False,
    operation_id: str | None = None,
    triggering_metric: str | dict | None = None,
) -> None:
    """Delete tracked VMs, reconcile the count, and optionally soft-delete the group."""
    from drover.services import nodegroup as nodegroup_store
    from drover.services import operations

    await delete_nodegroup_vms(
        project_id=project_id,
        cluster_id=cluster_id,
        nodegroup_id=nodegroup["id"],
        vm_entries=remove_entries,
    )
    verified = await reconcile_nodegroup_vms(project_id, cluster_id, nodegroup["id"])
    removed_vm_ids = [v["vm_id"] for v in remove_entries if v.get("vm_id")]

    if operation_id:
        await operations.append_operation_event(
            None,
            operation_id,
            phase="nodegroup_reconciled",
            message=f"Nodegroup {nodegroup['id']} scale-down reconciled ({len(verified)} active)",
            payload_json={
                "nodegroup_id": nodegroup["id"],
                "removed_count": len(remove_entries),
                "actual_count": len(verified),
                "vm_ids": removed_vm_ids,
                "triggering_metric": triggering_metric or "manual",
            },
        )

    if delete_group:
        deleted = await nodegroup_store.delete_nodegroup(cluster_id, nodegroup["id"])
        if not deleted:
            raise RuntimeError(f"nodegroup {nodegroup['id']} disappeared before deletion")


async def scale_agents(
    project_id: str,
    cluster_id: str,
    desired_count: int,
    operation_id: str | None = None,
    triggering_metric: str | dict | None = None,
) -> None:
    """Durably reconcile the legacy cluster agent count through the default nodegroup."""
    from drover.services import nodegroup as nodegroup_store
    from drover.services import operations, store

    cluster = await store.get_cluster(project_id, cluster_id)
    if not cluster:
        raise RuntimeError(f"cluster {cluster_id} not found")

    current_agent_ids = list(cluster.get("agent_vm_ids") or [])
    current_count = len(current_agent_ids)
    default_nodegroup_id = await nodegroup_store.get_default_agent_nodegroup_id(cluster_id)
    if not default_nodegroup_id:
        raise RuntimeError(f"cluster {cluster_id} has no default agent nodegroup")
    nodegroup = await nodegroup_store.get_nodegroup(cluster_id, default_nodegroup_id)
    if not nodegroup:
        raise RuntimeError(f"default agent nodegroup {default_nodegroup_id} not found")

    min_size = int(nodegroup.get("min_size", 0) if nodegroup.get("min_size") is not None else 0)
    max_size = int(nodegroup.get("max_size", 5) if nodegroup.get("max_size") is not None else 5)
    if desired_count < min_size or desired_count > max_size:
        raise ValueError(f"desired count {desired_count} is outside nodegroup bounds [{min_size}, {max_size}]")

    result_vm_ids: list[str] = []
    if desired_count > current_count:
        add_count = desired_count - current_count
        created = await provision_nodegroup_vms(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=default_nodegroup_id,
            add_count=add_count,
            flavor_id=nodegroup.get("flavor_id") or cluster.get("agent_flavor_id") or "",
            image_id=nodegroup.get("image_id"),
            labels=nodegroup.get("labels"),
            taints=nodegroup.get("taints"),
        )
        result_vm_ids = [v["vm_id"] for v in created]
        if created:
            await store.add_agent_vms(cluster_id, created)
        await reconcile_nodegroup_vms(project_id, cluster_id, default_nodegroup_id)
        latest = await nodegroup_store.get_nodegroup(cluster_id, default_nodegroup_id)
        actual_count = len((latest or {}).get("vms") or [])
        await store.update_agent_count(project_id, cluster_id, actual_count)
        if len(created) != add_count:
            raise RuntimeError(f"cluster {cluster_id} created {len(created)}/{add_count} requested agents")
    elif desired_count < current_count:
        remove_ids = current_agent_ids[desired_count:]
        result_vm_ids = list(remove_ids)
        name_map = await store.get_agent_vm_names(cluster_id, remove_ids)
        remove_entries = [{"vm_id": vm_id, "name": name_map.get(vm_id)} for vm_id in remove_ids]
        await delete_nodegroup_vms(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=default_nodegroup_id,
            vm_entries=remove_entries,
        )
        await store.remove_agent_vms(cluster_id, remove_ids)
        await reconcile_nodegroup_vms(project_id, cluster_id, default_nodegroup_id)
        latest = await nodegroup_store.get_nodegroup(cluster_id, default_nodegroup_id)
        actual_count = len((latest or {}).get("vms") or [])
        await store.update_agent_count(project_id, cluster_id, actual_count)

    if operation_id:
        await operations.append_operation_event(
            None,
            operation_id,
            phase="scale_reconciled",
            message=f"Cluster {cluster_id} scaled to {desired_count} agents",
            payload_json={
                "desired_count": desired_count,
                "vm_ids": result_vm_ids,
                "triggering_metric": triggering_metric or "manual",
            },
        )

    await store.update_cluster_status(project_id, cluster_id, "ACTIVE", "")
