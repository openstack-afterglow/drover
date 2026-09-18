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
) -> list[dict]:
    """노드그룹에 agent VM을 add_count개 생성해 k3s 클러스터에 join시킨다.

    labels/taints는 cloud-init extra_agent_args로 주입.
    생성된 VM 목록 {vm_id, name}을 반환한다 (실패한 것은 포함하지 않음).
    """
    from drover.services import cloudinit as k3s_cloudinit
    from drover.services import inventory
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

    conn = None
    if provisioning_key_prefix is None:
        from drover.services import cinder, keystone, nova

        try:
            conn = await keystone.get_project_manager_connection(project_id)
        except Exception as exc:
            _logger.error("provision_nodegroup_vms: OpenStack connection failed: %s", exc)
            return []
    try:
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
                    from drover.services import afterglow as afterglow_service

                    agent_metadata["drover.provisioning_idempotency_key"] = provisioning_key
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
                    new_entries.append({"vm_id": str(result["server_id"]), "name": agent_name})
                    _logger.info("stampede: nodegroup %s — agent %s 생성됨", nodegroup_id, agent_name)
                    continue

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
                    None,
                    cluster_id=cluster_id,
                    service="cinder",
                    resource_type="volume",
                    resource_id=vol.id,
                    name=f"{agent_name}-boot",
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
                )
                await inventory.record_resource(
                    None,
                    cluster_id=cluster_id,
                    service="nova",
                    resource_type="server",
                    resource_id=vm.id,
                    name=agent_name,
                )
                new_entries.append({"vm_id": vm.id, "name": agent_name})
                _logger.info("stampede: nodegroup %s — agent %s (%s) 생성됨", nodegroup_id, agent_name, vm.id)
            except ProvisioningInProgress:
                raise
            except Exception as e:
                _logger.error("stampede: nodegroup %s — agent %s 생성 실패: %s", nodegroup_id, agent_name, e)
                if provisioning_key is not None:
                    raise

        # DB에 VM 추적 레코드 추가
        if new_entries:
            await k3s_nodegroup.add_nodegroup_vms(nodegroup_id, cluster_id, new_entries)

        return new_entries
    finally:
        if conn is not None:
            await keystone.close_connection(conn)


async def delete_nodegroup_vms(
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
    vm_entries: list[dict],
) -> None:
    """노드그룹 VM을 cordon→drain→삭제한다.

    vm_entries: [{"vm_id": ..., "name": ...}, ...]
    """
    from drover.services import keystone, nova
    from drover.services import kube as k3s_kube
    from drover.services import nodegroup as k3s_nodegroup

    # cordon + drain
    node_names = [e["name"] for e in vm_entries if e.get("name")]
    for node_name in node_names:
        cordon_ok = await k3s_kube.cordon_node(cluster_id, node_name)
        if cordon_ok:
            drain_ok = await k3s_kube.drain_node(cluster_id, node_name)
            if not drain_ok:
                _logger.warning("stampede: drain %s timeout/실패, 강제 삭제 진행", node_name)

    # K8s 노드 오브젝트 삭제
    if node_names:
        await k3s_kube.delete_k8s_nodes(cluster_id, node_names)

    # Nova VM 삭제
    try:
        conn = await keystone.get_project_manager_connection(project_id)
    except Exception as e:
        _logger.error("delete_nodegroup_vms: OpenStack 연결 실패: %s", e)
        return
    try:
        vm_ids = [e["vm_id"] for e in vm_entries if e.get("vm_id")]
        for vm_id in vm_ids:
            try:
                await asyncio.to_thread(nova.delete_server, conn, vm_id)
                _logger.info("stampede: VM %s 삭제됨", vm_id)
            except Exception as e:
                _logger.warning("stampede: VM %s 삭제 실패: %s", vm_id, e)

        # DB 레코드 제거
        await k3s_nodegroup.remove_nodegroup_vms(nodegroup_id, vm_ids)

        # scale-down 완료 이벤트 (best-effort, project_id 없으면 스킵)
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
    conn = None
    try:
        conn = await keystone.get_project_manager_connection(project_id)
    except Exception as exc:
        _logger.debug("reconcile_nodegroup_vms: OpenStack connection unavailable: %s", exc)
    try:
        cluster_tag = f"drover.cluster_id={cluster_id}"
        for vm_entry in vms:
            vm_id = vm_entry.get("vm_id")
            if not vm_id:
                continue
            if conn is not None:
                try:
                    s = await asyncio.to_thread(nova.get_server, conn, vm_id)
                    if s:
                        status = str(
                            getattr(s, "status", None) or (s.get("status") if isinstance(s, dict) else "")
                        ).upper()
                        meta = getattr(s, "metadata", None) or (s.get("metadata") if isinstance(s, dict) else {})
                        tags = getattr(s, "tags", None) or (s.get("tags") if isinstance(s, dict) else [])
                        has_tag = (
                            isinstance(meta, dict)
                            and (
                                meta.get("drover.cluster_id") == cluster_id
                                or meta.get("k3s_horse_generator_nodegroup_id") == nodegroup_id
                            )
                        ) or (isinstance(tags, (list, tuple, set)) and cluster_tag in tags)
                        if status in ("ACTIVE", "BUILD") or (
                            has_tag and status not in ("ERROR", "DELETED", "SOFT_DELETED")
                        ):
                            verified_vms.append(vm_entry)
                except Exception as e:
                    _logger.debug("reconcile_nodegroup_vms: VM %s check failed: %s", vm_id, e)
            else:
                if vm_entry.get("status") != "ERROR":
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
