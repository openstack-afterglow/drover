"""k3s Stampede 노드그룹 VM 프로비저닝 서비스.

clusters.py의 _scale_agents 로직을 노드그룹 단위로 일반화·추출.
Stampede Reconciler(k3s_stampede.py)와 수동 스케일 핸들러(clusters.py) 양쪽에서 호출.
"""

import asyncio
import logging
import random
import string

from drover.config import get_settings

_logger = logging.getLogger(__name__)


def _rand_suffix(n: int = 5) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


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
) -> list[dict]:
    """노드그룹에 agent VM을 add_count개 생성해 k3s 클러스터에 join시킨다.

    labels/taints는 cloud-init extra_agent_args로 주입.
    생성된 VM 목록 {vm_id, name}을 반환한다 (실패한 것은 포함하지 않음).
    """
    from drover.services import (
        cinder,
        cloudinit as k3s_cloudinit,
        keystone,
        nodegroup as k3s_nodegroup,
        nova,
        plugins as k3s_plugins,
        store as k3s_db,
    )
    from drover.services import redis_store as k3s_cluster_svc

    s = get_settings()

    # 클러스터 기본 정보 조회 (admin: project_id 필터 없음)
    cluster = await k3s_db.get_cluster_admin(cluster_id)
    if not cluster:
        _logger.error("provision_nodegroup_vms: cluster %s 없음", cluster_id)
        return []

    node_token = await k3s_cluster_svc.get_cluster_node_token(project_id, cluster_id)
    server_ip = cluster.get("server_ip") or ""
    cluster_name = cluster.get("name") or cluster_id
    resource_snapshot = cluster.get("resource_policy_snapshot") or {}
    k3s_version = cluster.get("k3s_version") or ""
    os_type = cluster.get("os_type") or "ubuntu"
    network_id = cluster.get("network_id") or ""
    ssh_public_key = cluster.get("ssh_public_key") or None
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

    try:
        conn = keystone.get_admin_connection_for_project(project_id)
    except Exception as exc:
        _logger.error("provision_nodegroup_vms: OpenStack connection failed: %s", exc)
        return []

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
                metadata={
                    "k3s_horse_generator_role": "k3s_agent",
                    "k3s_horse_generator_cluster_id": cluster_id,
                    "k3s_horse_generator_nodegroup_id": nodegroup_id,
                },
                delete_boot_volume_on_termination=True,
                security_groups=[sg_id] if sg_id else None,
                config_drive=userdata.config_drive,
            )
            new_entries.append({"vm_id": vm.id, "name": agent_name})
            _logger.info("stampede: nodegroup %s — agent %s (%s) 생성됨", nodegroup_id, agent_name, vm.id)
        except Exception as e:
            _logger.error("stampede: nodegroup %s — agent %s 생성 실패: %s", nodegroup_id, agent_name, e)

    # DB에 VM 추적 레코드 추가
    if new_entries:
        await k3s_nodegroup.add_nodegroup_vms(nodegroup_id, cluster_id, new_entries)

    return new_entries


async def delete_nodegroup_vms(
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
    vm_entries: list[dict],
) -> None:
    """노드그룹 VM을 cordon→drain→삭제한다.

    vm_entries: [{"vm_id": ..., "name": ...}, ...]
    """
    from drover.services import keystone, kube as k3s_kube, nodegroup as k3s_nodegroup, nova

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
        conn = keystone.get_admin_connection_for_project(project_id)
    except Exception as e:
        _logger.error("delete_nodegroup_vms: OpenStack 연결 실패: %s", e)
        return

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


async def provision_nodegroup_and_reconcile(
    project_id: str,
    cluster_id: str,
    nodegroup: dict,
    add_count: int,
) -> None:
    """Provision a nodegroup and reconcile its desired count to tracked VMs."""
    from drover.services import nodegroup as nodegroup_store

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
    if len(created) != add_count:
        latest = await nodegroup_store.get_nodegroup(cluster_id, nodegroup["id"])
        actual = len((latest or {}).get("vms") or [])
        await nodegroup_store.set_nodegroup_count(cluster_id, nodegroup["id"], actual)
        raise RuntimeError(
            f"nodegroup {nodegroup['id']} provisioned {len(created)}/{add_count} requested nodes"
        )


async def delete_nodegroup_and_reconcile(
    project_id: str,
    cluster_id: str,
    nodegroup: dict,
    remove_entries: list[dict],
    *,
    delete_group: bool = False,
) -> None:
    """Delete tracked VMs, reconcile the count, and optionally soft-delete the group."""
    from drover.services import nodegroup as nodegroup_store

    await delete_nodegroup_vms(
        project_id=project_id,
        cluster_id=cluster_id,
        nodegroup_id=nodegroup["id"],
        vm_entries=remove_entries,
    )
    latest = await nodegroup_store.get_nodegroup(cluster_id, nodegroup["id"])
    actual = len((latest or {}).get("vms") or [])
    await nodegroup_store.set_nodegroup_count(cluster_id, nodegroup["id"], actual)
    if delete_group:
        deleted = await nodegroup_store.delete_nodegroup(cluster_id, nodegroup["id"])
        if not deleted:
            raise RuntimeError(f"nodegroup {nodegroup['id']} disappeared before deletion")


async def scale_agents(project_id: str, cluster_id: str, desired_count: int) -> None:
    """Durably reconcile the legacy cluster agent count through the default nodegroup."""
    from drover.services import nodegroup as nodegroup_store
    from drover.services import store

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
        if created:
            await store.add_agent_vms(cluster_id, created)
        actual_count = current_count + len(created)
        await nodegroup_store.set_nodegroup_count(cluster_id, default_nodegroup_id, actual_count)
        await store.update_agent_count(project_id, cluster_id, actual_count)
        if len(created) != add_count:
            raise RuntimeError(f"cluster {cluster_id} created {len(created)}/{add_count} requested agents")
    elif desired_count < current_count:
        remove_ids = current_agent_ids[desired_count:]
        name_map = await store.get_agent_vm_names(cluster_id, remove_ids)
        remove_entries = [{"vm_id": vm_id, "name": name_map.get(vm_id)} for vm_id in remove_ids]
        await delete_nodegroup_vms(
            project_id=project_id,
            cluster_id=cluster_id,
            nodegroup_id=default_nodegroup_id,
            vm_entries=remove_entries,
        )
        await store.remove_agent_vms(cluster_id, remove_ids)
        await nodegroup_store.set_nodegroup_count(cluster_id, default_nodegroup_id, desired_count)
        await store.update_agent_count(project_id, cluster_id, desired_count)

    await store.update_cluster_status(project_id, cluster_id, "ACTIVE", "")
