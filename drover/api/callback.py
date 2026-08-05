"""k3s 서버 VM으로부터 kubeconfig/node-token 콜백 수신 (인증 불필요)."""

import asyncio
import logging

from fastapi import APIRouter, HTTPException, Request

from drover.models.schemas import K3sCallbackRequest
from drover.rate_limit import _get_real_ip, limiter
from drover.services import jobs as _jobs_svc
from drover.services import store as k3s_cluster

router = APIRouter()
_logger = logging.getLogger(__name__)


@router.post("/callback")
@limiter.limit("10/minute")
async def k3s_callback(request: Request, req: K3sCallbackRequest):
    """k3s 서버 VM의 cloud-init에서 kubeconfig + node-token 수신.

    일회성 토큰으로 보안 보장. 단일 마스터: 토큰 소비 후 에이전트 VM 생성 백그라운드 처리.
    HA 서버#1: bootstrap_ha_servers 스폰. HA 서버#2/#3: 조인 카운터 증가, 모두 조인 시 provision_agents 스폰.
    Source IP 는 audit/forensic 목적으로 로그에 기록.
    """
    source_ip = "unknown"
    try:
        source_ip = _get_real_ip(request)
    except Exception:
        _logger.debug("callback source IP 추출 실패", exc_info=True)

    # HA 토큰 먼저 시도 (server_index 포함), 없으면 일반 토큰
    token_data = await k3s_cluster.consume_ha_callback_token(req.token)
    if token_data is None:
        token_data = await k3s_cluster.consume_callback_token(req.token)

    if not token_data:
        _logger.warning("k3s callback received with invalid/expired token from %s", source_ip)
        raise HTTPException(status_code=403, detail="Forbidden")

    project_id = token_data["project_id"]
    cluster_id = token_data["cluster_id"]
    server_index: int | None = token_data.get("server_index")
    _logger.info(
        "k3s callback consumed: cluster=%s project=%s source_ip=%s server_ip=%s server_index=%s",
        cluster_id,
        project_id,
        source_ip,
        req.server_ip,
        server_index,
    )

    if not req.success:
        error_msg = req.error or "서버 VM에서 알 수 없는 오류 발생"
        _logger.error(
            "k3s cluster %s server%s init failed: %s", cluster_id, f"#{server_index}" if server_index else "", error_msg
        )
        await k3s_cluster.update_cluster_status(project_id, cluster_id, "ERROR", f"서버 초기화 실패: {error_msg}")
        return {"ok": True}

    if server_index is not None and server_index >= 2:
        return await _handle_ha_joiner(project_id, cluster_id, server_index, req)

    if not req.kubeconfig or not req.node_token or not req.server_ip:
        _logger.error("k3s cluster %s callback missing fields", cluster_id)
        await k3s_cluster.update_cluster_status(
            project_id, cluster_id, "ERROR", "콜백 데이터 누락 (kubeconfig/node_token/server_ip)"
        )
        return {"ok": True}

    try:
        await k3s_cluster.store_kubeconfig(project_id, cluster_id, req.kubeconfig)
    except Exception as e:
        _logger.error("k3s cluster %s kubeconfig encryption failed: %s", cluster_id, e)
        await k3s_cluster.update_cluster_status(project_id, cluster_id, "ERROR", f"kubeconfig 저장 실패: {e}")
        return {"ok": True}

    api_address = f"https://{req.server_ip}:6443"
    await k3s_cluster.update_cluster_status(
        project_id,
        cluster_id,
        "PROVISIONING",
        server_ip=req.server_ip,
        api_address=api_address,
        node_token=req.node_token,
        plugin_status=req.plugin_status,
        secret_cloud_config_status=req.secret_cloud_config_status,
    )

    if req.occm_status:
        _logger.info("k3s cluster %s OCCM status (deprecated): %s", cluster_id, req.occm_status)
    if req.secret_cloud_config_status and req.secret_cloud_config_status != "ok":
        _logger.warning("k3s cluster %s cloud-config secret 생성 실패: %s", cluster_id, req.secret_cloud_config_status)
    if req.plugin_status:
        for name, info in req.plugin_status.items():
            st = info.get("status", info) if isinstance(info, dict) else info
            err = info.get("error", "") if isinstance(info, dict) else ""
            if st == "deployed":
                _logger.info("k3s cluster %s plugin %s: deployed", cluster_id, name)
            else:
                _logger.error("k3s cluster %s plugin %s: %s — %s", cluster_id, name, st, err)

    cluster_info = await k3s_cluster.get_cluster(project_id, cluster_id)
    master_count = int((cluster_info or {}).get("master_count") or 1)
    lb_pool_id = (cluster_info or {}).get("api_lb_pool_id") or ""
    lb_fip_address = (cluster_info or {}).get("api_fip_address") or ""

    if master_count >= 3:
        _logger.info("k3s cluster %s HA mode: queuing bootstrap_ha_servers job", cluster_id)
        await _jobs_svc.enqueue_job(
            cluster_id=cluster_id,
            project_id=project_id,
            kind="bootstrap_ha",
            payload={
                "server_ip": req.server_ip,
                "node_token": req.node_token,
                "master_count": master_count,
                "lb_pool_id": lb_pool_id,
                "lb_fip_address": lb_fip_address,
            },
        )
    else:
        _logger.info("k3s cluster %s server ready, queuing provision_agents job", cluster_id)
        await _jobs_svc.enqueue_job(
            cluster_id=cluster_id,
            project_id=project_id,
            kind="provision_agents",
            payload={
                "server_ip": req.server_ip,
                "node_token": req.node_token,
            },
        )

    return {"ok": True}


async def _handle_ha_joiner(
    project_id: str,
    cluster_id: str,
    server_index: int,
    req: K3sCallbackRequest,
) -> dict:
    from drover.services import octavia

    cluster_info = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster_info:
        _logger.error("HA joiner: cluster %s not found", cluster_id)
        return {"ok": True}

    master_count = int(cluster_info.get("master_count") or 3)
    lb_pool_id = cluster_info.get("api_lb_pool_id") or ""
    network_id = cluster_info.get("network_id") or ""

    if lb_pool_id and req.server_ip:
        conn = None
        try:
            from drover.services import keystone

            conn = await asyncio.to_thread(keystone.get_admin_connection_for_project, project_id)
            subnets = await asyncio.to_thread(lambda: list(conn.network.subnets(network_id=network_id)))
            subnet_id = subnets[0].id if subnets else None
            cluster_name = cluster_info.get("name") or cluster_id
            await asyncio.to_thread(
                octavia.add_member,
                conn,
                lb_pool_id,
                req.server_ip,
                6443,
                subnet_id=subnet_id,
                name=f"{cluster_name}-server{server_index}",
            )
            _logger.info("HA: server#%d %s added to LB pool %s", server_index, req.server_ip, lb_pool_id)
        except Exception as e:
            _logger.warning("HA: failed to add server#%d to LB pool: %s", server_index, e)
        finally:
            if conn is not None:
                await asyncio.to_thread(conn.close)

    join_count = await k3s_cluster.incr_ha_join_count(cluster_id)
    _logger.info("HA: cluster %s join count: %d / %d", cluster_id, join_count, master_count - 1)

    if join_count >= master_count - 1:
        server_ip = cluster_info.get("server_ip") or ""
        node_token = cluster_info.get("node_token") or ""
        if server_ip and node_token:
            _logger.info("HA: all servers joined for cluster %s, queuing provision_agents job", cluster_id)
            await _jobs_svc.enqueue_job(
                cluster_id=cluster_id,
                project_id=project_id,
                kind="provision_agents",
                payload={
                    "server_ip": server_ip,
                    "node_token": node_token,
                },
            )
        else:
            _logger.error("HA: cluster %s missing server_ip/node_token after HA join", cluster_id)

    return {"ok": True}
