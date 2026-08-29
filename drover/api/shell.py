"""k3s Cloud Shell — 티켓 발급 + WebSocket exec 프록시."""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time

import websockets
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect

from drover.auth import get_os_conn, get_token_info
from drover.services import cloud_shell as k3s_cloud_shell
from drover.services import kube as k3s_kube
from drover.services import store as k3s_cluster
from drover.services.activity import rec
from drover.services.cache import _get_redis
from drover.services.errors import K3sApiError

router = APIRouter()
_logger = logging.getLogger(__name__)

_TICKET_KEY_PREFIX = "afterglow:k3s-shell-ticket:"
_TICKET_TTL = 30
_IDLE_TIMEOUT = k3s_cloud_shell.IDLE_TIMEOUT_SECONDS


@router.post("/{cluster_id}/shell-ticket", status_code=201)
async def create_shell_ticket(
    cluster_id: str,
    conn=Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """Cloud Shell WebSocket 연결용 일회용 티켓 발급."""
    pid = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(pid, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    if cluster.get("status") != "ACTIVE":
        raise HTTPException(status_code=409, detail="클러스터가 ACTIVE 상태가 아닙니다")

    ticket = secrets.token_urlsafe(32)
    payload = json.dumps(
        {
            "cluster_id": cluster_id,
            "project_id": pid,
            "user_id": token_info["user_id"],
            "token": token_info["token"],
        }
    )
    r = await _get_redis()
    await r.setex(f"{_TICKET_KEY_PREFIX}{ticket}", _TICKET_TTL, payload)
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.shell.ticket",
        status="success",
        resource_id=cluster_id,
    )
    return {"ticket": ticket, "expires_in": _TICKET_TTL}


@router.websocket("/{cluster_id}/shell")
async def shell_ws(
    cluster_id: str,
    websocket: WebSocket,
    ticket: str = Query(...),
):
    """K8s exec WebSocket 프록시 (v4.channel.k8s.io 바이너리 프로토콜)."""
    await websocket.accept()
    r = await _get_redis()
    raw = await r.getdel(f"{_TICKET_KEY_PREFIX}{ticket}")
    if not raw:
        await websocket.close(code=4401, reason="invalid ticket")
        return
    info = json.loads(raw)
    if info.get("cluster_id") != cluster_id:
        await websocket.close(code=4403, reason="cluster mismatch")
        return

    project_id: str = info["project_id"]
    user_id: str = info["user_id"]

    try:
        pod = await k3s_cloud_shell.ensure_session(cluster_id, user_id, project_id=project_id)
    except (HTTPException, K3sApiError) as e:
        try:
            await websocket.send_bytes(b"\x02" + f"\r\n\x1b[31m{e.detail}\x1b[0m\r\n".encode())
            await websocket.close(code=4500, reason=str(e.detail))
        except Exception:
            pass
        return

    try:
        async with k3s_kube._kube_ws_params(cluster_id, project_id=project_id) as (ssl_ctx, ws_url):
            exec_url = (
                f"{ws_url}/api/v1/namespaces/{k3s_cloud_shell.SHELL_NAMESPACE}"
                f"/pods/{pod}/exec"
                f"?container=shell&stdin=true&stdout=true&stderr=true&tty=true"
                f"&command=sh&command=-c"
                f"&command=cd+/home/afterglow+%26%26+exec+bash+-l+2>/dev/null+||+exec+sh"
            )
            async with websockets.connect(
                exec_url,
                ssl=ssl_ctx,
                subprotocols=["v4.channel.k8s.io"],
                max_size=2**20,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=30,
            ) as backend:
                await _proxy(websocket, backend)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        _logger.warning("cloud shell proxy error (cluster=%s user=%s): %s", cluster_id, user_id[:8], e)
        try:
            await websocket.close()
        except Exception:
            pass
    finally:
        asyncio.create_task(
            _gc_pod(cluster_id, user_id, project_id),
            name=f"shell-gc-{user_id[:8]}",
        )


async def _proxy(client: WebSocket, backend) -> None:
    """양방향 프록시.

    K8s v4.channel.k8s.io 프레이밍:
      채널 0 = stdin, 1 = stdout, 2 = stderr, 3 = K8s error, 4 = resize
    브라우저 → 백엔드: binary frame, first byte = 채널
    백엔드 → 브라우저: channel 1/2/3 forward
    """
    last_activity = time.monotonic()

    async def watchdog() -> None:
        while True:
            await asyncio.sleep(30)
            if time.monotonic() - last_activity > _IDLE_TIMEOUT:
                _logger.info("cloud shell idle timeout")
                try:
                    await client.close(code=4408, reason="idle timeout")
                except Exception:
                    pass
                return

    async def browser_to_kube() -> None:
        nonlocal last_activity
        while True:
            try:
                data = await client.receive_bytes()
            except (WebSocketDisconnect, Exception):
                return
            last_activity = time.monotonic()
            try:
                await backend.send(data)
            except Exception:
                return

    async def kube_to_browser() -> None:
        nonlocal last_activity
        try:
            async for msg in backend:
                last_activity = time.monotonic()
                if isinstance(msg, bytes) and len(msg) > 0:
                    channel = msg[0]
                    payload = msg[1:]
                    if channel in (1, 2) and payload:
                        try:
                            await client.send_bytes(payload)
                        except Exception:
                            return
                    elif channel == 3 and payload:
                        try:
                            await client.send_bytes(b"\x1b[31m" + payload + b"\x1b[0m")
                        except Exception:
                            return
        except Exception:
            pass

    wd = asyncio.create_task(watchdog())
    b2k = asyncio.create_task(browser_to_kube())
    k2b = asyncio.create_task(kube_to_browser())
    done, pending = await asyncio.wait([b2k, k2b], return_when=asyncio.FIRST_COMPLETED)
    wd.cancel()
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


async def _gc_pod(cluster_id: str, user_id: str, project_id: str) -> None:
    """WS 종료 후 pod 삭제 (best-effort)."""
    try:
        await k3s_kube.delete_pod(
            cluster_id,
            k3s_cloud_shell.SHELL_NAMESPACE,
            k3s_cloud_shell.pod_name(user_id),
            project_id=project_id,
        )
        _logger.info("cloud shell pod deleted (user=%s)", user_id[:8])
    except Exception as e:
        _logger.warning("cloud shell pod GC failed: %s", e)
