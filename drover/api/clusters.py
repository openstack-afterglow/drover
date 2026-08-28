"""k3s 클러스터 CRUD + SSE 생성 엔드포인트."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import openstack
import asyncio
import hashlib
import json
import logging
import random
import string
import time
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.exc import InterfaceError, OperationalError
from starlette.requests import Request

from drover.auth import CacheMode, cache_mode, get_os_conn, get_token_info
from drover.config import get_settings
from drover.db import mark_db_unhealthy
from drover.models.schemas import (
    CreateK3sClusterRequest,
    K3sAttachInterfaceRequest,
    K3sClusterInfo,
    K3sInterfaceInfo,
    K3sProgressMessage,
    K3sProgressStep,
    ScaleK3sClusterRequest,
)
from drover.policy import authorize
from drover.services import instance_orchestration as _instance_orch
from drover.services import jobs as _jobs
from drover.services import nova, operations
from drover.services import store as k3s_cluster
from drover.services.activity import rec
from drover.services.cache import cached_call, invalidate, ttl_normal, ttl_slow
from drover.services.cache import invalidation as cache_invalidation
from drover.services.cache import keys as cache_keys
from drover.services.deletion import delete_cluster_progress as _delete_cluster_progress

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)
_logger = logging.getLogger(__name__)

_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
    "Content-Encoding": "identity",
}


def _rand_suffix(length: int = 5) -> str:
    """K8s 스타일 랜덤 suffix (소문자+숫자). 매 생성마다 고유한 리소스 이름 보장."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def _cluster_to_info(c: dict) -> K3sClusterInfo:
    agent_vm_ids = c.get("agent_vm_ids") or []
    if isinstance(agent_vm_ids, str):
        import json

        try:
            agent_vm_ids = json.loads(agent_vm_ids)
        except Exception:
            agent_vm_ids = []
    return K3sClusterInfo(
        id=c.get("id", ""),
        project_id=c.get("project_id", ""),
        name=c.get("name", ""),
        status=c.get("status", ""),
        status_reason=c.get("status_reason") or None,
        server_vm_id=c.get("server_vm_id") or None,
        agent_vm_ids=agent_vm_ids,
        agent_count=int(c.get("agent_count") or 0),
        api_address=c.get("api_address") or None,
        server_ip=c.get("server_ip") or None,
        network_id=c.get("network_id") or None,
        key_name=c.get("key_name") or None,
        k3s_version=c.get("k3s_version") or None,
        created_at=c.get("created_at") or None,
        updated_at=c.get("updated_at") or None,
        deleted_at=c.get("deleted_at") or None,
        deleted_by_user_id=c.get("deleted_by_user_id") or None,
        deleted_reason=c.get("deleted_reason") or None,
        occm_enabled=bool(c.get("occm_enabled", False)),
        plugins_enabled=c.get("plugins_enabled") or {},
        api_lb_id=c.get("api_lb_id") or None,
        api_fip_id=c.get("api_fip_id") or None,
        api_fip_address=c.get("api_fip_address") or None,
        master_count=int(c.get("master_count") or 1),
        stampede_enabled=bool(c.get("stampede_enabled", False)),
        last_reconciled_at=c.get("last_reconciled_at") or None,
        drift_status=c.get("drift_status") or None,
    )


@router.get("", response_model=list[K3sClusterInfo])
async def list_k3s_clusters(
    token_info: dict = Depends(get_token_info),
    include_deleted: bool = Query(default=False),
    cm: CacheMode = Depends(cache_mode),
):
    project_id = token_info["project_id"]
    authorize("drover:clusters:get", {"project_id": project_id}, token_info)
    sub = "all" if include_deleted else None
    cache_key = cache_keys.project_key("k3s", project_id, "clusters", sub=sub)

    async def _fetch():
        try:
            clusters = await k3s_cluster.list_clusters(project_id, include_deleted=include_deleted)
        except (OperationalError, InterfaceError):
            _logger.warning("k3s 클러스터 목록 DB 조회 실패 — 빈 목록 반환", exc_info=True)
            mark_db_unhealthy()
            return []
        return [_cluster_to_info(c) for c in clusters]

    try:
        return await cached_call(cache_key, ttl_normal(), _fetch, enabled=cm.enabled, refresh=cm.refresh)
    except (OperationalError, InterfaceError):
        _logger.warning("k3s 클러스터 목록 DB 조회 실패 — 빈 목록 반환", exc_info=True)
        mark_db_unhealthy()
        return []


@router.get("/{cluster_id}", response_model=K3sClusterInfo)
async def get_k3s_cluster(
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
    cm: CacheMode = Depends(cache_mode),
):
    project_id = token_info["project_id"]
    authorize("drover:clusters:get", {"project_id": project_id}, token_info)
    cache_key = cache_keys.project_key("k3s", project_id, "clusters", sub=cluster_id)

    async def _fetch():
        cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
        if not cluster:
            raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
        return _cluster_to_info(cluster)

    return await cached_call(cache_key, ttl_normal(), _fetch, enabled=cm.enabled, refresh=cm.refresh)


@router.get("/{cluster_id}/kubeconfig", operation_id="download_kubeconfig")
async def download_kubeconfig(
    request: Request,
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
    cm: CacheMode = Depends(cache_mode),
):
    """kubeconfig YAML 파일 다운로드. 아직 준비되지 않으면 404.

    매 호출마다 audit log 기록 — 토큰 탈취 시 다운로드 추적이 가능하도록.
    None 결과는 캐시하지 않는다 (초기화 중인 클러스터 UX 보호).
    """
    import json as _json

    from drover.services.cache import get_backend

    project_id = token_info["project_id"]
    authorize("drover:clusters:get", {"project_id": project_id}, token_info)
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    cache_key = cache_keys.project_key("k3s", project_id, "clusters", sub=f"{cluster_id}:kubeconfig")
    kubeconfig: bytes | None = None

    # 캐시 조회 (캐시 활성 + 강제 갱신 아님일 때만)
    if cm.enabled and not cm.refresh:
        try:
            backend = get_backend()
            cached_raw = await backend.get(cache_key)
            if cached_raw is not None:
                kubeconfig = _json.loads(cached_raw).encode()
        except Exception:
            pass  # 캐시 장애 → fallthrough

    if kubeconfig is None:
        try:
            kubeconfig = await k3s_cluster.get_kubeconfig(project_id, cluster_id)
        except Exception as e:
            _logger.error("kubeconfig 복호화 실패: %s", e)
            raise HTTPException(status_code=500, detail="kubeconfig 복호화에 실패했습니다. 관리자에게 문의하세요.")

        # None이 아닐 때만, 캐시 활성 시에만 저장
        if kubeconfig is not None and cm.enabled:
            try:
                backend = get_backend()
                await backend.set(cache_key, _json.dumps(kubeconfig.decode()), ttl_slow())
            except Exception:
                pass  # 캐시 장애 → silent fail

    if not kubeconfig:
        raise HTTPException(
            status_code=404, detail="kubeconfig가 아직 준비되지 않았습니다. 클러스터가 초기화 중입니다."
        )

    cluster_name = cluster.get("name", cluster_id)

    # audit log — HEAD 는 보통 브라우저 사전 요청이라 GET 일 때만 기록
    if request.method == "GET":
        try:
            from drover.rate_limit import _get_real_ip

            source_ip = _get_real_ip(request)
            await rec(
                token_info,
                None,
                resource_type="k3s_cluster",
                action="kubeconfig_download",
                resource_id=cluster_id,
                resource_name=cluster_name,
                extra={"source_ip": source_ip},
            )
        except Exception:
            _logger.warning("kubeconfig 다운로드 audit 기록 실패", exc_info=True)

    return Response(
        content=kubeconfig,
        media_type="application/yaml",
        headers={"Content-Disposition": f'attachment; filename="kubeconfig-{cluster_name}.yaml"'},
    )


@router.head("/{cluster_id}/kubeconfig", operation_id="head_kubeconfig")
async def head_kubeconfig(
    request: Request,
    cluster_id: str,
    token_info: dict = Depends(get_token_info),
    cm: CacheMode = Depends(cache_mode),
):
    """kubeconfig 메타데이터 조회 (HEAD)."""
    return await download_kubeconfig(request, cluster_id, token_info, cm)


def compute_request_hash(req: CreateK3sClusterRequest) -> str:
    data = req.model_dump(mode="json")
    canonical_json = json.dumps(data, sort_keys=True)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _map_phase_to_step(phase: str) -> K3sProgressStep:
    try:
        return K3sProgressStep(phase)
    except ValueError:
        if phase == "job_enqueued":
            return K3sProgressStep.SECURITY_GROUP
        if phase == "waiting_callback":
            return K3sProgressStep.WAITING_CALLBACK
        if phase == "completed":
            return K3sProgressStep.COMPLETED
        if phase == "failed":
            return K3sProgressStep.FAILED
        return K3sProgressStep.SERVER_CREATING


@router.post("/async")
@limiter.limit("5/minute")
async def create_k3s_cluster_async(
    request: Request,
    req: CreateK3sClusterRequest,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """k3s 클러스터 생성 — SSE 스트리밍 진행률 반환."""
    project_id = conn._afterglow_project_id
    authorize("drover:clusters:create", {"project_id": project_id}, token_info)
    s = get_settings()

    idempotency_key = request.headers.get("idempotency-key") or request.headers.get("Idempotency-Key")
    req_hash = compute_request_hash(req)

    existing_op = None
    if idempotency_key:
        existing_op = await operations.get_operation_by_idempotency_key(project_id, idempotency_key)
        if existing_op:
            if existing_op.request_hash != req_hash:
                raise HTTPException(
                    status_code=409,
                    detail=f"Idempotency key {idempotency_key!r} reused with a different request hash",
                )
            cluster_id = existing_op.cluster_id
            operation_id = existing_op.id

    if not existing_op:
        # Apply template defaults before resolving any policy. Explicit request
        # values retain precedence over the template and administrator defaults.
        _template_snapshot: dict | None = None
        template_default_image_id: str | None = None
        if req.template_id:
            from drover.services import template as _tmpl_svc

            _tmpl = await _tmpl_svc.get_template(req.template_id)
            if not _tmpl:
                raise HTTPException(status_code=400, detail=f"템플릿을 찾을 수 없습니다: {req.template_id}")
            _template_snapshot = dict(_tmpl)
            if req.agent_count == 1 and _tmpl.get("default_node_count") is not None:
                req = req.model_copy(update={"agent_count": _tmpl["default_node_count"]})
            if not req.agent_flavor_id and _tmpl.get("default_agent_flavor_id"):
                req = req.model_copy(update={"agent_flavor_id": _tmpl["default_agent_flavor_id"]})
            if req.os_type == "ubuntu" and _tmpl.get("os_type") and _tmpl["os_type"] != "ubuntu":
                req = req.model_copy(update={"os_type": _tmpl["os_type"]})
            template_default_image_id = _tmpl.get("default_image_id") or None

        from drover.services.resource_policies import validate_existing_selection
        from drover.services.resource_policy_store import (
            ResourcePolicyStorageUnavailable,
            get_policy_snapshot,
            get_required_runtime_setting,
            resolve_policy_snapshot,
        )

        os_type = req.os_type
        image_policy_key = "k3s.fcos_image" if os_type == "fcos" else "k3s.server_image"
        try:
            policy_snapshot = await resolve_policy_snapshot(
                conn=conn,
                keys=(
                    image_policy_key,
                    "k3s.server_flavor",
                    "k3s.default_agent_flavor",
                    "k3s.volume_availability_zone",
                ),
            )
        except ResourcePolicyStorageUnavailable as exc:
            raise HTTPException(status_code=503, detail="resource policy storage is unavailable") from exc

        server_image_id = policy_snapshot[image_policy_key]["id"]
        server_flavor_id = policy_snapshot["k3s.server_flavor"]["id"]
        policy_snapshot["k3s.volume_availability_zone"]["id"]

        if req.agent_flavor_id:
            agent_selection = await validate_existing_selection(conn, "k3s.default_agent_flavor", req.agent_flavor_id)
            agent_flavor_id = agent_selection["id"]
            policy_snapshot["k3s.default_agent_flavor"] = {"id": agent_selection["id"], "name": agent_selection["name"]}
        else:
            agent_flavor_id = policy_snapshot["k3s.default_agent_flavor"]["id"]

        if req.agent_count > 0 and not agent_flavor_id:
            raise HTTPException(status_code=503, detail="에이전트 플레이버가 설정되지 않았습니다. 관리자에게 문의하세요.")

        agent_image_snapshot = dict(policy_snapshot[image_policy_key])
        if template_default_image_id:
            agent_image = await validate_existing_selection(conn, image_policy_key, template_default_image_id)
            agent_image["id"]
            agent_image_snapshot = {"id": agent_image["id"], "name": agent_image["name"]}
        policy_snapshot["effective_agent_image"] = agent_image_snapshot

        optional_policy_keys = (
            "k3s.occm_floating_network",
            "k3s.occm_public_network",
            "k3s.lb_subnet",
            "k3s.api_lb_vip_network",
            "k3s.api_lb_floating_network",
            "k3s.octavia_ingress_floating_network",
        )
        stored_optional = await get_policy_snapshot(optional_policy_keys)
        for optional_key, stored_selection in stored_optional.items():
            if stored_selection is None:
                continue
            selection = await validate_existing_selection(conn, optional_key, stored_selection["id"])
            policy_snapshot[optional_key] = {"id": selection["id"], "name": selection["name"]}

        network_id = req.network_id or await _instance_orch.resolve_default_network(conn, s)
        k3s_version = await get_required_runtime_setting("k3s.version")

        cluster_id = str(uuid.uuid4())
        now = datetime.now(UTC).isoformat()
        _creator_user_id = conn._afterglow_user_id if hasattr(conn, "_afterglow_user_id") else None
        _creator_username = None
        if token_info and isinstance(token_info, dict):
            _creator_user_id = _creator_user_id or token_info.get("user_id")
            _creator_username = token_info.get("username")

        cluster_data = {
            "name": req.name,
            "status": "CREATING",
            "status_reason": "",
            "server_vm_id": None,
            "agent_vm_ids": [],
            "agent_count": req.agent_count,
            "server_flavor_id": server_flavor_id,
            "agent_flavor_id": agent_flavor_id,
            "server_image_id": server_image_id,
            "default_agent_image_id": agent_image_snapshot.get("id"),
            "network_id": network_id,
            "security_group_id": None,
            "server_ip": "",
            "api_address": "",
            "key_name": req.key_name or "",
            "ssh_public_key": None,
            "k3s_version": k3s_version,
            "occm_enabled": False,
            "plugins_enabled": None,
            "created_by_user_id": _creator_user_id or "",
            "created_by_username": _creator_username or "",
            "created_at": now,
            "updated_at": now,
            "master_count": req.master_count,
            "os_type": os_type,
            "template_id": req.template_id or None,
            "template_snapshot": _template_snapshot,
            "resource_policy_snapshot": policy_snapshot,
            "stampede_enabled": req.stampede_enabled,
            "allowed_cidrs": req.allowed_cidrs,
        }

        await k3s_cluster.create_cluster_record(project_id, cluster_id, cluster_data)

        request_id = getattr(request.state, "correlation_id", None) or request.headers.get("x-openstack-request-id")
        job_payload = dict(cluster_data)

        await _jobs.enqueue_job(
            cluster_id=cluster_id,
            project_id=project_id,
            kind="create",
            payload=job_payload,
            user_id=_creator_user_id,
            username=_creator_username,
            request_id=request_id,
            idempotency_key=idempotency_key,
            request_hash=req_hash,
            op_kind="create",
        )

        try:
            await invalidate(f"afterglow:k3s:{project_id}:*")
            await cache_invalidation.invalidate_mutation_count("k3s", project_id)
        except Exception:
            pass

        active_op = await operations.get_active_operation(None, cluster_id, kind="create")
        operation_id = active_op.id if active_op else ""

    async def progress_generator() -> AsyncGenerator[str, None]:
        _start_time = time.monotonic()
        yield ": " + " " * 2048 + "\n\n"

        seen_sequences = set()
        while True:
            events = await operations.get_operation_events(None, operation_id)
            for ev in events:
                if ev.sequence in seen_sequences:
                    continue
                seen_sequences.add(ev.sequence)
                elapsed = round(time.monotonic() - _start_time, 1)

                pj = ev.payload_json if isinstance(ev.payload_json, dict) else {}
                step_val = pj.get("step") or ev.phase
                step = _map_phase_to_step(step_val)
                progress = pj.get("progress") if "progress" in pj else 10
                msg = ev.message or ""
                error = pj.get("error")

                progress_msg = K3sProgressMessage(
                    step=step,
                    progress=progress,
                    message=msg,
                    cluster_id=cluster_id,
                    operation_id=operation_id,
                    sequence=ev.sequence,
                    error=error,
                    elapsed_seconds=elapsed,
                )
                yield f"data: {progress_msg.model_dump_json()}\n\n"

            op = await operations.get_operation(None, operation_id)
            if op and op.status in {"WAITING_CALLBACK", "SUCCEEDED", "FAILED", "CANCELLED"}:
                final_events = await operations.get_operation_events(None, operation_id)
                for ev in final_events:
                    if ev.sequence in seen_sequences:
                        continue
                    seen_sequences.add(ev.sequence)
                    elapsed = round(time.monotonic() - _start_time, 1)
                    pj = ev.payload_json if isinstance(ev.payload_json, dict) else {}
                    step_val = pj.get("step") or ev.phase
                    step = _map_phase_to_step(step_val)
                    progress = pj.get("progress") if "progress" in pj else 10
                    msg = ev.message or ""
                    error = pj.get("error")

                    progress_msg = K3sProgressMessage(
                        step=step,
                        progress=progress,
                        message=msg,
                        cluster_id=cluster_id,
                        operation_id=operation_id,
                        sequence=ev.sequence,
                        error=error,
                        elapsed_seconds=elapsed,
                    )
                    yield f"data: {progress_msg.model_dump_json()}\n\n"
                break
            await asyncio.sleep(0.1)

    return StreamingResponse(
        progress_generator(),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )

@router.patch("/{cluster_id}/scale")
@limiter.limit("10/minute")
async def scale_k3s_cluster(
    request: Request,
    cluster_id: str,
    req: ScaleK3sClusterRequest,
    token_info: dict = Depends(get_token_info),
):
    """에이전트 수 변경. 현재 ACTIVE 상태에서만 허용."""
    project_id = token_info["project_id"]
    authorize("drover:clusters:scale", {"project_id": project_id}, token_info)
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    if cluster["status"] != "ACTIVE":
        raise HTTPException(status_code=409, detail="ACTIVE 상태의 클러스터만 스케일링할 수 있습니다")

    current_agent_ids: list[str] = cluster.get("agent_vm_ids") or []
    if isinstance(current_agent_ids, str):
        import json as _json

        try:
            current_agent_ids = _json.loads(current_agent_ids)
        except Exception:
            current_agent_ids = []

    desired = req.agent_count
    current = len(current_agent_ids)

    if desired == current:
        return {"message": "변경 없음", "agent_count": current}

    await k3s_cluster.update_cluster_status(project_id, cluster_id, "SCALING")
    try:
        await invalidate(f"afterglow:k3s:{project_id}:*")
        await cache_invalidation.invalidate_mutation_count("k3s", project_id)
    except Exception:
        pass
    await _jobs.enqueue_job(
        cluster_id=cluster_id,
        project_id=project_id,
        kind="scale",
        payload={"desired_count": desired},
        user_id=token_info.get("user_id"),
        username=token_info.get("username"),
    )
    await rec(
        token_info,
        None,
        resource_type="k3s_cluster",
        action="scale",
        resource_id=cluster_id,
        extra={"desired_count": desired},
    )
    return {"message": f"스케일링 시작: {current} → {desired}", "agent_count": desired}





@router.delete("/{cluster_id}", status_code=204)
@limiter.limit("5/minute")
async def delete_k3s_cluster(
    request: Request,
    cluster_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """Persist a durable cluster-deletion job and return after it is queued."""
    project_id = conn._afterglow_project_id
    authorize("drover:clusters:delete", {"project_id": project_id}, token_info)
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    if cluster.get("deleted_at"):
        return

    try:
        await invalidate(f"afterglow:k3s:{project_id}:*")
        await cache_invalidation.invalidate_mutation_count("k3s", project_id)
    except Exception:
        pass

    await _jobs.enqueue_job(
        cluster_id=cluster_id,
        project_id=project_id,
        kind="delete",
        payload={
            "user_id": token_info.get("user_id"),
            "username": token_info.get("username"),
        },
        user_id=token_info.get("user_id"),
        username=token_info.get("username"),
    )
    await k3s_cluster.update_cluster_status(project_id, cluster_id, "DELETING")


@router.post("/{cluster_id}/delete-async")
@limiter.limit("5/minute")
async def delete_k3s_cluster_async(
    request: Request,
    cluster_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """k3s 클러스터 삭제 — SSE 스트리밍 진행률 반환."""
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    try:
        await invalidate(f"afterglow:k3s:{project_id}:*")
        await cache_invalidation.invalidate_mutation_count("k3s", project_id)
    except Exception:
        pass

    async def gen() -> AsyncGenerator[str, None]:
        yield ": " + " " * 2048 + "\n\n"
        start = time.monotonic()
        if cluster.get("deleted_at"):
            msg = K3sProgressMessage(
                step=K3sProgressStep.COMPLETED,
                progress=100,
                message="이미 삭제된 클러스터입니다",
                elapsed_seconds=0.0,
            )
            yield f"data: {msg.model_dump_json()}\n\n"
            return
        try:
            async for msg in _delete_cluster_progress(conn, project_id, cluster, token_info):
                msg.elapsed_seconds = round(time.monotonic() - start, 1)
                yield f"data: {msg.model_dump_json()}\n\n"
        except Exception as e:
            _logger.error("k3s cluster %s async delete failed: %s", cluster_id, e, exc_info=True)
            fail = K3sProgressMessage(
                step=K3sProgressStep.FAILED,
                progress=0,
                message=f"삭제 실패: {e}",
                error=str(e),
                elapsed_seconds=round(time.monotonic() - start, 1),
            )
            yield f"data: {fail.model_dump_json()}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)


# ---------------------------------------------------------------------------
# 노드 네트워크 인터페이스 attach / detach
# ---------------------------------------------------------------------------


def _assert_vm_in_cluster(vm_id: str, cluster: dict) -> str:
    """vm_id 가 해당 클러스터에 속하면 'server'|'agent' 반환, 아니면 HTTPException 403."""
    if cluster.get("server_vm_id") == vm_id:
        return "server"
    if vm_id in (cluster.get("agent_vm_ids") or []):
        return "agent"
    raise HTTPException(status_code=403, detail="해당 VM은 이 클러스터에 속하지 않습니다")


@router.get("/{cluster_id}/nodes/{vm_id}/interfaces")
async def list_node_interfaces(
    cluster_id: str,
    vm_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    node_role = _assert_vm_in_cluster(vm_id, cluster)
    ifaces = await asyncio.to_thread(lambda: list(conn.compute.server_interfaces(vm_id)))
    primary_network_id = cluster.get("network_id") or ""
    return [
        K3sInterfaceInfo(
            port_id=i.port_id,
            net_id=i.net_id,
            fixed_ips=i.fixed_ips or [],
            vm_id=vm_id,
            node_role=node_role,
            is_primary=bool(primary_network_id and i.net_id == primary_network_id),
        )
        for i in ifaces
    ]


@router.post("/{cluster_id}/nodes/{vm_id}/interfaces", status_code=201)
async def attach_node_interface(
    cluster_id: str,
    vm_id: str,
    body: K3sAttachInterfaceRequest,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    node_role = _assert_vm_in_cluster(vm_id, cluster)
    try:
        if cluster.get("status") != "ACTIVE":
            detail = "ACTIVE 상태의 클러스터만 네트워크를 연결할 수 있습니다"
            await rec(
                token_info,
                conn,
                resource_type="k3s_cluster",
                action="k3s.attach_interface",
                status="failed",
                resource_id=cluster_id,
                error_message=detail,
            )
            raise HTTPException(status_code=409, detail=detail)

        current_ifaces = await asyncio.to_thread(lambda: list(conn.compute.server_interfaces(vm_id)))
        primary_network_id = cluster.get("network_id") or ""
        if body.net_id == primary_network_id or any(i.net_id == body.net_id for i in current_ifaces):
            detail = "이미 연결된 네트워크입니다"
            await rec(
                token_info,
                conn,
                resource_type="k3s_cluster",
                action="k3s.attach_interface",
                status="failed",
                resource_id=cluster_id,
                error_message=detail,
            )
            raise HTTPException(status_code=409, detail=detail)

        result = await asyncio.to_thread(nova.attach_interface, conn, vm_id, body.net_id)
        await invalidate(f"afterglow:neutron:{project_id}:ports:{vm_id}")
        await invalidate(f"afterglow:neutron:{project_id}:port_mac_map")
        await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}")
        await rec(
            token_info,
            conn,
            resource_type="k3s_cluster",
            action="k3s.attach_interface",
            status="success",
            resource_id=cluster_id,
            extra={"vm_id": vm_id, "net_id": body.net_id, "node_role": node_role},
        )
        return K3sInterfaceInfo(
            port_id=result["port_id"],
            net_id=result["net_id"],
            fixed_ips=result["fixed_ips"],
            vm_id=vm_id,
            node_role=node_role,
            is_primary=False,
        )
    except HTTPException:
        raise
    except Exception as e:
        await rec(
            token_info,
            conn,
            resource_type="k3s_cluster",
            action="k3s.attach_interface",
            status="failed",
            resource_id=cluster_id,
            error_message=str(e)[:500],
        )
        raise HTTPException(status_code=500, detail="인터페이스 attach 실패")


@router.delete("/{cluster_id}/nodes/{vm_id}/interfaces/{port_id}", status_code=204)
async def detach_node_interface(
    cluster_id: str,
    vm_id: str,
    port_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")
    node_role = _assert_vm_in_cluster(vm_id, cluster)
    try:
        current_ifaces = await asyncio.to_thread(lambda: list(conn.compute.server_interfaces(vm_id)))
        resolved = next((iface for iface in current_ifaces if iface.port_id == port_id), None)
        if resolved is None:
            detail = "인터페이스를 찾을 수 없습니다"
            await rec(
                token_info,
                conn,
                resource_type="k3s_cluster",
                action="k3s.detach_interface",
                status="failed",
                resource_id=cluster_id,
                error_message=detail,
            )
            raise HTTPException(status_code=404, detail=detail)

        primary_network_id = cluster.get("network_id") or ""
        if not primary_network_id:
            detail = "기본 인터페이스를 판별할 수 없습니다"
            await rec(
                token_info,
                conn,
                resource_type="k3s_cluster",
                action="k3s.detach_interface",
                status="failed",
                resource_id=cluster_id,
                error_message=detail,
            )
            raise HTTPException(status_code=409, detail=detail)
        if resolved.net_id == primary_network_id:
            detail = "기본 인터페이스는 제거할 수 없습니다"
            await rec(
                token_info,
                conn,
                resource_type="k3s_cluster",
                action="k3s.detach_interface",
                status="failed",
                resource_id=cluster_id,
                error_message=detail,
            )
            raise HTTPException(status_code=409, detail=detail)

        await asyncio.to_thread(nova.detach_interface, conn, vm_id, port_id)
        await invalidate(f"afterglow:neutron:{project_id}:ports:{vm_id}")
        await invalidate(f"afterglow:neutron:{project_id}:port_mac_map")
        await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}")
        await rec(
            token_info,
            conn,
            resource_type="k3s_cluster",
            action="k3s.detach_interface",
            status="success",
            resource_id=cluster_id,
            extra={"vm_id": vm_id, "port_id": port_id, "node_role": node_role},
        )
    except HTTPException:
        raise
    except Exception as e:
        await rec(
            token_info,
            conn,
            resource_type="k3s_cluster",
            action="k3s.detach_interface",
            status="failed",
            resource_id=cluster_id,
            error_message=str(e)[:500],
        )
        raise HTTPException(status_code=500, detail="인터페이스 detach 실패")


# ---------------------------------------------------------------------------
# Stampede 오토스케일 API
# ---------------------------------------------------------------------------


@router.post("/{cluster_id}/stampede/enable", status_code=200)
async def enable_stampede(
    cluster_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """클러스터의 Stampede 오토스케일 모드를 활성화한다."""

    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    if not get_settings().drover_stampede_enabled:
        raise HTTPException(status_code=400, detail="Stampede 기능이 서버에서 비활성화 상태입니다")

    await k3s_cluster.update_cluster_status(project_id, cluster_id, cluster["status"], "")
    # stampede_enabled 컬럼 갱신
    from sqlalchemy import select as _select

    from drover.db import get_session_factory, is_db_available
    from drover.models.orm import K3sCluster

    if is_db_available():
        factory = get_session_factory()
        async with factory() as session:
            stmt = _select(K3sCluster).where(K3sCluster.id == cluster_id)
            result = await session.execute(stmt)
            c = result.scalar_one_or_none()
            if c:
                c.stampede_enabled = True
                await session.commit()

    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.stampede_enable",
        status="success",
        resource_id=cluster_id,
    )
    return {"message": "Stampede 모드가 활성화되었습니다", "cluster_id": cluster_id}


@router.post("/{cluster_id}/stampede/disable", status_code=200)
async def disable_stampede(
    cluster_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """클러스터의 Stampede 오토스케일 모드를 비활성화한다."""
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    from sqlalchemy import select as _select

    from drover.db import get_session_factory, is_db_available
    from drover.models.orm import K3sCluster

    if is_db_available():
        factory = get_session_factory()
        async with factory() as session:
            stmt = _select(K3sCluster).where(K3sCluster.id == cluster_id)
            result = await session.execute(stmt)
            c = result.scalar_one_or_none()
            if c:
                c.stampede_enabled = False
                await session.commit()

    await invalidate(f"afterglow:k3s:{project_id}:cluster:{cluster_id}")
    await rec(
        token_info,
        conn,
        resource_type="k3s_cluster",
        action="k3s.stampede_disable",
        status="success",
        resource_id=cluster_id,
    )
    return {"message": "Stampede 모드가 비활성화되었습니다", "cluster_id": cluster_id}


@router.get("/{cluster_id}/stampede")
async def get_stampede_status(
    cluster_id: str,
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """클러스터의 Stampede 오토스케일 상태를 조회한다."""
    from drover.services import nodegroup as _k3s_ng

    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    all_ngs = await _k3s_ng.list_nodegroups(cluster_id)
    stampede_ngs = []
    for ng in all_ngs:
        state = ng.get("stampede_state") or {}
        stampede_ngs.append(
            {
                "id": ng["id"],
                "name": ng["name"],
                "role": ng.get("role"),
                "flavor_id": ng.get("flavor_id"),
                "stampede_enabled": ng["stampede_enabled"],
                "min_size": ng["min_size"],
                "max_size": ng["max_size"],
                "node_count": ng["node_count"],
                "in_flight": int(state.get("in_flight_count", 0) or 0),
                "capacity": state.get("capacity") or {},
                "pending_assignments": state.get("pending_assignments") or [],
                "blocked_reasons": state.get("blocked_reasons") or [],
                "last_decision": state.get("last_decision") or "",
                "last_blocked_reason": state.get("last_blocked_reason") or "",
                "flavor_summary": state.get("flavor_summary") or {},
                "quota_state": state.get("quota_state") or {},
                "stampede_state": state,
            }
        )

    return {
        "cluster_id": cluster_id,
        "stampede_enabled": bool(cluster.get("stampede_enabled", False)),
        "global_stampede_enabled": get_settings().drover_stampede_enabled,
        "nodegroups": stampede_ngs,
    }


@router.get("/{cluster_id}/stampede/events")
async def get_stampede_events(
    cluster_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    conn: openstack.connection.Connection = Depends(get_os_conn),
    token_info: dict = Depends(get_token_info),
):
    """Stampede 스케일 이벤트 이력 조회 (최신순)."""
    project_id = conn._afterglow_project_id
    cluster = await k3s_cluster.get_cluster(project_id, cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="클러스터를 찾을 수 없습니다")

    from drover.services.activity import list_stampede_events

    return await list_stampede_events(cluster_id, limit)
