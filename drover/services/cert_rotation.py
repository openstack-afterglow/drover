"""k3s 인증서 회전 서비스 — rolling restart via K8s Job (hostPID + nsenter).

k3s는 재시작 시 만료 90일 이내 인증서를 자동 갱신한다.
SSH 없이 K8s Job으로 각 control-plane 노드의 k3s를 순차 재시작한다.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import AsyncGenerator

from drover.models.schemas import K3sProgressMessage, K3sProgressStep
from drover.services import kube as k3s_kube
from drover.services import store as k3s_db

_logger = logging.getLogger(__name__)

_REDIS_LOCK_TTL = 900  # 15분
_JOB_NAMESPACE = "kube-system"


def _lock_key(cluster_id: str) -> str:
    return f"afterglow:k3s:{cluster_id}:rotation"


async def acquire_rotation_lock(cluster_id: str) -> bool:
    """Redis SET NX EX로 분산 락 획득. 성공 시 True, 이미 잠겨 있으면 False."""
    try:
        from drover.services.cache import _get_client

        client = _get_client()
        result = await client.set(_lock_key(cluster_id), "1", ex=_REDIS_LOCK_TTL, nx=True)
        return result is not None
    except Exception as e:
        _logger.warning("rotation lock 획득 오류: %s — 락 없이 진행", e)
        return True


async def release_rotation_lock(cluster_id: str) -> None:
    try:
        from drover.services.cache import _get_client

        client = _get_client()
        await client.delete(_lock_key(cluster_id))
    except Exception:
        pass


async def _invalidate_expiry_cache(cluster_id: str, project_id: str) -> None:
    try:
        from drover.services.cache import _get_client
        from drover.services.cache import keys as cache_keys

        key = cache_keys.project_key("k3s", project_id, cluster_id, sub="cert_expiry")
        client = _get_client()
        await client.delete(key)
    except Exception as e:
        _logger.debug("expiry 캐시 무효화 실패: %s", e)


def _build_restart_job(node_name: str, image: str) -> dict:
    suffix = uuid.uuid4().hex[:8]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"k3s-cert-rotate-{suffix}",
            "namespace": _JOB_NAMESPACE,
            "labels": {"app": "afterglow-cert-rotation", "node": node_name},
        },
        "spec": {
            "ttlSecondsAfterFinished": 300,
            "backoffLimit": 0,
            "template": {
                "spec": {
                    "hostPID": True,
                    "restartPolicy": "Never",
                    "nodeSelector": {"kubernetes.io/hostname": node_name},
                    "tolerations": [{"operator": "Exists"}],
                    "containers": [
                        {
                            "name": "k3s-restart",
                            "image": image,
                            "command": [
                                "nsenter",
                                "-t",
                                "1",
                                "-m",
                                "-u",
                                "-i",
                                "-n",
                                "-p",
                                "--",
                                "systemctl",
                                "restart",
                                "k3s",
                            ],
                            "securityContext": {"privileged": True},
                        }
                    ],
                }
            },
        },
    }


async def rotate_certificates(
    cluster_id: str,
    project_id: str,
    initiated_by: str,
    *,
    node_timeout: float = 300.0,
    job_image: str = "registry.k8s.io/util-linux/util-linux:latest",
) -> AsyncGenerator[K3sProgressMessage, None]:
    """인증서 회전 SSE 제너레이터.

    각 control-plane 노드에 K8s Job을 생성해 systemctl restart k3s 를 실행하고,
    노드가 Ready 상태로 돌아올 때까지 대기한 뒤 다음 노드로 진행한다.
    """
    start = time.monotonic()

    def _msg(
        step: K3sProgressStep,
        progress: int,
        message: str,
        error: str | None = None,
    ) -> K3sProgressMessage:
        return K3sProgressMessage(
            step=step,
            progress=progress,
            message=message,
            cluster_id=cluster_id,
            error=error,
            elapsed_seconds=round(time.monotonic() - start, 1),
        )

    # control-plane 노드 검색
    yield _msg(K3sProgressStep.ROTATE_DISCOVER, 5, "control-plane 노드 검색 중...")
    try:
        server_nodes = await k3s_kube.list_server_nodes(cluster_id)
    except Exception as e:
        yield _msg(K3sProgressStep.FAILED, 0, "노드 목록 조회 실패", error=str(e))
        return

    if not server_nodes:
        yield _msg(K3sProgressStep.FAILED, 0, "control-plane 노드를 찾을 수 없습니다", error="no_nodes")
        return

    n = len(server_nodes)
    yield _msg(
        K3sProgressStep.ROTATE_DISCOVER,
        10,
        f"{n}개 control-plane 노드 발견: {', '.join(server_nodes)}",
    )

    # 노드별 rolling restart
    for i, node_name in enumerate(server_nodes):
        base = 10 + int((i / n) * 75)

        yield _msg(K3sProgressStep.ROTATE_SERVER, base, f"[{i + 1}/{n}] {node_name}: restart Job 생성 중...")

        job = _build_restart_job(node_name, job_image)
        job_name = job["metadata"]["name"]
        try:
            await k3s_kube.create_job(cluster_id, _JOB_NAMESPACE, job)
        except Exception as e:
            yield _msg(K3sProgressStep.FAILED, 0, f"Job 생성 실패: {node_name}", error=str(e))
            return

        yield _msg(
            K3sProgressStep.ROTATE_SERVER,
            base + 5,
            f"[{i + 1}/{n}] {node_name}: k3s restart 완료 대기 중 (Job: {job_name})...",
        )

        completed = await k3s_kube.wait_job_completed(cluster_id, _JOB_NAMESPACE, job_name, timeout=120.0)
        if not completed:
            yield _msg(
                K3sProgressStep.FAILED,
                0,
                f"restart Job 실패 또는 타임아웃: {job_name}",
                error="job_failed",
            )
            return

        yield _msg(K3sProgressStep.ROTATE_SERVER, base + 15, f"[{i + 1}/{n}] {node_name}: 노드 Ready 대기 중...")

        # 노드 Ready 대기 — 긴 대기 동안 SSE keepalive 전송
        ready_task = asyncio.create_task(k3s_kube.wait_node_ready(cluster_id, node_name, timeout=node_timeout))
        while not ready_task.done():
            await asyncio.sleep(10)
            if not ready_task.done():
                yield _msg(
                    K3sProgressStep.ROTATE_SERVER,
                    base + 15,
                    f"[{i + 1}/{n}] {node_name}: 노드 Ready 대기 중...",
                )

        node_ready = await ready_task
        if not node_ready:
            yield _msg(
                K3sProgressStep.FAILED,
                0,
                f"노드 Ready 타임아웃: {node_name}",
                error="node_not_ready",
            )
            return

        yield _msg(K3sProgressStep.ROTATE_SERVER, base + 24, f"[{i + 1}/{n}] {node_name}: 회전 완료 ✓")

    # 완료 처리
    yield _msg(K3sProgressStep.ROTATE_VERIFY, 88, "DB 기록 및 캐시 무효화 중...")

    try:
        await k3s_db.record_rotation(cluster_id, initiated_by)
    except Exception as e:
        _logger.warning("record_rotation 실패 (무시): %s", e)

    await _invalidate_expiry_cache(cluster_id, project_id)

    yield _msg(K3sProgressStep.COMPLETED, 100, f"{n}개 노드 인증서 회전 완료")
