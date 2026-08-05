"""k3s 클러스터 상태 관리 — Redis CRUD + 콜백 토큰 처리."""

import asyncio
import json
import logging
import secrets
from datetime import UTC, datetime, timedelta

from drover.services.cache import _get_client
from drover.crypto import decrypt_kubeconfig, encrypt_kubeconfig

_logger = logging.getLogger(__name__)

_CALLBACK_TTL = 1800  # 30분


# ---------------------------------------------------------------------------
# 키 헬퍼
# ---------------------------------------------------------------------------


def _cluster_key(project_id: str, cluster_id: str) -> str:
    return f"afterglow:k3s:{project_id}:cluster:{cluster_id}"


def _ha_callback_key(token: str) -> str:
    return f"afterglow:k3s:ha_cb:{token}"


def _ha_join_key(cluster_id: str) -> str:
    return f"afterglow:k3s:{cluster_id}:ha_joined"


def _clusters_set_key(project_id: str) -> str:
    return f"afterglow:k3s:{project_id}:clusters"


def _kubeconfig_key(project_id: str, cluster_id: str) -> str:
    return f"afterglow:k3s:{project_id}:kubeconfig:{cluster_id}"


def _callback_key(token: str) -> str:
    return f"afterglow:k3s:callback:{token}"


# ---------------------------------------------------------------------------
# Read-only dashboard statistics
# ---------------------------------------------------------------------------

_DASHBOARD_STATS_TIMEOUT_SECONDS = 0.5
_DASHBOARD_STATS_SCAN_COUNT = 200
_DASHBOARD_STATS_MAX_ITERATIONS = 32
_DASHBOARD_STATS_MAX_IDS = 1000


class K3sStatsUnavailable(RuntimeError):
    """The bounded, read-only dashboard source could not be read completely."""


def _decode_redis_value(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


async def _read_set_cluster_ids(client, membership_key: str) -> set[str]:
    cursor = 0
    cluster_ids: set[str] = set()
    for _ in range(_DASHBOARD_STATS_MAX_ITERATIONS):
        cursor, values = await client.sscan(
            membership_key,
            cursor=cursor,
            count=_DASHBOARD_STATS_SCAN_COUNT,
        )
        cluster_ids.update(_decode_redis_value(value) for value in values)
        if len(cluster_ids) > _DASHBOARD_STATS_MAX_IDS:
            raise K3sStatsUnavailable("K3s membership cap exceeded")
        if int(cursor) == 0:
            return cluster_ids
    raise K3sStatsUnavailable("K3s membership cursor did not complete")


async def _scan_cluster_hash_ids(client, project_id: str) -> set[str]:
    cursor = 0
    candidates = 0
    candidate_keys: list[str] = []
    pattern = f"afterglow:k3s:{project_id}:cluster:*"
    for _ in range(_DASHBOARD_STATS_MAX_ITERATIONS):
        cursor, values = await client.scan(
            cursor=cursor,
            match=pattern,
            count=_DASHBOARD_STATS_SCAN_COUNT,
        )
        candidates += len(values)
        if candidates > _DASHBOARD_STATS_MAX_IDS:
            raise K3sStatsUnavailable("K3s cluster candidate cap exceeded")
        for value in values:
            key = _decode_redis_value(value)
            parts = key.split(":")
            if (
                len(parts) == 5
                and parts[0] == "afterglow"
                and parts[1] == "k3s"
                and parts[2] == project_id
                and parts[3] == "cluster"
                and parts[4]
            ):
                candidate_keys.append(key)
        if int(cursor) != 0:
            continue

        cluster_ids: set[str] = set()
        for start in range(0, len(candidate_keys), _DASHBOARD_STATS_SCAN_COUNT):
            chunk = candidate_keys[start : start + _DASHBOARD_STATS_SCAN_COUNT]
            pipeline = client.pipeline(transaction=False)
            for key in chunk:
                pipeline.type(key)
            key_types = await pipeline.execute()
            for key, key_type in zip(chunk, key_types, strict=True):
                if _decode_redis_value(key_type) != "hash":
                    continue
                cluster_ids.add(key.rsplit(":", 1)[1])
                if len(cluster_ids) > _DASHBOARD_STATS_MAX_IDS:
                    raise K3sStatsUnavailable("K3s cluster ID cap exceeded")
        return cluster_ids
    raise K3sStatsUnavailable("K3s cluster cursor did not complete")


async def dashboard_cluster_stats(project_id: str) -> dict[str, int]:
    """Return bounded project counts without changing any K3s source key.

    This is intentionally separate from ``list_clusters``.  It avoids the
    existing HTTP-cache/source-key collision and must never repair, cache, or
    invalidate Redis state.
    """
    try:
        async with asyncio.timeout(_DASHBOARD_STATS_TIMEOUT_SECONDS):
            client = _get_client()
            membership_key = _clusters_set_key(project_id)
            key_type = _decode_redis_value(await client.type(membership_key))
            if key_type == "set":
                cluster_ids = await _read_set_cluster_ids(client, membership_key)
            else:
                cluster_ids = await _scan_cluster_hash_ids(client, project_id)

            total = 0
            active = 0
            ids = sorted(cluster_ids)
            for start in range(0, len(ids), _DASHBOARD_STATS_SCAN_COUNT):
                chunk = ids[start : start + _DASHBOARD_STATS_SCAN_COUNT]
                pipeline = client.pipeline(transaction=False)
                for cluster_id in chunk:
                    pipeline.hmget(
                        _cluster_key(project_id, cluster_id),
                        "status",
                        "provisioning_status",
                        "deleted_at",
                    )
                rows = await pipeline.execute()
                for row in rows:
                    if not row or all(value is None for value in row):
                        continue
                    status, provisioning_status, deleted_at = (list(row) + [None, None, None])[:3]
                    if deleted_at not in (None, "", b""):
                        continue
                    total += 1
                    if status in ("ACTIVE", b"ACTIVE") or provisioning_status in ("ACTIVE", b"ACTIVE"):
                        active += 1
            return {"total": total, "active": active}
    except TimeoutError as exc:
        raise K3sStatsUnavailable("K3s dashboard stats timed out") from exc


# ---------------------------------------------------------------------------
# Cluster CRUD
# ---------------------------------------------------------------------------


async def create_cluster_record(project_id: str, cluster_id: str, data: dict) -> None:
    """클러스터 HASH 생성 + 프로젝트 SET에 ID 추가."""
    client = _get_client()
    key = _cluster_key(project_id, cluster_id)
    # HASH는 모든 값을 문자열로 저장
    str_data = {
        k: json.dumps(v) if isinstance(v, (list, dict)) else str(v) if v is not None else "" for k, v in data.items()
    }
    await client.hset(key, mapping=str_data)
    await client.sadd(_clusters_set_key(project_id), cluster_id)


async def get_cluster(project_id: str, cluster_id: str) -> dict | None:
    """클러스터 HASH → dict 반환. 없으면 None."""
    client = _get_client()
    raw = await client.hgetall(_cluster_key(project_id, cluster_id))
    if not raw:
        return None
    result: dict = {}
    for k, v in raw.items():
        k_str = k.decode() if isinstance(k, bytes) else k
        v_str = v.decode() if isinstance(v, bytes) else v
        # agent_vm_ids는 JSON 배열로 저장
        if k_str == "agent_vm_ids":
            try:
                result[k_str] = json.loads(v_str) if v_str else []
            except Exception:
                result[k_str] = []
        else:
            result[k_str] = v_str if v_str != "" else None
    result["id"] = cluster_id
    return result


async def list_clusters(project_id: str) -> list[dict]:
    """프로젝트의 모든 클러스터 목록 반환."""
    client = _get_client()
    ids = await client.smembers(_clusters_set_key(project_id))
    clusters = []
    for cid_bytes in ids:
        cid = cid_bytes.decode() if isinstance(cid_bytes, bytes) else cid_bytes
        cluster = await get_cluster(project_id, cid)
        if cluster:
            clusters.append(cluster)
    clusters.sort(key=lambda c: c.get("created_at") or "", reverse=True)
    return clusters


async def list_all_clusters() -> list[dict]:
    """전체 프로젝트의 k3s 클러스터 목록 반환 (관리자용)."""
    client = _get_client()
    all_clusters = []
    async for key in client.scan_iter(match="afterglow:k3s:*:clusters", count=100):
        key_str = key.decode() if isinstance(key, bytes) else key
        parts = key_str.split(":")
        if len(parts) < 4:
            continue
        pid = parts[2]
        clusters = await list_clusters(pid)
        for c in clusters:
            c["project_id"] = pid
        all_clusters.extend(clusters)
    all_clusters.sort(key=lambda c: c.get("created_at") or "", reverse=True)
    return all_clusters


async def update_cluster_status(
    project_id: str,
    cluster_id: str,
    status: str,
    status_reason: str | None = None,
    **extra_fields,
) -> None:
    """클러스터 status + updated_at + 추가 필드 업데이트."""
    client = _get_client()
    key = _cluster_key(project_id, cluster_id)
    updates: dict = {
        "status": status,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    if status_reason is not None:
        updates["status_reason"] = status_reason
    for k, v in extra_fields.items():
        if isinstance(v, (list, dict)):
            updates[k] = json.dumps(v)
        elif v is None:
            updates[k] = ""
        else:
            updates[k] = str(v)
    await client.hset(key, mapping=updates)


async def delete_cluster_record(project_id: str, cluster_id: str) -> None:
    """클러스터 HASH, kubeconfig 키, SET 항목 삭제."""
    client = _get_client()
    await client.delete(_cluster_key(project_id, cluster_id))
    await client.delete(_kubeconfig_key(project_id, cluster_id))
    await client.srem(_clusters_set_key(project_id), cluster_id)


# ---------------------------------------------------------------------------
# 콜백 토큰
# ---------------------------------------------------------------------------


async def create_callback_token(project_id: str, cluster_id: str) -> str:
    """일회성 콜백 토큰 생성 (TTL 30분)."""
    token = secrets.token_urlsafe(48)
    client = _get_client()
    payload = json.dumps({"project_id": project_id, "cluster_id": cluster_id})
    await client.setex(_callback_key(token), _CALLBACK_TTL, payload)
    return token


async def consume_callback_token(token: str) -> dict | None:
    """콜백 토큰을 원자적으로 GET+DELETE. 없거나 만료되면 None."""
    client = _get_client()
    key = _callback_key(token)
    raw = await client.getdel(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Kubeconfig
# ---------------------------------------------------------------------------


async def store_kubeconfig(project_id: str, cluster_id: str, kubeconfig_yaml: str) -> None:
    """kubeconfig를 AES-256-GCM 암호화하여 Redis에 저장."""
    client = _get_client()
    encrypted = encrypt_kubeconfig(kubeconfig_yaml)
    await client.set(_kubeconfig_key(project_id, cluster_id), encrypted)


async def get_kubeconfig(project_id: str, cluster_id: str) -> str | None:
    """Redis에서 kubeconfig를 복호화하여 반환. 없으면 None."""
    client = _get_client()
    raw = await client.get(_kubeconfig_key(project_id, cluster_id))
    if not raw:
        return None
    raw_str = raw.decode() if isinstance(raw, bytes) else raw
    return decrypt_kubeconfig(raw_str)


# ---------------------------------------------------------------------------
# HA 콜백 토큰 (server_index 포함)
# ---------------------------------------------------------------------------


async def create_ha_callback_token(project_id: str, cluster_id: str, server_index: int) -> str:
    """HA 조인용 일회성 콜백 토큰 (TTL 30분). server_index=1이 초기화 서버."""
    token = secrets.token_urlsafe(48)
    client = _get_client()
    payload = json.dumps({"project_id": project_id, "cluster_id": cluster_id, "server_index": server_index})
    await client.setex(_ha_callback_key(token), _CALLBACK_TTL, payload)
    return token


async def consume_ha_callback_token(token: str) -> dict | None:
    """HA 콜백 토큰을 원자적으로 GET+DELETE. 없거나 만료되면 None."""
    client = _get_client()
    raw = await client.getdel(_ha_callback_key(token))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def get_ha_join_count(cluster_id: str) -> int:
    """HA 서버 조인 완료 수 조회. 키 없으면 0."""
    client = _get_client()
    raw = await client.get(_ha_join_key(cluster_id))
    if not raw:
        return 0
    try:
        return int(raw)
    except Exception:
        return 0


async def incr_ha_join_count(cluster_id: str) -> int:
    """HA 서버 조인 카운터 증가(INCR). TTL=2h. 증가 후 값 반환."""
    client = _get_client()
    key = _ha_join_key(cluster_id)
    count = await client.incr(key)
    await client.expire(key, 7200)
    return int(count)


# ---------------------------------------------------------------------------
# 스테일 클러스터 정리
# ---------------------------------------------------------------------------


async def check_stale_clusters(timeout_minutes: int = 30) -> None:
    """CREATING 상태에서 timeout_minutes 초과한 클러스터를 ERROR로 변경."""
    client = _get_client()
    # 모든 프로젝트의 k3s 클러스터 SET 키 스캔
    cutoff = datetime.now(UTC) - timedelta(minutes=timeout_minutes)
    try:
        async for key in client.scan_iter("afterglow:k3s:*:clusters"):
            key_str = key.decode() if isinstance(key, bytes) else key
            # afterglow:k3s:{project_id}:clusters 형식에서 project_id 추출
            parts = key_str.split(":")
            if len(parts) != 4:
                continue
            project_id = parts[2]
            ids = await client.smembers(key_str)
            for cid_bytes in ids:
                cid = cid_bytes.decode() if isinstance(cid_bytes, bytes) else cid_bytes
                cluster = await get_cluster(project_id, cid)
                if not cluster or cluster.get("status") not in ("CREATING", "PROVISIONING"):
                    continue
                created_at_str = cluster.get("created_at")
                if not created_at_str:
                    continue
                try:
                    created_at = datetime.fromisoformat(created_at_str)
                    if created_at < cutoff:
                        await update_cluster_status(
                            project_id, cid, "ERROR", "콜백 타임아웃: 서버 VM이 k3s 설치 후 응답하지 않았습니다."
                        )
                        _logger.warning("k3s cluster %s marked as ERROR (stale)", cid)
                except Exception:
                    pass
    except Exception as e:
        _logger.warning("k3s stale cluster check error: %s", e)
