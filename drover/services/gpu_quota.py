"""GPU 프로젝트별 Quota 관리 서비스.

DB가 초기화되어 있어야 동작 (is_db_available() 확인 후 호출).

기본 정책: quota 미설정 시 0 (GPU VM 생성 불가). 관리자가 명시적으로 quota를 설정해야 함.
전체 프로젝트 기본 quota는 project_id = "__default__"로 저장.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, datetime

from sqlalchemy import delete, select

from drover.db import get_session_factory, is_db_available

_logger = logging.getLogger(__name__)

DEFAULT_PROJECT_ID = "__default__"


def normalize_gpu_alias(alias: str) -> str:
    """GPU alias를 대표(canonical) 이름으로 정규화.

    구두점(하이픈, 언더스코어, 공백, 점 등) 및 대소문자 차이를 흡수하여 대문자 알파벳/숫자로 반환.
    예: "RTX-3090" → "RTX3090", "rtx_3090" → "RTX3090", "TITAN-X" → "TITANX"
    audio alias는 제외 (빈 문자열 반환).
    """
    if not alias:
        return ""
    if "audio" in alias.lower():
        return ""
    return re.sub(r"[^a-zA-Z0-9]", "", alias).upper()


def validate_quota_params(gpu_type: str, limit: int) -> str:
    """gpu_type 및 limit 입력값 검증 후 정규화된 gpu_type 반환."""
    if not gpu_type or not gpu_type.strip():
        raise ValueError("gpu_type은 비어있을 수 없습니다")
    if len(gpu_type) > 64:
        raise ValueError("gpu_type은 최대 64자까지 허용됩니다")
    if "audio" in gpu_type.lower():
        raise ValueError("오디오 디바이스 alias는 GPU quota 항목으로 사용할 수 없습니다")
    if limit < -1:
        raise ValueError("quota limit은 -1 이상이어야 합니다 (-1 = 무제한)")
    canonical = normalize_gpu_alias(gpu_type)
    if not canonical:
        raise ValueError("유효하지 않은 gpu_type입니다")
    return canonical


def _parse_alias_counts(extra_specs: dict) -> dict[str, int]:
    """flavor extra_specs의 pci_passthrough:alias에서 PCI alias → count 매핑 반환.

    예: "RTX3090:1,RTX3090Audio:1" → {"RTX3090": 1}  (Audio 제외)
    """
    alias_str = extra_specs.get("pci_passthrough:alias", "")
    result: dict[str, int] = {}
    if not alias_str:
        return result
    for entry in alias_str.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        alias, _, num_str = entry.rpartition(":")
        alias = alias.strip()
        if "audio" in alias.lower():
            continue
        try:
            cnt = int(num_str)
        except ValueError:
            cnt = 1
        result[alias] = result.get(alias, 0) + cnt
    return result


async def get_project_gpu_quotas(project_id: str) -> list[dict]:
    """프로젝트의 GPU quota 목록 반환."""
    if not is_db_available():
        raise RuntimeError("DB가 초기화되지 않았습니다")
    factory = get_session_factory()
    if not factory:
        raise RuntimeError("DB가 초기화되지 않았습니다")
    from drover.models.orm import GpuQuota

    async with factory() as session:
        rows = await session.execute(select(GpuQuota).where(GpuQuota.project_id == project_id))
        return [
            {
                "gpu_type": normalize_gpu_alias(r.gpu_type) or r.gpu_type,
                "limit": r.limit,
                "id": r.id,
            }
            for r in rows.scalars().all()
        ]


async def set_project_gpu_quota(project_id: str, gpu_type: str, limit: int) -> dict:
    """프로젝트의 GPU quota upsert."""
    canonical_type = validate_quota_params(gpu_type, limit)
    if not is_db_available():
        raise RuntimeError("DB가 초기화되지 않았습니다")
    factory = get_session_factory()
    if not factory:
        raise RuntimeError("DB가 초기화되지 않았습니다")
    from drover.models.orm import GpuQuota

    async with factory() as session, session.begin():
        result = await session.execute(
            select(GpuQuota).where(GpuQuota.project_id == project_id, GpuQuota.gpu_type == canonical_type)
        )
        row = result.scalar_one_or_none()
        now = datetime.now(UTC)
        if row:
            row.limit = limit
            row.updated_at = now
        else:
            row = GpuQuota(project_id=project_id, gpu_type=canonical_type, limit=limit, created_at=now, updated_at=now)
            session.add(row)
    return {"project_id": project_id, "gpu_type": canonical_type, "limit": limit}


async def delete_project_gpu_quota(project_id: str, gpu_type: str) -> None:
    """프로젝트의 특정 GPU quota 삭제."""
    if not is_db_available():
        raise RuntimeError("DB가 초기화되지 않았습니다")
    factory = get_session_factory()
    if not factory:
        raise RuntimeError("DB가 초기화되지 않았습니다")
    canonical_type = normalize_gpu_alias(gpu_type) or gpu_type
    from drover.models.orm import GpuQuota

    async with factory() as session, session.begin():
        await session.execute(
            delete(GpuQuota).where(GpuQuota.project_id == project_id, GpuQuota.gpu_type == canonical_type)
        )


async def get_project_gpu_usage(conn, project_id: str) -> dict[str, int]:
    """프로젝트의 현재 GPU 사용량을 인스턴스 flavor 기반으로 집계.

    conn의 프로젝트와 target project_id가 다른 경우 (admin이 다른 프로젝트 조회)
    all_projects=True + project_id 필터로 해당 프로젝트 서버를 조회.
    반환: {alias: count} (예: {"RTX3090": 2})
    """
    from drover.services import nova

    def _collect():
        conn_project = getattr(conn, "_afterglow_project_id", None)
        if conn_project and conn_project != project_id:
            # admin이 다른 프로젝트를 조회하는 경우
            servers_raw = list(conn.compute.servers(details=True, all_projects=True, project_id=project_id))
        else:
            # 자기 프로젝트 조회
            servers_raw = list(conn.compute.servers(details=True))

        all_flavors = nova.list_flavors(conn)
        flavors_by_id = {f.id: f for f in all_flavors}
        flavors_by_name = {f.name: f for f in all_flavors}
        usage: dict[str, int] = {}
        for s in servers_raw:
            if s.status not in ("ACTIVE", "SHUTOFF", "PAUSED", "SUSPENDED", "RESIZE"):
                continue
            flavor = s.flavor if hasattr(s, "flavor") else {}
            if isinstance(flavor, dict):
                flavor_id = flavor.get("id", "")
                flavor_name = flavor.get("original_name", "")
            else:
                flavor_id = getattr(s, "flavor_id", "") or ""
                flavor_name = getattr(s, "flavor_name", "") or ""
            fl = flavors_by_id.get(flavor_id)
            if not fl and flavor_name:
                fl = flavors_by_name.get(flavor_name)
            if not fl:
                continue
            for alias, cnt in _parse_alias_counts(fl.extra_specs or {}).items():
                canonical = normalize_gpu_alias(alias)
                if canonical:
                    usage[canonical] = usage.get(canonical, 0) + cnt
        return usage

    return await asyncio.to_thread(_collect)


async def get_effective_gpu_quotas(project_id: str) -> dict[str, int]:
    """프로젝트의 유효 GPU quota 맵 반환 (프로젝트별 > 기본값 > 0).

    반환: {alias: limit}
    """
    if not is_db_available():
        raise RuntimeError("DB가 초기화되지 않았습니다")
    project_quotas = await get_project_gpu_quotas(project_id)
    default_quotas = await get_project_gpu_quotas(DEFAULT_PROJECT_ID)

    default_map = {q["gpu_type"]: q["limit"] for q in default_quotas}
    effective: dict[str, int] = dict(default_map)

    # 프로젝트별 설정이 기본값을 오버라이드
    for q in project_quotas:
        effective[q["gpu_type"]] = q["limit"]

    return effective


async def check_gpu_quota(conn, project_id: str, flavor_extra_specs: dict) -> tuple[bool, str]:
    """VM 생성 전 GPU quota 초과 여부 확인.

    기본 정책: quota 미설정 시 0 (거부). 관리자가 명시적으로 설정해야 허용.
    반환: (ok: bool, message: str)
    """
    if not is_db_available():
        raise RuntimeError("DB가 초기화되지 않았습니다")
    raw_requested = _parse_alias_counts(flavor_extra_specs)
    if not raw_requested:
        return True, ""
    requested: dict[str, int] = {}
    for alias, cnt in raw_requested.items():
        canonical = normalize_gpu_alias(alias)
        if canonical:
            requested[canonical] = requested.get(canonical, 0) + cnt

    if not requested:
        return True, ""

    effective = await get_effective_gpu_quotas(project_id)
    usage = await get_project_gpu_usage(conn, project_id)

    for alias, count in requested.items():
        limit = effective.get(alias, 0)  # 미설정 = 0 (거부)
        if limit == -1:
            continue  # 무제한
        current = usage.get(alias, 0)
        if current + count > limit:
            if limit == 0:
                return False, f"GPU quota 미할당: {alias} — 관리자에게 GPU quota 요청이 필요합니다"
            return False, f"GPU quota 초과: {alias} — 현재 {current}개 사용 중, quota {limit}개, 요청 {count}개"
    return True, ""
