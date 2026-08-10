"""Database authority for discovered OpenStack policies and scalar runtime settings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from drover.db import get_session_factory, is_db_available, mark_db_unhealthy
from drover.models.orm import ResourcePolicy, RuntimeSetting
from drover.services.resource_policies import (
    PolicySpec,
    ResourcePolicyValidationError,
    get_spec,
    list_specs,
    validate_existing_selection,
    validate_selection,
)


class ResourcePolicyStorageUnavailable(RuntimeError):
    """Database authority is unavailable; callers must fail closed."""


class RuntimeSettingValidationError(ValueError):
    """Runtime-setting value violates its allowlisted contract."""


@dataclass(frozen=True)
class RuntimeSettingSpec:
    key: str
    title: str
    help_text: str


RUNTIME_SETTING_SPECS = {
    "k3s.version": RuntimeSettingSpec("k3s.version", "K3s version", "Version used for new K3s clusters."),
}


def _require_db():
    if not is_db_available() or (factory := get_session_factory()) is None:
        raise ResourcePolicyStorageUnavailable("resource policy storage is unavailable")
    return factory


def _public(row: ResourcePolicy | None, spec: PolicySpec) -> dict[str, Any]:
    resource_id = row.resource_id if row else None
    return {
        "key": spec.key,
        "resource_kind": spec.resource_kind,
        "title": spec.title,
        "group": spec.group,
        "help_text": spec.help_text,
        "execution_scope": spec.execution_scope,
        "dependency": spec.dependency,
        "required_when": spec.required_when,
        "external_only": spec.external_only,
        "shared_only": spec.shared_only,
        "resource_id": resource_id,
        "resource_name": row.resource_name if row else None,
        "constraints": row.constraints if row else None,
        "updated_by_user_id": row.updated_by_user_id if row else None,
        "updated_at": row.updated_at.isoformat() if row and row.updated_at else None,
        "state": "missing" if not resource_id else "configured",
    }


def _runtime_public(row: RuntimeSetting | None, spec: RuntimeSettingSpec) -> dict[str, Any]:
    return {
        "key": spec.key,
        "title": spec.title,
        "help_text": spec.help_text,
        "value": row.value_json if row else None,
        "updated_by_user_id": row.updated_by_user_id if row else None,
        "updated_at": row.updated_at.isoformat() if row and row.updated_at else None,
        "state": "missing" if row is None else "configured",
    }


async def list_policies() -> list[dict[str, Any]]:
    factory = _require_db()
    try:
        async with factory() as session:
            rows = (await session.execute(select(ResourcePolicy))).scalars().all()
            by_key = {row.policy_key: row for row in rows}
            return [_public(by_key.get(spec.key), spec) for spec in list_specs()]
    except OperationalError as exc:
        mark_db_unhealthy()
        raise ResourcePolicyStorageUnavailable("resource policy storage failed") from exc


async def inspect_policies(conn) -> list[dict[str, Any]]:
    """Return policy rows annotated with current exact-ID validation state."""
    policies = await list_policies()
    for policy in policies:
        if policy["state"] == "missing":
            continue
        spec = get_spec(policy["key"])
        try:
            selected = await validate_existing_selection(conn, spec.key, policy["resource_id"])
            policy["resolved_name"] = selected["name"]
        except ResourcePolicyValidationError:
            policy["state"] = "stale"
        except Exception:
            policy["state"] = "unavailable"
    return policies


async def get_policy_snapshot(keys: tuple[str, ...]) -> dict[str, dict[str, str] | None]:
    """Return stored IDs and display-name snapshots without OpenStack access."""
    specs = {key: get_spec(key) for key in keys}
    factory = _require_db()
    try:
        async with factory() as session:
            rows = (
                (await session.execute(select(ResourcePolicy).where(ResourcePolicy.policy_key.in_(specs))))
                .scalars()
                .all()
            )
    except OperationalError as exc:
        mark_db_unhealthy()
        raise ResourcePolicyStorageUnavailable("resource policy storage failed") from exc
    by_key = {row.policy_key: row for row in rows}
    return {
        key: (
            {"id": row.resource_id, "name": row.resource_name or row.resource_id}
            if (row := by_key.get(key)) and row.resource_id
            else None
        )
        for key in specs
    }


async def set_policy(*, conn, key: str, resource_id: str | None, updated_by_user_id: str) -> dict[str, Any]:
    spec = get_spec(key)
    selected = await validate_selection(conn, key, resource_id)
    factory = _require_db()
    try:
        async with factory() as session, session.begin():
            row = await session.get(ResourcePolicy, spec.key, with_for_update=True)
            if row is None:
                row = ResourcePolicy(policy_key=spec.key, resource_kind=spec.resource_kind)
                session.add(row)
            row.resource_id = selected["id"] if selected else None
            row.resource_name = selected["name"] if selected else None
            row.constraints = {
                "external_only": spec.external_only,
                "shared_only": spec.shared_only,
                "execution_scope": spec.execution_scope,
            }
            row.updated_by_user_id = updated_by_user_id
            await session.flush()
            result = _public(row, spec)
    except OperationalError as exc:
        mark_db_unhealthy()
        raise ResourcePolicyStorageUnavailable("resource policy storage failed") from exc
    return result


async def resolve_policy_snapshot(*, conn, keys: tuple[str, ...]) -> dict[str, dict[str, str]]:
    """Validate and freeze effective IDs/names in each policy's real execution scope."""
    specs: dict[str, PolicySpec] = {key: get_spec(key) for key in keys}
    stored = await get_policy_snapshot(tuple(specs))
    missing = [key for key, selected in stored.items() if selected is None]
    if missing:
        raise ResourcePolicyValidationError(f"required resource policies are not configured: {', '.join(missing)}")

    resolved: dict[str, dict[str, str]] = {}
    for key, _spec in specs.items():
        selected = await validate_existing_selection(conn, key, stored[key]["id"])
        resolved[key] = {"id": selected["id"], "name": selected["name"]}
    return resolved


async def resolve_policies(*, conn, keys: tuple[str, ...]) -> dict[str, str]:
    """Compatibility helper returning only immutable validated IDs."""
    snapshot = await resolve_policy_snapshot(conn=conn, keys=keys)
    return {key: value["id"] for key, value in snapshot.items()}


def _runtime_spec(key: str) -> RuntimeSettingSpec:
    try:
        return RUNTIME_SETTING_SPECS[key]
    except KeyError as exc:
        raise RuntimeSettingValidationError("unknown runtime setting") from exc


def _validate_runtime_value(key: str, value: object) -> object:
    if key == "k3s.version":
        if not isinstance(value, str) or not value.strip() or len(value.strip()) > 32:
            raise RuntimeSettingValidationError("k3s.version must be a nonblank version string")
        return value.strip()
    raise RuntimeSettingValidationError("unknown runtime setting")


async def list_runtime_settings() -> list[dict[str, Any]]:
    factory = _require_db()
    try:
        async with factory() as session:
            rows = (await session.execute(select(RuntimeSetting))).scalars().all()
    except OperationalError as exc:
        mark_db_unhealthy()
        raise ResourcePolicyStorageUnavailable("runtime setting storage failed") from exc
    by_key = {row.setting_key: row for row in rows}
    return [_runtime_public(by_key.get(key), spec) for key, spec in RUNTIME_SETTING_SPECS.items()]


async def get_runtime_setting(key: str) -> object | None:
    _runtime_spec(key)
    factory = _require_db()
    try:
        async with factory() as session:
            row = await session.get(RuntimeSetting, key)
    except OperationalError as exc:
        mark_db_unhealthy()
        raise ResourcePolicyStorageUnavailable("runtime setting storage failed") from exc
    return row.value_json if row is not None else None


async def get_required_runtime_setting(key: str) -> object:
    value = await get_runtime_setting(key)
    if value is None:
        raise RuntimeSettingValidationError(f"required runtime setting is not configured: {key}")
    return _validate_runtime_value(key, value)


async def set_runtime_setting(*, key: str, value: object, updated_by_user_id: str) -> dict[str, Any]:
    spec = _runtime_spec(key)
    normalized = _validate_runtime_value(key, value)
    factory = _require_db()
    try:
        async with factory() as session, session.begin():
            row = await session.get(RuntimeSetting, key, with_for_update=True)
            if row is None:
                row = RuntimeSetting(setting_key=key, value_json=normalized)
                session.add(row)
            else:
                row.value_json = normalized
            row.updated_by_user_id = updated_by_user_id
            await session.flush()
            return _runtime_public(row, spec)
    except OperationalError as exc:
        mark_db_unhealthy()
        raise ResourcePolicyStorageUnavailable("runtime setting storage failed") from exc
