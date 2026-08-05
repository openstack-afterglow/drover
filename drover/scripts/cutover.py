"""Move Drover-owned state out of the Afterglow database and Redis.

Run during a maintenance window after applying the Drover baseline migration and
before applying Afterglow migration. Source URLs are environment-only so
credentials do not appear in process listings::

    AFTERGLOW_SOURCE_DATABASE_URL=... AFTERGLOW_SOURCE_REDIS_URL=... \
      python -m drover.scripts.cutover
    AFTERGLOW_SOURCE_DATABASE_URL=... AFTERGLOW_SOURCE_REDIS_URL=... \
      python -m drover.scripts.cutover --apply

The destination database/Redis URLs and encryption key come from ``drover.conf``
or the normal ``DATABASE_URL``, ``REDIS_URL``, and
``DROVER_KUBECONFIG_ENCRYPTION_KEY`` environment variables. The copy is restart-safe:
identical rows are retained, missing rows are inserted, conflicting rows fail closed,
and seeded empty policy/setting rows are replaced with Afterglow's authoritative selections.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from drover.config import get_settings
from drover.crypto import decrypt_kubeconfig, decrypt_manager_password, decrypt_node_token


class CutoverError(RuntimeError):
    """The cutover cannot proceed without risking state loss."""


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_key: str
    columns: tuple[str, ...]
    json_columns: tuple[str, ...] = ()
    optional_source: bool = False


@dataclass(frozen=True)
class TablePlan:
    inserts: tuple[dict[str, Any], ...]
    updates: tuple[dict[str, Any], ...] = ()


_TABLES = (
    TableSpec(
        "k3s_clusters",
        "id",
        (
            "id",
            "project_id",
            "name",
            "status",
            "status_reason",
            "server_vm_id",
            "server_flavor_id",
            "agent_flavor_id",
            "server_image_id",
            "network_id",
            "security_group_id",
            "api_lb_id",
            "api_lb_pool_id",
            "api_fip_id",
            "api_fip_address",
            "server_ip",
            "api_address",
            "k3s_version",
            "node_token",
            "key_name",
            "ssh_public_key",
            "kubeconfig_encrypted",
            "created_by_user_id",
            "created_by_username",
            "agent_count",
            "occm_enabled",
            "plugins_enabled",
            "plugin_status",
            "secret_cloud_config_status",
            "os_type",
            "app_credential_id",
            "created_at",
            "updated_at",
            "deleted_at",
            "deleted_by_user_id",
            "deleted_reason",
            "master_count",
            "template_id",
            "template_snapshot",
            "resource_policy_snapshot",
            "stampede_enabled",
            "last_rotation_at",
            "last_rotation_initiated_by",
        ),
        ("plugins_enabled", "plugin_status", "template_snapshot", "resource_policy_snapshot"),
    ),
    TableSpec(
        "k3s_agent_vms",
        "id",
        (
            "id",
            "cluster_id",
            "vm_id",
            "name",
            "status",
            "created_at",
        ),
    ),
    TableSpec(
        "k3s_nodegroups",
        "id",
        (
            "id",
            "cluster_id",
            "name",
            "role",
            "node_count",
            "flavor_id",
            "image_id",
            "labels",
            "taints",
            "is_default",
            "stampede_enabled",
            "min_size",
            "max_size",
            "stampede_state",
            "created_at",
            "updated_at",
            "deleted_at",
        ),
        ("labels", "taints", "stampede_state"),
    ),
    TableSpec(
        "k3s_nodegroup_vms",
        "id",
        (
            "id",
            "nodegroup_id",
            "cluster_id",
            "vm_id",
            "name",
            "status",
            "created_at",
        ),
    ),
    TableSpec(
        "k3s_cluster_templates",
        "id",
        (
            "id",
            "name",
            "description",
            "k3s_version",
            "default_node_count",
            "default_agent_flavor_id",
            "default_image_id",
            "plugins_enabled",
            "os_type",
            "public_visible",
            "created_by",
            "created_at",
            "updated_at",
            "deleted_at",
        ),
        ("plugins_enabled",),
    ),
    TableSpec(
        "project_manager_credentials",
        "project_id",
        (
            "project_id",
            "user_id",
            "username",
            "encrypted_password",
            "created_at",
            "updated_at",
        ),
    ),
    TableSpec(
        "gpu_quotas",
        "id",
        (
            "id",
            "project_id",
            "gpu_type",
            "limit",
            "created_at",
            "updated_at",
        ),
    ),
    TableSpec(
        "drover_jobs",
        "id",
        (
            "id",
            "cluster_id",
            "project_id",
            "kind",
            "status",
            "payload_json",
            "attempts",
            "last_error",
            "user_id",
            "username",
            "claimed_at",
            "created_at",
            "updated_at",
        ),
        ("payload_json",),
        optional_source=True,
    ),
)

_POLICY_TABLE = TableSpec(
    "resource_policies",
    "policy_key",
    (
        "policy_key",
        "resource_kind",
        "resource_id",
        "resource_name",
        "constraints",
        "updated_by_user_id",
        "created_at",
        "updated_at",
    ),
    ("constraints",),
)

_RUNTIME_SETTINGS_TABLE = TableSpec(
    "runtime_settings",
    "setting_key",
    (
        "setting_key",
        "value_json",
        "updated_by_user_id",
        "created_at",
        "updated_at",
    ),
    ("value_json",),
)

_POLICY_KEYS = frozenset(
    {
        "k3s.server_image",
        "k3s.fcos_image",
        "k3s.server_flavor",
        "k3s.default_agent_flavor",
        "k3s.volume_availability_zone",
        "k3s.default_network",
        "k3s.occm_floating_network",
        "k3s.occm_public_network",
        "k3s.lb_subnet",
        "k3s.api_lb_vip_network",
        "k3s.api_lb_floating_network",
        "k3s.octavia_ingress_floating_network",
    }
)

_REQUIRED_POLICY_KEYS = frozenset(
    {
        "k3s.server_image",
        "k3s.server_flavor",
        "k3s.default_agent_flavor",
        "k3s.volume_availability_zone",
        "k3s.default_network",
    }
)

_POLICY_KEY_MAPPING = {
    "nova.default_network": "k3s.default_network",
    "cinder.default_volume_availability_zone": "k3s.volume_availability_zone",
}

_REQUIRED_RUNTIME_SETTINGS = frozenset({"k3s.version"})

_REDIS_PATTERNS = (
    "afterglow:k3s:*:cluster:*",
    "afterglow:k3s:*:clusters",
    "afterglow:k3s:*:kubeconfig:*",
    "afterglow:k3s:callback:*",
    "afterglow:k3s:ha_cb:*",
    "afterglow:k3s:*:ha_joined",
    "afterglow:k3s:*:rotation",
    "afterglow:k3s-shell-ticket:*",
    "drover:health:*",
)


def _mysql_async_url(raw_url: str) -> URL:
    if not raw_url.strip():
        raise CutoverError("database URL is required")
    url = make_url(raw_url)
    if url.get_backend_name() != "mysql":
        raise CutoverError("Drover cutover supports MySQL/MariaDB databases only")
    if url.drivername in {"mysql", "mysql+asyncmy"}:
        url = url.set(drivername="mysql+aiomysql")
    if url.drivername != "mysql+aiomysql":
        raise CutoverError("database URL must use mysql, mysql+asyncmy, or mysql+aiomysql")
    return url


def _canonical(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in {"[", "{"}:
            try:
                return _canonical(json.loads(stripped))
            except json.JSONDecodeError:
                return value
        return value
    if isinstance(value, Mapping):
        return {key: _canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical(item) for item in value]
    return value


def _rows_equal(left: Mapping[str, Any], right: Mapping[str, Any], columns: Sequence[str]) -> bool:
    return all(_canonical(left.get(column)) == _canonical(right.get(column)) for column in columns)


def _seeded_policy(row: Mapping[str, Any]) -> bool:
    return row.get("resource_id") is None and row.get("resource_name") is None and row.get("updated_by_user_id") is None


def _plan_table_changes(
    spec: TableSpec,
    source_rows: Sequence[Mapping[str, Any]],
    destination_rows: Sequence[Mapping[str, Any]],
    *,
    allow_seeded_policy_updates: bool = False,
) -> TablePlan:
    source = {row[spec.primary_key]: row for row in source_rows}
    destination = {row[spec.primary_key]: row for row in destination_rows}
    if len(source) != len(source_rows) or len(destination) != len(destination_rows):
        raise CutoverError(f"duplicate primary key detected in {spec.name}")

    extra = sorted(set(destination) - set(source), key=str)
    if extra:
        raise CutoverError(f"destination {spec.name} contains rows absent from source")

    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    for key, source_row in source.items():
        destination_row = destination.get(key)
        if destination_row is None:
            inserts.append(dict(source_row))
        elif _rows_equal(source_row, destination_row, spec.columns):
            continue
        elif allow_seeded_policy_updates and _seeded_policy(destination_row):
            updates.append(dict(source_row))
        else:
            raise CutoverError(f"conflicting {spec.name} row for primary key {key!r}")
    return TablePlan(tuple(inserts), tuple(updates))


def _map_and_deduplicate_policies(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    mapped_dict: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        original_key = str(item["policy_key"])
        new_key = _POLICY_KEY_MAPPING.get(original_key, original_key)
        item["policy_key"] = new_key
        if new_key in mapped_dict:
            existing = mapped_dict[new_key]
            if not _rows_equal(existing, item, _POLICY_TABLE.columns):
                raise CutoverError(f"conflicting source resource_policies mapping for policy_key {new_key!r}")
        else:
            mapped_dict[new_key] = item
    return list(mapped_dict.values())


def _validate_policy_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    by_key = {str(row["policy_key"]): row for row in rows}
    missing = sorted(_POLICY_KEYS - set(by_key))
    if missing:
        raise CutoverError(f"Afterglow is missing Drover resource policies: {', '.join(missing)}")
    unconfigured = sorted(key for key in _REQUIRED_POLICY_KEYS if not by_key[key].get("resource_id"))
    if unconfigured:
        raise CutoverError(f"required Drover resource policies are not configured: {', '.join(unconfigured)}")


def _filter_and_validate_runtime_settings(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    settings_dict = {str(row["setting_key"]): dict(row) for row in rows}
    missing = sorted(_REQUIRED_RUNTIME_SETTINGS - set(settings_dict))
    if missing:
        raise CutoverError(f"required runtime settings are missing from Afterglow: {', '.join(missing)}")
    for key in _REQUIRED_RUNTIME_SETTINGS:
        val = settings_dict[key].get("value_json")
        if val is None or (isinstance(val, str) and not val.strip()):
            raise CutoverError(f"required runtime setting {key!r} is empty in Afterglow")
    return list(settings_dict.values())


def _rewrite_cluster_policy_snapshot(row: dict[str, Any]) -> None:
    snapshot_raw = row.get("resource_policy_snapshot")
    if snapshot_raw is None:
        return
    snapshot = snapshot_raw
    is_json_str = False
    if isinstance(snapshot_raw, str):
        try:
            snapshot = json.loads(snapshot_raw)
            is_json_str = True
        except json.JSONDecodeError:
            return
    if isinstance(snapshot, dict):
        if "cinder.default_volume_availability_zone" in snapshot:
            cinder_val = snapshot["cinder.default_volume_availability_zone"]
            if "k3s.volume_availability_zone" in snapshot:
                k3s_val = snapshot["k3s.volume_availability_zone"]
                if _canonical(cinder_val) != _canonical(k3s_val):
                    raise CutoverError(
                        f"conflicting volume_availability_zone keys in cluster {row.get('id')!r} resource_policy_snapshot"
                    )
            snapshot["k3s.volume_availability_zone"] = cinder_val
            del snapshot["cinder.default_volume_availability_zone"]
            if is_json_str:
                row["resource_policy_snapshot"] = json.dumps(snapshot, separators=(",", ":"), sort_keys=True)
            else:
                row["resource_policy_snapshot"] = snapshot


def _database_identity(url: URL) -> tuple[Any, ...]:
    return (url.drivername, url.username, url.password, url.host, url.port, url.database, tuple(sorted(url.query.items())))


async def _fetch_rows(
    connection: AsyncConnection,
    spec: TableSpec,
    *,
    policies_only: bool = False,
    settings_only: bool = False,
) -> list[dict[str, Any]]:
    columns = ", ".join(f"`{col}`" for col in spec.columns)
    if policies_only:
        suffix = (
            " WHERE `policy_key` LIKE 'k3s.%' "
            "OR `policy_key` IN ('nova.default_network', 'cinder.default_volume_availability_zone')"
        )
    elif settings_only:
        suffix = " WHERE `setting_key` LIKE 'k3s.%'"
    else:
        suffix = ""
    result = await connection.execute(text(f"SELECT {columns} FROM `{spec.name}`{suffix}"))
    return [dict(row) for row in result.mappings().all()]


async def _table_exists(connection: AsyncConnection, table_name: str) -> bool:
    result = await connection.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = :table_name LIMIT 1"
        ),
        {"table_name": table_name},
    )
    return result.scalar_one_or_none() is not None


async def _read_source_tables(connection: AsyncConnection) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = {}
    for spec in _TABLES:
        if spec.optional_source and not await _table_exists(connection, spec.name):
            rows[spec.name] = []
        else:
            rows[spec.name] = await _fetch_rows(connection, spec)
    return rows


def _bindable_row(spec: TableSpec, row: Mapping[str, Any]) -> dict[str, Any]:
    values = dict(row)
    for column in spec.json_columns:
        value = values.get(column)
        if value is not None and not isinstance(value, str):
            values[column] = json.dumps(value, separators=(",", ":"), sort_keys=True)
    return values


async def _insert_rows(connection: AsyncConnection, spec: TableSpec, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    columns = ", ".join(f"`{column}`" for column in spec.columns)
    bindings = ", ".join(f":{column}" for column in spec.columns)
    await connection.execute(
        text(f"INSERT INTO `{spec.name}` ({columns}) VALUES ({bindings})"),
        [_bindable_row(spec, row) for row in rows],
    )


async def _update_rows(connection: AsyncConnection, spec: TableSpec, rows: Sequence[Mapping[str, Any]]) -> None:
    assignments = ", ".join(f"`{column}` = :{column}" for column in spec.columns if column != spec.primary_key)
    statement = text(f"UPDATE `{spec.name}` SET {assignments} WHERE `{spec.primary_key}` = :{spec.primary_key}")
    for row in rows:
        await connection.execute(statement, _bindable_row(spec, row))


def _verify_ciphertext(
    source_tables: Mapping[str, Sequence[Mapping[str, Any]]],
    destination_tables: Mapping[str, Sequence[Mapping[str, Any]]],
) -> bool:
    verified_any = False

    source_clusters = source_tables.get("k3s_clusters", [])
    dest_clusters = {row["id"]: row for row in destination_tables.get("k3s_clusters", [])}
    for sample in source_clusters:
        cluster_id = sample["id"]
        dest_row = dest_clusters.get(cluster_id, {})

        kubeconfig_ct = sample.get("kubeconfig_encrypted")
        if isinstance(kubeconfig_ct, str) and kubeconfig_ct.strip():
            copied = dest_row.get("kubeconfig_encrypted")
            if copied != kubeconfig_ct:
                raise CutoverError("Drover cluster kubeconfig ciphertext was not copied byte-identically")
            try:
                decrypt_kubeconfig(kubeconfig_ct)
            except Exception as exc:
                raise CutoverError("Drover cluster kubeconfig ciphertext cannot be decrypted with the configured key") from exc
            verified_any = True

        node_token_ct = sample.get("node_token")
        if isinstance(node_token_ct, str) and node_token_ct.strip():
            copied = dest_row.get("node_token")
            if copied != node_token_ct:
                raise CutoverError("Drover cluster node_token ciphertext was not copied byte-identically")
            try:
                decrypt_node_token(node_token_ct)
            except Exception as exc:
                raise CutoverError("Drover cluster node_token ciphertext cannot be decrypted with the configured key") from exc
            verified_any = True

    source_creds = source_tables.get("project_manager_credentials", [])
    dest_creds = {row["project_id"]: row for row in destination_tables.get("project_manager_credentials", [])}
    for sample in source_creds:
        proj_id = sample["project_id"]
        dest_row = dest_creds.get(proj_id, {})
        mgr_pw_ct = sample.get("encrypted_password")
        if isinstance(mgr_pw_ct, str) and mgr_pw_ct.strip():
            copied = dest_row.get("encrypted_password")
            if copied != mgr_pw_ct:
                raise CutoverError("Drover project manager password ciphertext was not copied byte-identically")
            try:
                decrypt_manager_password(mgr_pw_ct)
            except Exception as exc:
                raise CutoverError(
                    "Drover project manager password ciphertext cannot be decrypted with the configured key"
                ) from exc
            verified_any = True

    return verified_any


async def migrate_database(source_url: str, destination_url: str, *, apply: bool) -> dict[str, Any]:
    source_dsn = _mysql_async_url(source_url)
    destination_dsn = _mysql_async_url(destination_url)
    if _database_identity(source_dsn) == _database_identity(destination_dsn):
        raise CutoverError("source and destination databases must be different")

    source_engine = create_async_engine(source_dsn, pool_pre_ping=True)
    destination_engine = create_async_engine(destination_dsn, pool_pre_ping=True)
    try:
        async with source_engine.connect() as source_connection:
            source_tables = await _read_source_tables(source_connection)
            for cluster_row in source_tables.get("k3s_clusters", []):
                _rewrite_cluster_policy_snapshot(cluster_row)

            raw_source_policies = await _fetch_rows(source_connection, _POLICY_TABLE, policies_only=True)
            raw_source_runtime = await _fetch_rows(source_connection, _RUNTIME_SETTINGS_TABLE, settings_only=True)

        mapped_policies = _map_and_deduplicate_policies(raw_source_policies)
        _validate_policy_rows(mapped_policies)

        source_runtime = _filter_and_validate_runtime_settings(raw_source_runtime)

        plans: dict[str, TablePlan] = {}
        async with destination_engine.begin() as destination_connection:
            for spec in _TABLES:
                destination_rows = await _fetch_rows(destination_connection, spec)
                plan = _plan_table_changes(spec, source_tables[spec.name], destination_rows)
                plans[spec.name] = plan
                if apply:
                    await _insert_rows(destination_connection, spec, plan.inserts)

            destination_policies = await _fetch_rows(destination_connection, _POLICY_TABLE, policies_only=True)
            policy_plan = _plan_table_changes(
                _POLICY_TABLE,
                mapped_policies,
                destination_policies,
                allow_seeded_policy_updates=True,
            )
            plans[_POLICY_TABLE.name] = policy_plan
            if apply:
                await _insert_rows(destination_connection, _POLICY_TABLE, policy_plan.inserts)
                await _update_rows(destination_connection, _POLICY_TABLE, policy_plan.updates)

            destination_runtime = await _fetch_rows(destination_connection, _RUNTIME_SETTINGS_TABLE, settings_only=True)
            runtime_plan = _plan_table_changes(
                _RUNTIME_SETTINGS_TABLE,
                source_runtime,
                destination_runtime,
            )
            plans[_RUNTIME_SETTINGS_TABLE.name] = runtime_plan
            if apply:
                await _insert_rows(destination_connection, _RUNTIME_SETTINGS_TABLE, runtime_plan.inserts)
                await _update_rows(destination_connection, _RUNTIME_SETTINGS_TABLE, runtime_plan.updates)

        report: dict[str, Any] = {
            name: {"source": len(source_tables[name]), "inserted": len(plan.inserts), "updated": len(plan.updates)}
            for name, plan in plans.items()
            if name not in (_POLICY_TABLE.name, _RUNTIME_SETTINGS_TABLE.name)
        }
        report[_POLICY_TABLE.name] = {
            "source": len(mapped_policies),
            "inserted": len(policy_plan.inserts),
            "updated": len(policy_plan.updates),
        }
        report[_RUNTIME_SETTINGS_TABLE.name] = {
            "source": len(source_runtime),
            "inserted": len(runtime_plan.inserts),
            "updated": len(runtime_plan.updates),
        }
        report["ciphertext_verified"] = False
        if not apply:
            return report

        async with destination_engine.connect() as destination_connection:
            dest_tables_data: dict[str, list[dict[str, Any]]] = {}
            for spec in _TABLES:
                destination_rows = await _fetch_rows(destination_connection, spec)
                dest_tables_data[spec.name] = destination_rows
                if len(destination_rows) != len(source_tables[spec.name]):
                    raise CutoverError(f"row count mismatch after copying {spec.name}")
                if _plan_table_changes(spec, source_tables[spec.name], destination_rows) != TablePlan(()):
                    raise CutoverError(f"row verification failed after copying {spec.name}")

            report["ciphertext_verified"] = _verify_ciphertext(source_tables, dest_tables_data)

            destination_policies = await _fetch_rows(destination_connection, _POLICY_TABLE, policies_only=True)
            if _plan_table_changes(_POLICY_TABLE, mapped_policies, destination_policies) != TablePlan(()):
                raise CutoverError("resource policy verification failed")

            destination_runtime = await _fetch_rows(destination_connection, _RUNTIME_SETTINGS_TABLE, settings_only=True)
            if _plan_table_changes(_RUNTIME_SETTINGS_TABLE, source_runtime, destination_runtime) != TablePlan(()):
                raise CutoverError("runtime setting verification failed")

        return report
    finally:
        await source_engine.dispose()
        await destination_engine.dispose()


async def _copy_redis_pattern(source: Redis, destination: Redis, pattern: str, *, apply: bool) -> int:
    count = 0
    async for key in source.scan_iter(match=pattern):
        payload = await source.dump(key)
        ttl_ms = await source.pttl(key)
        if payload is None or ttl_ms == -2:
            raise CutoverError(f"Redis key disappeared during maintenance copy: {key!r}")
        if apply:
            await destination.restore(key, max(ttl_ms, 0), payload, replace=True)
        count += 1
    return count


async def migrate_redis(source_url: str, destination_url: str, *, apply: bool) -> dict[str, int]:
    if not source_url.strip() or not destination_url.strip():
        raise CutoverError("source and destination Redis URLs are required")
    source = Redis.from_url(source_url)
    destination = Redis.from_url(destination_url)
    try:
        counts = {
            pattern: await _copy_redis_pattern(source, destination, pattern, apply=apply) for pattern in _REDIS_PATTERNS
        }
        if apply:
            for pattern, expected in counts.items():
                actual = sum(1 async for _ in destination.scan_iter(match=pattern))
                if actual != expected:
                    raise CutoverError(f"Redis key count mismatch after copying {pattern}")
        return counts
    finally:
        await source.aclose()
        await destination.aclose()


async def cutover(*, apply: bool) -> dict[str, Any]:
    settings = get_settings()
    source_database_url = os.environ.get("AFTERGLOW_SOURCE_DATABASE_URL", "")
    source_redis_url = os.environ.get("AFTERGLOW_SOURCE_REDIS_URL", "")
    database_report = await migrate_database(source_database_url, settings.database_url, apply=apply)
    redis_report = await migrate_redis(source_redis_url, settings.redis_url, apply=apply)
    return {"mode": "apply" if apply else "dry-run", "database": database_report, "redis": redis_report}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the copy; default is a read-only dry run")
    args = parser.parse_args()
    try:
        report = asyncio.run(cutover(apply=args.apply))
    except CutoverError as exc:
        raise SystemExit(f"Drover cutover refused: {exc}") from exc
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
