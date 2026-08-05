from __future__ import annotations
import fnmatch

from collections.abc import AsyncIterator
from typing import Any

import pytest

from drover.scripts import cutover


def _policy(key: str, resource_id: str | None) -> dict[str, Any]:
    return {
        "policy_key": key,
        "resource_kind": "network",
        "resource_id": resource_id,
        "resource_name": resource_id,
        "constraints": {},
        "updated_by_user_id": None,
        "created_at": "2026-08-04T00:00:00",
        "updated_at": "2026-08-04T00:00:00",
    }


def _runtime_setting(key: str, value: str | None) -> dict[str, Any]:
    return {
        "setting_key": key,
        "value_json": value,
        "updated_by_user_id": None,
        "created_at": "2026-08-04T00:00:00",
        "updated_at": "2026-08-04T00:00:00",
    }


def test_table_plan_is_restart_safe_and_rejects_conflicts():
    spec = cutover.TableSpec("records", "id", ("id", "value"))
    source = [{"id": "one", "value": {"nested": [1, 2]}}]

    assert cutover._plan_table_changes(spec, source, []) == cutover.TablePlan((source[0],))
    assert cutover._plan_table_changes(spec, source, [{"id": "one", "value": '{"nested":[1,2]}'}]) == cutover.TablePlan(())

    with pytest.raises(cutover.CutoverError, match="conflicting records row"):
        cutover._plan_table_changes(spec, source, [{"id": "one", "value": {"nested": [2, 1]}}])
    with pytest.raises(cutover.CutoverError, match="rows absent from source"):
        cutover._plan_table_changes(spec, source, [*source, {"id": "two", "value": None}])


def test_gpu_quotas_cutover_plan_and_conflicts():
    spec = next(s for s in cutover._TABLES if s.name == "gpu_quotas")
    source = [
        {
            "id": 1,
            "project_id": "proj-1",
            "gpu_type": "RTX3090",
            "limit": 4,
            "created_at": "2026-08-04T00:00:00",
            "updated_at": "2026-08-04T00:00:00",
        }
    ]

    assert cutover._plan_table_changes(spec, source, []) == cutover.TablePlan((source[0],))
    assert cutover._plan_table_changes(spec, source, list(source)) == cutover.TablePlan(())

    conflicting = [dict(source[0], limit=8)]
    with pytest.raises(cutover.CutoverError, match="conflicting gpu_quotas row"):
        cutover._plan_table_changes(spec, source, conflicting)

    with pytest.raises(cutover.CutoverError, match="rows absent from source"):
        cutover._plan_table_changes(spec, source, [*source, dict(source[0], id=2, gpu_type="A100")])


def test_policy_plan_and_mapping():
    raw_source = [
        _policy("nova.default_network", "net-1"),
        _policy("cinder.default_volume_availability_zone", "zone-1"),
        _policy("k3s.server_image", "img-1"),
    ]
    mapped = cutover._map_and_deduplicate_policies(raw_source)
    mapped_keys = {p["policy_key"] for p in mapped}
    assert mapped_keys == {"k3s.default_network", "k3s.volume_availability_zone", "k3s.server_image"}

    # Duplicate mapping with same value is allowed
    raw_with_dupe = [*raw_source, _policy("k3s.default_network", "net-1")]
    assert len(cutover._map_and_deduplicate_policies(raw_with_dupe)) == 3

    # Conflict in mapped policies raises CutoverError
    raw_conflict = [*raw_source, _policy("k3s.default_network", "net-2")]
    with pytest.raises(cutover.CutoverError, match="conflicting source resource_policies mapping"):
        cutover._map_and_deduplicate_policies(raw_conflict)

    # Seeded baseline update
    seeded = [_policy("k3s.server_image", None)]
    source = [_policy("k3s.server_image", "img-1")]
    plan = cutover._plan_table_changes(
        cutover._POLICY_TABLE,
        source,
        seeded,
        allow_seeded_policy_updates=True,
    )
    assert plan == cutover.TablePlan((), (source[0],))


def test_policy_validation_requires_all_rows_and_required_selections():
    all_policies = [
        _policy("k3s.server_image", "img-1"),
        _policy("k3s.fcos_image", "fcos-1"),
        _policy("k3s.server_flavor", "flv-1"),
        _policy("k3s.default_agent_flavor", "flv-2"),
        _policy("k3s.volume_availability_zone", "az-1"),
        _policy("k3s.default_network", "net-1"),
        _policy("k3s.occm_floating_network", None),
        _policy("k3s.occm_public_network", None),
        _policy("k3s.lb_subnet", None),
        _policy("k3s.api_lb_vip_network", None),
        _policy("k3s.api_lb_floating_network", None),
        _policy("k3s.octavia_ingress_floating_network", None),
    ]
    cutover._validate_policy_rows(all_policies)

    with pytest.raises(cutover.CutoverError, match="missing Drover resource policies"):
        cutover._validate_policy_rows(all_policies[:-1])

    unconfigured_required = list(all_policies)
    unconfigured_required[0] = _policy("k3s.server_image", None)
    with pytest.raises(cutover.CutoverError, match="not configured"):
        cutover._validate_policy_rows(unconfigured_required)


def test_runtime_settings_validation_and_plan():
    valid = [_runtime_setting("k3s.version", "v1.28.2+k3s1")]
    assert cutover._filter_and_validate_runtime_settings(valid) == valid

    with pytest.raises(cutover.CutoverError, match="missing from Afterglow"):
        cutover._filter_and_validate_runtime_settings([])

    with pytest.raises(cutover.CutoverError, match="empty in Afterglow"):
        cutover._filter_and_validate_runtime_settings([_runtime_setting("k3s.version", "  ")])

    # Standard plan insertion when missing in destination
    plan = cutover._plan_table_changes(cutover._RUNTIME_SETTINGS_TABLE, valid, [])
    assert plan == cutover.TablePlan((valid[0],))

    # Retained when identical in destination
    plan_retained = cutover._plan_table_changes(cutover._RUNTIME_SETTINGS_TABLE, valid, valid)
    assert plan_retained == cutover.TablePlan(())

    # Conflicting destination value raises CutoverError
    conflicting = [_runtime_setting("k3s.version", "v1.27.0")]
    with pytest.raises(cutover.CutoverError, match="conflicting runtime_settings row"):
        cutover._plan_table_changes(cutover._RUNTIME_SETTINGS_TABLE, valid, conflicting)


def test_cluster_snapshot_rewrite_and_conflict():
    row_dict = {
        "id": "c-1",
        "resource_policy_snapshot": {
            "cinder.default_volume_availability_zone": {"id": "nova", "name": "nova"},
            "k3s.server_flavor": {"id": "f-1"},
        },
    }
    cutover._rewrite_cluster_policy_snapshot(row_dict)
    assert "cinder.default_volume_availability_zone" not in row_dict["resource_policy_snapshot"]
    assert row_dict["resource_policy_snapshot"]["k3s.volume_availability_zone"] == {"id": "nova", "name": "nova"}

    # String JSON rewrite
    row_json = {
        "id": "c-2",
        "resource_policy_snapshot": '{"cinder.default_volume_availability_zone": {"id": "nova"}}',
    }
    cutover._rewrite_cluster_policy_snapshot(row_json)
    assert "k3s.volume_availability_zone" in row_json["resource_policy_snapshot"]
    assert "cinder.default_volume_availability_zone" not in row_json["resource_policy_snapshot"]

    # Both exist and equal -> success
    row_equal = {
        "id": "c-3",
        "resource_policy_snapshot": {
            "cinder.default_volume_availability_zone": {"id": "nova"},
            "k3s.volume_availability_zone": {"id": "nova"},
        },
    }
    cutover._rewrite_cluster_policy_snapshot(row_equal)
    assert "cinder.default_volume_availability_zone" not in row_equal["resource_policy_snapshot"]

    # Both exist and conflicting -> CutoverError
    row_conflict = {
        "id": "c-4",
        "resource_policy_snapshot": {
            "cinder.default_volume_availability_zone": {"id": "nova-1"},
            "k3s.volume_availability_zone": {"id": "nova-2"},
        },
    }
    with pytest.raises(cutover.CutoverError, match="conflicting volume_availability_zone keys"):
        cutover._rewrite_cluster_policy_snapshot(row_conflict)


def test_ciphertext_verification_requires_identical_copy_and_valid_key(monkeypatch):
    monkeypatch.setattr(cutover, "decrypt_kubeconfig", lambda val: "kc-plain")
    monkeypatch.setattr(cutover, "decrypt_node_token", lambda val: "token-plain")
    monkeypatch.setattr(cutover, "decrypt_manager_password", lambda val: "pass-plain")

    source_tables = {
        "k3s_clusters": [
            {
                "id": "c-1",
                "kubeconfig_encrypted": "v3:kc-cipher",
                "node_token": "v3:token-cipher",
            }
        ],
        "project_manager_credentials": [
            {
                "project_id": "p-1",
                "encrypted_password": "v3:pass-cipher",
            }
        ],
    }
    dest_tables = {
        "k3s_clusters": [
            {
                "id": "c-1",
                "kubeconfig_encrypted": "v3:kc-cipher",
                "node_token": "v3:token-cipher",
            }
        ],
        "project_manager_credentials": [
            {
                "project_id": "p-1",
                "encrypted_password": "v3:pass-cipher",
            }
        ],
    }

    assert cutover._verify_ciphertext(source_tables, dest_tables) is True

    # Verification without cluster kubeconfig (e.g. manager password only)
    source_no_cluster = {
        "k3s_clusters": [],
        "project_manager_credentials": [{"project_id": "p-1", "encrypted_password": "v3:pass-cipher"}],
    }
    dest_no_cluster = dict(source_no_cluster)
    assert cutover._verify_ciphertext(source_no_cluster, dest_no_cluster) is True

    # Byte mismatch
    dest_mismatch = {
        "k3s_clusters": source_tables["k3s_clusters"],
        "project_manager_credentials": [{"project_id": "p-1", "encrypted_password": "v3:other"}],
    }
    with pytest.raises(cutover.CutoverError, match="byte-identically"):
        cutover._verify_ciphertext(source_tables, dest_mismatch)

    # Decryption error
    def _fail_decrypt(_val):
        raise ValueError("bad key")

    monkeypatch.setattr(cutover, "decrypt_kubeconfig", _fail_decrypt)
    with pytest.raises(cutover.CutoverError, match="cannot be decrypted"):
        cutover._verify_ciphertext(source_tables, dest_tables)


class _FakeRedis:
    def __init__(self, values: dict[bytes, tuple[bytes, int]]):
        self.values = values
        self.restored: list[tuple[bytes, int, bytes, bool]] = []

    async def scan_iter(self, *, match: str) -> AsyncIterator[bytes]:
        for key in self.values:
            if fnmatch.fnmatch(key.decode("utf-8"), match):
                yield key

    async def dump(self, key: bytes) -> bytes | None:
        item = self.values.get(key)
        return item[0] if item else None

    async def pttl(self, key: bytes) -> int:
        item = self.values.get(key)
        return item[1] if item else -2

    async def restore(self, key: bytes, ttl: int, payload: bytes, *, replace: bool) -> None:
        self.restored.append((key, ttl, payload, replace))


@pytest.mark.asyncio
async def test_redis_copy_preserves_payload_ttl_and_is_dry_run_safe():
    source = _FakeRedis(
        {
            b"afterglow:k3s:p1:cluster:c1": (b"dump-cluster", 5000),
            b"afterglow:k3s:p1:clusters": (b"dump-clusters", -1),
            b"drover:health:c1": (b"dump-health", 1000),
        }
    )
    destination = _FakeRedis({})

    count = await cutover._copy_redis_pattern(
        source,
        destination,
        "afterglow:k3s:*:cluster:*",
        apply=False,
    )
    assert count == 1
    assert destination.restored == []

    count = await cutover._copy_redis_pattern(
        source,
        destination,
        "drover:health:*",
        apply=True,
    )
    assert count == 1
    assert destination.restored == [(b"drover:health:c1", 1000, b"dump-health", True)]


def test_database_url_normalization_and_backend_rejection():
    assert cutover._mysql_async_url("mysql+asyncmy://user:secret@db/source").drivername == "mysql+aiomysql"
    assert cutover._mysql_async_url("mysql://user:secret@db/source").drivername == "mysql+aiomysql"
    with pytest.raises(cutover.CutoverError, match="MySQL/MariaDB"):
        cutover._mysql_async_url("postgresql://user:secret@db/source")


class _ConnectionContext:
    def __init__(self, connection: object):
        self.connection = connection

    async def __aenter__(self) -> object:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _FakeEngine:
    def __init__(self, connection: object):
        self.connection = connection

    def connect(self) -> _ConnectionContext:
        return _ConnectionContext(self.connection)

    def begin(self) -> _ConnectionContext:
        return _ConnectionContext(self.connection)

    async def dispose(self) -> None:
        return None


async def _async_value(value: Any) -> Any:
    return value


@pytest.mark.asyncio
async def test_migrate_database_applies_when_legacy_source_has_no_drover_jobs_table(monkeypatch):
    source_connection = object()
    destination_connection = object()
    source_tables = {spec.name: [] for spec in cutover._TABLES}
    source_tables["k3s_clusters"] = [
        {
            "id": "c-1",
            "project_id": "p-1",
            "name": "cluster-1",
            "status": "ACTIVE",
            "status_reason": None,
            "server_vm_id": "vm-1",
            "server_flavor_id": "flv-1",
            "agent_flavor_id": "flv-1",
            "server_image_id": "img-1",
            "network_id": "net-1",
            "security_group_id": "sg-1",
            "api_lb_id": None,
            "api_lb_pool_id": None,
            "api_fip_id": None,
            "api_fip_address": None,
            "server_ip": "10.0.0.1",
            "api_address": "https://10.0.0.1:6443",
            "k3s_version": "v1.28.2+k3s1",
            "node_token": "v3:token-cipher",
            "key_name": None,
            "ssh_public_key": None,
            "kubeconfig_encrypted": "v3:kc-cipher",
            "created_by_user_id": "u-1",
            "created_by_username": "user1",
            "agent_count": 1,
            "occm_enabled": False,
            "plugins_enabled": {},
            "plugin_status": {},
            "secret_cloud_config_status": None,
            "os_type": "ubuntu",
            "app_credential_id": None,
            "created_at": "2026-08-04T00:00:00",
            "updated_at": "2026-08-04T00:00:00",
            "deleted_at": None,
            "deleted_by_user_id": None,
            "deleted_reason": None,
            "master_count": 1,
            "template_id": None,
            "template_snapshot": None,
            "resource_policy_snapshot": {
                "cinder.default_volume_availability_zone": {"id": "nova"},
            },
            "stampede_enabled": False,
            "last_rotation_at": None,
            "last_rotation_initiated_by": None,
        }
    ]
    source_tables["gpu_quotas"] = [
        {
            "id": 1,
            "project_id": "p-1",
            "gpu_type": "RTX3090",
            "limit": 4,
            "created_at": "2026-08-04T00:00:00",
            "updated_at": "2026-08-04T00:00:00",
        }
    ]
    source_policies = [
        _policy("nova.default_network", "net-1"),
        _policy("cinder.default_volume_availability_zone", "zone-1"),
        _policy("k3s.server_image", "img-1"),
        _policy("k3s.fcos_image", "fcos-1"),
        _policy("k3s.server_flavor", "flv-1"),
        _policy("k3s.default_agent_flavor", "flv-2"),
        _policy("k3s.occm_floating_network", None),
        _policy("k3s.occm_public_network", None),
        _policy("k3s.lb_subnet", None),
        _policy("k3s.api_lb_vip_network", None),
        _policy("k3s.api_lb_floating_network", None),
        _policy("k3s.octavia_ingress_floating_network", None),
    ]
    source_runtime = [_runtime_setting("k3s.version", "v1.28.2+k3s1")]

    destination_tables = {spec.name: [] for spec in cutover._TABLES}
    destination_tables["resource_policies"] = [_policy(key, None) for key in cutover._POLICY_KEYS]
    destination_tables["runtime_settings"] = []

    engines = iter((_FakeEngine(source_connection), _FakeEngine(destination_connection)))
    monkeypatch.setattr(cutover, "create_async_engine", lambda *_args, **_kwargs: next(engines))
    monkeypatch.setattr(
        cutover,
        "_table_exists",
        lambda _conn, name: _async_value(name != "drover_jobs"),
    )

    async def fetch(connection, spec, *, policies_only=False, settings_only=False):
        if connection is source_connection:
            if policies_only:
                return list(source_policies)
            if settings_only:
                return list(source_runtime)
            return list(source_tables[spec.name])
        if policies_only:
            return list(destination_tables["resource_policies"])
        if settings_only:
            return list(destination_tables["runtime_settings"])
        return list(destination_tables[spec.name])

    async def insert(_conn, spec, rows):
        destination_tables[spec.name].extend(dict(r) for r in rows)

    async def update(_conn, spec, rows):
        by_key = {r[spec.primary_key]: dict(r) for r in destination_tables[spec.name]}
        by_key.update({r[spec.primary_key]: dict(r) for r in rows})
        destination_tables[spec.name] = list(by_key.values())

    monkeypatch.setattr(cutover, "_fetch_rows", fetch)
    monkeypatch.setattr(cutover, "_insert_rows", insert)
    monkeypatch.setattr(cutover, "_update_rows", update)
    monkeypatch.setattr(cutover, "decrypt_kubeconfig", lambda _ct: "kc-plain")
    monkeypatch.setattr(cutover, "decrypt_node_token", lambda _ct: "token-plain")
    monkeypatch.setattr(cutover, "decrypt_manager_password", lambda _ct: "pass-plain")

    report = await cutover.migrate_database(
        "mysql+asyncmy://afterglow:secret@db/afterglow",
        "mysql+aiomysql://drover:secret@db/drover",
        apply=True,
    )

    assert report["drover_jobs"] == {"source": 0, "inserted": 0, "updated": 0}
    assert report["k3s_clusters"]["inserted"] == 1
    assert report["gpu_quotas"]["inserted"] == 1
    assert report["resource_policies"]["updated"] == 6
    assert report["runtime_settings"]["inserted"] == 1
    assert report["ciphertext_verified"] is True
