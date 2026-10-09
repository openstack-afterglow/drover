"""Standalone Drover configuration compatibility contracts."""

import os
from unittest.mock import patch

import pytest
from pydantic_settings import SettingsError

from drover import config


def test_stampede_toml_defaults_preserve_fractional_thresholds(monkeypatch):
    monkeypatch.setattr(config, "load_raw_toml", lambda: {"drover": {}})

    values = config._load_toml()

    assert values["drover_stampede_scale_down_threshold"] == 0.5
    assert values["drover_stampede_resource_headroom_factor"] == 0.3


def test_kubeconfig_encryption_key_uses_drover_toml_field(monkeypatch):
    monkeypatch.setattr(config, "load_raw_toml", lambda: {"drover": {"kubeconfig_encryption_key": "a" * 64}})

    assert config._load_toml()["drover_kubeconfig_encryption_key"] == "a" * 64


def test_kubeconfig_encryption_key_loads_from_drover_environment(monkeypatch):
    monkeypatch.setenv("DROVER_KUBECONFIG_ENCRYPTION_KEY", "b" * 64)

    assert config.Settings(_env_file=None).drover_kubeconfig_encryption_key == "b" * 64


@pytest.fixture
def isolated_toml_settings(tmp_path, monkeypatch):
    """Use a real TOML file, isolated from machine config and protected secrets."""
    path = tmp_path / "drover.conf"
    monkeypatch.setattr(config, "_config_candidates", lambda: [path])
    environment = {"DROVER_CONFIG_FILE": str(path)}
    # Explicit nonexistent paths avoid consulting /etc/drover/secrets.
    for key in ("OS_PASSWORD_FILE", "DATABASE_PASSWORD_FILE", "REDIS_PASSWORD_FILE",
                "DROVER_KUBECONFIG_ENCRYPTION_KEY_FILE", "DROVER_AFTERGLOW_ADMISSION_TOKEN_FILE"):
        environment[key] = str(tmp_path / key.lower())
    with patch.dict(os.environ, environment, clear=True):
        config.load_raw_toml.cache_clear()
        config.get_settings.cache_clear()
        try:
            yield path
        finally:
            config.get_settings.cache_clear()
            config.load_raw_toml.cache_clear()




def test_get_settings_keeps_explicit_empty_auth_endpoint(isolated_toml_settings):
    isolated_toml_settings.write_text('[openstack]\nauth_url = "https://file.example.test/v3"\n', encoding="utf-8")
    os.environ["OS_AUTH_URL"] = ""

    assert config.get_settings().os_auth_url == ""


def test_explicit_empty_role_environment_is_not_replaced_by_toml(isolated_toml_settings):
    isolated_toml_settings.write_text('[drover]\ndelegated_required_roles = ["member"]\n', encoding="utf-8")
    os.environ["DROVER_DELEGATED_REQUIRED_ROLES"] = ""

    with pytest.raises(SettingsError):
        config.get_settings()


@pytest.mark.parametrize("optional_toml,optional_roles", [
    ('["load-balancer_member"]', ["load-balancer_member"]), ("[]", []),
])
def test_get_settings_loads_actual_toml_arrays(isolated_toml_settings, optional_toml, optional_roles):
    isolated_toml_settings.write_text(
        '[drover]\ndelegated_required_roles = ["member"]\n'
        f'delegated_optional_roles = {optional_toml}\n'
        'callback_allowed_cidrs = ["192.0.2.0/24", "2001:db8::/64"]\n', encoding="utf-8",
    )

    settings = config.get_settings()

    assert settings.drover_delegated_required_roles == ["member"]
    assert settings.drover_delegated_optional_roles == optional_roles
    assert settings.drover_callback_allowed_cidrs == ["192.0.2.0/24", "2001:db8::/64"]


@pytest.mark.parametrize("optional_json,optional_roles", [('["load-balancer_member"]', ["load-balancer_member"]), ("[]", [])])
def test_get_settings_explicit_json_environment_overrides_toml_arrays(isolated_toml_settings, optional_json, optional_roles):
    isolated_toml_settings.write_text(
        '[openstack]\nauth_url = "https://file.example.test/v3"\n'
        '[drover]\ndelegated_required_roles = ["file_member"]\ndelegated_optional_roles = ["file_optional"]\n'
        'callback_allowed_cidrs = ["192.0.2.0/24"]\n', encoding="utf-8",
    )
    os.environ["DROVER_DELEGATED_REQUIRED_ROLES"] = '["member"]'
    os.environ["DROVER_DELEGATED_OPTIONAL_ROLES"] = optional_json
    os.environ["DROVER_CALLBACK_ALLOWED_CIDRS"] = '["198.51.100.0/24"]'
    os.environ["OS_AUTH_URL"] = "https://environment.example.test/v3"

    settings = config.get_settings()

    assert settings.drover_delegated_required_roles == ["member"]
    assert settings.drover_delegated_optional_roles == optional_roles
    assert settings.drover_callback_allowed_cidrs == ["198.51.100.0/24"]
    assert settings.os_auth_url == "https://environment.example.test/v3"


def test_afterglow_openstack_section_is_mapped(monkeypatch):
    monkeypatch.setattr(
        config,
        "load_raw_toml",
        lambda: {"openstack": {"auth_url": "https://keystone.example.test/v3", "region_name": "RegionTwo"}},
    )

    settings = config._load_toml()

    assert settings["os_auth_url"] == "https://keystone.example.test/v3"
    assert settings["os_region_name"] == "RegionTwo"



def test_declared_runtime_settings_toml_mapping(monkeypatch):
    monkeypatch.setattr(
        config,
        "load_raw_toml",
        lambda: {
            "openstack": {
                "service_project_id": "service-proj-999",
                "project_name": "drover-service",
                "admin_legacy_project_policy": True,
            },
            "cache": {
                "sentinel_enabled": True,
                "sentinel_master_name": "valkey",
                "sentinel_hosts": ["cache-1:26379", "cache-2:26379"],
                "ttl_fast": 20,
            },
            "drover": {
                "k3s_health_interval": 300,
                "reconcile_interval": 600,
                "reconcile_concurrency_per_project": 4,
            },
        },
    )

    values = config._load_toml()

    assert values["os_service_project_id"] == "service-proj-999"
    assert values["os_project_name"] == "drover-service"
    assert values["admin_legacy_project_policy"] is True
    assert values["sentinel_enabled"] is True
    assert values["sentinel_master_name"] == "valkey"
    assert values["sentinel_hosts"] == "cache-1:26379,cache-2:26379"
    assert values["cache_ttl_fast"] == 20
    assert values["k3s_health_interval"] == 300
    assert values["drover_reconcile_interval"] == 600
    assert values["drover_reconcile_concurrency_per_project"] == 4


def test_sentinel_hosts_toml_string_is_preserved(monkeypatch):
    monkeypatch.setattr(
        config,
        "load_raw_toml",
        lambda: {"cache": {"sentinel_hosts": "cache-1:26379,cache-2:26379"}},
    )

    values = config._load_toml()

    assert values["sentinel_hosts"] == "cache-1:26379,cache-2:26379"


def test_sentinel_hosts_toml_null_becomes_empty(monkeypatch):
    monkeypatch.setattr(config, "load_raw_toml", lambda: {"cache": {"sentinel_hosts": None}})

    values = config._load_toml()

    assert values["sentinel_hosts"] == ""




def test_declared_runtime_settings_env_override(monkeypatch):

    monkeypatch.setenv("K3S_HEALTH_INTERVAL", "240")
    monkeypatch.setenv("DROVER_RECONCILE_INTERVAL", "120")
    monkeypatch.setenv("DROVER_RECONCILE_CONCURRENCY_PER_PROJECT", "5")

    monkeypatch.setenv("OS_SERVICE_PROJECT_ID", "env-service-proj-123")

    monkeypatch.setenv("ADMIN_LEGACY_PROJECT_POLICY", "true")
    monkeypatch.setenv("SENTINEL_ENABLED", "true")
    monkeypatch.setenv("SENTINEL_MASTER_NAME", "valkey")
    monkeypatch.setenv("SENTINEL_HOSTS", "cache-1:26379,cache-2:26379")



    settings = config.Settings(_env_file=None)



    assert settings.k3s_health_interval == 240
    assert settings.drover_reconcile_interval == 120
    assert settings.drover_reconcile_concurrency_per_project == 5
    assert settings.os_service_project_id == "env-service-proj-123"
    assert settings.admin_legacy_project_policy is True
    assert settings.sentinel_enabled is True
    assert settings.sentinel_master_name == "valkey"
    assert settings.sentinel_hosts == "cache-1:26379,cache-2:26379"




def test_validate_config_missing_required_fields(monkeypatch):
    config.get_settings.cache_clear()
    monkeypatch.setattr(config, "load_raw_toml", lambda: {})
    with patch.dict(os.environ, {}, clear=True):
        empty_settings = config.Settings(_env_file=None)

        with pytest.raises(config.ConfigurationError) as exc_info:
            config.validate_config(empty_settings)

        err_msg = str(exc_info.value)
        assert "database_url" in err_msg
        assert "drover_callback_base_url" in err_msg
        assert "drover_kubeconfig_encryption_key" in err_msg
        assert "os_auth_url" in err_msg
        assert "os_username" in err_msg
        assert "os_password" in err_msg

def test_validate_config_success():
    valid_settings = config.Settings(
        _env_file=None,
        database_url="sqlite:///test.db",
        drover_callback_base_url="https://callback.example.test",
        drover_kubeconfig_encryption_key="a" * 64,
        os_auth_url="https://keystone.example.test/v3",
        os_username="drover_service",
        os_password="secretpassword",
    )

    result = config.validate_config(valid_settings)
    assert result is valid_settings


@pytest.mark.asyncio
async def test_api_lifespan_triggers_validation(monkeypatch):
    import pytest

    from drover.main import lifespan

    config.get_settings.cache_clear()
    monkeypatch.setattr(config, "load_raw_toml", lambda: {})
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(config.ConfigurationError):
            async with lifespan(None):
                pass


@pytest.mark.asyncio
async def test_worker_main_async_triggers_validation(monkeypatch):
    import pytest

    from drover.worker import _main_async

    config.get_settings.cache_clear()
    monkeypatch.setattr(config, "load_raw_toml", lambda: {})
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(config.ConfigurationError):
            await _main_async()
