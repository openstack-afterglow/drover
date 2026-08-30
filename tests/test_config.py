"""Standalone Drover configuration compatibility contracts."""

import os
from unittest.mock import patch

import pytest

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


def test_empty_environment_value_falls_back_to_toml(monkeypatch):
    monkeypatch.setattr(config, "_load_toml", lambda: {"os_auth_url": "https://keystone.example.test/v3"})

    with patch.dict(os.environ, {"OS_AUTH_URL": ""}, clear=True):
        config.get_settings.cache_clear()
        try:
            assert config.get_settings().os_auth_url == "https://keystone.example.test/v3"
        finally:
            config.get_settings.cache_clear()


def test_afterglow_openstack_section_is_mapped(monkeypatch):
    monkeypatch.setattr(
        config,
        "load_raw_toml",
        lambda: {"openstack": {"auth_url": "https://keystone.example.test/v3", "region_name": "RegionTwo"}},
    )

    settings = config._load_toml()

    assert settings["os_auth_url"] == "https://keystone.example.test/v3"
    assert settings["os_region_name"] == "RegionTwo"


def test_declared_runtime_settings_defaults(monkeypatch):
    monkeypatch.setattr(config, "load_raw_toml", lambda: {})

    settings = config.Settings(_env_file=None)

    assert settings.k3s_health_interval == 180
    assert settings.drover_reconcile_interval == 300
    assert settings.drover_reconcile_concurrency_per_project == 2
    assert settings.os_service_project_id == ""
    assert settings.os_project_name == ""
    assert settings.admin_legacy_project_policy is False

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
    assert values["k3s_health_interval"] == 300
    assert values["drover_reconcile_interval"] == 600
    assert values["drover_reconcile_concurrency_per_project"] == 4




def test_declared_runtime_settings_env_override(monkeypatch):

    monkeypatch.setenv("K3S_HEALTH_INTERVAL", "240")
    monkeypatch.setenv("DROVER_RECONCILE_INTERVAL", "120")
    monkeypatch.setenv("DROVER_RECONCILE_CONCURRENCY_PER_PROJECT", "5")

    monkeypatch.setenv("OS_SERVICE_PROJECT_ID", "env-service-proj-123")

    monkeypatch.setenv("ADMIN_LEGACY_PROJECT_POLICY", "true")



    settings = config.Settings(_env_file=None)



    assert settings.k3s_health_interval == 240
    assert settings.drover_reconcile_interval == 120
    assert settings.drover_reconcile_concurrency_per_project == 5
    assert settings.os_service_project_id == "env-service-proj-123"
    assert settings.admin_legacy_project_policy is True




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
