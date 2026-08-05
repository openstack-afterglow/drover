"""Standalone Drover configuration compatibility contracts."""

import os
from unittest.mock import patch


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
