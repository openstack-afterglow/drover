"""Drover configuration loaded from environment or ``drover.conf``."""

from __future__ import annotations

import os
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigurationError(RuntimeError):
    """Raised when required deployment configuration is missing or invalid."""


def _config_candidates() -> list[Path]:
    configured = os.environ.get("DROVER_CONFIG_FILE", "").strip()
    candidates = [Path(configured)] if configured else []
    candidates.extend(
        [
            Path.cwd() / "drover.conf",
            Path.cwd().parent / "drover.conf",
            Path("/etc/drover/drover.conf"),
            Path("/app/drover.conf"),
        ]
    )
    return candidates


@lru_cache
def load_raw_toml() -> dict:
    for path in _config_candidates():
        if path.is_file() and path.stat().st_size > 0:
            with path.open("rb") as handle:
                return tomllib.load(handle)
    return {}

def _inject_db_password(url: str, password: str) -> str:
    if not url or not password:
        return url
    parts = urlsplit(url)
    if not parts.netloc:
        return url
    if "@" in parts.netloc:
        userinfo, host = parts.netloc.rsplit("@", 1)
        user = userinfo.split(":", 1)[0]
    else:
        user = "drover"
        host = parts.netloc
    new_netloc = f"{user}:{password}@{host}"
    return urlunsplit((parts.scheme, new_netloc, parts.path, parts.query, parts.fragment))


def _inject_redis_password(url: str, password: str) -> str:
    if not url or not password:
        return url
    parts = urlsplit(url)
    if not parts.netloc:
        return url
    if "@" in parts.netloc:
        _, host = parts.netloc.rsplit("@", 1)
    else:
        host = parts.netloc
    new_netloc = f":{password}@{host}"
    return urlunsplit((parts.scheme, new_netloc, parts.path, parts.query, parts.fragment))


def _load_toml() -> dict:
    data = load_raw_toml()
    if not data:
        return {}
    keystone = data.get("keystone") or data.get("openstack", {})
    database = data.get("database", {})
    cache = data.get("cache", {})
    drover = data.get("drover", {})
    sentinel_hosts = cache.get("sentinel_hosts") or ""
    if isinstance(sentinel_hosts, list):
        sentinel_hosts = ",".join(str(host) for host in sentinel_hosts)
    return {
        "os_auth_url": keystone.get("auth_url", ""),
        "os_username": keystone.get("username", ""),
        "os_password": keystone.get("password", ""),
        "os_password_file": keystone.get("password_file", keystone.get("os_password_file", "")),
        "os_project_name": keystone.get("project_name", ""),
        "os_project_domain_name": keystone.get("project_domain_name", "Default"),
        "os_user_domain_name": keystone.get("user_domain_name", "Default"),
        "os_region_name": keystone.get("region", keystone.get("region_name", "RegionOne")),
        "os_interface": keystone.get("interface", "internal"),
        "os_insecure": keystone.get("insecure", False),
        "os_cacert": keystone.get("cacert", ""),
        "os_service_project_id": keystone.get("service_project_id", keystone.get("os_service_project_id", "")),
        "admin_legacy_project_policy": keystone.get(
            "admin_legacy_project_policy", keystone.get("legacy_project_policy", False)
        ),
        "database_url": database.get("url", ""),
        "database_password_file": database.get("password_file", database.get("database_password_file", "")),
        "database_pool_size": database.get("pool_size", 5),
        "database_max_overflow": database.get("max_overflow", 10),
        "database_connect_timeout": database.get("connect_timeout", 10),
        "database_pool_timeout": database.get("pool_timeout", 10),
        "redis_url": cache.get("redis_url", "redis://localhost:6379/7"),
        "redis_password_file": cache.get("password_file", cache.get("redis_password_file", "")),
        "sentinel_enabled": cache.get("sentinel_enabled", False),
        "sentinel_master_name": cache.get("sentinel_master_name", ""),
        "sentinel_hosts": str(sentinel_hosts),
        "cache_ttl_fast": cache.get("ttl_fast", 15),
        "cache_ttl_normal": cache.get("ttl_normal", 30),
        "cache_ttl_slow": cache.get("ttl_slow", 60),
        "cache_ttl_static": cache.get("ttl_static", 300),
        "cache_dynamic_threshold_low": cache.get("dynamic_threshold_low", 5),
        "cache_dynamic_threshold_high": cache.get("dynamic_threshold_high", 20),
        "cache_ttl_identity_stable": cache.get("ttl_identity_stable", 86400),
        "cache_ttl_catalog_slow": cache.get("ttl_catalog_slow", 900),
        "cache_ttl_project_meta": cache.get("ttl_project_meta", 300),
        "cache_ttl_operational_live": cache.get("ttl_operational_live", 30),
        "cache_ttl_admin_overview": cache.get("ttl_admin_overview", 60),
        "cache_ttl_auth_token": cache.get("ttl_auth_token", 60),
        "drover_callback_base_url": drover.get("callback_base_url", ""),
        "drover_kubeconfig_encryption_key": drover.get("kubeconfig_encryption_key", ""),
        "drover_kubeconfig_encryption_key_file": drover.get(
            "kubeconfig_encryption_key_file", drover.get("encryption_key_file", "")
        ),
        "drover_boot_volume_size_gb": drover.get("boot_volume_size_gb", 30),
        "drover_occm_enabled": drover.get("occm_enabled", True),
        "drover_occm_image": drover.get("occm_image", "ghcr.io/openstack-afterglow/openstack-cloud-controller-manager:v1.28.0"),
        "drover_cinder_csi_enabled": drover.get("cinder_csi_enabled", True),
        "drover_cinder_csi_image": drover.get("cinder_csi_image", "registry.k8s.io/provider-os/cinder-csi-plugin:v1.28.0"),
        "drover_manila_csi_enabled": drover.get("manila_csi_enabled", False),
        "drover_manila_csi_image": drover.get("manila_csi_image", "registry.k8s.io/provider-os/manila-csi-plugin:v1.28.0"),
        "drover_manila_csi_nfs_image": drover.get("manila_csi_nfs_image", "registry.k8s.io/sig-storage/nfsplugin:v4.4.0"),
        "drover_manila_csi_share_protocol": drover.get("manila_csi_share_protocol", "NFS"),
        "drover_keystone_auth_enabled": drover.get("keystone_auth_enabled", False),
        "drover_keystone_auth_image": drover.get("keystone_auth_image", "registry.k8s.io/provider-os/k8s-keystone-auth:v1.28.0"),
        "drover_keystone_auth_policy": drover.get("keystone_auth_policy", ""),
        "drover_octavia_ingress_enabled": drover.get("octavia_ingress_enabled", False),
        "drover_octavia_ingress_image": drover.get("octavia_ingress_image", "registry.k8s.io/provider-os/octavia-ingress-controller:v1.28.0"),
        "drover_barbican_kms_enabled": drover.get("barbican_kms_enabled", False),
        "drover_barbican_kms_image": drover.get("barbican_kms_image", "registry.k8s.io/provider-os/k8s-barbican-kms:v1.28.0"),
        "drover_barbican_kms_kek_id": drover.get("barbican_kms_kek_id", ""),
        "drover_cert_rotation_node_timeout_sec": drover.get("cert_rotation_node_timeout_sec", 300),
        "drover_cert_rotation_job_image": drover.get("cert_rotation_job_image", "rancher/k3s:v1.28.4-k3s2"),
        "k3s_health_interval": drover.get("k3s_health_interval", drover.get("health_interval", 180)),
        "drover_stampede_enabled": drover.get("stampede_enabled", False),
        "drover_stampede_interval": drover.get("stampede_interval", 60),
        "drover_stampede_scale_down_threshold": drover.get("stampede_scale_down_threshold", 0.5),
        "drover_stampede_scale_down_window": drover.get("stampede_scale_down_window", 600),
        "drover_stampede_scale_up_cooldown": drover.get("stampede_scale_up_cooldown", 120),
        "drover_stampede_scale_down_cooldown": drover.get("stampede_scale_down_cooldown", 300),
        "drover_stampede_resource_headroom_factor": drover.get("stampede_resource_headroom_factor", 0.3),
        "drover_reconcile_interval": drover.get("reconcile_interval", drover.get("drover_reconcile_interval", 300)),
        "drover_reconcile_concurrency_per_project": drover.get("reconcile_concurrency_per_project", drover.get("max_concurrent_reconciles_per_project", 2)),
        "drover_callback_allowed_cidrs": drover.get("callback_allowed_cidrs", drover.get("drover_callback_allowed_cidrs", [])),
        "drover_policy_file": drover.get("policy_file", drover.get("drover_policy_file", "/etc/drover/policy.yaml")),
        "trusted_proxies": drover.get("trusted_proxies", "127.0.0.1/32,::1/128"),
    }


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    os_auth_url: str = ""
    os_username: str = ""
    os_password: str = ""
    os_password_file: str = ""
    os_project_name: str = ""
    os_project_domain_name: str = "Default"
    os_user_domain_name: str = "Default"
    os_region_name: str = "RegionOne"
    os_interface: str = "internal"
    os_insecure: bool = False
    os_cacert: str = ""
    os_service_project_id: str = ""
    admin_legacy_project_policy: bool = False

    database_url: str = ""
    database_password_file: str = ""
    database_pool_size: int = 5
    database_max_overflow: int = 10
    database_connect_timeout: int = 10
    database_pool_timeout: int = 10

    redis_url: str = "redis://localhost:6379/7"
    redis_password_file: str = ""
    sentinel_enabled: bool = False
    sentinel_master_name: str = ""
    sentinel_hosts: str = ""
    cache_ttl_fast: int = 15
    cache_ttl_normal: int = 30
    cache_ttl_slow: int = 60
    cache_ttl_static: int = 300
    cache_dynamic_threshold_low: int = 5
    cache_dynamic_threshold_high: int = 20
    cache_ttl_identity_stable: int = 86400
    cache_ttl_catalog_slow: int = 900
    cache_ttl_project_meta: int = 300
    cache_ttl_operational_live: int = 30
    cache_ttl_admin_overview: int = 60
    cache_ttl_auth_token: int = 60

    drover_callback_base_url: str = ""
    drover_policy_file: str = "/etc/drover/policy.yaml"
    drover_kubeconfig_encryption_key: str = ""
    drover_kubeconfig_encryption_key_file: str = ""
    drover_boot_volume_size_gb: int = 30
    drover_occm_enabled: bool = True
    drover_occm_image: str = "ghcr.io/openstack-afterglow/openstack-cloud-controller-manager:v1.28.0"
    drover_cinder_csi_enabled: bool = True
    drover_cinder_csi_image: str = "registry.k8s.io/provider-os/cinder-csi-plugin:v1.28.0"
    drover_manila_csi_enabled: bool = False
    drover_manila_csi_image: str = "registry.k8s.io/provider-os/manila-csi-plugin:v1.28.0"
    drover_manila_csi_nfs_image: str = "registry.k8s.io/sig-storage/nfsplugin:v4.4.0"
    drover_manila_csi_share_protocol: str = "NFS"
    drover_keystone_auth_enabled: bool = False
    drover_keystone_auth_image: str = "registry.k8s.io/provider-os/k8s-keystone-auth:v1.28.0"
    drover_keystone_auth_policy: str = ""
    drover_octavia_ingress_enabled: bool = False
    drover_octavia_ingress_image: str = "registry.k8s.io/provider-os/octavia-ingress-controller:v1.28.0"
    drover_barbican_kms_enabled: bool = False
    drover_barbican_kms_image: str = "registry.k8s.io/provider-os/k8s-barbican-kms:v1.28.0"
    drover_barbican_kms_kek_id: str = ""
    drover_cert_rotation_node_timeout_sec: int = 300
    drover_cert_rotation_job_image: str = "rancher/k3s:v1.28.4-k3s2"
    k3s_health_interval: int = 180
    drover_stampede_enabled: bool = False
    drover_stampede_interval: int = 60
    drover_stampede_scale_down_threshold: float = 0.5
    drover_stampede_scale_down_window: int = 600
    drover_stampede_scale_up_cooldown: int = 120
    drover_stampede_scale_down_cooldown: int = 300
    drover_stampede_resource_headroom_factor: float = 0.3
    drover_reconcile_interval: int = 300
    drover_reconcile_concurrency_per_project: int = 2
    drover_callback_allowed_cidrs: list[str] = []
    trusted_proxies: str = "127.0.0.1/32,::1/128"

    @field_validator("drover_callback_allowed_cidrs", mode="before")
    @classmethod
    def validate_callback_allowed_cidrs(cls, value: Any) -> list[str]:
        if isinstance(value, str):
            if not value.strip():
                return []
            return [c.strip() for c in value.split(",") if c.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(c).strip() for c in value if str(c).strip()]
        return []
    @field_validator("drover_kubeconfig_encryption_key")
    @classmethod
    def validate_encryption_key(cls, value: str) -> str:
        value = value.strip()
        if value:
            if len(value) != 64:
                raise ValueError("drover.kubeconfig_encryption_key must be 64 hexadecimal characters")
            try:
                bytes.fromhex(value)
            except ValueError as exc:
                raise ValueError("drover.kubeconfig_encryption_key must be hexadecimal") from exc
        return value


    @model_validator(mode="after")
    def _load_file_secrets(self) -> Settings:
        target_os_pass_file = self.os_password_file or os.environ.get("OS_PASSWORD_FILE", "")
        if not target_os_pass_file and not self.os_password and Path("/etc/drover/secrets/os_password").is_file():
            target_os_pass_file = "/etc/drover/secrets/os_password"
        if target_os_pass_file:
            self.os_password_file = target_os_pass_file
            p = Path(target_os_pass_file)
            if p.is_file():
                self.os_password = p.read_text().strip()

        target_enc_file = (
            self.drover_kubeconfig_encryption_key_file
            or os.environ.get("DROVER_KUBECONFIG_ENCRYPTION_KEY_FILE", "")
        )
        if (
            not target_enc_file
            and not self.drover_kubeconfig_encryption_key
            and Path("/etc/drover/secrets/kubeconfig_encryption_key").is_file()
        ):
            target_enc_file = "/etc/drover/secrets/kubeconfig_encryption_key"
        if target_enc_file:
            self.drover_kubeconfig_encryption_key_file = target_enc_file
            p = Path(target_enc_file)
            if p.is_file():
                self.drover_kubeconfig_encryption_key = p.read_text().strip()

        target_db_pass_file = self.database_password_file or os.environ.get("DATABASE_PASSWORD_FILE", "")
        if not target_db_pass_file and Path("/etc/drover/secrets/database_password").is_file():
            target_db_pass_file = "/etc/drover/secrets/database_password"
        if target_db_pass_file:
            self.database_password_file = target_db_pass_file
            p = Path(target_db_pass_file)
            if p.is_file():
                db_pass = p.read_text().strip()
                self.database_url = _inject_db_password(self.database_url, db_pass)

        target_redis_pass_file = self.redis_password_file or os.environ.get("REDIS_PASSWORD_FILE", "")
        if not target_redis_pass_file and Path("/etc/drover/secrets/redis_password").is_file():
            target_redis_pass_file = "/etc/drover/secrets/redis_password"
        if target_redis_pass_file:
            self.redis_password_file = target_redis_pass_file
            p = Path(target_redis_pass_file)
            if p.is_file():
                redis_pass = p.read_text().strip()
                self.redis_url = _inject_redis_password(self.redis_url, redis_pass)

        return self
    @property
    def ssl_verify(self) -> bool | str:
        if self.os_insecure:
            return False
        return self.os_cacert or True


@lru_cache
def get_settings() -> Settings:
    for key, value in _load_toml().items():
        if not os.environ.get(key.upper()):
            os.environ[key.upper()] = str(value)
    return Settings()


def validate_config(settings: Settings | None = None) -> Settings:
    """Validate core deployment configuration required for service operation.

    Raises ConfigurationError if any required setting is missing or invalid.
    """
    if settings is None:
        settings = get_settings()

    missing: list[str] = []
    if not settings.database_url or not settings.database_url.strip():
        missing.append("database_url (DATABASE_URL)")
    if not settings.drover_callback_base_url or not settings.drover_callback_base_url.strip():
        missing.append("drover_callback_base_url (DROVER_CALLBACK_BASE_URL)")
    if not settings.drover_kubeconfig_encryption_key or not settings.drover_kubeconfig_encryption_key.strip():
        missing.append("drover_kubeconfig_encryption_key (DROVER_KUBECONFIG_ENCRYPTION_KEY)")
    if not settings.os_auth_url or not settings.os_auth_url.strip():
        missing.append("os_auth_url (OS_AUTH_URL)")
    if not settings.os_username or not settings.os_username.strip():
        missing.append("os_username (OS_USERNAME)")
    if not settings.os_password or not settings.os_password.strip():
        missing.append("os_password (OS_PASSWORD)")

    if missing:
        raise ConfigurationError(
            f"Missing required core deployment configuration: {', '.join(missing)}"
        )

    return settings
