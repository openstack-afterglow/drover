"""K3s 플러그인 레지스트리.

활성 플러그인 집계 및 cloud-init 생성에 필요한 데이터를 제공한다.
"""

import inspect
import logging
from dataclasses import dataclass
from typing import Any

from drover.config import Settings

from .barbican_kms import BarbicanKmsPlugin
from .cinder_csi import CinderCsiPlugin
from .keystone_auth import KeystoneAuthPlugin
from .manila_csi import ManilaCsiPlugin
from .occm import OccmPlugin
from .octavia_ingress import OctaviaIngressPlugin

_logger = logging.getLogger(__name__)

# 플러그인 등록 순서 = 배포 순서
ALL_PLUGINS = [
    OccmPlugin(),
    CinderCsiPlugin(),
    ManilaCsiPlugin(),
    OctaviaIngressPlugin(),
    KeystoneAuthPlugin(),
    BarbicanKmsPlugin(),
]


@dataclass(frozen=True)
class K3sPluginContext:
    """Read-only plugin view of deployment settings plus a frozen resource snapshot."""

    settings: Settings
    resource_snapshot: dict[str, dict[str, Any]]

    def __getattr__(self, name: str) -> Any:
        return getattr(self.settings, name)

    def resource_id(self, key: str) -> str:
        return str((self.resource_snapshot.get(key) or {}).get("id") or "")

    def resource_name(self, key: str) -> str:
        return str((self.resource_snapshot.get(key) or {}).get("name") or "")


def with_resource_policy_snapshot(settings: Settings, snapshot: dict[str, dict[str, Any]] | None) -> K3sPluginContext:
    """Bind plugin rendering to the operation's immutable policy snapshot."""
    return K3sPluginContext(settings=settings, resource_snapshot=snapshot or {})


def get_active_plugins(settings: Settings) -> list:
    """설정에서 활성화된 플러그인 목록 반환."""
    return [p for p in ALL_PLUGINS if p.should_deploy(settings)]


def needs_external_cloud_provider(settings: Settings) -> bool:
    """하나 이상의 플러그인이 --disable-cloud-controller 필요 시 True."""
    return any(p.needs_external_cloud_provider(settings) for p in get_active_plugins(settings))


def aggregate_cloud_conf(project_id: str, settings: Settings, internal_network_name: str = "") -> str | None:
    """활성 플러그인의 cloud.conf를 합산 반환.

    cloud.conf가 필요한 플러그인이 없으면 None 반환.
    [Global] 섹션은 OCCM 플러그인이 전체 내용으로 제공하며,
    다른 플러그인은 추가 섹션만 제공한다.
    """
    active = get_active_plugins(settings)
    sections: list[str] = []
    for plugin in active:
        if isinstance(plugin, OccmPlugin):
            section = plugin.cloud_conf_sections(project_id, settings, internal_network_name=internal_network_name)
        else:
            section = plugin.cloud_conf_sections(project_id, settings)
        if section:
            sections.append(section.strip())

    if not sections:
        return None
    return "\n\n".join(sections) + "\n"


def aggregate_manifests(
    cluster_name: str, project_id: str, settings: Settings, **kwargs
) -> tuple[list[dict[str, str]], list[str]]:
    """활성 플러그인의 매니페스트 목록 반환.

    Returns:
        (manifests, failures) — manifests: [{"name": "occm", "content": "..."}, ...],
        failures: generate_manifests 예외가 발생한 플러그인 이름 목록.
    """
    result: list[dict[str, str]] = []
    failures: list[str] = []
    for plugin in get_active_plugins(settings):
        try:
            content = plugin.generate_manifests(cluster_name, project_id, settings, **kwargs)
            result.append({"name": plugin.name, "content": content})
        except Exception:
            _logger.exception("플러그인 %s 매니페스트 생성 실패", plugin.name)
            failures.append(plugin.name)
    return result, failures


def aggregate_extra_write_files(
    project_id: str,
    cluster_name: str,
    settings: Settings,
    app_credential: dict | None = None,
    kek_id: str | None = None,
) -> list[dict]:
    """활성 플러그인의 추가 write_files 항목 합산.

    Args:
        app_credential: cluster 별 app credential (PR1) — barbican_kms 등 인증 필요한 plugin 에 전달.
        kek_id: project 별 동적 KEK (PR2) — barbican_kms 의 cloud.conf [KeyManager] key-id 에 사용.
    """
    result = []
    for plugin in get_active_plugins(settings):
        params = inspect.signature(plugin.extra_write_files).parameters
        kwargs: dict = {}
        if "app_credential" in params:
            kwargs["app_credential"] = app_credential
        if "kek_id" in params:
            kwargs["kek_id"] = kek_id
        files = plugin.extra_write_files(project_id, cluster_name, settings, **kwargs)
        result.extend(files)
    return result


def aggregate_server_args(settings: Settings) -> list[str]:
    """활성 플러그인의 K3s 서버 인자 합산 (중복 제거, 순서 유지)."""
    seen = set()
    result = []
    for plugin in get_active_plugins(settings):
        for arg in plugin.server_install_args(settings):
            if arg not in seen:
                seen.add(arg)
                result.append(arg)
    return result


def aggregate_agent_args(settings: Settings) -> list[str]:
    """활성 플러그인의 K3s 에이전트 인자 합산 (중복 제거, 순서 유지)."""
    seen = set()
    result = []
    for plugin in get_active_plugins(settings):
        for arg in plugin.agent_install_args(settings):
            if arg not in seen:
                seen.add(arg)
                result.append(arg)
    return result


def get_active_plugin_names(settings: Settings) -> dict[str, bool]:
    """플러그인명 → True 매핑 반환 (DB 저장용)."""
    return {p.name: True for p in get_active_plugins(settings)}
