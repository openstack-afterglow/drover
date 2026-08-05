"""OCCM (OpenStack Cloud Controller Manager) 플러그인."""

import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from drover.config import Settings

_logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent.parent.parent / "templates"
_jinja = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    undefined=StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
)


class OccmPlugin:
    name = "occm"

    def should_deploy(self, settings: Settings) -> bool:
        if not settings.drover_occm_enabled:
            return False
        if not settings.os_auth_url:
            _logger.warning("OCCM 활성화됨이지만 os_auth_url 미설정")
            return False
        if not settings.os_username or not settings.os_password:
            _logger.warning("OCCM 활성화됨이지만 OpenStack 인증 정보 미설정")
            return False
        return True

    def cloud_conf_sections(self, project_id: str, settings: Settings, internal_network_name: str = "") -> str:
        """OCCM의 cloud.conf 전체 내용 반환 (Global + LoadBalancer + Networking)."""
        tmpl = _jinja.get_template("occm/cloud_config.conf.j2")
        return tmpl.render(
            auth_url=settings.os_auth_url,
            region=settings.os_region_name,
            username=settings.os_username,
            password=settings.os_password,
            user_domain_name=settings.os_user_domain_name,
            project_id=project_id,
            ca_file="" if settings.os_insecure else (settings.os_cacert or ""),
            floating_network_id=settings.resource_id("k3s.occm_floating_network"),
            public_network_name=settings.resource_name("k3s.occm_public_network"),
            lb_subnet_id=settings.resource_id("k3s.lb_subnet"),
            internal_network_name=internal_network_name,
        )

    def generate_manifests(self, cluster_name: str, project_id: str, settings: Settings, **kwargs) -> str:
        tmpl = _jinja.get_template("occm/manifests.yaml.j2")
        return tmpl.render(
            occm_image=settings.drover_occm_image,
            cluster_name=cluster_name,
        )

    def extra_write_files(self, project_id: str, cluster_name: str, settings: Settings) -> list[dict]:
        return []

    def server_install_args(self, settings: Settings) -> list[str]:
        return []  # cloud-provider=external은 레지스트리가 needs_external_cloud_provider()로 처리

    def agent_install_args(self, settings: Settings) -> list[str]:
        return []

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        return True
