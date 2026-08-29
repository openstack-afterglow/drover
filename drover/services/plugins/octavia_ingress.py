"""Octavia Ingress Controller Plugin — Ingress → Octavia LB 라우팅."""

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


class OctaviaIngressPlugin:
    name = "octavia_ingress"

    def should_deploy(self, settings: Settings) -> bool:
        if not settings.drover_octavia_ingress_enabled:
            return False
        if not settings.os_auth_url:
            _logger.warning("OctaviaIngress 활성화됨이지만 os_auth_url 미설정")
            return False
        return True

    def cloud_conf_sections(self, project_id: str, settings: Settings) -> str:
        return ""

    def generate_manifests(
        self,
        cluster_name: str,
        project_id: str,
        settings: Settings,
        *,
        subnet_id: str,
        app_credential: dict,
        floating_network_id: str | None = None,
        **_,
    ) -> str:
        """매니페스트 생성.

        Args:
            subnet_id: 클러스터 네트워크에서 도출된 Octavia LB 생성 대상 subnet ID.
            app_credential: {"id": ..., "secret": ..., "user_id": ...} — per-cluster App Cred.
            floating_network_id: 외부 네트워크 ID (옵션).
        """
        if not subnet_id:
            raise ValueError("subnet_id는 필수입니다 (클러스터 네트워크에서 도출)")
        if not app_credential or not app_credential.get("id") or not app_credential.get("secret"):
            raise ValueError("app_credential에 id와 secret이 필요합니다")

        return _jinja.get_template("k3s_plugins/octavia_ingress/manifests.yaml.j2").render(
            octavia_ingress_image=settings.drover_octavia_ingress_image,
            cluster_name=cluster_name,
            os_auth_url=settings.os_auth_url,
            os_region=settings.os_region_name,
            app_credential_id=app_credential["id"],
            app_credential_secret=app_credential["secret"],
            subnet_id=subnet_id,
            floating_network_id=floating_network_id or settings.resource_id("k3s.octavia_ingress_floating_network"),
        )

    def extra_write_files(self, project_id: str, cluster_name: str, settings: Settings) -> list[dict]:
        return []

    def server_install_args(self, settings: Settings) -> list[str]:
        return []

    def agent_install_args(self, settings: Settings) -> list[str]:
        return []

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        return False
