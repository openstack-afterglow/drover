"""Cinder CSI Plugin — Cinder 블록 스토리지 기반 PV/PVC 프로비저닝."""

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


class CinderCsiPlugin:
    name = "cinder_csi"

    def should_deploy(self, settings: Settings) -> bool:
        if not settings.drover_cinder_csi_enabled:
            return False
        if not settings.os_auth_url or not settings.os_username or not settings.os_password:
            _logger.warning("CinderCSI 활성화됨이지만 OpenStack 인증 정보 미설정")
            return False
        return True

    def cloud_conf_sections(self, project_id: str, settings: Settings) -> str:
        """[BlockStorage] 섹션 반환. cloud.conf [Global] 섹션과 병합된다."""
        return "[BlockStorage]\nbs-version=v3\n"

    def generate_manifests(self, cluster_name: str, project_id: str, settings: Settings, **kwargs) -> str:
        tmpl = _jinja.get_template("k3s_plugins/cinder_csi/manifests.yaml.j2")
        return tmpl.render(
            cinder_csi_image=settings.drover_cinder_csi_image,
            cluster_name=cluster_name,
            default_az=settings.resource_id("k3s.volume_availability_zone"),
        )

    def extra_write_files(self, project_id: str, cluster_name: str, settings: Settings) -> list[dict]:
        return []

    def server_install_args(self, settings: Settings) -> list[str]:
        return []

    def agent_install_args(self, settings: Settings) -> list[str]:
        return []

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        return False  # Cinder CSI는 kubelet 인자 불필요 (CSI 드라이버 방식)
