"""Manila CSI Plugin — Manila 공유 파일시스템 기반 PV/PVC 프로비저닝."""

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


class ManilaCsiPlugin:
    name = "manila_csi"

    def should_deploy(self, settings: Settings) -> bool:
        if not settings.drover_manila_csi_enabled:
            return False
        if not settings.os_auth_url or not settings.os_username or not settings.os_password:
            _logger.warning("ManilaCsI 활성화됨이지만 OpenStack 인증 정보 미설정")
            return False
        return True

    def cloud_conf_sections(self, project_id: str, settings: Settings) -> str:
        """Manila CSI는 별도 Secret 사용 — cloud.conf에 추가 섹션 없음."""
        return ""

    def generate_manifests(self, cluster_name: str, project_id: str, settings: Settings, **kwargs) -> str:
        """Manila CSI + NFS CSI 드라이버 매니페스트 반환."""
        manila = _jinja.get_template("k3s_plugins/manila_csi/manifests.yaml.j2").render(
            manila_csi_image=settings.drover_manila_csi_image,
            cluster_name=cluster_name,
            share_protocol=settings.drover_manila_csi_share_protocol,
            os_auth_url=settings.os_auth_url,
            os_region=settings.os_region_name,
            os_username=settings.os_username,
            os_password=settings.os_password,
            os_domain=settings.os_user_domain_name,
            project_id=project_id,
        )
        nfs = _jinja.get_template("k3s_plugins/manila_csi/nfs_csi.yaml.j2").render(
            nfs_image=settings.drover_manila_csi_nfs_image,
        )
        return manila + "\n---\n" + nfs

    def extra_write_files(self, project_id: str, cluster_name: str, settings: Settings) -> list[dict]:
        return []

    def server_install_args(self, settings: Settings) -> list[str]:
        return []

    def agent_install_args(self, settings: Settings) -> list[str]:
        return []

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        return False
