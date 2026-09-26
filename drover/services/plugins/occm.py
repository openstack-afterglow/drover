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
        return True

    def cloud_conf_sections(
        self,
        project_id: str,
        settings: Settings,
        internal_network_name: str = "",
        app_credential: dict | None = None,
    ) -> str:
        """OCCM의 cloud.conf 전체 내용 반환 (Global + LoadBalancer + Networking)."""
        if not app_credential or not app_credential.get("id") or not app_credential.get("secret"):
            raise ValueError("app_credential containing id and secret is required for OCCM plugin")
        public_network_name = settings.resource_name("k3s.occm_public_network")
        # OCCM's public classification removes InternalIP, even on a pinned provider network.
        if public_network_name == internal_network_name:
            public_network_name = ""
        tmpl = _jinja.get_template("occm/cloud_config.conf.j2")
        return tmpl.render(
            auth_url=settings.os_auth_url,
            region=settings.os_region_name,
            app_credential_id=app_credential["id"],
            app_credential_secret=app_credential["secret"],
            project_id=project_id,
            ca_file="" if settings.os_insecure else (settings.os_cacert or ""),
            floating_network_id=settings.resource_id("k3s.occm_floating_network"),
            public_network_name=public_network_name,
            lb_subnet_id=settings.resource_id("k3s.lb_subnet"),
            internal_network_name=internal_network_name,
        )


    def generate_manifests(
        self, cluster_name: str, project_id: str, settings: Settings, *, cluster_id: str = "", **kwargs
    ) -> str:
        # OCCM names Octavia resources from --cluster-name; the immutable ID makes delete ownership exact.
        if not cluster_id:
            raise ValueError("cluster_id is required for OCCM resource ownership")
        tmpl = _jinja.get_template("occm/manifests.yaml.j2")
        return tmpl.render(
            occm_image=settings.drover_occm_image,
            cluster_name=cluster_id,
        )

    def extra_write_files(self, project_id: str, cluster_name: str, settings: Settings) -> list[dict]:
        return []

    def server_install_args(self, settings: Settings) -> list[str]:
        return ["--disable=servicelb"]  # OCCM owns LoadBalancer Services; do not run the K3s controller.

    def agent_install_args(self, settings: Settings) -> list[str]:
        return ["--kubelet-arg=cloud-provider=external"]

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        return True
