"""Barbican KMS Plugin — K8s Secret을 Barbican으로 암호화.

§ PR3 재설계 — k3s 외부 systemd service 방식.

이전 host static pod 패턴(§ 7f1de4c)은 K3s 1.34 의 kubelet startup sequence 와
양립 불가 (kubelet 이 apiserver ready 후 시작 → static pod 영원히 미띄움 →
KMS sock 영원히 미생성 → 데드락).

본 재설계는 KMS plugin 을 **k3s 외부 systemd service** 로 분리:
1. k3s install (INSTALL_K3S_SKIP_START=true) — binary 만 준비, service 미시작
2. install_kms.sh — podman 으로 KMS image pull + binary 추출 → /usr/local/bin/
3. systemctl start barbican-kms.service → /var/lib/kms/kms.sock 생성
4. systemctl start k3s.service → apiserver 가 이미 존재하는 sock 와 통신, 정상 부팅

statically linked Go binary 라 host install 가능 (libc 의존성 없음).
"""

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


class BarbicanKmsPlugin:
    name = "barbican_kms"

    def should_deploy(self, settings: Settings) -> bool:
        if not settings.drover_barbican_kms_enabled:
            return False
        # PR2 — KEK ID 글로벌 미설정 시에도 동적 발급으로 대체 가능. caller 가 ensure_project_kek
        # 호출하여 cluster 별 KEK 결정.
        if not settings.os_username or not settings.os_password:
            _logger.warning("Barbican KMS 활성화됨이지만 OpenStack 인증 정보 미설정")
            return False
        return True

    def cloud_conf_sections(self, project_id: str, settings: Settings) -> str:
        """OCCM 의 cluster cloud.conf 에 추가될 [KeyManager] 섹션 반환.

        주의: 이 섹션은 **OCCM 이 사용하는 cluster cloud.conf** 에 추가되는 것이며,
        KMS systemd service 가 사용하는 host file `/etc/kubernetes/barbican-cloud.conf` 와는
        별개다 (KMS 는 apiserver 와 무관한 외부 service).
        """
        return "[KeyManager]\nuse-barbican=true\n"

    def generate_manifests(self, cluster_name: str, project_id: str, settings: Settings, **kwargs) -> str:
        """K3s manifest 배포 불필요 (외부 systemd service). 빈 문자열 반환."""
        return ""

    def extra_write_files(
        self,
        project_id: str,
        cluster_name: str,
        settings: Settings,
        app_credential: dict | None = None,
        kek_id: str | None = None,
    ) -> list[dict]:
        """KMS 운영에 필요한 host file 4건 작성."""
        if not app_credential or not app_credential.get("id") or not app_credential.get("secret"):
            raise ValueError("app_credential containing id and secret is required for Barbican KMS plugin")
        encryption_config = _jinja.get_template("k3s_plugins/barbican_kms/encryption_config.yaml.j2").render()
        effective_kek_id = kek_id or settings.drover_barbican_kms_kek_id
        systemd_unit = _jinja.get_template("k3s_plugins/barbican_kms/systemd_unit.j2").render()
        install_script = _jinja.get_template("k3s_plugins/barbican_kms/install_kms.sh.j2").render(
            kms_image=settings.drover_barbican_kms_image,
        )
        cloud_conf = _jinja.get_template("k3s_plugins/barbican_kms/cloud_conf.yaml.j2").render(
            auth_url=settings.os_auth_url,
            region=settings.os_region_name,
            app_credential_id=app_credential["id"],
            app_credential_secret=app_credential["secret"],
            ca_file="" if settings.os_insecure else (settings.os_cacert or ""),
            kek_id=effective_kek_id,
        )
        return [
            {
                "path": "/etc/kubernetes/encryption-config.yaml",
                "permissions": "0600",
                "content": encryption_config,
            },
            {
                "path": "/etc/systemd/system/barbican-kms.service",
                "permissions": "0644",
                "content": systemd_unit,
            },
            {
                "path": "/opt/k3s/install_kms.sh",
                "permissions": "0750",
                "content": install_script,
            },
            {
                "path": "/etc/kubernetes/barbican-cloud.conf",
                "permissions": "0600",
                "content": cloud_conf,
            },
        ]

    def server_install_args(self, settings: Settings) -> list[str]:
        """K3s 서버에 encryption-provider-config 만 전달.
        kubelet pod-manifest-path 는 host static pod 패턴 폐기로 불필요.
        """
        return [
            "--kube-apiserver-arg=encryption-provider-config=/etc/kubernetes/encryption-config.yaml",
        ]

    def agent_install_args(self, settings: Settings) -> list[str]:
        return []

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        return False
