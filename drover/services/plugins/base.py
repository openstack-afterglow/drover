"""K3s 플러그인 Protocol 정의."""

from typing import Protocol

from drover.config import Settings


class K3sPlugin(Protocol):
    """cloud-provider-openstack 플러그인 인터페이스."""

    name: str  # e.g. "occm", "cinder_csi"

    def should_deploy(self, settings: Settings) -> bool:
        """이 플러그인을 배포할지 여부 (설정 완전성 검사)."""
        ...

    def cloud_conf_sections(self, project_id: str, settings: Settings) -> str:
        """cloud.conf에 추가할 INI 섹션 문자열 반환 (없으면 빈 문자열)."""
        ...

    def generate_manifests(self, cluster_name: str, project_id: str, settings: Settings, **kwargs) -> str:
        """K8s 매니페스트 YAML 반환."""
        ...

    def extra_write_files(self, project_id: str, cluster_name: str, settings: Settings) -> list[dict]:
        """cloud-init write_files에 추가할 항목 목록 반환.

        각 항목은 {"path": str, "permissions": str, "content": str} 형식.
        """
        ...

    def server_install_args(self, settings: Settings) -> list[str]:
        """K3s 서버 설치 시 추가할 인자 목록."""
        ...

    def agent_install_args(self, settings: Settings) -> list[str]:
        """K3s 에이전트 설치 시 추가할 인자 목록."""
        ...

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        """True이면 --disable-cloud-controller + --kubelet-arg=cloud-provider=external 필요."""
        ...
