"""Keystone Webhook Auth Plugin — K8s 인증을 OpenStack Keystone과 연동.

8.14 데드락 해소 — hostNetwork host static pod 방식.

기존엔 cluster service URL(``k8s-keystone-auth.kube-system.svc.cluster.local:8443``)을
webhook endpoint로 사용했으나, 부팅 직후 apiserver가 ClusterDNS/kube-proxy 미준비로
이 service URL을 resolve 못 해 crash loop가 발생했다. 본 재설계는 webhook을
**hostNetwork=true static pod**로 띄우고 endpoint를 ``https://127.0.0.1:8443/webhook``
으로 변경하여 apiserver가 같은 호스트의 localhost로 직접 호출한다.

또한 기존엔 정책을 ConfigMap으로 주입했으나, static pod는 ServiceAccount/in-cluster
config가 없으므로 ``--keystone-policy-file``로 host file을 직접 사용한다 (upstream에서
PolicyFile은 PolicyConfigMap보다 우선 — `cloud-provider-openstack/pkg/identity/keystone/config.go`).
"""

import base64
import datetime
import ipaddress
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

_STATIC_POD_DIR = "/var/lib/rancher/k3s/agent/pod-manifests"
_AUTH_DIR = "/etc/kubernetes/keystone-auth"


def _generate_self_signed_cert() -> tuple[bytes, bytes]:
    """k8s-keystone-auth용 self-signed TLS 인증서 생성. SAN에 127.0.0.1 포함."""
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        _logger.error("cryptography 패키지 필요: uv add cryptography")
        raise

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "k8s-keystone-auth"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "union-k3s"),
        ]
    )
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC))
        .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=3650))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("k8s-keystone-auth"),
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )

    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return cert_pem, key_pem


class KeystoneAuthPlugin:
    name = "keystone_auth"

    # 동일 클러스터 내 server/agent 일관성을 위해 인증서 캐싱
    _cert_cache: dict[str, tuple[bytes, bytes]] = {}

    def should_deploy(self, settings: Settings) -> bool:
        if not settings.drover_keystone_auth_enabled:
            return False
        if not settings.drover_keystone_auth_image:
            _logger.warning("Keystone Auth 활성화됨이지만 이미지 미설정")
            return False
        if not settings.os_auth_url:
            _logger.warning("Keystone Auth 활성화됨이지만 os_auth_url 미설정")
            return False
        return True

    def cloud_conf_sections(self, project_id: str, settings: Settings) -> str:
        return ""

    def _get_or_create_cert(self, cluster_name: str) -> tuple[bytes, bytes]:
        if cluster_name not in self._cert_cache:
            self._cert_cache[cluster_name] = _generate_self_signed_cert()
        return self._cert_cache[cluster_name]

    def generate_manifests(self, cluster_name: str, project_id: str, settings: Settings, **kwargs) -> str:
        """Static pod로 전환되어 K8s 매니페스트 배포 불필요. 빈 문자열 반환."""
        return ""

    def extra_write_files(self, project_id: str, cluster_name: str, settings: Settings) -> list[dict]:
        """Static pod 운영에 필요한 host file 5건 작성.

        1. webhook config — apiserver가 부팅 시 읽음 (endpoint=127.0.0.1:8443)
        2. static pod manifest — kubelet이 감시 → keystone-auth pod 즉시 띄움
        3. tls.crt — webhook TLS 인증서 (host hostPath로 컨테이너에 마운트)
        4. tls.key — webhook TLS 개인키
        5. policy.json — 인증/인가 정책 (ConfigMap 의존 제거)
        """
        cert_pem, key_pem = self._get_or_create_cert(cluster_name)
        cert_b64 = base64.b64encode(cert_pem).decode()

        webhook_config = _jinja.get_template("k3s_plugins/keystone_auth/webhook_config.yaml.j2").render(
            cert_b64=cert_b64,
        )
        static_pod = _jinja.get_template("k3s_plugins/keystone_auth/static_pod.yaml.j2").render(
            keystone_auth_image=settings.drover_keystone_auth_image,
            os_auth_url=settings.os_auth_url,
        )
        policy_json = settings.drover_keystone_auth_policy.strip() or "[]"

        return [
            {
                "path": "/etc/kubernetes/keystone-webhook.yaml",
                "permissions": "0600",
                "content": webhook_config,
            },
            {
                "path": f"{_STATIC_POD_DIR}/k8s-keystone-auth.yaml",
                "permissions": "0600",
                "content": static_pod,
            },
            {
                "path": f"{_AUTH_DIR}/tls.crt",
                "permissions": "0644",
                "content": cert_pem.decode(),
            },
            {
                "path": f"{_AUTH_DIR}/tls.key",
                "permissions": "0600",
                "content": key_pem.decode(),
            },
            {
                "path": f"{_AUTH_DIR}/policy.json",
                "permissions": "0644",
                "content": policy_json,
            },
        ]

    def server_install_args(self, settings: Settings) -> list[str]:
        return [
            "--kube-apiserver-arg=authentication-token-webhook-config-file=/etc/kubernetes/keystone-webhook.yaml",
            f"--kubelet-arg=pod-manifest-path={_STATIC_POD_DIR}",
        ]

    def agent_install_args(self, settings: Settings) -> list[str]:
        return []

    def needs_external_cloud_provider(self, settings: Settings) -> bool:
        return False
