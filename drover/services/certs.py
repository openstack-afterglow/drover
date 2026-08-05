"""k3s 인증서 분석 서비스 — CA 추출, 만료 파싱, TLS 프로브."""

from __future__ import annotations

import asyncio
import base64
import logging
import socket
import ssl
from datetime import UTC, datetime
from typing import Any

import yaml

_logger = logging.getLogger(__name__)


def extract_ca_pem(kubeconfig_yaml: str) -> str:
    """kubeconfig의 certificate-authority-data를 PEM 문자열로 반환."""
    kc = yaml.safe_load(kubeconfig_yaml)
    cluster_data = kc["clusters"][0]["cluster"]
    ca_b64 = cluster_data.get("certificate-authority-data", "")
    if not ca_b64:
        raise ValueError("kubeconfig에 certificate-authority-data가 없습니다")
    ca_der = base64.b64decode(ca_b64)
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    cert = x509.load_der_x509_certificate(ca_der)
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def _cert_info_from_pem(pem_bytes: bytes) -> dict[str, Any]:
    """PEM bytes → {not_after, not_before, subject, issuer, days_remaining}."""
    from cryptography import x509

    cert = x509.load_pem_x509_certificate(pem_bytes)
    now = datetime.now(UTC)
    not_after = cert.not_valid_after_utc
    not_before = cert.not_valid_before_utc
    days_remaining = (not_after - now).days
    return {
        "not_after": not_after.isoformat(),
        "not_before": not_before.isoformat(),
        "subject": cert.subject.rfc4514_string(),
        "issuer": cert.issuer.rfc4514_string(),
        "days_remaining": days_remaining,
    }


def _cert_info_from_b64(b64_data: str) -> dict[str, Any]:
    """base64 DER → cert_info dict."""
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    der = base64.b64decode(b64_data)
    cert = x509.load_der_x509_certificate(der)
    pem = cert.public_bytes(serialization.Encoding.PEM)
    return _cert_info_from_pem(pem)


def parse_kubeconfig_certs(kubeconfig_yaml: str) -> dict[str, Any]:
    """kubeconfig에서 CA / 클라이언트 인증서 만료 정보를 추출한다.

    Returns:
        {
            "ca": {...cert_info...},
            "client": {...cert_info...},
        }
    """
    kc = yaml.safe_load(kubeconfig_yaml)
    result: dict[str, Any] = {}

    cluster_data = kc["clusters"][0]["cluster"]
    ca_b64 = cluster_data.get("certificate-authority-data", "")
    if ca_b64:
        try:
            result["ca"] = _cert_info_from_b64(ca_b64)
        except Exception as e:
            _logger.warning("CA 인증서 파싱 실패: %s", e)
            result["ca"] = None

    user_data = kc["users"][0]["user"]
    client_b64 = user_data.get("client-certificate-data", "")
    if client_b64:
        try:
            result["client"] = _cert_info_from_b64(client_b64)
        except Exception as e:
            _logger.warning("클라이언트 인증서 파싱 실패: %s", e)
            result["client"] = None

    return result


async def probe_tls_server_cert(host: str, port: int = 6443, timeout: float = 3.0) -> list[dict[str, Any]]:
    """TLS 핸드셰이크로 서버 인증서 체인을 조회한다. 실패 시 빈 배열 반환."""
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        def _probe() -> list[dict[str, Any]]:
            with (
                socket.create_connection((host, port), timeout=timeout) as sock,
                ctx.wrap_socket(sock, server_hostname=host) as ssock,
            ):
                der = ssock.getpeercert(binary_form=True)
                if not der:
                    return []
                from cryptography import x509
                from cryptography.hazmat.primitives import serialization

                cert = x509.load_der_x509_certificate(der)
                pem = cert.public_bytes(serialization.Encoding.PEM)
                return [_cert_info_from_pem(pem)]

        return await asyncio.to_thread(_probe)
    except Exception as e:
        _logger.debug("TLS 프로브 실패 %s:%d — %s", host, port, e)
        return []
