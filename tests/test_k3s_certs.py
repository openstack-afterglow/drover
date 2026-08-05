"""PR 3-A — k3s 인증서 CA 다운로드 / 만료 조회 테스트."""

from __future__ import annotations

import base64
import datetime
from unittest.mock import AsyncMock, patch

import pytest
import yaml

# ---------------------------------------------------------------------------
# 테스트 인증서 생성 헬퍼
# ---------------------------------------------------------------------------


def _make_self_signed_cert_b64(
    common_name: str = "test-ca",
    days_valid: int = 365,
    is_ca: bool = True,
) -> str:
    """cryptography 라이브러리로 자체 서명 인증서를 만들고 base64 DER를 반환.

    days_valid < 0이면 이미 만료된 인증서를 생성한다.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.UTC)
    if days_valid < 0:
        not_before = now + datetime.timedelta(days=days_valid - 1)
        not_after = now + datetime.timedelta(days=days_valid)
    else:
        not_before = now
        not_after = now + datetime.timedelta(days=days_valid)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    der = cert.public_bytes(serialization.Encoding.DER)
    return base64.b64encode(der).decode()


def _make_kubeconfig(ca_b64: str, client_b64: str, server_url: str = "https://10.0.0.1:6443") -> str:
    """최소한의 kubeconfig YAML을 생성한다."""
    return yaml.dump(
        {
            "clusters": [{"cluster": {"server": server_url, "certificate-authority-data": ca_b64}, "name": "test"}],
            "users": [{"user": {"client-certificate-data": client_b64, "client-key-data": ""}, "name": "admin"}],
            "contexts": [{"context": {"cluster": "test", "user": "admin"}, "name": "test"}],
            "current-context": "test",
        }
    )


# ---------------------------------------------------------------------------
# k3s_certs 서비스 단위 테스트
# ---------------------------------------------------------------------------


def test_extract_ca_pem_returns_pem_header():
    ca_b64 = _make_self_signed_cert_b64("test-ca")
    kc = _make_kubeconfig(ca_b64, ca_b64)
    from drover.services.certs import extract_ca_pem

    pem = extract_ca_pem(kc)
    assert pem.startswith("-----BEGIN CERTIFICATE-----")


def test_parse_kubeconfig_certs_ca_subject():
    ca_b64 = _make_self_signed_cert_b64("my-ca")
    client_b64 = _make_self_signed_cert_b64("my-client", is_ca=False)
    kc = _make_kubeconfig(ca_b64, client_b64)
    from drover.services.certs import parse_kubeconfig_certs

    result = parse_kubeconfig_certs(kc)
    assert result["ca"]["subject"] == "CN=my-ca"
    assert result["client"]["subject"] == "CN=my-client"


def test_parse_kubeconfig_certs_days_remaining_positive():
    ca_b64 = _make_self_signed_cert_b64("ca", days_valid=100)
    kc = _make_kubeconfig(ca_b64, ca_b64)
    from drover.services.certs import parse_kubeconfig_certs

    result = parse_kubeconfig_certs(kc)
    assert result["ca"]["days_remaining"] > 0


def test_parse_kubeconfig_certs_expired_cert_negative():
    ca_b64 = _make_self_signed_cert_b64("old-ca", days_valid=-10)
    kc = _make_kubeconfig(ca_b64, ca_b64)
    from drover.services.certs import parse_kubeconfig_certs

    result = parse_kubeconfig_certs(kc)
    assert result["ca"]["days_remaining"] < 0


@pytest.mark.asyncio
async def test_probe_tls_server_cert_unreachable_returns_empty():
    from drover.services.certs import probe_tls_server_cert

    result = await probe_tls_server_cert("192.0.2.1", 6443, timeout=0.5)
    assert result == []


# ---------------------------------------------------------------------------
# API 엔드포인트 테스트
# ---------------------------------------------------------------------------


@pytest.fixture()
def _fake_kc():
    ca_b64 = _make_self_signed_cert_b64("test-ca", days_valid=200)
    client_b64 = _make_self_signed_cert_b64("test-client", days_valid=180, is_ca=False)
    return _make_kubeconfig(ca_b64, client_b64)


@pytest.mark.asyncio
async def test_ca_certificate_download_content_type(client, _fake_kc):
    cluster_rec = {"id": "c1", "name": "mycluster", "status": "ACTIVE"}
    with (
        patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=cluster_rec)),
        patch("drover.api.certificates.k3s_db.get_kubeconfig", new=AsyncMock(return_value=_fake_kc)),
    ):
        resp = await client.get("/v1/clusters/c1/ca-certificate")
    assert resp.status_code == 200
    assert "application/x-pem-file" in resp.headers["content-type"]


@pytest.mark.asyncio
async def test_ca_certificate_download_pem_body(client, _fake_kc):
    cluster_rec = {"id": "c1", "name": "mycluster", "status": "ACTIVE"}
    with (
        patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=cluster_rec)),
        patch("drover.api.certificates.k3s_db.get_kubeconfig", new=AsyncMock(return_value=_fake_kc)),
    ):
        resp = await client.get("/v1/clusters/c1/ca-certificate")
    body = resp.text
    assert body.startswith("-----BEGIN CERTIFICATE-----")


@pytest.mark.asyncio
async def test_ca_certificate_download_filename_header(client, _fake_kc):
    cluster_rec = {"id": "c1", "name": "mycluster", "status": "ACTIVE"}
    with (
        patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=cluster_rec)),
        patch("drover.api.certificates.k3s_db.get_kubeconfig", new=AsyncMock(return_value=_fake_kc)),
    ):
        resp = await client.get("/v1/clusters/c1/ca-certificate")
    assert "ca-mycluster.pem" in resp.headers.get("content-disposition", "")


@pytest.mark.asyncio
async def test_ca_certificate_cluster_not_found(client):
    with patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=None)):
        resp = await client.get("/v1/clusters/nonexistent/ca-certificate")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_ca_certificate_no_kubeconfig(client):
    cluster_rec = {"id": "c1", "name": "mycluster", "status": "ACTIVE"}
    with (
        patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=cluster_rec)),
        patch("drover.api.certificates.k3s_db.get_kubeconfig", new=AsyncMock(return_value=None)),
    ):
        resp = await client.get("/v1/clusters/c1/ca-certificate")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_certificate_expiry_structure(client, _fake_kc):
    cluster_rec = {"id": "c1", "name": "mycluster", "status": "ACTIVE"}

    async def _cached(key, ttl, fn, refresh=False, enabled=True):
        return await fn()

    with (
        patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=cluster_rec)),
        patch("drover.api.certificates.k3s_db.get_kubeconfig", new=AsyncMock(return_value=_fake_kc)),
        patch("drover.api.certificates.cached_call", new=AsyncMock(side_effect=_cached)),
    ):
        resp = await client.get("/v1/clusters/c1/certificate-expiry")
    assert resp.status_code == 200
    data = resp.json()
    assert "ca" in data
    assert "client" in data
    assert "server_via_tls" in data
    assert data["ca"]["days_remaining"] > 0
    assert data["client"]["days_remaining"] > 0


@pytest.mark.asyncio
async def test_certificate_expiry_unauthenticated():
    from httpx import ASGITransport, AsyncClient

    from drover.main import app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/clusters/c1/certificate-expiry")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_certificate_expiry_different_project_404(client):
    """다른 project_id의 cluster_id → 404."""
    with patch("drover.api.certificates.k3s_db.get_cluster", new=AsyncMock(return_value=None)):
        resp = await client.get("/v1/clusters/other-project-cluster/certificate-expiry")
    assert resp.status_code == 404
