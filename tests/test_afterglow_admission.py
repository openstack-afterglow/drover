"""Unit tests for Afterglow GPU admission client."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from drover.config import Settings
from drover.services.afterglow import _admission_url, check_gpu_admission


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        ("https://afterglow.internal", "https://afterglow.internal/api/v1/internal/k3s/gpu-admission"),
        ("https://afterglow.internal/api/v1", "https://afterglow.internal/api/v1/internal/k3s/gpu-admission"),
        (
            "https://afterglow.internal/gateway/api/v1",
            "https://afterglow.internal/gateway/api/v1/internal/k3s/gpu-admission",
        ),
    ],
)
def test_admission_url_preserves_gateway_prefix_without_version_duplication(base_url, expected):
    assert _admission_url(base_url) == expected


@pytest.mark.asyncio
async def test_check_gpu_admission_missing_url_or_token():
    settings = MagicMock()
    settings.drover_afterglow_admission_url = ""
    settings.drover_afterglow_admission_token = "secret"

    gpu_required, reason = await check_gpu_admission("proj-1", "fl-1", settings)
    assert gpu_required is False
    assert reason == "afterglow_admission_url_missing"

    settings.drover_afterglow_admission_url = "http://afterglow.internal:8010"
    settings.drover_afterglow_admission_token = ""

    gpu_required, reason = await check_gpu_admission("proj-1", "fl-1", settings)
    assert gpu_required is False
    assert reason == "afterglow_admission_token_missing"


@pytest.mark.asyncio
async def test_check_gpu_admission_success_gpu():
    settings = MagicMock()
    settings.drover_afterglow_admission_url = "http://afterglow.internal:8010"
    settings.drover_afterglow_admission_token = "my-secret-token"
    settings.ssl_verify = False

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"gpu_required": True}

    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        gpu_required, reason = await check_gpu_admission("proj-123", "gpu.large", settings)

        assert gpu_required is True
        assert reason is None

        mock_post.assert_awaited_once()
        url = mock_post.call_args.args[0]
        assert url == "http://afterglow.internal:8010/api/v1/internal/k3s/gpu-admission"
        headers = mock_post.call_args.kwargs["headers"]
        assert headers["X-Afterglow-K3s-Admission-Token"] == "my-secret-token"
        payload = mock_post.call_args.kwargs["json"]
        assert payload == {"project_id": "proj-123", "flavor_id": "gpu.large"}


@pytest.mark.asyncio
async def test_check_gpu_admission_success_non_gpu():
    settings = MagicMock()
    settings.drover_afterglow_admission_url = "http://afterglow.internal:8010"
    settings.drover_afterglow_admission_token = "my-secret-token"
    settings.ssl_verify = False

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"gpu_required": False}

    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=mock_resp)):
        gpu_required, reason = await check_gpu_admission("proj-123", "cpu.small", settings)

        assert gpu_required is False
        assert reason is None


@pytest.mark.asyncio
async def test_check_gpu_admission_quota_denied_409():
    settings = MagicMock()
    settings.drover_afterglow_admission_url = "http://afterglow.internal:8010"
    settings.drover_afterglow_admission_token = "my-secret-token"

    mock_resp = MagicMock()
    mock_resp.status_code = 409

    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=mock_resp)):
        gpu_required, reason = await check_gpu_admission("proj-123", "gpu.large", settings)

        assert gpu_required is False
        assert reason == "gpu_quota_exceeded"


@pytest.mark.asyncio
async def test_check_gpu_admission_unavailable_503():
    settings = MagicMock()
    settings.drover_afterglow_admission_url = "http://afterglow.internal:8010"
    settings.drover_afterglow_admission_token = "my-secret-token"

    mock_resp = MagicMock()
    mock_resp.status_code = 503

    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=mock_resp)):
        gpu_required, reason = await check_gpu_admission("proj-123", "gpu.large", settings)

        assert gpu_required is False
        assert reason == "gpu_admission_unavailable"


@pytest.mark.asyncio
async def test_check_gpu_admission_unauthorized_401():
    settings = MagicMock()
    settings.drover_afterglow_admission_url = "http://afterglow.internal:8010"
    settings.drover_afterglow_admission_token = "invalid-token"

    mock_resp = MagicMock()
    mock_resp.status_code = 401

    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=mock_resp)):
        gpu_required, reason = await check_gpu_admission("proj-123", "gpu.large", settings)

        assert gpu_required is False
        assert reason == "afterglow_admission_unauthorized"


@pytest.mark.asyncio
async def test_check_gpu_admission_network_error():
    settings = MagicMock()
    settings.drover_afterglow_admission_url = "http://afterglow.internal:8010"
    settings.drover_afterglow_admission_token = "my-secret-token"

    with patch("httpx.AsyncClient.post", new=AsyncMock(side_effect=httpx.ConnectError("Connection refused"))):
        gpu_required, reason = await check_gpu_admission("proj-123", "gpu.large", settings)

        assert gpu_required is False
        assert reason == "afterglow_admission_network_error"


@pytest.mark.parametrize(
    "url",
    [
        "https://user:password@afterglow.internal",
        "https://afterglow.internal/api?token=secret",
        "https://afterglow.internal/api#secret",
    ],
)
def test_admission_url_rejects_credentials_and_opaque_components(url):
    with pytest.raises(ValueError, match="credential-free HTTP or HTTPS URL"):
        Settings(drover_afterglow_admission_url=url)
