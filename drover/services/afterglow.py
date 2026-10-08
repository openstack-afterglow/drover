"""Client service for Afterglow internal GPU admission control."""

from __future__ import annotations

import logging
from urllib.parse import urlsplit, urlunsplit

import httpx

from drover.config import Settings, get_settings

_logger = logging.getLogger(__name__)


def _admission_url(base_url: str) -> str:
    """Append the internal admission path without duplicating an API version suffix."""
    parts = urlsplit(base_url)
    path = parts.path.rstrip("/")
    for api_suffix in ("/api/v1", "/v1"):
        if path == api_suffix or path.endswith(api_suffix):
            path = path[: -len(api_suffix)]
            break
    endpoint_path = f"{path}/api/v1/internal/k3s/gpu-admission"
    return urlunsplit((parts.scheme, parts.netloc, endpoint_path, "", ""))


async def check_gpu_admission(
    project_id: str,
    flavor_id: str,
    settings: Settings | None = None,
) -> tuple[bool, str | None]:
    """Request K3s GPU node admission decision from Afterglow.

    Returns:
        (gpu_required, blocked_reason)
        If admission is granted, blocked_reason is None and gpu_required is the authority boolean.
        If admission is denied or unavailable or fails, blocked_reason is a non-empty string and gpu_required is False.
    """
    if settings is None:
        settings = get_settings()

    base_url = str(getattr(settings, "drover_afterglow_admission_url", "") or "").strip()
    token = str(getattr(settings, "drover_afterglow_admission_token", "") or "").strip()

    if not base_url:
        _logger.warning("Afterglow admission URL not configured")
        return False, "afterglow_admission_url_missing"

    if not token:
        _logger.warning("Afterglow admission token not configured")
        return False, "afterglow_admission_token_missing"

    url = _admission_url(base_url)
    headers = {
        "X-Afterglow-K3s-Admission-Token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "project_id": project_id,
        "flavor_id": flavor_id,
    }

    try:
        verify = getattr(settings, "ssl_verify", True)
        async with httpx.AsyncClient(verify=verify, timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=headers)

        if resp.status_code == 200:
            try:
                data = resp.json()
                if not isinstance(data, dict) or "gpu_required" not in data:
                    _logger.warning("Afterglow admission returned malformed response body")
                    return False, "afterglow_admission_malformed_response"
                gpu_required = bool(data["gpu_required"])
                return gpu_required, None
            except Exception as exc:
                _logger.warning("Failed to parse Afterglow admission JSON response: %s", exc)
                return False, "afterglow_admission_malformed_json"

        elif resp.status_code == 409:
            _logger.info(
                "Afterglow GPU quota denied for project %s, flavor %s",
                project_id,
                flavor_id,
            )
            return False, "gpu_quota_exceeded"

        elif resp.status_code == 503:
            _logger.warning(
                "Afterglow GPU admission service unavailable for project %s",
                project_id,
            )
            return False, "gpu_admission_unavailable"

        elif resp.status_code == 401:
            _logger.error("Afterglow GPU admission authentication failed (401)")
            return False, "afterglow_admission_unauthorized"

        elif resp.status_code == 400:
            _logger.warning("Afterglow GPU admission returned 400 bad request (flavor missing)")
            return False, "afterglow_admission_bad_request"

        else:
            _logger.warning(
                "Afterglow GPU admission returned unexpected status %d",
                resp.status_code,
            )
            return False, f"afterglow_admission_error_{resp.status_code}"

    except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError) as exc:
        _logger.warning("Network error reaching Afterglow admission service (%s): %s", url, exc)
        return False, "afterglow_admission_network_error"
    except Exception as exc:
        _logger.exception("Unexpected error during Afterglow GPU admission check: %s", exc)
        return False, "afterglow_admission_unexpected_error"


class GpuAdmissionDenied(RuntimeError):
    """Afterglow did not admit a GPU worker immediately before its Nova server create."""


async def require_gpu_admission(project_id: str, flavor_id: str, settings: Settings | None = None) -> None:
    """Re-check live GPU usage against the project limit right before creating a GPU server.

    Afterglow admission counts live Nova servers rather than reserving capacity, so the decision taken at enqueue
    time must be repeated next to the create. Denial, unavailability or transport failure raises before any
    resource is created.
    """
    settings = settings or get_settings()
    if not str(getattr(settings, "drover_afterglow_admission_url", "") or "").strip():
        return
    _gpu_required, reason = await check_gpu_admission(project_id, flavor_id, settings=settings)
    if reason:
        raise GpuAdmissionDenied(reason)
