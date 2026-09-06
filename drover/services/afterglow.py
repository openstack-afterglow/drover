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


class ProvisioningConfigurationError(RuntimeError):
    """Raised when the internal provisioning client is not safely configured."""


class ProvisioningRemoteError(RuntimeError):
    """A safe summary of an unsuccessful Afterglow provisioning response."""

    def __init__(
        self,
        status_code: int,
        *,
        state: str | None = None,
        detail: str | None = None,
        no_duplicate: bool = False,
    ) -> None:
        self.status_code = status_code
        self.state = state
        self.detail = detail
        self.no_duplicate = no_duplicate
        super().__init__(detail or f"Afterglow provisioning request failed ({status_code})")


def _provisioning_url(base_url: str, idempotency_key: str | None = None) -> str:
    """Build the internal URL while retaining any gateway prefix."""
    parts = urlsplit(str(base_url or "").strip())
    if (
        parts.scheme not in ("http", "https")
        or not parts.netloc
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise ProvisioningConfigurationError(
            "drover_afterglow_provisioning_url must be a credential-free HTTP or HTTPS URL"
        )
    path = parts.path.rstrip("/")
    for api_suffix in ("/api/v1", "/v1"):
        if path == api_suffix or path.endswith(api_suffix):
            path = path[: -len(api_suffix)]
            break
    endpoint_path = f"{path}/api/v1/internal/k3s/provisioning-intents"
    if idempotency_key is not None:
        from urllib.parse import quote

        endpoint_path += f"/{quote(idempotency_key, safe='')}"
    return urlunsplit((parts.scheme, parts.netloc, endpoint_path, "", ""))


def _provisioning_credentials(settings: Settings) -> tuple[str, str]:
    base_url = str(getattr(settings, "drover_afterglow_provisioning_url", "") or "").strip()
    token = str(getattr(settings, "drover_afterglow_provisioning_token", "") or "").strip()
    if not base_url:
        raise ProvisioningConfigurationError("Afterglow provisioning URL is not configured")
    if not token:
        raise ProvisioningConfigurationError("Afterglow provisioning token is not configured")
    return base_url, token


def _provisioning_error(resp: httpx.Response) -> ProvisioningRemoteError:
    state = None
    detail = None
    no_duplicate = False
    try:
        body = resp.json()
    except Exception:
        body = {}
    if isinstance(body, dict):
        nested_detail = body.get("detail")
        if isinstance(nested_detail, dict):
            state = nested_detail.get("state") if isinstance(nested_detail.get("state"), str) else None
            no_duplicate = bool(nested_detail.get("no_duplicate", False))
        else:
            state = body.get("state") if isinstance(body.get("state"), str) else None
            detail = nested_detail if isinstance(nested_detail, str) else None
            no_duplicate = bool(body.get("no_duplicate", False))
    return ProvisioningRemoteError(
        resp.status_code,
        state=state,
        detail=detail,
        no_duplicate=no_duplicate,
    )


async def create_provisioning_intent(
    *,
    idempotency_key: str,
    project_id: str,
    cluster_id: str,
    nodegroup_id: str,
    name: str,
    flavor_id: str,
    image_id: str,
    network_id: str,
    boot_volume_size_gb: int,
    volume_availability_zone: str,
    security_group_id: str | None = None,
    metadata: dict | None = None,
    config_drive: bool = False,
    settings: Settings | None = None,
) -> dict:
    """Create or retrieve a durable Afterglow node provisioning intent."""
    if settings is None:
        settings = get_settings()
    base_url, token = _provisioning_credentials(settings)
    payload = {
        "idempotency_key": idempotency_key,
        "project_id": project_id,
        "cluster_id": cluster_id,
        "nodegroup_id": nodegroup_id,
        "name": name,
        "flavor_id": flavor_id,
        "image_id": image_id,
        "network_id": network_id,
        "boot_volume_size_gb": boot_volume_size_gb,
        "volume_availability_zone": volume_availability_zone,
        "security_group_id": security_group_id,
        "metadata": dict(metadata or {}),
        "config_drive": bool(config_drive),
    }
    headers = {
        "X-Afterglow-K3s-Provisioning-Token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        async with httpx.AsyncClient(verify=getattr(settings, "ssl_verify", True), timeout=30.0) as client:
            resp = await client.post(_provisioning_url(base_url), json=payload, headers=headers)
    except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError) as exc:
        raise ProvisioningRemoteError(503, detail="Afterglow provisioning service unavailable") from exc
    if resp.status_code not in (200, 201):
        raise _provisioning_error(resp)
    try:
        body = resp.json()
    except Exception as exc:
        raise ProvisioningRemoteError(
            resp.status_code, detail="Afterglow returned malformed provisioning intent"
        ) from exc
    if not isinstance(body, dict):
        raise ProvisioningRemoteError(resp.status_code, detail="Afterglow returned malformed provisioning intent")
    return body


async def get_provisioning_intent(
    idempotency_key: str,
    *,
    settings: Settings | None = None,
) -> dict:
    """Retrieve a durable intent using the authenticated service boundary."""
    if settings is None:
        settings = get_settings()
    base_url, token = _provisioning_credentials(settings)
    headers = {"X-Afterglow-K3s-Provisioning-Token": token, "Accept": "application/json"}
    try:
        async with httpx.AsyncClient(verify=getattr(settings, "ssl_verify", True), timeout=30.0) as client:
            resp = await client.get(_provisioning_url(base_url, idempotency_key), headers=headers)
    except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError) as exc:
        raise ProvisioningRemoteError(503, detail="Afterglow provisioning service unavailable") from exc
    if resp.status_code != 200:
        raise _provisioning_error(resp)
    try:
        body = resp.json()
    except Exception as exc:
        raise ProvisioningRemoteError(
            resp.status_code, detail="Afterglow returned malformed provisioning intent"
        ) from exc
    if not isinstance(body, dict):
        raise ProvisioningRemoteError(resp.status_code, detail="Afterglow returned malformed provisioning intent")
    return body


async def submit_provisioning_intent(
    idempotency_key: str,
    userdata: str,
    *,
    settings: Settings | None = None,
) -> dict:
    """Submit transient, already-base64-encoded bootstrap data to an intent."""
    if settings is None:
        settings = get_settings()
    base_url, token = _provisioning_credentials(settings)
    if not isinstance(userdata, str) or not userdata:
        raise ValueError("provisioning userdata must be a non-empty base64 string")
    headers = {
        "X-Afterglow-K3s-Provisioning-Token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        async with httpx.AsyncClient(verify=getattr(settings, "ssl_verify", True), timeout=60.0) as client:
            resp = await client.post(
                _provisioning_url(base_url, idempotency_key) + "/submit",
                json={"userdata": userdata},
                headers=headers,
            )
    except (httpx.ConnectError, httpx.TimeoutException, httpx.RequestError) as exc:
        raise ProvisioningRemoteError(503, detail="Afterglow provisioning service unavailable") from exc
    if resp.status_code != 200:
        raise _provisioning_error(resp)
    try:
        body = resp.json()
    except Exception as exc:
        raise ProvisioningRemoteError(
            resp.status_code, detail="Afterglow returned malformed provisioning result"
        ) from exc
    if not isinstance(body, dict):
        raise ProvisioningRemoteError(resp.status_code, detail="Afterglow returned malformed provisioning result")
    return body


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
