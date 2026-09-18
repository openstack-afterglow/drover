"""Focused tests for the Stampede provisioning handoff boundary."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drover.config import Settings
from drover.services.afterglow import (
    ProvisioningConfigurationError,
    _provisioning_url,
    create_provisioning_intent,
    submit_provisioning_intent,
)


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        (
            "https://afterglow.internal/gateway/api/v1/",
            "https://afterglow.internal/gateway/api/v1/internal/k3s/provisioning-intents",
        ),
        (
            "https://afterglow.internal/gateway/v1",
            "https://afterglow.internal/gateway/api/v1/internal/k3s/provisioning-intents/k%3A1",
        ),
    ],
)
def test_provisioning_url_preserves_gateway_prefix(base_url, expected):
    key = "k:1" if base_url.endswith("v1") else None
    assert _provisioning_url(base_url, key) == expected


def test_provisioning_url_rejects_credentials_and_opaque_components():
    with pytest.raises(ProvisioningConfigurationError):
        _provisioning_url("https://user:password@afterglow.internal/api/v1")
    with pytest.raises(ProvisioningConfigurationError):
        _provisioning_url("https://afterglow.internal/api/v1?token=secret")
    with pytest.raises(ProvisioningConfigurationError):
        _provisioning_url("https://afterglow.internal/api/v1#secret")


def _settings():
    return Settings(
        drover_afterglow_provisioning_url="https://afterglow.internal/gateway/api/v1",
        drover_afterglow_provisioning_token="service-secret",
    )


@pytest.mark.asyncio
async def test_create_intent_uses_dedicated_authenticated_boundary():
    response = MagicMock(status_code=201)
    response.json.return_value = {"state": "pending", "idempotency_key": "job:node:0"}
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=response)) as post:
        result = await create_provisioning_intent(
            idempotency_key="job:node:0",
            project_id="project",
            cluster_id="cluster",
            nodegroup_id="nodegroup",
            name="cluster-stampede-abcd",
            flavor_id="flavor",
            image_id="image",
            network_id="network",
            boot_volume_size_gb=30,
            volume_availability_zone="az1",
            metadata={"drover.managed": "true"},
            settings=_settings(),
        )
    assert result["state"] == "pending"
    post.assert_awaited_once()
    assert post.call_args.args[0].endswith("/internal/k3s/provisioning-intents")
    assert post.call_args.kwargs["headers"]["X-Afterglow-K3s-Provisioning-Token"] == "service-secret"
    payload = post.call_args.kwargs["json"]
    assert payload["idempotency_key"] == "job:node:0"
    assert "userdata" not in payload


@pytest.mark.asyncio
async def test_submit_sends_userdata_only_in_transient_request():
    response = MagicMock(status_code=200)
    response.json.return_value = {
        "state": "succeeded",
        "server_id": "server-id",
        "volume_id": "volume-id",
        "name": "cluster-stampede-abcd",
    }
    with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=response)) as post:
        result = await submit_provisioning_intent("job:node:0", "base64-bootstrap", settings=_settings())
    assert result["server_id"] == "server-id"
    payload = post.call_args.kwargs["json"]
    assert payload == {"userdata": "base64-bootstrap"}
    assert "X-Afterglow-K3s-Provisioning-Token" in post.call_args.kwargs["headers"]


def test_kolla_provisioning_credential_is_secret_file_only():
    role = Path(__file__).parents[1] / "deploy/kolla/ansible/roles/drover"
    defaults = (role / "defaults/main.yml").read_text()
    task = (role / "tasks/config.yml").read_text()
    template = (role / "templates/drover.conf.j2").read_text()
    assert "drover_afterglow_provisioning_url" in defaults
    assert "afterglow_k3s_provisioning_token" in task
    assert 'mode: "0640"' in task
    assert "afterglow_provisioning_token_file" in template
    assert "afterglow_provisioning_token =" not in template
