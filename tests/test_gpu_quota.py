"""Tests for Drover GPU quota service and API endpoints."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drover.services import nova
from drover.services.gpu_quota import (
    DEFAULT_PROJECT_ID,
    _parse_alias_counts,
    check_gpu_quota,
    delete_project_gpu_quota,
    get_effective_gpu_quotas,
    get_project_gpu_quotas,
    get_project_gpu_usage,
    normalize_gpu_alias,
    set_project_gpu_quota,
    validate_quota_params,
)


def test_normalize_gpu_alias_canonicalization():
    assert normalize_gpu_alias("RTX-3090") == "RTX3090"
    assert normalize_gpu_alias("rtx_3090") == "RTX3090"
    assert normalize_gpu_alias("rtx 3090") == "RTX3090"
    assert normalize_gpu_alias("TITAN-X") == "TITANX"
    assert normalize_gpu_alias("titan_x") == "TITANX"


def test_audio_exclusion():
    assert normalize_gpu_alias("RTX3090Audio") == ""
    assert normalize_gpu_alias("rtx-audio") == ""
    parsed = _parse_alias_counts({"pci_passthrough:alias": "RTX3090:1,RTX3090Audio:1"})
    assert parsed == {"RTX3090": 1}

    parsed = _parse_alias_counts(
        {"pci_passthrough:alias": "RTX3090,sriov_nic:1,A100:0"}
    )
    assert parsed == {"RTX3090": 1, "A100": 1}

    with pytest.raises(ValueError, match="오디오"):
        validate_quota_params("RTX3090Audio", 1)


def test_validate_quota_params():
    with pytest.raises(ValueError, match="비어있을 수 없습니다"):
        validate_quota_params("", 1)
    with pytest.raises(ValueError, match="최대 64자"):
        validate_quota_params("A" * 65, 1)
    with pytest.raises(ValueError, match="-1 이상"):
        validate_quota_params("RTX3090", -2)
    assert validate_quota_params("rtx-3090", 5) == "RTX3090"


@pytest.mark.asyncio
async def test_db_unavailable_fails_closed():
    with patch("drover.services.gpu_quota.is_db_available", return_value=False):
        with pytest.raises(RuntimeError, match="DB가 초기화되지 않았습니다"):
            await get_project_gpu_quotas("proj-1")
        with pytest.raises(RuntimeError, match="DB가 초기화되지 않았습니다"):
            await set_project_gpu_quota("proj-1", "RTX3090", 2)
        with pytest.raises(RuntimeError, match="DB가 초기화되지 않았습니다"):
            await delete_project_gpu_quota("proj-1", "RTX3090")
        with pytest.raises(RuntimeError, match="DB가 초기화되지 않았습니다"):
            await get_effective_gpu_quotas("proj-1")
        with pytest.raises(RuntimeError, match="DB가 초기화되지 않았습니다"):
            await check_gpu_quota(MagicMock(), "proj-1", {"pci_passthrough:alias": "RTX3090:1"})


@pytest.mark.asyncio
async def test_defaults_override_and_unconfigured_zero():
    with patch("drover.services.gpu_quota.is_db_available", return_value=True):
        with patch(
            "drover.services.gpu_quota.get_project_gpu_quotas",
            side_effect=lambda pid: (
                [{"gpu_type": "RTX3090", "limit": 4}, {"gpu_type": "RTX4090", "limit": 2}]
                if pid == DEFAULT_PROJECT_ID
                else [{"gpu_type": "RTX3090", "limit": 8}]
            ),
        ):
            effective = await get_effective_gpu_quotas("proj-1")
            assert effective["RTX3090"] == 8  # Project overrides default
            assert effective["RTX4090"] == 2  # Default applies when project unconfigured
            assert effective.get("A100", 0) == 0  # Unconfigured = 0 fail-closed


@pytest.mark.asyncio
async def test_usage_calculation(mock_conn):
    server = MagicMock()
    server.status = "ACTIVE"
    server.flavor = {"id": "flv-1", "original_name": "gpu.rtx3090"}

    flavor = MagicMock()
    flavor.id = "flv-1"
    flavor.name = "gpu.rtx3090"
    flavor.extra_specs = {"pci_passthrough:alias": "RTX-3090:2"}

    mock_conn.compute.servers.return_value = [server]
    with patch("drover.services.nova.list_flavors", return_value=[flavor]):
        usage = await get_project_gpu_usage(mock_conn, "test-project-123")
        assert usage == {"RTX3090": 2}


@pytest.mark.asyncio
async def test_gpu_usage_calculation_with_real_flavor_info_model(mock_conn):
    """Regression test for FlavorInfo constructed by nova.list_flavors with ram/disk/extra_specs."""
    from drover.models.openstack import FlavorInfo

    server = MagicMock()
    server.status = "ACTIVE"
    server.flavor = {"id": "flv-1", "original_name": "gpu.rtx3090"}

    sdk_flavor = MagicMock()
    sdk_flavor.id = "flv-1"
    sdk_flavor.name = "gpu.rtx3090"
    sdk_flavor.vcpus = 8
    sdk_flavor.ram = 16384
    sdk_flavor.disk = 100
    sdk_flavor.is_public = True
    sdk_flavor.extra_specs = {"pci_passthrough:alias": "RTX-3090:2"}

    mock_conn.compute.flavors.return_value = [sdk_flavor]
    mock_conn.compute.servers.return_value = [server]

    flavors = nova.list_flavors(mock_conn)
    assert len(flavors) == 1
    flavor_info = flavors[0]
    assert isinstance(flavor_info, FlavorInfo)
    assert flavor_info.ram == 16384
    assert flavor_info.ram_mb == 16384
    assert flavor_info.disk == 100
    assert flavor_info.disk_gb == 100
    assert flavor_info.extra_specs == {"pci_passthrough:alias": "RTX-3090:2"}

    usage = await get_project_gpu_usage(mock_conn, "test-project-123")
    assert usage == {"RTX3090": 2}

@pytest.mark.asyncio
async def test_check_gpu_quota_rejection_and_unlimited(mock_conn):
    with patch("drover.services.gpu_quota.is_db_available", return_value=True):
        with patch(
            "drover.services.gpu_quota.get_effective_gpu_quotas",
            return_value={"RTX3090": 2, "A100": -1},
        ):
            with patch(
                "drover.services.gpu_quota.get_project_gpu_usage",
                return_value={"RTX3090": 2},
            ):
                # Unlimited A100 -> Pass
                ok, msg = await check_gpu_quota(mock_conn, "proj-1", {"pci_passthrough:alias": "A100:5"})
                assert ok is True

                # Exceeded RTX3090 -> Reject
                ok, msg = await check_gpu_quota(mock_conn, "proj-1", {"pci_passthrough:alias": "RTX3090:1"})
                assert ok is False
                assert "초과" in msg

                # Unconfigured V100 -> Reject with unassigned message
                ok, msg = await check_gpu_quota(mock_conn, "proj-1", {"pci_passthrough:alias": "V100:1"})
                assert ok is False
                assert "미할당" in msg


# -----------------------------------------------------------------------------
# API Endpoint Tests
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tenant_effective_and_status(client, mock_conn):
    with patch("drover.api.gpu_quotas.get_effective_gpu_quotas", new=AsyncMock(return_value={"RTX3090": 4})):
        resp = await client.get("/v1/gpu-quotas/effective")
        assert resp.status_code == 200
        assert resp.json() == {"RTX3090": 4}

    with patch("drover.api.gpu_quotas.get_effective_gpu_quotas", new=AsyncMock(return_value={"RTX3090": 4})):
        with patch("drover.api.gpu_quotas.get_project_gpu_usage", new=AsyncMock(return_value={"RTX3090": 1})):
            resp = await client.get("/v1/gpu-quotas/status")
            assert resp.status_code == 200
            assert resp.json() == [{"gpu_type": "RTX3090", "limit": 4, "in_use": 1, "available": 3}]


@pytest.mark.asyncio
async def test_tenant_check(client, mock_conn):
    with patch("drover.api.gpu_quotas.check_gpu_quota", new=AsyncMock(return_value=(True, ""))):
        resp = await client.post("/v1/gpu-quotas/check", json={"extra_specs": {"pci_passthrough:alias": "RTX3090:1"}})
        assert resp.status_code == 200
        assert resp.json() == {"ok": True, "detail": ""}


@pytest.mark.asyncio
async def test_admin_endpoints_require_admin(non_admin_client):
    routes = [
        ("GET", "/v1/admin/gpu-quotas/defaults", None),
        ("PUT", "/v1/admin/gpu-quotas/defaults", {"gpu_type": "RTX3090", "limit": 4}),
        ("DELETE", "/v1/admin/gpu-quotas/defaults/RTX3090", None),
        ("GET", "/v1/admin/gpu-quotas/proj-1", None),
        ("PUT", "/v1/admin/gpu-quotas/proj-1", {"gpu_type": "RTX3090", "limit": 2}),
        ("DELETE", "/v1/admin/gpu-quotas/proj-1/RTX3090", None),
    ]
    for method, path, json_data in routes:
        if method == "GET":
            resp = await non_admin_client.get(path)
        elif method == "PUT":
            resp = await non_admin_client.put(path, json=json_data)
        else:
            resp = await non_admin_client.delete(path)
        assert resp.status_code == 403


@pytest.mark.asyncio
async def test_admin_endpoints_success(admin_client, mock_conn):
    with patch("drover.api.gpu_quotas.get_project_gpu_quotas", new=AsyncMock(return_value=[{"gpu_type": "RTX3090", "limit": 4}])):
        resp = await admin_client.get("/v1/admin/gpu-quotas/defaults")
        assert resp.status_code == 200
        assert resp.json() == [{"gpu_type": "RTX3090", "limit": 4}]

    with patch("drover.api.gpu_quotas.set_project_gpu_quota", new=AsyncMock(return_value={"project_id": "__default__", "gpu_type": "RTX3090", "limit": 4})):
        resp = await admin_client.put("/v1/admin/gpu-quotas/defaults", json={"gpu_type": "RTX3090", "limit": 4})
        assert resp.status_code == 200
        assert resp.json()["limit"] == 4

    with patch("drover.api.gpu_quotas.delete_project_gpu_quota", new=AsyncMock()):
        resp = await admin_client.delete("/v1/admin/gpu-quotas/defaults/RTX3090")
        assert resp.status_code == 204

    with patch("drover.api.gpu_quotas.get_effective_gpu_quotas", new=AsyncMock(return_value={"RTX3090": 4})):
        with patch("drover.api.gpu_quotas.get_project_gpu_usage", new=AsyncMock(return_value={"RTX3090": 2})):
            resp = await admin_client.get("/v1/admin/gpu-quotas/proj-1")
            assert resp.status_code == 200
            assert resp.json() == [{"gpu_type": "RTX3090", "limit": 4, "in_use": 2, "available": 2}]


@pytest.mark.asyncio
async def test_api_db_unavailable_returns_503(client, admin_client):
    with patch("drover.api.gpu_quotas.get_effective_gpu_quotas", side_effect=RuntimeError("DB가 초기화되지 않았습니다")):
        resp = await client.get("/v1/gpu-quotas/effective")
        assert resp.status_code == 503

    with patch("drover.api.gpu_quotas.get_project_gpu_quotas", side_effect=RuntimeError("DB가 초기화되지 않았습니다")):
        resp = await admin_client.get("/v1/admin/gpu-quotas/defaults")
        assert resp.status_code == 503
