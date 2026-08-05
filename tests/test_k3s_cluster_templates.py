"""k3s 클러스터 템플릿 API 단위 테스트."""

from unittest.mock import AsyncMock, patch

import pytest

_TMPL = {
    "id": "tmpl-1111-2222-3333-444444444444",
    "name": "standard",
    "description": "표준 템플릿",
    "k3s_version": "v1.32.0+k3s1",
    "default_node_count": 3,
    "default_agent_flavor_id": "flavor-gpu",
    "default_image_id": "img-001",
    "plugins_enabled": {"cinder_csi": True},
    "os_type": "ubuntu",
    "public_visible": True,
    "created_by": "admin-user-id",
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}

_PRIVATE_TMPL = {**_TMPL, "id": "tmpl-private-0000", "public_visible": False, "created_by": "other-user"}


# ---------------------------------------------------------------------------
# 권한 (admin 전용 엔드포인트)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_template_requires_admin(non_admin_client):
    resp = await non_admin_client.post(
        "/v1/cluster-templates",
        json={"name": "test-tmpl", "os_type": "ubuntu"},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_update_template_requires_admin(non_admin_client):
    resp = await non_admin_client.patch("/v1/cluster-templates/tmpl-1", json={"description": "x"})
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_delete_template_requires_admin(non_admin_client):
    resp = await non_admin_client.delete("/v1/cluster-templates/tmpl-1")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# 생성 (admin)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_template_success(admin_client):
    with patch("drover.services.template.create_template", new=AsyncMock(return_value=_TMPL)):
        resp = await admin_client.post(
            "/v1/cluster-templates",
            json={"name": "standard", "os_type": "ubuntu", "default_node_count": 3},
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "standard"
    assert data["default_node_count"] == 3


@pytest.mark.asyncio
async def test_create_template_invalid_name(admin_client):
    with patch("drover.services.template.create_template", new=AsyncMock(return_value=_TMPL)):
        resp = await admin_client.post(
            "/v1/cluster-templates",
            json={"name": "bad name!", "os_type": "ubuntu"},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_template_invalid_os_type(admin_client):
    with patch("drover.services.template.create_template", new=AsyncMock(return_value=_TMPL)):
        resp = await admin_client.post(
            "/v1/cluster-templates",
            json={"name": "t", "os_type": "windows"},
        )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 목록/조회 (사용자)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_templates_returns_public(admin_client):
    with patch("drover.services.template.list_templates", new=AsyncMock(return_value=[_TMPL])):
        resp = await admin_client.get("/v1/cluster-templates")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


@pytest.mark.asyncio
async def test_get_template_not_found(admin_client):
    with patch("drover.services.template.get_template", new=AsyncMock(return_value=None)):
        resp = await admin_client.get("/v1/cluster-templates/does-not-exist")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_private_template_by_non_owner(non_admin_client):
    """public=False이고 본인 소유 아닌 템플릿은 404 반환."""
    with patch("drover.services.template.get_template", new=AsyncMock(return_value=_PRIVATE_TMPL)):
        resp = await non_admin_client.get(f"/v1/cluster-templates/{_PRIVATE_TMPL['id']}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 수정/삭제 (admin)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_template_success(admin_client):
    updated = {**_TMPL, "description": "업데이트됨"}
    with patch("drover.services.template.update_template", new=AsyncMock(return_value=updated)):
        resp = await admin_client.patch(
            f"/v1/cluster-templates/{_TMPL['id']}",
            json={"description": "업데이트됨"},
        )
    assert resp.status_code == 200
    assert resp.json()["description"] == "업데이트됨"


@pytest.mark.asyncio
async def test_update_template_not_found(admin_client):
    with patch("drover.services.template.update_template", new=AsyncMock(return_value=None)):
        resp = await admin_client.patch("/v1/cluster-templates/no-such-id", json={"description": "x"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_template_success(admin_client):
    with patch("drover.services.template.delete_template", new=AsyncMock(return_value=True)):
        resp = await admin_client.delete(f"/v1/cluster-templates/{_TMPL['id']}")
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_delete_template_not_found(admin_client):
    with patch("drover.services.template.delete_template", new=AsyncMock(return_value=False)):
        resp = await admin_client.delete("/v1/cluster-templates/no-such-id")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# admin 미러 (GET /api/admin/k3s-cluster-templates)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_list_templates_requires_admin(non_admin_client):
    resp = await non_admin_client.get("/v1/admin/cluster-templates")
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_admin_list_templates_returns_all(admin_client):
    """admin 목록은 private 포함 전체 반환."""
    with patch(
        "drover.services.template.list_templates",
        new=AsyncMock(return_value=[_TMPL, _PRIVATE_TMPL]),
    ):
        resp = await admin_client.get("/v1/admin/cluster-templates")
    assert resp.status_code == 200
    assert len(resp.json()) == 2
