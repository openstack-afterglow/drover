"""K3s callback 엔드포인트 테스트 (/api/k3s/callback)."""

import shlex
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from drover.main import app


@pytest.mark.asyncio
async def test_callback_invalid_token_returns_403():
    """무효 토큰으로 콜백 요청 시 403을 반환해야 한다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with patch("drover.api.callback.k3s_cluster") as mock_db:
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(return_value=None)
            resp = await ac.post(
                "/v1/callback",
                json={
                    "token": "invalid-token",
                    "success": True,
                    "kubeconfig": "apiVersion: v1\n",
                    "node_token": "K10dummytoken123abc",
                    "server_ip": "10.0.0.1",
                },
            )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_callback_failure_updates_status():
    """success=false 콜백 시 클러스터 상태를 ERROR로 업데이트해야 한다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with patch("drover.api.callback.k3s_cluster") as mock_db:
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(return_value={"project_id": "proj-1", "cluster_id": "cluster-1"})
            mock_db.update_cluster_status = AsyncMock()
            resp = await ac.post(
                "/v1/callback",
                json={
                    "token": "valid-token",
                    "success": False,
                    "error": "k3s 설치 실패",
                },
            )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    mock_db.update_cluster_status.assert_called_once_with(
        "proj-1", "cluster-1", "ERROR", "서버 초기화 실패: k3s 설치 실패"
    )


@pytest.mark.asyncio
async def test_callback_missing_kubeconfig_updates_error():
    """success=true이지만 kubeconfig 누락 시 ERROR로 처리해야 한다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with patch("drover.api.callback.k3s_cluster") as mock_db:
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(return_value={"project_id": "proj-1", "cluster_id": "cluster-1"})
            mock_db.update_cluster_status = AsyncMock()
            resp = await ac.post(
                "/v1/callback",
                json={
                    "token": "valid-token",
                    "success": True,
                    # kubeconfig, node_token, server_ip 모두 누락
                },
            )
    assert resp.status_code == 200
    mock_db.update_cluster_status.assert_called_once()
    call_args = mock_db.update_cluster_status.call_args
    assert call_args.args[2] == "ERROR"


@pytest.mark.asyncio
async def test_callback_missing_node_token_updates_error():
    """node_token 누락 시 ERROR로 처리해야 한다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with patch("drover.api.callback.k3s_cluster") as mock_db:
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(return_value={"project_id": "proj-1", "cluster_id": "cluster-1"})
            mock_db.update_cluster_status = AsyncMock()
            resp = await ac.post(
                "/v1/callback",
                json={
                    "token": "valid-token",
                    "success": True,
                    "kubeconfig": "apiVersion: v1\n",
                    # node_token 누락
                    "server_ip": "10.0.0.1",
                },
            )
    assert resp.status_code == 200
    mock_db.update_cluster_status.assert_called_once()
    call_args = mock_db.update_cluster_status.call_args
    assert call_args.args[2] == "ERROR"


@pytest.mark.asyncio
async def test_callback_success_enqueues_agent_provisioning():
    """A successful callback persists state and durably enqueues agent provisioning."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with (
            patch("drover.api.callback.k3s_cluster") as mock_db,
            patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()) as enqueue,
        ):
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(
                return_value={"project_id": "proj-1", "cluster_id": "cluster-1"}
            )
            mock_db.update_cluster_status = AsyncMock()
            mock_db.store_kubeconfig = AsyncMock()
            mock_db.get_cluster = AsyncMock(return_value={"master_count": 1})
            resp = await ac.post(
                "/v1/callback",
                json={
                    "token": "valid-token",
                    "success": True,
                    "kubeconfig": "apiVersion: v1\nkind: Config\n",
                    "node_token": "K10secret::server:abc123",
                    "server_ip": "10.0.0.1",
                },
            )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    mock_db.store_kubeconfig.assert_awaited_once_with(
        "proj-1", "cluster-1", "apiVersion: v1\nkind: Config\n"
    )
    enqueue.assert_awaited_once_with(
        cluster_id="cluster-1",
        project_id="proj-1",
        kind="provision_agents",
        payload={"server_ip": "10.0.0.1", "node_token": "K10secret::server:abc123"},
    )


@pytest.mark.asyncio
async def test_callback_token_consumed_only_once():
    """일회성 토큰: 첫 번째 요청 후 같은 토큰으로 재시도 시 403이어야 한다."""
    call_count = 0

    async def side_effect(token):
        nonlocal call_count
        call_count += 1
        # 첫 호출에만 유효 데이터 반환, 이후는 None
        if call_count == 1:
            return {"project_id": "proj-1", "cluster_id": "cluster-1"}
        return None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with patch("drover.api.callback.k3s_cluster") as mock_db:
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(side_effect=side_effect)
            mock_db.update_cluster_status = AsyncMock()
            mock_db.store_kubeconfig = AsyncMock()
            mock_db.get_cluster = AsyncMock(return_value={"master_count": 1})
            with patch("drover.api.callback.asyncio"):
                # 첫 번째 성공 요청 (success=false라서 update만)
                resp1 = await ac.post(
                    "/v1/callback",
                    json={"token": "one-time-token", "success": False, "error": "fail"},
                )
                assert resp1.status_code == 200

            # 두 번째 동일 토큰 → 403
            resp2 = await ac.post(
                "/v1/callback",
                json={"token": "one-time-token", "success": False},
            )
    assert resp2.status_code == 403


@pytest.mark.asyncio
async def test_callback_stores_plugin_status_dict():
    """plugin_status 객체 형식이 update_cluster_status에 전달되어야 한다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with (
            patch("drover.api.callback.k3s_cluster") as mock_db,
            patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()),
        ):
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(
                return_value={"project_id": "proj-1", "cluster_id": "cluster-1"}
            )
            mock_db.update_cluster_status = AsyncMock()
            mock_db.store_kubeconfig = AsyncMock()
            mock_db.get_cluster = AsyncMock(return_value={"master_count": 1})
            await ac.post(
                "/v1/callback",
                json={
                    "token": "valid-token",
                    "success": True,
                    "kubeconfig": "apiVersion: v1\n",
                    "node_token": "K10secret::server:abc",
                    "server_ip": "10.0.0.1",
                    "plugin_status": {"occm": {"status": "deployed", "error": ""}},
                    "secret_cloud_config_status": "ok",
                },
            )
    call_kwargs = mock_db.update_cluster_status.call_args.kwargs
    assert call_kwargs.get("plugin_status") == {"occm": {"status": "deployed", "error": ""}}
    assert call_kwargs.get("secret_cloud_config_status") == "ok"


@pytest.mark.asyncio
async def test_callback_accepts_legacy_string_plugin_status():
    """기존 string 형식 plugin_status도 수용해야 한다 (backward compat)."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        with (
            patch("drover.api.callback.k3s_cluster") as mock_db,
            patch("drover.api.callback._jobs_svc.enqueue_job", new=AsyncMock()),
        ):
            mock_db.consume_ha_callback_token = AsyncMock(return_value=None)
            mock_db.consume_callback_token = AsyncMock(
                return_value={"project_id": "proj-1", "cluster_id": "cluster-1"}
            )
            mock_db.update_cluster_status = AsyncMock()
            mock_db.store_kubeconfig = AsyncMock()
            mock_db.get_cluster = AsyncMock(return_value={"master_count": 1})
            await ac.post(
                "/v1/callback",
                json={
                    "token": "valid-token",
                    "success": True,
                    "kubeconfig": "apiVersion: v1\n",
                    "node_token": "K10secret::server:abc",
                    "server_ip": "10.0.0.1",
                    "plugin_status": {"occm": "deployed"},
                },
            )
    call_kwargs = mock_db.update_cluster_status.call_args.kwargs
    assert call_kwargs.get("plugin_status") == {"occm": "deployed"}


@pytest.mark.asyncio
async def test_callback_requires_post():
    """GET /api/k3s/callback은 405를 반환해야 한다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/v1/callback")
    assert resp.status_code == 405


@pytest.mark.asyncio
async def test_callback_invalid_json_returns_422():
    """유효하지 않은 JSON body는 422를 반환해야 한다."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/v1/callback",
            content=b"not-json",
            headers={"Content-Type": "application/json"},
        )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# A6: k3s callback.sh 재시작 루프 감지 — 템플릿 생성 스크립트 검증
# ---------------------------------------------------------------------------


def _render_callback_script(k3s_version: str = "v1.31.0+k3s1") -> str:
    """k3s_server.yaml.j2를 렌더링하여 callback.sh 본문을 추출한다."""
    from pathlib import Path

    from jinja2 import Environment, FileSystemLoader

    tpl_dir = Path(__file__).parent.parent / "drover" / "templates"
    env = Environment(loader=FileSystemLoader(str(tpl_dir)))
    env.filters["shlex_quote"] = shlex.quote
    tpl = env.get_template("k3s_server.yaml.j2")

    rendered = tpl.render(
        callback_url="http://backend:8000",
        callback_token="tok-test",
        k3s_version=k3s_version,
        server_node_name="k3s-server",
        extra_tls_sans=[],
        extra_server_args=[],
        needs_external_cloud_provider=False,
        cloud_conf=None,
        plugins=[],
        pin_script="#!/bin/bash\nexit 1",
    )
    # callback.sh 본문만 추출 (path: /opt/k3s/callback.sh 이후 runcmd 이전)
    start = rendered.find("path: /opt/k3s/callback.sh")
    end = rendered.find("runcmd:")
    return rendered[start:end]


def test_callback_script_contains_restart_check():
    """callback.sh에 NRestarts 기반 재시작 루프 감지 로직이 포함돼야 한다."""
    script = _render_callback_script()
    assert "NRestarts" in script
    assert "RESTART_THRESHOLD" in script


def test_callback_script_sends_failure_on_restart_loop():
    """재시작 루프 감지 시 success=false 콜백을 전송하는 curl 명령이 있어야 한다."""
    script = _render_callback_script()
    assert "restart loop detected" in script
    # YAML 이스케이프: \"success\" → \\"success\\" 로 렌더링됨
    assert "success" in script and "false" in script


def test_callback_script_uses_systemctl_show():
    """systemctl show k3s.service -p NRestarts 명령으로 재시작 횟수를 조회해야 한다."""
    script = _render_callback_script()
    assert "systemctl show k3s.service" in script
    assert "-p NRestarts" in script


def test_callback_script_exits_on_restart_loop():
    """재시작 루프 감지 시 exit 1로 종료해야 한다."""
    script = _render_callback_script()
    # restart loop 감지 이후 exit 1 패턴
    loop_pos = script.find("restart loop detected")
    exit_pos = script.find("exit 1", loop_pos)
    assert exit_pos != -1, "restart loop 감지 후 exit 1이 있어야 한다"
