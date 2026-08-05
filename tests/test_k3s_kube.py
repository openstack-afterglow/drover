"""k3s_kube.py 유닛 테스트 — K8s API 노드 삭제."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 픽스처 / 헬퍼
# ---------------------------------------------------------------------------

_FAKE_KUBECONFIG = """
apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: dGVzdA==
    server: https://10.0.0.1:6443
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: default
  name: default
current-context: default
kind: Config
users:
- name: default
  user:
    client-certificate-data: dGVzdA==
    client-key-data: dGVzdA==
"""


def _make_response(status_code: int):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = ""
    return resp


# ---------------------------------------------------------------------------
# _parse_kubeconfig 테스트
# ---------------------------------------------------------------------------


def test_parse_kubeconfig_returns_server_url():
    from drover.services.kube import _parse_kubeconfig

    cert, key, url = _parse_kubeconfig(_FAKE_KUBECONFIG)
    assert url == "https://10.0.0.1:6443"
    assert isinstance(cert, bytes)
    assert isinstance(key, bytes)


# ---------------------------------------------------------------------------
# delete_k8s_node 테스트
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_k8s_node_no_kubeconfig():
    """kubeconfig 없을 때 False 반환."""
    with patch("drover.services.kube.k3s_db") as mock_db:
        mock_db.get_kubeconfig_admin = AsyncMock(return_value=None)
        from drover.services.kube import delete_k8s_node

        result = await delete_k8s_node("cluster-1", "test-node")
    assert result is False


def _make_mock_http_client(status_code: int):
    mock_client = AsyncMock()
    mock_client.delete = AsyncMock(return_value=_make_response(status_code))
    return mock_client


@pytest.mark.asyncio
async def test_delete_k8s_node_success(monkeypatch):
    """K8s API 200 응답 시 True 반환 — 실제 DELETE URL/headers를 검증한다."""
    monkeypatch.setattr("drover.services.kube._make_ssl_context", lambda *a, **k: None)
    mock_client = _make_mock_http_client(200)
    with patch("drover.services.kube.k3s_db") as mock_db:
        mock_db.get_kubeconfig_admin = AsyncMock(return_value=_FAKE_KUBECONFIG)
        with patch("drover.services.kube.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            from drover.services.kube import delete_k8s_node

            result = await delete_k8s_node("cluster-1", "test-node")
    assert result is True
    mock_client.delete.assert_called_once_with(
        "https://10.0.0.1:6443/api/v1/nodes/test-node",
        headers={"Accept": "application/json"},
    )


@pytest.mark.asyncio
async def test_delete_k8s_node_already_gone(monkeypatch):
    """K8s API 404 응답 시에도 True 반환 (이미 삭제된 노드)."""
    monkeypatch.setattr("drover.services.kube._make_ssl_context", lambda *a, **k: None)
    mock_client = _make_mock_http_client(404)
    with patch("drover.services.kube.k3s_db") as mock_db:
        mock_db.get_kubeconfig_admin = AsyncMock(return_value=_FAKE_KUBECONFIG)
        with patch("drover.services.kube.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            from drover.services.kube import delete_k8s_node

            result = await delete_k8s_node("cluster-1", "test-node")
    assert result is True
    mock_client.delete.assert_called_once_with(
        "https://10.0.0.1:6443/api/v1/nodes/test-node",
        headers={"Accept": "application/json"},
    )


@pytest.mark.asyncio
async def test_delete_k8s_node_api_error(monkeypatch):
    """K8s API 500 응답 시 False 반환."""
    monkeypatch.setattr("drover.services.kube._make_ssl_context", lambda *a, **k: None)
    mock_client = _make_mock_http_client(500)
    with patch("drover.services.kube.k3s_db") as mock_db:
        mock_db.get_kubeconfig_admin = AsyncMock(return_value=_FAKE_KUBECONFIG)
        with patch("drover.services.kube.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            from drover.services.kube import delete_k8s_node

            result = await delete_k8s_node("cluster-1", "test-node")
    assert result is False


@pytest.mark.asyncio
async def test_delete_k8s_node_connection_error(monkeypatch):
    """연결 오류 시 False 반환 (예외 전파 안 됨)."""
    monkeypatch.setattr("drover.services.kube._make_ssl_context", lambda *a, **k: None)
    mock_client = AsyncMock()
    mock_client.delete = AsyncMock(side_effect=Exception("Connection refused"))
    with patch("drover.services.kube.k3s_db") as mock_db:
        mock_db.get_kubeconfig_admin = AsyncMock(return_value=_FAKE_KUBECONFIG)
        with patch("drover.services.kube.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            from drover.services.kube import delete_k8s_node

            result = await delete_k8s_node("cluster-1", "test-node")
    assert result is False


@pytest.mark.asyncio
async def test_delete_k8s_nodes_calls_each():
    """delete_k8s_nodes는 각 노드에 delete_k8s_node를 호출한다."""
    with patch("drover.services.kube.delete_k8s_node", new_callable=AsyncMock) as mock_del:
        mock_del.return_value = True
        from drover.services.kube import delete_k8s_nodes

        await delete_k8s_nodes("cluster-1", ["node-a", "node-b", "node-c"])

    assert mock_del.call_count == 3
    called_names = [call.args[1] for call in mock_del.call_args_list]
    assert called_names == ["node-a", "node-b", "node-c"]


@pytest.mark.asyncio
async def test_delete_k8s_nodes_continues_on_failure():
    """일부 노드 삭제 실패해도 나머지 계속 진행한다."""
    results = [False, True, False]
    call_count = 0

    async def mock_delete(cluster_id, node_name):
        nonlocal call_count
        result = results[call_count]
        call_count += 1
        return result

    with patch("drover.services.kube.delete_k8s_node", side_effect=mock_delete):
        from drover.services.kube import delete_k8s_nodes

        await delete_k8s_nodes("cluster-1", ["node-a", "node-b", "node-c"])

    assert call_count == 3


@pytest.mark.asyncio
async def test_get_pod_resource_usage_sums_gpu_and_init_containers(monkeypatch):
    monkeypatch.setattr("drover.services.kube._make_ssl_context", lambda *a, **k: None)
    resp = _make_response(200)
    resp.json.return_value = {
        "items": [
            {
                "metadata": {
                    "name": "gpu-job",
                    "namespace": "ml",
                    "ownerReferences": [{"kind": "Job"}],
                    "annotations": {},
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [
                        {"resources": {"requests": {"cpu": "500m", "memory": "1Gi", "nvidia.com/gpu": "1"}}}
                    ],
                    "initContainers": [
                        {"resources": {"requests": {"cpu": "250m", "memory": "512Mi", "nvidia.com/gpu": "1"}}}
                    ],
                },
            }
        ]
    }
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=resp)
    with patch("drover.services.kube.k3s_db") as mock_db:
        mock_db.get_kubeconfig_admin = AsyncMock(return_value=_FAKE_KUBECONFIG)
        with patch("drover.services.kube.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            from drover.services.kube import get_pod_resource_usage

            result = await get_pod_resource_usage("cluster-1")

    assert result == [
        {
            "node": "node-a",
            "namespace": "ml",
            "name": "gpu-job",
            "cpu_m": 750,
            "memory_bytes": 1536 * 1024**2,
            "gpu": 2,
            "is_daemonset": False,
            "is_mirror": False,
        }
    ]
