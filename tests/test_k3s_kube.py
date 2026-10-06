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
async def test_get_pod_resource_usage_uses_init_peak_not_sum(monkeypatch):
    monkeypatch.setattr("drover.services.kube._make_ssl_context", lambda *a, **k: None)
    resp = _make_response(200)
    resp.json.return_value = {
        "items": [
            {
                "metadata": {
                    "name": "gpu-job",
                    "namespace": "ml",
                    "ownerReferences": [{"kind": "Job", "controller": True}],
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

    assert len(result) == 1
    pod = result[0]
    assert pod["node"] == "node-a"
    assert pod["namespace"] == "ml"
    assert pod["name"] == "gpu-job"
    assert pod["resource_requests"] == {
        "cpu_m": 500,
        "memory_bytes": 1024**3,
        "gpu": 1,
        "pods": 1,
        "extended_resources": {},
    }
    for key, value in pod["resource_requests"].items():
        assert pod[key] == value
    assert pod["has_controller"] is True
    assert pod["safe_to_evict"] is True
    assert pod["is_daemonset"] is False
    assert pod["is_mirror"] is False


@pytest.mark.asyncio
async def test_list_service_annotations_keys_every_namespace_by_service(monkeypatch):
    """Cluster deletion reads keep-floatingip from here; a dropped annotation would delete a retained IP."""
    monkeypatch.setattr("drover.services.kube._make_ssl_context", lambda *a, **k: None)
    resp = _make_response(200)
    resp.json.return_value = {
        "items": [
            {"metadata": {"namespace": "default", "name": "web", "annotations": {"loadbalancer.openstack.org/keep-floatingip": "true"}}},
            {"metadata": {"namespace": "kube-system", "name": "traefik"}},
        ]
    }
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=resp)
    with patch("drover.services.kube.k3s_db") as mock_db:
        mock_db.get_kubeconfig_admin = AsyncMock(return_value=_FAKE_KUBECONFIG)
        with patch("drover.services.kube.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            from drover.services.kube import list_service_annotations

            result = await list_service_annotations("cluster-1")

    assert result == {
        "default/web": {"loadbalancer.openstack.org/keep-floatingip": "true"},
        "kube-system/traefik": {},
    }
    mock_client.get.assert_called_once_with(
        "https://10.0.0.1:6443/api/v1/services", headers={"Accept": "application/json"}
    )


@pytest.fixture
def stampede_client(monkeypatch):
    """Isolated admin API fixture: no configuration, certificates or external IO."""
    import contextlib

    client = AsyncMock()

    @contextlib.asynccontextmanager
    async def fake_client(cluster_id):
        yield client, "https://kube.invalid"

    monkeypatch.setattr("drover.services.kube._kube_client", fake_client)
    return client


def _list_response(items, *, token=""):
    response = _make_response(200)
    response.json.return_value = {"items": items, "metadata": {"continue": token}}
    return response


def _managed_pod(name="work", **overrides):
    pod = {
        "metadata": {
            "name": name,
            "namespace": "default",
            "uid": f"uid-{name}",
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "spec": {"nodeName": "node-a", "containers": [{"resources": {"requests": {"cpu": "1"}}}]},
        "status": {"phase": "Running"},
    }
    pod.update(overrides)
    return pod


@pytest.mark.parametrize(
    ("quantity", "expected"),
    [("0.0001", 1), ("100u", 1), ("1n", 1), ("1.1m", 2), ("1e-3", 1), ("0.123456", 124), ("2", 2000)],
)
def test_cpu_quantities_round_up_without_float_loss(quantity, expected):
    from drover.services.kube import _parse_cpu_millicores

    assert _parse_cpu_millicores(quantity) == expected


@pytest.mark.parametrize(
    ("quantity", "expected"),
    [("1.5Gi", 1610612736), ("1e6", 1000000), ("1G", 1000000000), ("1k", 1000),
     ("1m", 1), ("1.1", 2), ("1Pi", 1024**5), ("1Ei", 1024**6)],
)
def test_memory_quantities_support_binary_decimal_and_exponent(quantity, expected):
    from drover.services.kube import _parse_memory_bytes

    assert _parse_memory_bytes(quantity) == expected


@pytest.mark.parametrize("quantity", ["invalid", "-1", "NaN", "1GiB", ""])
def test_invalid_quantities_are_not_zero_capacity(quantity):
    from drover.services.kube import _parse_memory_bytes

    with pytest.raises(ValueError):
        _parse_memory_bytes(quantity)


@pytest.mark.asyncio
async def test_pending_and_assigned_requests_share_scheduler_accounting(stampede_client):
    from drover.services.kube import get_pod_resource_usage, list_unschedulable_pods

    pod = _managed_pod()
    pod["status"] = {"phase": "Pending", "conditions": [
        {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "Insufficient cpu"}
    ]}
    pod["spec"].update({
        "containers": [
            {"resources": {"requests": {"cpu": "100u"}, "limits": {"memory": "1Gi", "nvidia.com/gpu": "1"}}},
            {"resources": {"requests": {"cpu": "100u", "memory": "512Mi"}}},
        ],
        "initContainers": [
            {"restartPolicy": "Always", "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}},
            {"resources": {"requests": {"cpu": "2", "memory": "1Gi", "nvidia.com/gpu": "2"}}},
            {"restartPolicy": "Always", "resources": {"requests": {"cpu": "200m", "memory": "128Mi"}}},
            {"resources": {"requests": {"cpu": "1", "memory": "2Gi", "nvidia.com/mig-1g.5gb": "1"}}},
        ],
        "overhead": {"cpu": "100u", "memory": "64Mi"},
    })
    stampede_client.get.return_value = _list_response([pod])
    pending = await list_unschedulable_pods("cluster-1")
    assigned = await get_pod_resource_usage("cluster-1")
    expected = {
        "cpu_m": 2101,
        "memory_bytes": (2048 + 256 + 64) * 1024**2,
        "gpu": 2,
        "pods": 1,
        "extended_resources": {"nvidia.com/mig-1g.5gb": 1},
    }
    assert pending[0]["resource_requests"] == assigned[0]["resource_requests"] == expected
    assert pending[0]["message"] == "Insufficient cpu"


@pytest.mark.asyncio
async def test_small_container_cpu_is_rounded_after_aggregation(stampede_client):
    from drover.services.kube import get_pod_resource_usage

    pod = _managed_pod()
    pod["spec"]["containers"] = [{"resources": {"requests": {"cpu": "100u"}}}] * 2
    stampede_client.get.return_value = _list_response([pod])
    assert (await get_pod_resource_usage("cluster-1"))[0]["cpu_m"] == 1


@pytest.mark.asyncio
async def test_assigned_pending_and_terminating_pods_consume_capacity(stampede_client):
    from drover.services.kube import get_pod_resource_usage

    pending = _managed_pod("pending", status={"phase": "Pending"})
    terminating = _managed_pod("terminating")
    terminating["metadata"]["deletionTimestamp"] = "2026-10-06T00:00:00Z"
    terminal = [_managed_pod(phase, status={"phase": phase}) for phase in ["Succeeded", "Failed"]]
    unassigned = _managed_pod("unassigned", spec={"containers": []})
    stampede_client.get.return_value = _list_response([pending, terminating, *terminal, unassigned])
    pods = await get_pod_resource_usage("cluster-1")
    assert [pod["name"] for pod in pods] == ["pending", "terminating"]
    assert sum(pod["cpu_m"] for pod in pods) == 2000
    assert pods[1]["deleting"] is True
    assert "fieldSelector" not in stampede_client.get.call_args.kwargs.get("params", {})


@pytest.mark.asyncio
async def test_scheduling_and_eviction_metadata_survives_observation(stampede_client):
    from drover.services.kube import get_pod_resource_usage

    pod = _managed_pod()
    pod["metadata"]["annotations"] = {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}
    pod["spec"].update({
        "nodeSelector": {"accelerator": "gpu"},
        "tolerations": [{"key": "gpu", "operator": "Exists"}],
        "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": []}}},
        "topologySpreadConstraints": [{"topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule"}],
        "schedulerName": "custom-scheduler",
        "volumes": [{"name": "scratch", "emptyDir": {}}, {"name": "data", "persistentVolumeClaim": {"claimName": "data"}}],
    })
    pod["spec"]["containers"][0]["ports"] = [{"hostPort": 8080, "hostIP": "127.0.0.1"}]
    stampede_client.get.return_value = _list_response([pod])
    observed = (await get_pod_resource_usage("cluster-1"))[0]
    assert observed["node_name"] == "node-a"
    assert observed["node_selector"] == pod["spec"]["nodeSelector"]
    assert observed["tolerations"] == pod["spec"]["tolerations"]
    assert observed["affinity"] == pod["spec"]["affinity"]
    assert observed["topology_spread_constraints"] == pod["spec"]["topologySpreadConstraints"]
    assert observed["scheduler_name"] == "custom-scheduler"
    assert observed["host_ports"] == [{"host_ip": "127.0.0.1", "port": 8080, "protocol": "TCP"}]
    assert observed["has_local_storage"] is True
    assert observed["has_pvc"] is True
    assert observed["safe_to_evict"] is False


@pytest.mark.asyncio
async def test_node_capacity_has_cordon_slots_and_nonstandard_resources(stampede_client):
    from drover.services.kube import get_node_capacity

    stampede_client.get.return_value = _list_response([{
        "metadata": {"name": "node-a", "labels": {"gpu": "true"}, "annotations": {"drover.io/removing-vm-id": "vm-a"}},
        "spec": {"unschedulable": True, "taints": [{"key": "gpu", "effect": "NoSchedule"}]},
        "status": {"allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110", "nvidia.com/gpu": "1", "nvidia.com/mig-1g.5gb": "2"},
                   "conditions": [{"type": "Ready", "status": "True"}]},
    }])
    assert await get_node_capacity("cluster-1") == [{
        "name": "node-a",
        "allocatable": {"cpu_m": 4000, "memory_bytes": 8 * 1024**3, "gpu": 1, "pods": 110,
                        "extended_resources": {"nvidia.com/mig-1g.5gb": 2}},
        "labels": {"gpu": "true"}, "taints": [{"key": "gpu", "effect": "NoSchedule"}],
        "ready": True, "unschedulable": True, "removal_vm_id": "vm-a",
    }]


@pytest.mark.asyncio
@pytest.mark.parametrize("helper", ["list_unschedulable_pods", "get_node_capacity", "get_pod_resource_usage"])
@pytest.mark.parametrize("failure", ["http", "transport", "malformed"])
async def test_observation_errors_never_return_empty_healthy_state(stampede_client, helper, failure):
    from drover.services import kube
    from drover.services.errors import K3sApiError

    if failure == "http":
        response = _make_response(503)
        response.json.return_value = {"message": "unavailable"}
        stampede_client.get.return_value = response
        expected = K3sApiError
    elif failure == "transport":
        stampede_client.get.side_effect = RuntimeError("network unavailable")
        expected = RuntimeError
    else:
        response = _make_response(200)
        response.json.return_value = {"kind": "Status"}
        stampede_client.get.return_value = response
        expected = KeyError
    with pytest.raises(expected):
        await getattr(kube, helper)("cluster-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("helper", ["list_unschedulable_pods", "get_node_capacity", "get_pod_resource_usage"])
async def test_observations_require_admin_kubeconfig(monkeypatch, helper):
    from drover.services import kube
    from drover.services.errors import K3sApiError

    monkeypatch.setattr(kube.k3s_db, "get_kubeconfig_admin", AsyncMock(return_value=None))
    with pytest.raises(K3sApiError):
        await getattr(kube, helper)("cluster-1")


@pytest.mark.asyncio
async def test_paginated_usage_includes_all_nodes_and_page_errors_propagate(stampede_client):
    from drover.services.errors import K3sApiError
    from drover.services.kube import get_pod_resource_usage

    stampede_client.get.side_effect = [
        _list_response([_managed_pod("first")], token="next-page"),
        _list_response([_managed_pod("second")]),
    ]
    assert [pod["name"] for pod in await get_pod_resource_usage("cluster-1")] == ["first", "second"]
    assert stampede_client.get.call_args.kwargs["params"] == {"continue": "next-page"}
    failed = _make_response(503)
    failed.json.return_value = {"message": "unavailable"}
    stampede_client.get.side_effect = [_list_response([_managed_pod()], token="next"), failed]
    with pytest.raises(K3sApiError):
        await get_pod_resource_usage("cluster-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 409, 500])
async def test_drain_eviction_error_blocks_removal(stampede_client, status):
    from drover.services.kube import drain_node

    stampede_client.get.return_value = _list_response([_managed_pod()])
    stampede_client.post.return_value = _make_response(status)
    assert await drain_node("cluster-1", "node-a") is False


@pytest.mark.asyncio
async def test_drain_waits_for_actual_absence_and_uses_uid(stampede_client, monkeypatch):
    from drover.services.kube import drain_node

    pod = _managed_pod()
    terminating = _managed_pod()
    terminating["metadata"]["deletionTimestamp"] = "2026-10-06T00:00:00Z"
    stampede_client.get.side_effect = [_list_response([pod]), _list_response([terminating]), _list_response([])]
    stampede_client.post.return_value = _make_response(201)
    sleep = AsyncMock()
    monkeypatch.setattr("asyncio.sleep", sleep)
    assert await drain_node("cluster-1", "node-a") is True
    assert stampede_client.get.await_count == 3
    assert sleep.await_count == 2
    stampede_client.post.assert_awaited_once()
    body = stampede_client.post.call_args.kwargs["json"]
    assert body["deleteOptions"]["preconditions"] == {"uid": "uid-work"}


@pytest.mark.asyncio
async def test_drain_pdb_retries_are_bounded_and_never_success(stampede_client, monkeypatch):
    from drover.services.kube import drain_node

    stampede_client.get.return_value = _list_response([_managed_pod()])
    stampede_client.post.return_value = _make_response(429)
    monkeypatch.setattr("asyncio.sleep", AsyncMock())
    assert await drain_node("cluster-1", "node-a", timeout=1000) is False
    assert stampede_client.post.await_count == 5


@pytest.mark.asyncio
async def test_drain_terminating_pod_timeout_is_failure(stampede_client, monkeypatch):
    import time

    from drover.services.kube import drain_node

    now = [time.monotonic()]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    async def fake_sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    pod = _managed_pod()
    pod["metadata"]["deletionTimestamp"] = "2026-10-06T00:00:00Z"
    stampede_client.get.return_value = _list_response([pod])
    assert await drain_node("cluster-1", "node-a", timeout=3) is False
    stampede_client.post.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("protection", ["bare", "annotation", "emptydir", "hostpath", "critical", "system", "noncontroller"])
async def test_drain_preflights_every_pod_before_any_eviction(stampede_client, protection):
    from drover.services.kube import drain_node

    blocked = _managed_pod("blocked")
    if protection == "bare":
        blocked["metadata"]["ownerReferences"] = []
    elif protection == "noncontroller":
        blocked["metadata"]["ownerReferences"] = [{"kind": "Job", "controller": False}]
    elif protection == "annotation":
        blocked["metadata"]["annotations"] = {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}
    elif protection in {"emptydir", "hostpath"}:
        blocked["spec"]["volumes"] = [{"name": "local", "emptyDir" if protection == "emptydir" else "hostPath": {}}]
    elif protection == "critical":
        blocked["spec"]["priorityClassName"] = "system-node-critical"
    else:
        blocked["metadata"]["namespace"] = "kube-system"
    stampede_client.get.return_value = _list_response([_managed_pod("safe"), blocked])
    assert await drain_node("cluster-1", "node-a") is False
    stampede_client.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_drain_ignores_only_daemon_mirror_and_terminal(stampede_client):
    from drover.services.kube import drain_node

    daemon = _managed_pod("daemon")
    daemon["metadata"]["ownerReferences"] = [{"kind": "DaemonSet", "controller": True}]
    mirror = _managed_pod("mirror")
    mirror["metadata"]["annotations"] = {"kubernetes.io/config.mirror": "mirror-hash"}
    terminal = _managed_pod("completed", status={"phase": "Succeeded"})
    stampede_client.get.return_value = _list_response([daemon, mirror, terminal])
    assert await drain_node("cluster-1", "node-a") is True
    stampede_client.post.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["list", "transport"])
async def test_drain_read_failure_is_failure(stampede_client, failure):
    from drover.services.kube import drain_node

    if failure == "list":
        response = _make_response(500)
        response.json.return_value = {"message": "unavailable"}
        stampede_client.get.return_value = response
    else:
        stampede_client.get.side_effect = RuntimeError("network unavailable")
    assert await drain_node("cluster-1", "node-a") is False
    stampede_client.post.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 404, 500])
async def test_uncordon_restores_scheduling_and_reports_errors(stampede_client, status):
    from drover.services.kube import uncordon_node

    stampede_client.patch.return_value = _make_response(status)
    assert await uncordon_node("cluster-1", "node-a") is (status == 200)
    # Merge-patch null also clears the removal marker that let a retry resume this cordon.
    assert stampede_client.patch.call_args.kwargs["json"] == {
        "metadata": {"annotations": {"drover.io/removing-vm-id": None}}, "spec": {"unschedulable": False},
    }


@pytest.mark.asyncio
async def test_invalid_requests_fail_the_consumer_observation(stampede_client):
    from drover.services.kube import get_pod_resource_usage

    pod = _managed_pod()
    pod["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "broken"
    stampede_client.get.return_value = _list_response([pod])
    with pytest.raises(ValueError):
        await get_pod_resource_usage("cluster-1")


@pytest.mark.asyncio
async def test_successful_eviction_does_not_mask_followup_read_failure(stampede_client, monkeypatch):
    from drover.services.kube import drain_node

    stampede_client.get.side_effect = [_list_response([_managed_pod()]), RuntimeError("read failure")]
    stampede_client.post.return_value = _make_response(201)
    monkeypatch.setattr("asyncio.sleep", AsyncMock())
    assert await drain_node("cluster-1", "node-a") is False
    stampede_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_drain_name_reuse_evicts_new_uid_separately(stampede_client, monkeypatch):
    from drover.services.kube import drain_node

    original = _managed_pod()
    replacement = _managed_pod()
    replacement["metadata"]["uid"] = "replacement-uid"
    stampede_client.get.side_effect = [
        _list_response([original]), _list_response([replacement]), _list_response([]),
    ]
    stampede_client.post.return_value = _make_response(201)
    monkeypatch.setattr("asyncio.sleep", AsyncMock())
    assert await drain_node("cluster-1", "node-a") is True
    assert [call.kwargs["json"]["deleteOptions"]["preconditions"]["uid"]
            for call in stampede_client.post.call_args_list] == ["uid-work", "replacement-uid"]
