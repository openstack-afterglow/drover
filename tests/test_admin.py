"""Drover system-admin cluster control contracts."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from drover.services.cert_rotation import _lock_key

pytestmark = pytest.mark.asyncio

_ROTATION_LOCK_KEY = _lock_key("cluster-1")


def _cluster(cluster_id: str, status: str = "ACTIVE") -> dict:
    return {
        "id": cluster_id,
        "project_id": "project-1",
        "name": cluster_id,
        "status": status,
        "agent_vm_ids": [],
        "agent_count": 0,
    }


async def test_admin_clusters_require_system_admin(non_admin_client):
    response = await non_admin_client.get("/v1/admin/clusters")
    assert response.status_code == 403


async def test_admin_cluster_status_filter_accepts_dashboard_status_set(admin_client):
    clusters = [
        _cluster("creating", "CREATE_IN_PROGRESS"),
        _cluster("error", "ERROR"),
        _cluster("active", "ACTIVE"),
    ]
    with patch(
        "drover.api.admin.k3s_cluster.list_all_clusters",
        new=AsyncMock(return_value=clusters),
    ):
        response = await admin_client.get(
            "/v1/admin/clusters",
            params={"status": "CREATE_IN_PROGRESS,UPDATE_IN_PROGRESS,ERROR"},
        )

    assert response.status_code == 200
    assert [item["id"] for item in response.json()] == ["creating", "error"]
    assert {item["project_id"] for item in response.json()} == {"project-1"}


async def test_admin_scale_persists_durable_job_before_success(admin_client):
    enqueue = AsyncMock(return_value="job-1")
    update = AsyncMock()
    with (
        patch(
            "drover.api.admin.k3s_cluster.get_cluster_admin",
            new=AsyncMock(return_value=_cluster("cluster-1")),
        ),
        patch("drover.api.admin.k3s_cluster.update_cluster_status", new=update),
        patch("drover.api.admin._jobs_svc.enqueue_job", new=enqueue),
    ):
        response = await admin_client.patch(
            "/v1/admin/clusters/cluster-1/scale",
            json={"agent_count": 3},
        )

    assert response.status_code == 200
    update.assert_awaited_once_with(
        "project-1",
        "cluster-1",
        "SCALING",
        "에이전트 노드 스케일 변경 중",
    )
    enqueue.assert_awaited_once_with(
        cluster_id="cluster-1",
        project_id="project-1",
        kind="scale",
        payload={"desired_count": 3},
        user_id="admin",
        username="system-admin",
    )


async def test_admin_delete_persists_durable_job(admin_client):
    enqueue = AsyncMock(return_value="job-1")
    update = AsyncMock()
    with (
        patch(
            "drover.api.admin.k3s_cluster.get_cluster_admin",
            new=AsyncMock(return_value=_cluster("cluster-1")),
        ),
        patch("drover.api.admin.k3s_cluster.update_cluster_status", new=update),
        patch("drover.api.admin._jobs_svc.enqueue_job", new=enqueue),
    ):
        response = await admin_client.delete("/v1/admin/clusters/cluster-1")

    assert response.status_code == 204
    enqueue.assert_awaited_once_with(
        cluster_id="cluster-1",
        project_id="project-1",
        kind="delete",
        payload={"user_id": "admin", "username": "system-admin"},
        user_id="admin",
        username="system-admin",
    )
    update.assert_awaited_once_with(
        "project-1",
        "cluster-1",
        "DELETING",
        "관리자 삭제 요청",
    )


def _cert_info(subject: str) -> dict:
    return {
        "not_after": "2027-09-24T00:00:00+00:00",
        "not_before": "2026-09-24T00:00:00+00:00",
        "subject": subject,
        "issuer": "CN=k3s-server-ca",
        "days_remaining": 365,
    }


@pytest.mark.parametrize(
    ("api_address", "server_ip", "expected_target"),
    [
        ("https://10.0.0.5:6443", "10.0.0.5", ("10.0.0.5", 6443)),
        ("https://k3s-api.example:7443", "10.0.0.5", ("k3s-api.example", 7443)),
        ("10.0.0.5:7443", "10.0.0.9", ("10.0.0.5", 7443)),
        ("https://k3s-api.example:abc", "10.0.0.9", ("10.0.0.9", 6443)),
        ("https://[fd00::1:6443", "10.0.0.9", ("10.0.0.9", 6443)),
        ("", "10.0.0.7", ("10.0.0.7", 6443)),
    ],
)
async def test_admin_certificate_expiry_awaits_tls_probe(admin_client, api_address, server_ip, expected_target):
    cluster = {**_cluster("cluster-1"), "api_address": api_address, "server_ip": server_ip}
    server_certs = [_cert_info("CN=k3s")]
    probe = AsyncMock(return_value=server_certs)
    with (
        patch("drover.api.admin.k3s_cluster.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.api.admin.k3s_cluster.get_kubeconfig_admin", new=AsyncMock(return_value="kubeconfig")),
        patch(
            "drover.api.admin.certs.parse_kubeconfig_certs",
            return_value={"ca": _cert_info("CN=k3s-server-ca"), "client": _cert_info("CN=system:admin")},
        ),
        patch("drover.api.admin.certs.probe_tls_server_cert", new=probe),
    ):
        response = await admin_client.get("/v1/admin/clusters/cluster-1/certificate-expiry")

    assert response.status_code == 200
    body = response.json()
    assert body["server_via_tls"] == server_certs
    assert body["ca"]["subject"] == "CN=k3s-server-ca"
    probe.assert_awaited_once_with(*expected_target)


async def test_admin_certificate_expiry_without_api_endpoint_skips_probe(admin_client):
    cluster = {**_cluster("cluster-1"), "api_address": "", "server_ip": ""}
    probe = AsyncMock(return_value=[_cert_info("CN=k3s")])
    with (
        patch("drover.api.admin.k3s_cluster.get_cluster_admin", new=AsyncMock(return_value=cluster)),
        patch("drover.api.admin.k3s_cluster.get_kubeconfig_admin", new=AsyncMock(return_value="kubeconfig")),
        patch("drover.api.admin.certs.parse_kubeconfig_certs", return_value={}),
        patch("drover.api.admin.certs.probe_tls_server_cert", new=probe),
    ):
        response = await admin_client.get("/v1/admin/clusters/cluster-1/certificate-expiry")

    assert response.status_code == 200
    assert response.json()["server_via_tls"] == []
    probe.assert_not_awaited()


def _sse_events(body: str) -> list[dict]:
    return [json.loads(line.removeprefix("data: ")) for line in body.splitlines() if line.startswith("data: ")]


@pytest.fixture
def rotation_settings(monkeypatch):
    monkeypatch.setenv("DROVER_CERT_ROTATION_NODE_TIMEOUT_SEC", "42")
    monkeypatch.setenv("DROVER_CERT_ROTATION_JOB_IMAGE", "registry.example/k3s:test")


async def test_admin_rotate_certs_streams_service_messages(admin_client, _fake_redis, rotation_settings):
    from drover.models.schemas import K3sProgressMessage, K3sProgressStep

    calls: list[tuple[tuple, dict]] = []

    async def fake_rotate(cluster_id, project_id, initiated_by, *, node_timeout=300.0, job_image="default"):
        calls.append(((cluster_id, project_id, initiated_by), {"node_timeout": node_timeout, "job_image": job_image}))
        assert await _fake_redis.get(_ROTATION_LOCK_KEY) is not None
        yield K3sProgressMessage(
            step=K3sProgressStep.ROTATE_DISCOVER, progress=5, message="검색 중", cluster_id=cluster_id
        )
        yield K3sProgressMessage(step=K3sProgressStep.COMPLETED, progress=100, message="완료", cluster_id=cluster_id)

    with (
        patch("drover.api.admin.k3s_cluster.get_cluster_admin", new=AsyncMock(return_value=_cluster("cluster-1"))),
        patch("drover.api.admin.cert_rotation.rotate_certificates", new=fake_rotate),
    ):
        response = await admin_client.post("/v1/admin/clusters/cluster-1/rotate-certs")

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert calls == [
        (("cluster-1", "project-1", "system-admin"), {"node_timeout": 42.0, "job_image": "registry.example/k3s:test"})
    ]
    events = _sse_events(response.text)
    assert [event["step"] for event in events] == ["rotate_discover", "completed"]
    assert {event["cluster_id"] for event in events} == {"cluster-1"}
    assert await _fake_redis.get(_ROTATION_LOCK_KEY) is None


async def test_admin_rotate_certs_service_error_emits_failed_and_releases_lock(
    admin_client, _fake_redis, rotation_settings
):
    async def failing_rotate(cluster_id, project_id, initiated_by, *, node_timeout=300.0, job_image="default"):
        assert await _fake_redis.get(_ROTATION_LOCK_KEY) is not None
        raise RuntimeError("kube api unavailable")
        yield  # pragma: no cover - makes this an async generator

    with (
        patch("drover.api.admin.k3s_cluster.get_cluster_admin", new=AsyncMock(return_value=_cluster("cluster-1"))),
        patch("drover.api.admin.cert_rotation.rotate_certificates", new=failing_rotate),
    ):
        response = await admin_client.post("/v1/admin/clusters/cluster-1/rotate-certs")

    assert response.status_code == 200
    events = _sse_events(response.text)
    assert events[-1]["step"] == "failed"
    assert events[-1]["error"] == "kube api unavailable"
    assert await _fake_redis.get(_ROTATION_LOCK_KEY) is None


async def test_admin_rotate_certs_rejects_concurrent_rotation(admin_client, _fake_redis, rotation_settings):
    await _fake_redis.set(_ROTATION_LOCK_KEY, "1")
    rotate = AsyncMock()
    with (
        patch("drover.api.admin.k3s_cluster.get_cluster_admin", new=AsyncMock(return_value=_cluster("cluster-1"))),
        patch("drover.api.admin.cert_rotation.rotate_certificates", new=rotate),
    ):
        response = await admin_client.post("/v1/admin/clusters/cluster-1/rotate-certs")

    assert response.status_code == 409
    rotate.assert_not_called()
    assert await _fake_redis.get(_ROTATION_LOCK_KEY) == "1"


async def test_admin_managed_resources_requires_admin(non_admin_client):
    response = await non_admin_client.get("/v1/admin/managed-resources")
    assert response.status_code == 403


async def test_admin_managed_resources_filtering_and_sanitization(admin_client):
    from datetime import UTC, datetime

    from drover.models.orm import ManagedOpenStackResource

    now = datetime.now(UTC)

    r1 = ManagedOpenStackResource(
        id="res-1",
        cluster_id="cluster-aaa",
        operation_id="op-111",
        service="nova",
        resource_type="server",
        resource_id="srv-001",
        name="k3s-master-1",
        state="ACTIVE",
        metadata_json={
            "drover.cluster_id": "cluster-aaa",
            "drover.operation_id": "op-111",
            "drover.managed": "true",
            "encrypted_kubeconfig": "SECRET_KUBECONFIG_DATA",
            "callback_token": "SENSITIVE_CALLBACK_TOKEN",
            "os_password": "SECRET_PASSWORD",
            "job_payload": {"secret_key": "val"},
            "custom_tag": "safe_value",
        },
        created_at=now,
        last_seen_at=now,
        deleted_at=None,
    )

    r2 = ManagedOpenStackResource(
        id="res-2",
        cluster_id="cluster-bbb",
        operation_id="op-222",
        service="neutron",
        resource_type="security_group",
        resource_id="sg-002",
        name="k3s-sg",
        state="ACTIVE",
        metadata_json=[
            "drover.cluster_id=cluster-bbb",
            "drover.managed=true",
            "callback_token=SENSITIVE_TOKEN_IN_TAG",
            "encrypted_kubeconfig=SECRET_KUBECONFIG",
        ],
        created_at=now,
        last_seen_at=now,
        deleted_at=None,
    )

    r3_deleted = ManagedOpenStackResource(
        id="res-3",
        cluster_id="cluster-aaa",
        operation_id="op-111",
        service="cinder",
        resource_type="volume",
        resource_id="vol-003",
        name="k3s-vol",
        state="DELETED",
        metadata_json=None,
        created_at=now,
        last_seen_at=now,
        deleted_at=now,
    )

    all_resources = [r1, r2, r3_deleted]

    async def fake_list_managed_resources(session_or_factory=None, cluster_id="", operation_id="", active_only=True):
        res = all_resources
        if cluster_id:
            res = [r for r in res if r.cluster_id == cluster_id]
        if operation_id:
            res = [r for r in res if r.operation_id == operation_id]
        if active_only:
            res = [r for r in res if r.deleted_at is None]
        return res

    with patch("drover.api.admin._inventory_svc.list_managed_resources", side_effect=fake_list_managed_resources):
        # 1. Default call (active only, no filters)
        resp = await admin_client.get("/v1/admin/managed-resources")
        assert resp.status_code == 200
        items = resp.json()
        assert len(items) == 2

        # Verify sanitization on r1 item:
        r1_item = next(i for i in items if i["id"] == "res-1")
        assert r1_item["cluster_id"] == "cluster-aaa"
        assert r1_item["operation_id"] == "op-111"
        assert r1_item["resource_id"] == "srv-001"
        assert r1_item["state"] == "ACTIVE"
        assert "last_seen_at" in r1_item

        # Strictly check no secrets leaked in metadata or json response string
        raw_json_str = resp.text
        assert "SECRET_KUBECONFIG_DATA" not in raw_json_str
        assert "SENSITIVE_CALLBACK_TOKEN" not in raw_json_str
        assert "SECRET_PASSWORD" not in raw_json_str
        assert "SENSITIVE_TOKEN_IN_TAG" not in raw_json_str
        assert "SECRET_KUBECONFIG" not in raw_json_str

        # 2. Filter by cluster_id
        resp_cluster = await admin_client.get("/v1/admin/managed-resources", params={"cluster_id": "cluster-aaa"})
        assert resp_cluster.status_code == 200
        cluster_items = resp_cluster.json()
        assert len(cluster_items) == 1
        assert cluster_items[0]["id"] == "res-1"

        # 3. Filter by operation_id
        resp_op = await admin_client.get("/v1/admin/managed-resources", params={"operation_id": "op-222"})
        assert resp_op.status_code == 200
        op_items = resp_op.json()
        assert len(op_items) == 1
        assert op_items[0]["id"] == "res-2"

        # 4. Include deleted
        resp_del = await admin_client.get(
            "/v1/admin/managed-resources", params={"cluster_id": "cluster-aaa", "include_deleted": "true"}
        )
        assert resp_del.status_code == 200
        del_items = resp_del.json()
        assert len(del_items) == 2
        assert {i["id"] for i in del_items} == {"res-1", "res-3"}
