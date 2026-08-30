"""Regression coverage for the fail-closed live staging contract."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from tests.integration import conftest as staging

_REQUIRED_ENV = {
    "DROVER_INTEGRATION_CLOUD": "1",
    "OS_AUTH_URL": "https://keystone.example.test/v3",
    "OS_USERNAME": "drover-ci",
    "OS_PASSWORD": "secret",
    "OS_PROJECT_NAME": "drover-ci",
    "DROVER_INTEGRATION_NETWORK_ID": "network-id",
    "DROVER_INTEGRATION_SUBNET_ID": "subnet-id",
    "DROVER_INTEGRATION_IMAGE_ID": "image-id",
    "DROVER_INTEGRATION_FLAVOR_ID": "flavor-id",
    "DROVER_INTEGRATION_VOLUME_AZ": "nova",
    "DROVER_API_URL": "https://drover.example.test",
}


@pytest.fixture(autouse=True)
def _isolate_staging_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = {
        *_REQUIRED_ENV,
        "OS_PROJECT_ID",
        "DROVER_INTEGRATION_NETWORK_NAME",
        "DROVER_INTEGRATION_IMAGE_NAME",
        "DROVER_INTEGRATION_FLAVOR_NAME",
        "DROVER_INTEGRATION_EXTERNAL_NET_ID",
        "SERVICE_DROVER_INTERNAL_URL",
    }
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def _set_staging_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for key, value in (_REQUIRED_ENV | overrides).items():
        monkeypatch.setenv(key, value)


def test_enabled_staging_rejects_incomplete_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DROVER_INTEGRATION_CLOUD", "1")

    with pytest.raises(ValueError, match="DROVER_INTEGRATION_SUBNET_ID"):
        staging.get_staging_config()


def test_enabled_staging_accepts_complete_isolated_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_staging_env(monkeypatch)

    config = staging.get_staging_config()

    assert config["network_id"] == "network-id"
    assert config["subnet_id"] == "subnet-id"
    assert config["volume_availability_zone"] == "nova"
    assert config["drover_url"] == "https://drover.example.test"


class _DroverCleanupProxy:
    def __init__(
        self,
        *,
        active_resources: list[dict] | None = None,
        operation_id: str | None = "delete-op",
        deleted_status: str = "DELETED",
        operation_status: str = "SUCCEEDED",
    ) -> None:
        self.stream_consumed = False
        self.active_resources = active_resources or []
        self.operation_id = operation_id
        self.deleted_status = deleted_status
        self.operation_status = operation_status

    def get_cluster(self, _cluster_id: str) -> dict[str, str]:
        return {"status": "ACTIVE"}

    def delete_cluster_async(self, _cluster_id: str):
        self.stream_consumed = True
        payload = {"step": "completed"}
        if self.operation_id:
            payload["operation_id"] = self.operation_id
        yield "data: " + json.dumps(payload)

    def get_operation(self, operation_id: str) -> dict[str, str]:
        assert operation_id == self.operation_id
        return {"status": self.operation_status}

    def admin_cluster(self, _cluster_id: str) -> dict[str, str]:
        return {"status": self.deleted_status}

    def admin_managed_resources(self, **_query) -> list[dict]:
        return self.active_resources


def test_cleanup_consumes_delete_stream_and_proves_no_active_resources() -> None:
    proxy = _DroverCleanupProxy()
    connection = SimpleNamespace(drover=proxy)

    staging.cleanup_disposable_clusters(connection, ["cluster-id"], timeout=1)

    assert proxy.stream_consumed is True


def test_cleanup_accepts_completed_delete_stream_without_operation_id() -> None:
    proxy = _DroverCleanupProxy(operation_id=None)
    connection = SimpleNamespace(drover=proxy)

    staging.cleanup_disposable_clusters(connection, ["cluster-id"], timeout=1)

    assert proxy.stream_consumed is True


def test_cleanup_fails_when_managed_resources_remain() -> None:
    proxy = _DroverCleanupProxy(active_resources=[{"resource_id": "server-id"}])
    connection = SimpleNamespace(drover=proxy)

    with pytest.raises(pytest.fail.Exception, match="active managed resources remain"):
        staging.cleanup_disposable_clusters(connection, ["cluster-id"], timeout=1)


def test_cleanup_fails_when_delete_stream_leaves_cluster_active() -> None:
    proxy = _DroverCleanupProxy(operation_id=None, deleted_status="ERROR")
    connection = SimpleNamespace(drover=proxy)

    with pytest.raises(pytest.fail.Exception, match="cluster status ERROR"):
        staging.cleanup_disposable_clusters(connection, ["cluster-id"], timeout=1)


@pytest.mark.parametrize("operation_status", ["FAILED", "CANCELLED"])
def test_cleanup_fails_when_delete_operation_is_terminal_failure(operation_status: str) -> None:
    proxy = _DroverCleanupProxy(operation_status=operation_status)
    connection = SimpleNamespace(drover=proxy)

    with pytest.raises(pytest.fail.Exception, match=f"delete operation reached {operation_status}"):
        staging.cleanup_disposable_clusters(connection, ["cluster-id"], timeout=1)
