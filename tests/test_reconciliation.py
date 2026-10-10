"""Focused unit tests for Drover cluster reconciliation service."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

pytest_plugins = ("pytest_asyncio",)

import pytest
from sqlalchemy.dialects import mysql

from drover.models.orm import DroverOperation, DroverOperationEvent, K3sCluster, ManagedOpenStackResource
from drover.services import cluster_authority, execution, inventory, jobs, operations, reconciliation, store

_persist_cluster_reconciliation = store.update_cluster_reconciliation


class DummyServer:
    def __init__(self, server_id: str, name: str, project_id: str, cluster_id: str, status: str = "ACTIVE"):
        self.id = server_id
        self.name = name
        self.project_id = project_id
        self.status = status
        self.metadata = {"drover.cluster_id": cluster_id, "drover.managed": "true"}
        self.tags = [f"drover.cluster_id={cluster_id}", "drover.managed=true"]


class DummySecurityGroup:
    def __init__(self, sg_id: str, name: str, project_id: str, cluster_id: str):
        self.id = sg_id
        self.name = name
        self.project_id = project_id
        self.tags = [f"drover.cluster_id={cluster_id}", "drover.managed=true"]


class DummyVolume:
    def __init__(self, vol_id: str, name: str, project_id: str, cluster_id: str, status: str = "in-use"):
        self.id = vol_id
        self.name = name
        self.project_id = project_id
        self.status = status
        self.metadata = {"drover.cluster_id": cluster_id, "drover.managed": "true"}


class ReconciliationTestStore:
    def __init__(self):
        self.clusters = {}
        self.managed_resources = {}
        self.events = {}
        self.jobs = {}
        self.create_operations = {}

    def add_cluster(self, project_id: str, cluster_id: str, data: dict):
        cl = {
            "id": cluster_id,
            "project_id": project_id,
            "name": data.get("name", ""),
            "status": data.get("status", "ACTIVE"),
            "status_reason": data.get("status_reason"),
            "server_vm_id": data.get("server_vm_id"),
            "kubeconfig": data.get("kubeconfig", "apiVersion: v1\nkind: Config\n"),
            "security_group_id": data.get("security_group_id"),
            "api_lb_id": data.get("api_lb_id"),
            "api_fip_id": data.get("api_fip_id"),
            "app_credential_id": data.get("app_credential_id"),
            "agent_vm_ids": data.get("agent_vm_ids") or [],
            "deleted_at": None,
            "last_reconciled_at": None,
            "drift_status": None,
        }
        self.clusters[cluster_id] = cl
        return cl


def test_fetch_octavia_member_passes_pool_positionally():
    connection = MagicMock()
    member = object()
    connection.load_balancer.find_member.return_value = member

    result = reconciliation._fetch_octavia_member(
        connection,
        "member-1",
        metadata={"pool_id": "pool-1"},
    )

    assert result is member
    connection.load_balancer.find_member.assert_called_once_with(
        "member-1",
        "pool-1",
        ignore_missing=True,
    )

def test_fetch_octavia_member_without_pool_metadata_skips_lookup():
    connection = MagicMock()

    assert reconciliation._fetch_octavia_member(connection, "member-1", metadata=None) is None
    connection.load_balancer.find_member.assert_not_called()



@pytest.fixture
def test_store():
    return ReconciliationTestStore()


@pytest.fixture(autouse=True)
def mock_services(monkeypatch, test_store):
    async def fake_get_cluster(project_id, cluster_id):
        cluster = test_store.clusters.get(cluster_id)
        return dict(cluster) if cluster else None

    async def fake_get_kubeconfig(project_id, cluster_id):
        return test_store.clusters[cluster_id]["kubeconfig"]

    async def fake_latest_create(project_id, cluster_id):
        return test_store.create_operations.get(cluster_id)

    async def fake_list_managed_resources(session_or_factory=None, cluster_id="", active_only=True):
        res_list = [r for (cid, *_), r in test_store.managed_resources.items() if cid == cluster_id]
        if active_only:
            res_list = [r for r in res_list if r.deleted_at is None]
        return res_list

    async def fake_record_resource(
        session_or_factory=None,
        *,
        cluster_id="",
        service="",
        resource_type="",
        resource_id="",
        operation_id=None,
        name=None,
        state=None,
        metadata=None,
    ):
        key = (cluster_id, service, resource_type, resource_id)
        existing = test_store.managed_resources.get(key)
        now = datetime.now(UTC)
        if existing is not None:
            existing.last_seen_at = now
            if state is not None:
                existing.state = state
            return existing
        res = ManagedOpenStackResource(
            id=f"{service}-{resource_id}",
            cluster_id=cluster_id,
            service=service,
            resource_type=resource_type,
            resource_id=resource_id,
            name=name,
            state=state,
            created_at=now,
            last_seen_at=now,
        )
        test_store.managed_resources[key] = res
        return res

    async def fake_update_cluster_reconciliation(
        cluster_id, last_reconciled_at, drift_status, status=None, status_reason=None
    ):
        cl = test_store.clusters.get(cluster_id)
        if cl:
            cl["last_reconciled_at"] = last_reconciled_at
            cl["drift_status"] = drift_status
            if status is not None:
                cl["status"] = status
            if status_reason is not None:
                cl["status_reason"] = status_reason

    async def fake_append_operation_event(session_or_factory, operation_id, phase, message, payload_json=None):
        ev_list = test_store.events.setdefault(operation_id, [])
        seq = len(ev_list) + 1
        ev = DroverOperationEvent(
            id=len(ev_list) + 1,
            operation_id=operation_id,
            sequence=seq,
            phase=phase,
            message=message,
            payload_json=payload_json,
            created_at=datetime.now(UTC),
        )
        ev_list.append(ev)
        return ev

    monkeypatch.setattr(store, "get_cluster", fake_get_cluster)
    monkeypatch.setattr(store, "get_kubeconfig", fake_get_kubeconfig)
    monkeypatch.setattr(operations, "get_latest_create_operation", fake_latest_create)
    monkeypatch.setattr(cluster_authority, "active_credentials", AsyncMock(return_value=[]))
    monkeypatch.setattr(inventory, "list_managed_resources", fake_list_managed_resources)
    monkeypatch.setattr(inventory, "record_resource", fake_record_resource)
    monkeypatch.setattr(store, "update_cluster_reconciliation", fake_update_cluster_reconciliation)
    monkeypatch.setattr(operations, "append_operation_event", fake_append_operation_event)


@pytest.mark.asyncio
async def test_reconcile_all_present(test_store):
    """Scenario 1: All recorded resources present in OpenStack -> zero drift, clean status."""
    project_id = "proj-test-1"
    cluster_id = "cls-all-present"
    server_id = "srv-vm-001"
    sg_id = "sg-001"
    op_id = "op-reconcile-1"

    test_store.add_cluster(project_id, cluster_id, {
        "name": "test-cluster",
        "status": "ACTIVE",
        "server_vm_id": server_id,
        "security_group_id": sg_id,
    })

    await inventory.record_resource(
        cluster_id=cluster_id,
        service="nova",
        resource_type="server",
        resource_id=server_id,
        name="server-vm",
        state="ACTIVE",
    )
    await inventory.record_resource(
        cluster_id=cluster_id,
        service="neutron",
        resource_type="security_group",
        resource_id=sg_id,
        name="k3s-sg",
        state="ACTIVE",
    )

    mock_conn = MagicMock()
    server_obj = DummyServer(server_id, "server-vm", project_id, cluster_id, status="ACTIVE")
    sg_obj = DummySecurityGroup(sg_id, "k3s-sg", project_id, cluster_id)

    mock_conn.compute.find_server.side_effect = lambda sid, ignore_missing=True: server_obj if sid == server_id else None
    mock_conn.network.find_security_group.side_effect = lambda sid, ignore_missing=True: sg_obj if sid == sg_id else None
    mock_conn.compute.servers.return_value = [server_obj]
    mock_conn.network.security_groups.return_value = [sg_obj]
    mock_conn.block_storage.volumes.return_value = []
    mock_conn.load_balancer.load_balancers.return_value = []

    drift = await reconciliation.reconcile_cluster(
        project_id=project_id,
        cluster_id=cluster_id,
        conn=mock_conn,
        operation_id=op_id,
    )

    assert drift["has_drift"] is False
    assert drift["missing_count"] == 0
    assert drift["orphan_count"] == 0
    assert drift["mismatch_count"] == 0

    cl = test_store.clusters[cluster_id]
    assert cl["status"] == "ACTIVE"
    assert cl["last_reconciled_at"] is not None
    assert cl["drift_status"]["has_drift"] is False

    events = test_store.events[op_id]
    phases = [e.phase for e in events]
    assert "reconcile_start" in phases
    assert "reconcile_inventory" in phases
    assert "reconcile_complete" in phases


@pytest.mark.asyncio
async def test_reconcile_missing_required(test_store):
    """Scenario 2: Required resource missing -> transitions cluster to ERROR with actionable reason."""
    project_id = "proj-test-2"
    cluster_id = "cls-missing-req"
    server_id = "srv-vm-missing"
    op_id = "op-reconcile-2"

    test_store.add_cluster(project_id, cluster_id, {
        "name": "missing-cluster",
        "status": "ACTIVE",
        "server_vm_id": server_id,
    })

    mock_conn = MagicMock()
    mock_conn.compute.find_server.return_value = None
    mock_conn.compute.servers.return_value = []
    mock_conn.block_storage.volumes.return_value = []
    mock_conn.network.security_groups.return_value = []
    mock_conn.load_balancer.load_balancers.return_value = []

    drift = await reconciliation.reconcile_cluster(
        project_id=project_id,
        cluster_id=cluster_id,
        conn=mock_conn,
        operation_id=op_id,
    )

    assert drift["has_drift"] is True
    assert drift["missing_count"] == 1
    assert "missing or deleted in OpenStack" in drift["missing"][0]["reason"]

    cl = test_store.clusters[cluster_id]
    assert cl["status"] == "ERROR"
    assert cl["status_reason"] is not None
    assert "srv-vm-missing" in cl["status_reason"]
    assert "missing or deleted in OpenStack" in cl["status_reason"]

    events = test_store.events[op_id]
    missing_events = [e for e in events if e.phase == "reconcile_drift_missing"]
    assert len(missing_events) == 1
    assert missing_events[0].payload_json["missing"][0]["resource_id"] == server_id


@pytest.mark.asyncio
async def test_reconcile_transient_error_retry_propagation(test_store):
    """Scenario 3: OpenStack transient exception bubbles up to job executor for retry."""
    project_id = "proj-test-3"
    cluster_id = "cls-transient"
    server_id = "srv-vm-transient"

    test_store.add_cluster(project_id, cluster_id, {
        "name": "transient-cluster",
        "status": "ACTIVE",
        "server_vm_id": server_id,
    })

    mock_conn = MagicMock()
    mock_conn.compute.find_server.side_effect = RuntimeError("OpenStack Compute API 500 Internal Server Error")

    # Direct call raises RuntimeError (bubbles up)
    with pytest.raises(RuntimeError, match="500 Internal Server Error"):
        await reconciliation.reconcile_cluster(
            project_id=project_id,
            cluster_id=cluster_id,
            conn=mock_conn,
        )

    # Reconcile job execution also bubbles exception
    with patch("drover.services.execution.open_connection", return_value=mock_conn):
        with pytest.raises(RuntimeError, match="500 Internal Server Error"):
            await jobs._execute_job_direct(
                kind="reconcile",
                payload={},
                cluster_id=cluster_id,
                project_id=project_id,
            )

    cl = test_store.clusters[cluster_id]
    # Cluster status is NOT changed to ERROR or HEALTHY by transient exception
    assert cl["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_reconcile_orphan_report_no_delete(test_store):
    """Scenario 4: Unknown tagged resource reported as orphan, never auto-deleted."""
    project_id = "proj-test-4"
    cluster_id = "cls-orphan"
    known_server_id = "srv-vm-known"
    orphan_vol_id = "vol-orphan-999"
    op_id = "op-reconcile-4"

    test_store.add_cluster(project_id, cluster_id, {
        "name": "orphan-cluster",
        "status": "ACTIVE",
        "server_vm_id": known_server_id,
    })

    mock_conn = MagicMock()
    known_server = DummyServer(known_server_id, "known-server", project_id, cluster_id)
    orphan_vol = DummyVolume(orphan_vol_id, "untracked-volume", project_id, cluster_id)

    mock_conn.compute.find_server.side_effect = lambda sid, ignore_missing=True: known_server if sid == known_server_id else None
    mock_conn.compute.servers.return_value = [known_server]
    mock_conn.block_storage.volumes.return_value = [orphan_vol]
    mock_conn.network.security_groups.return_value = []
    mock_conn.load_balancer.load_balancers.return_value = []

    # First observation
    drift1 = await reconciliation.reconcile_cluster(
        project_id=project_id,
        cluster_id=cluster_id,
        conn=mock_conn,
        operation_id=op_id,
    )

    assert drift1["has_drift"] is True
    assert drift1["orphan_count"] == 1
    assert drift1["orphans"][0]["resource_id"] == orphan_vol_id
    assert drift1["orphans"][0]["service"] == "cinder"

    # Assert NO delete call on OpenStack connection
    mock_conn.block_storage.delete_volume.assert_not_called()
    mock_conn.compute.delete_server.assert_not_called()

    # Second observation (confirm report-only persistence)
    drift2 = await reconciliation.reconcile_cluster(
        project_id=project_id,
        cluster_id=cluster_id,
        conn=mock_conn,
    )
    assert drift2["orphan_count"] == 1
    mock_conn.block_storage.delete_volume.assert_not_called()

    events = test_store.events[op_id]
    orphan_events = [e for e in events if e.phase == "reconcile_drift_orphan"]
    assert len(orphan_events) == 1
    assert orphan_events[0].payload_json["orphans"][0]["resource_id"] == orphan_vol_id


def _empty_connection():
    conn = MagicMock()
    conn.compute.find_server.return_value = None
    conn.compute.servers.return_value = []
    conn.block_storage.volumes.return_value = []
    conn.network.security_groups.return_value = []
    conn.load_balancer.load_balancers.return_value = []
    return conn


@pytest.mark.asyncio
@pytest.mark.parametrize("server_id,kubeconfig", [(None, None), ("server-1", None), (None, "config")])
async def test_unprovisioned_cluster_cannot_recover_from_empty_inventory(test_store, server_id, kubeconfig):
    cluster = test_store.add_cluster("project-1", "cluster-1", {
        "status": "ERROR", "status_reason": "Cluster creation failed: token lifetime exceeded",
        "server_vm_id": server_id, "kubeconfig": kubeconfig,
    })
    conn = _empty_connection()
    if server_id:
        conn.compute.find_server.return_value = DummyServer(server_id, "primary", "project-1", "cluster-1")

    drift = await reconciliation.reconcile_cluster("project-1", "cluster-1", conn=conn, operation_id="reconcile-1")

    assert cluster["status"] == "ERROR"
    assert drift["has_drift"] is True
    assert {r["resource_type"] for r in drift["missing"]} == (
        ({"server"} if not server_id else set()) | ({"kubeconfig"} if not kubeconfig else set())
    )
    assert all(r["is_required"] for r in drift["missing"])
    assert "not provisioned or recorded" in cluster["status_reason"]
    assert cluster["drift_status"] == drift
    assert test_store.events["reconcile-1"][-1].payload_json == drift


@pytest.mark.asyncio
@pytest.mark.parametrize("create_status", ["FAILED", "CANCELLED"])
@pytest.mark.parametrize("provisioned", [False, True])
async def test_failed_create_is_not_recovered_by_inventory(test_store, create_status, provisioned):
    cluster = test_store.add_cluster("project-1", "cluster-1", {
        "status": "ERROR", "server_vm_id": "server-1" if provisioned else None,
        "kubeconfig": "config" if provisioned else None,
        "status_reason": "Reconciliation failed: earlier missing server",
    })
    test_store.create_operations["cluster-1"] = DroverOperation(
        id="create-1", project_id="project-1", cluster_id="cluster-1", kind="create",
        status=create_status, error="Cloud-init failed", created_at=datetime.now(UTC),
    )
    conn = _empty_connection()
    conn.compute.find_server.return_value = DummyServer("server-1", "primary", "project-1", "cluster-1")

    for _ in range(2):
        drift = await reconciliation.reconcile_cluster("project-1", "cluster-1", conn=conn)
        assert cluster["status"] == "ERROR"
        assert drift["has_drift"] is True
        assert drift["missing_count"] == (0 if provisioned else 2)
        assert drift["mismatches"][0]["state"] == create_status
        assert "Cloud-init failed" in cluster["status_reason"]


@pytest.mark.asyncio
async def test_missing_resource_error_recovers_after_server_returns(test_store):
    cluster = test_store.add_cluster("project-1", "cluster-1", {
        "server_vm_id": "server-1", "kubeconfig": "config", "status": "ACTIVE",
    })
    conn = _empty_connection()
    missing = await reconciliation.reconcile_cluster("project-1", "cluster-1", conn=conn)
    assert missing["has_drift"] is True
    assert cluster["status"] == "ERROR"

    conn.compute.find_server.return_value = DummyServer("server-1", "primary", "project-1", "cluster-1")
    recovered = await reconciliation.reconcile_cluster("project-1", "cluster-1", conn=conn)
    assert recovered["has_drift"] is False
    assert cluster["status"] == "ACTIVE"
    assert not cluster["status_reason"]


@pytest.mark.asyncio
async def test_unrelated_error_is_not_cleared_by_healthy_inventory(test_store):
    cluster = test_store.add_cluster("project-1", "cluster-1", {
        "server_vm_id": "server-1", "status": "ERROR", "status_reason": "Certificate rotation failed",
    })
    conn = _empty_connection()
    conn.compute.find_server.return_value = DummyServer("server-1", "primary", "project-1", "cluster-1")
    await reconciliation.reconcile_cluster("project-1", "cluster-1", conn=conn)
    assert cluster["status"] == "ERROR"
    assert cluster["status_reason"] == "Certificate rotation failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["CREATING", "DELETING", "DELETED"])
async def test_reconciliation_preserves_transition_boundaries(test_store, monkeypatch, status):
    cluster = test_store.add_cluster("project-1", "cluster-1", {"status": status, "kubeconfig": None})
    open_conn = AsyncMock()
    monkeypatch.setattr(execution, "open_connection", open_conn)
    await reconciliation.reconcile_cluster("project-1", "cluster-1")
    assert cluster["status"] == status
    assert cluster["last_reconciled_at"] is None
    open_conn.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("provisioned", [False, True])
async def test_missing_authority_preserves_provisioning_drift_and_active_reauthorization(test_store, monkeypatch, provisioned):
    cluster = test_store.add_cluster("project-1", "cluster-1", {
        "status": "ACTIVE" if provisioned else "ERROR",
        "server_vm_id": "server-1" if provisioned else None,
        "kubeconfig": "config" if provisioned else None,
    })
    monkeypatch.setattr(execution, "open_connection", AsyncMock(side_effect=execution.ReauthorizationRequired("reauthorize")))
    drift = await reconciliation.reconcile_cluster("project-1", "cluster-1")
    assert drift["reauthorization_required"] is True
    assert drift["has_drift"] is (not provisioned)
    assert cluster["status"] == ("ACTIVE" if provisioned else "ERROR")
    if not provisioned:
        assert cluster["drift_status"] == drift


@pytest.mark.asyncio
async def test_missing_control_credential_keeps_healthy_cluster_active(test_store, monkeypatch):
    cluster = test_store.add_cluster("project-1", "cluster-1", {"server_vm_id": "server-1"})
    monkeypatch.setattr(cluster_authority, "active_credentials", AsyncMock(return_value=[{
        "app_credential_id": "cred-1", "purpose": "control", "generation": 1,
    }]))
    conn = _empty_connection()
    conn.compute.find_server.return_value = DummyServer("server-1", "primary", "project-1", "cluster-1")
    conn.identity.get_application_credential.side_effect = reconciliation.ResourceNotFound("credential missing")
    drift = await reconciliation.reconcile_cluster("project-1", "cluster-1", conn=conn)
    assert drift["has_drift"] is True
    assert drift["missing_count"] == 1
    assert drift["missing"][0]["is_required"] is False
    assert cluster["status"] == "ACTIVE"


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["missing_resource", "recovered_resource", "missing_authority"])
@pytest.mark.parametrize("next_status,soft_deleted", [("DELETING", False), ("DELETED", True), ("ACTIVE", True)])
async def test_final_persistence_preserves_lifecycle_change_after_observation(
    test_store, monkeypatch, path, next_status, soft_deleted,
):
    """Exercise the real store writer after the observed cluster starts/completes deletion.

    The SQL-boundary double applies the locking-read eligibility predicates to a live row;
    this defines consumer behavior, not a claim of real database concurrency verification.
    """
    cluster = test_store.add_cluster("project-1", "cluster-1", {
        "status": "ACTIVE" if path == "missing_resource" else "ERROR",
        "status_reason": "Reconciliation failed: earlier missing server",
        "server_vm_id": None if path == "missing_authority" else "server-1",
        "kubeconfig": None if path == "missing_authority" else "config",
    })
    row = K3sCluster(
        id="cluster-1", project_id="project-1", name="cluster", status=cluster["status"],
        status_reason=cluster["status_reason"], deleted_at=None,
    )

    def advance_lifecycle():
        row.status = next_status
        row.status_reason = "Deletion owns this state"
        row.deleted_at = datetime.now(UTC) if soft_deleted else None
        cluster.update(status=row.status, status_reason=row.status_reason, deleted_at=row.deleted_at)

    class Result:
        def scalar_one_or_none(self):
            return row if row.deleted_at is None and row.status in {"ACTIVE", "ERROR"} else None

    class Session:
        commits = 0
        statements = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def execute(self, statement):
            compiled = statement.compile(dialect=mysql.dialect())
            sql = str(compiled).upper()
            assert "FOR UPDATE" in sql
            assert "DELETED_AT IS NULL" in sql
            assert ".STATUS IN " in sql
            assert ["ACTIVE", "ERROR"] in compiled.params.values()
            self.statements.append(statement)
            return Result()

        async def commit(self):
            self.commits += 1

    session = Session()
    monkeypatch.setattr(store, "update_cluster_reconciliation", _persist_cluster_reconciliation)
    monkeypatch.setattr(store, "is_db_available", lambda: True)
    monkeypatch.setattr(store, "get_session_factory", lambda: lambda: session)

    if path == "missing_authority":
        async def no_authority(_project_id):
            advance_lifecycle()
            raise execution.ReauthorizationRequired("reauthorize")

        monkeypatch.setattr(execution, "open_connection", no_authority)
        drift = await reconciliation.reconcile_cluster("project-1", "cluster-1")
        assert drift["reauthorization_required"] is True
    else:
        conn = _empty_connection()

        def fetch_after_lifecycle_change(_server_id, ignore_missing=True):
            advance_lifecycle()
            if path == "recovered_resource":
                return DummyServer("server-1", "primary", "project-1", "cluster-1")
            return None

        conn.compute.find_server.side_effect = fetch_after_lifecycle_change
        drift = await reconciliation.reconcile_cluster("project-1", "cluster-1", conn=conn)

    assert drift["has_drift"] is (path != "recovered_resource")
    assert len(session.statements) == 1
    assert session.commits == 0
    assert row.status == next_status
    assert row.status_reason == "Deletion owns this state"
    assert row.last_reconciled_at is None
    assert row.drift_status is None
    assert row.updated_at is None
