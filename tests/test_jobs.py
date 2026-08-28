"""Durable Drover queue ownership, retry, and dispatch contracts."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import mysql

from drover.models.orm import DroverJob, K3sCluster
from drover.services import jobs

pytestmark = pytest.mark.asyncio


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_args):
        return False


class _Session:
    def __init__(self, *, execute_values=(), objects=None):
        self.execute_values = list(execute_values)
        self.objects = objects or {}
        self.added = []
        self.statements = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def begin(self):
        return _Transaction(self)

    def add(self, value):
        self.added.append(value)

    async def execute(self, statement):
        self.statements.append(statement)
        value = self.execute_values.pop(0) if self.execute_values else None
        return _Result(value)

    async def get(self, model, object_id, **_kwargs):
        return self.objects.get((model, object_id))


def _factory(session):
    return lambda: session


async def test_enqueue_persists_supported_job_before_return(monkeypatch):
    session = _Session()
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    job_id = await jobs.enqueue_job(
        "cluster-1",
        "project-1",
        "scale",
        {"desired_count": 4},
        user_id="user-1",
        username="alice",
    )

    job = [obj for obj in session.added if isinstance(obj, DroverJob)][0]
    assert isinstance(job, DroverJob)
    assert job.id == job_id
    assert job.status == "queued"
    assert job.payload_json == {"desired_count": 4}
    assert job.user_id == "user-1"


async def test_enqueue_rejects_unknown_job_kind(monkeypatch):
    monkeypatch.setattr(jobs, "get_session_factory", lambda: None)
    with pytest.raises(ValueError, match="unsupported Drover job kind"):
        await jobs.enqueue_job("cluster-1", "project-1", "unknown", {})


async def test_scale_dispatch_uses_persisted_desired_count(monkeypatch):
    scale = AsyncMock()
    monkeypatch.setattr("drover.services.autoscale.scale_agents", scale)

    await jobs._execute_job_direct("scale", {"desired_count": 7}, "cluster-1", "project-1")

    scale.assert_awaited_once_with("project-1", "cluster-1", 7)



async def test_stampede_provision_dispatches_tracked_worker_operation(monkeypatch):
    tracked = AsyncMock()
    monkeypatch.setattr("drover.services.stampede._provision_and_track", tracked)

    await jobs._execute_job_direct(
        "stampede_provision",
        {
            "nodegroup_id": "nodegroup-1",
            "add_count": 2,
            "flavor_id": "gpu",
            "image_id": "image-1",
            "labels": {"gpu": "true"},
            "taints": [],
            "gpu_required": True,
        },
        "cluster-1",
        "project-1",
    )

    tracked.assert_awaited_once_with(
        project_id="project-1",
        cluster_id="cluster-1",
        nodegroup_id="nodegroup-1",
        add_count=2,
        flavor_id="gpu",
        image_id="image-1",
        labels={"gpu": "true"},
        taints=[],
        gpu_required=True,
    )

async def test_old_worker_cannot_complete_reclaimed_attempt(monkeypatch):
    job = SimpleNamespace(status="running", attempts=2, claimed_at=object(), last_error=None, updated_at=None)
    session = _Session(objects={(DroverJob, "job-1"): job})
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    assert await jobs._complete("job-1", attempt=1) is False
    assert job.status == "running"



async def test_lease_renewal_is_fenced_to_the_claimed_attempt(monkeypatch):
    job = SimpleNamespace(status="running", attempts=2, claimed_at=None, updated_at=None)
    session = _Session(objects={(DroverJob, "job-1"): job})
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    assert await jobs._renew_lease("job-1", attempt=1) is False
    assert job.claimed_at is None
    assert await jobs._renew_lease("job-1", attempt=2) is True
    assert job.claimed_at is not None

async def test_second_failure_requeues_job(monkeypatch):
    job = SimpleNamespace(status="running", attempts=2, claimed_at=object(), last_error=None, updated_at=None)
    session = _Session(objects={(DroverJob, "job-1"): job})
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    assert await jobs._retry_or_fail("job-1", attempt=2, error="Nova unavailable") is True
    assert job.status == "queued"
    assert job.last_error == "Nova unavailable"
    assert job.claimed_at is None


async def test_third_failure_terminalizes_job_and_cluster(monkeypatch):
    job = SimpleNamespace(
        id="job-1",
        cluster_id="cluster-1",
        project_id="project-1",
        status="running",
        attempts=3,
        claimed_at=object(),
        last_error=None,
        updated_at=None,
    )
    cluster = SimpleNamespace(project_id="project-1", deleted_at=None, status="SCALING", status_reason=None, updated_at=None)
    session = _Session(objects={(DroverJob, "job-1"): job, (K3sCluster, "cluster-1"): cluster})
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    assert await jobs._retry_or_fail("job-1", attempt=3, error="quota exceeded") is True
    assert job.status == "failed"
    assert cluster.status == "ERROR"
    assert cluster.status_reason == "quota exceeded"


async def test_claim_reclaims_stale_jobs_with_skip_locked(monkeypatch):
    job = SimpleNamespace(
        id="job-1",
        cluster_id="cluster-1",
        project_id="project-1",
        kind="scale",
        status="running",
        attempts=1,
        claimed_at=None,
        last_error="worker exited",
        payload_json={"desired_count": 2},
        updated_at=None,
    )
    cluster = SimpleNamespace(project_id="project-1", deleted_at=None)
    session = _Session(
        execute_values=[job],
        objects={(K3sCluster, "cluster-1"): cluster},
    )
    monkeypatch.setattr(jobs, "get_session_factory", lambda: _factory(session))

    claimed = await jobs._claim_one()

    assert claimed == ("job-1", 2, "scale", "cluster-1", "project-1", {"desired_count": 2})
    assert job.status == "running"
    assert job.attempts == 2
    assert job.claimed_at is not None
    sql = str(session.statements[0].compile(dialect=mysql.dialect())).upper()
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "DROVER_JOBS.CLAIMED_AT" in sql


async def test_process_failure_is_requeued_with_claimed_attempt(monkeypatch):
    retry = AsyncMock(return_value=True)
    monkeypatch.setattr(
        jobs,
        "_claim_one",
        AsyncMock(return_value=("job-1", 2, "scale", "cluster-1", "project-1", {"desired_count": 3})),
    )
    monkeypatch.setattr(jobs, "_execute_job_direct", AsyncMock(side_effect=RuntimeError("capacity unavailable")))
    monkeypatch.setattr(jobs, "_retry_or_fail", retry)

    assert await jobs.process_one_job() is True
    retry.assert_awaited_once_with("job-1", attempt=2, error="capacity unavailable")
