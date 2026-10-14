"""Native consumer cancellation order and operation propagation; test-defined."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from drover.services import jobs


@pytest.mark.asyncio
async def test_cancelled_dispatch_is_not_requeued_until_real_work_drains(monkeypatch):
    entered = asyncio.Event()
    draining = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()
    heartbeat_stopped = asyncio.Event()

    async def heartbeat(job_id, *, attempt, stop, guard, dispatch):
        try:
            await stop.wait()
        finally:
            heartbeat_stopped.set()

    async def execute(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            draining.set()
            await release.wait()
            finished.set()
            raise

    retry = AsyncMock(return_value=True)
    monkeypatch.setattr(jobs, "_claim_one", AsyncMock(return_value=("job", 1, "create", "cluster", "project", {})))
    monkeypatch.setattr(jobs, "_execute_job_direct", execute)
    monkeypatch.setattr(jobs, "_retry_or_fail", retry)
    monkeypatch.setattr(jobs._delegation, "delete_released", AsyncMock())
    monkeypatch.setattr(jobs, "_heartbeat_lease", heartbeat)
    consumer = asyncio.create_task(jobs.process_one_job())
    try:
        await entered.wait()
        consumer.cancel()
        await draining.wait()
        await asyncio.sleep(0)
        assert not heartbeat_stopped.is_set(), "Cancellation abandoned the SQL lease before SDK drain"
        assert not finished.is_set()
        assert not consumer.done()
        retry.assert_not_awaited()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await consumer
        assert finished.is_set()
        assert heartbeat_stopped.is_set()
        retry.assert_awaited_once_with("job", attempt=1, error="Execution cancelled")
    finally:
        release.set()
        if not consumer.done():
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["create", "bootstrap_ha", "scale", "delete", "reconcile", "reauthorize"])
async def test_sql_claim_operation_id_reaches_every_dispatch_kind(monkeypatch, kind):
    payload = {"_operation_id": "operation", "native_payload": True}
    execute = AsyncMock()
    monkeypatch.setattr(jobs, "_claim_one", AsyncMock(return_value=("job", 1, kind, "cluster", "project", payload)))
    monkeypatch.setattr(jobs, "_execute_job_direct", execute)
    monkeypatch.setattr(jobs, "_complete", AsyncMock(return_value=True))
    monkeypatch.setattr(jobs._delegation, "delete_released", AsyncMock())
    await jobs.process_one_job()
    execute.assert_awaited_once_with(kind, {"native_payload": True}, "cluster", "project", "operation")
