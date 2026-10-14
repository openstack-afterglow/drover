"""Native worker: SQL competing jobs plus independently coordinated periodic work."""

from __future__ import annotations

import asyncio
import logging
import signal
import threading
import time

from drover.config import get_settings, validate_config
from drover.db import close_db, init_db
from drover.logging import configure_logging
from drover.services import cache
from drover.services.worker_ownership import OwnershipLost, bound, checkpoint

_logger = logging.getLogger("drover.worker")


async def discover_targets() -> list[str]:
    """Only the discovery owner reads the shared SQL cluster registry."""
    from drover.services import store

    await checkpoint()
    clusters = await store.list_all_clusters(include_deleted=False)
    targets = []
    for cluster in clusters:
        cluster_id = cluster["id"]
        if cluster.get("status") in {"ACTIVE", "ERROR"}:
            targets.append(f"reconcile:{cluster_id}")
        if cluster.get("status") == "ACTIVE":
            targets.append(f"health:{cluster_id}")
            if get_settings().drover_stampede_enabled and cluster.get("stampede_enabled"):
                targets.append(f"stampede:{cluster_id}")
    await checkpoint()
    return targets


async def execute_owned(key: str, registry: list[str]) -> None:
    from drover.services import delegation, health, operations, reconciliation, stampede

    await checkpoint()
    if key == "recovery":
        await operations.recover_expired_callback_operations(timeout_seconds=1800)
        await checkpoint()
        await operations.recover_deleted_cluster_operations()
    elif key == "reconciliation":
        await reconciliation.schedule_worker_reconciliations(
            [item.removeprefix("reconcile:") for item in registry if item.startswith("reconcile:")],
            max_per_project=get_settings().drover_reconcile_concurrency_per_project,
        )
    elif key == "delegation":
        await delegation.sweep()
    elif key.startswith("health:"):
        await health.check_active_cluster(key.removeprefix("health:"))
    elif key.startswith("stampede:"):
        await stampede.run_cluster(key.removeprefix("stampede:"))
    else:
        raise ValueError("Unsupported worker assignment")


def work_interval(key: str) -> float:
    settings = get_settings()
    if key == "discovery":
        return 30
    if key.startswith("health:"):
        return settings.k3s_health_interval
    if key.startswith("stampede:"):
        return settings.drover_stampede_interval or 60
    if key == "delegation":
        return 300
    return settings.drover_reconcile_interval


class WorkerRuntime:
    """Control runs on its own OS thread; SQL and domain tasks stay on one loop.

    Discovery is a domain future, never awaited by membership or scheduling. The
    injected callbacks are also used by the native loopback smoke; production
    always uses the SQL discovery and authority-preserving services above.
    """

    def __init__(self, *, coordinator_factory=None, discover=None, execute=None, interval=None, control_interval=1.0):
        self.coordinator_factory = coordinator_factory or self._coordinator
        self.discover = discover or discover_targets
        self.execute = execute or execute_owned
        self.interval = interval or work_interval
        self.control_interval = control_interval
        self.stopping = threading.Event()
        self.ready = threading.Event()
        self.thread = None
        self.coordinator = None
        self.domain_loop = None
        self.domain_tasks = set()
        self.running_keys = set()
        self.error = None

    @staticmethod
    def _coordinator():
        from drover.services.cache.redis_backend import RedisBackend
        from drover.services.worker_ownership import Coordinator

        # A backend local to the control thread preserves Sentinel/direct
        # selection without sharing the domain loop's connection pool; closing
        # it also closes Sentinel discovery clients.
        backend = RedisBackend()
        return Coordinator(backend.raw_client(), close_client=backend.close)

    async def start(self):
        self.domain_loop = asyncio.get_running_loop()
        self.failed = asyncio.Event()
        self.thread = threading.Thread(target=self._thread_main, name="drover-worker-control", daemon=False)
        self.thread.start()
        await asyncio.to_thread(self.ready.wait)
        if self.error:
            raise self.error

    def request_stop(self):
        self.stopping.set()
        if self.coordinator is not None:
            self.coordinator.revoke_local()

    async def stop(self):
        self.request_stop()
        if self.thread is not None:
            await asyncio.to_thread(self.thread.join)
        if self.error:
            raise self.error

    def _thread_main(self):
        try:
            asyncio.run(self._control())
        except BaseException as exc:
            self.error = exc
            _logger.exception("Worker control stopped unexpectedly")
            if self.domain_loop is not None:
                self.domain_loop.call_soon_threadsafe(self.failed.set)
        finally:
            self.ready.set()

    async def _domain_work(self, key, lease, registry):
        # A cancelled cross-thread Future is not proof its SDK thread drained.
        # Keep the per-key gate until the actual old domain coroutine exits.
        if key in self.running_keys or self.stopping.is_set():
            return
        self.running_keys.add(key)
        task = asyncio.current_task()
        self.domain_tasks.add(task)
        try:
            with bound(lease):
                await checkpoint()
                if key == "discovery":
                    return await self.discover()
                await self.execute(key, registry)
        except OwnershipLost:
            _logger.info("Worker assignment revoked role=%s", key.split(":", 1)[0])
        except Exception:
            _logger.warning("Worker assignment failed role=%s", key.split(":", 1)[0], exc_info=True)
        finally:
            self.domain_tasks.discard(task)
            self.running_keys.discard(key)

    async def _drain_domain(self):
        tasks = tuple(self.domain_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _control(self):
        self.coordinator = self.coordinator_factory()
        active = {}
        due = {}
        self.ready.set()
        try:
            while not self.stopping.is_set():
                try:
                    await self.coordinator.heartbeat()
                    assignments = await self.coordinator.assignments()
                    registry = await self.coordinator.registry()
                    for key, (lease, future) in list(active.items()):
                        current = assignments.get(key)
                        if current is not lease or lease.revoked:
                            lease.revoke()
                            future.cancel()
                            del active[key]
                            due.pop(key, None)
                            continue
                        if future.done():
                            if not future.cancelled():
                                result = future.result()
                                if key == "discovery" and result is not None:
                                    await self.coordinator.publish(result, lease)
                            del active[key]
                            due[key] = time.monotonic() + max(0.01, self.interval(key))
                    for key, lease in assignments.items():
                        if self.stopping.is_set():
                            break
                        if key not in active and time.monotonic() >= due.get(key, 0):
                            future = asyncio.run_coroutine_threadsafe(self._domain_work(key, lease, registry), self.domain_loop)
                            active[key] = lease, future
                except Exception:
                    _logger.warning("Worker coordination unavailable; periodic work fenced", exc_info=True)
                    self.coordinator.revoke_local()
                    for _, future in active.values():
                        future.cancel()
                    active.clear()
                    due.clear()
                await asyncio.sleep(self.control_interval)
        finally:
            self.coordinator.revoke_local()
            for _, future in active.values():
                future.cancel()
            try:
                await self.coordinator.begin_drain()
            except OwnershipLost:
                _logger.warning("Worker drain cannot renew ownership; admission remains revoked")
            # A concurrent Future becomes cancelled before its coroutine finishes:
            # drain real domain tasks before releasing Redis ownership.
            drain = asyncio.run_coroutine_threadsafe(self._drain_domain(), self.domain_loop)
            await asyncio.wrap_future(drain)
            try:
                await self.coordinator.close()
            finally:
                await self.coordinator.close_client()


async def _jobs_worker_loop():
    from drover.services.jobs import claim_and_run_jobs

    while True:
        try:
            processed = await claim_and_run_jobs()
            await asyncio.sleep(1 if processed else 5)
        except Exception:
            _logger.warning("Jobs worker loop failed", exc_info=True)
            await asyncio.sleep(5)


async def _main_async():
    configure_logging()
    settings = get_settings()
    validate_config(settings)
    init_db(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        connect_timeout=settings.database_connect_timeout,
        pool_timeout=settings.database_pool_timeout,
    )
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    runtime = WorkerRuntime()

    def stop(signum, frame):
        from drover.services.execution_safety import revoke_all_local

        revoke_all_local()
        # POSIX Python signal handler revokes even while domain SDK blocks the loop.
        runtime.request_stop()
        loop.call_soon_threadsafe(stop_event.set)

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    jobs_task = None
    waiters = []
    try:
        await runtime.start()
        jobs_task = asyncio.create_task(_jobs_worker_loop(), name="drover-sql-jobs")
        waiters = [asyncio.create_task(stop_event.wait()), asyncio.create_task(runtime.failed.wait())]
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    finally:
        from drover.services.execution_safety import revoke_all_local

        revoke_all_local()
        runtime.request_stop()
        if jobs_task is not None:
            jobs_task.cancel()
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        try:
            await runtime.stop()
        finally:
            if jobs_task is not None:
                await asyncio.gather(jobs_task, return_exceptions=True)
            await cache.get_backend().close()
            await close_db()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    _logger.info("Drover worker drained and stopped")


def main():
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
