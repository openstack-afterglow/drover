"""Drover background worker process for health checks, Stampede reconciler, stale cleanup, and durable jobs."""

from __future__ import annotations

import asyncio
import logging
import signal

from drover.config import get_settings, validate_config
from drover.db import close_db, init_db
from drover.logging import configure_logging

_logger = logging.getLogger("drover.worker")


async def _reconcile_worker_loop():
    from drover.services import operations, reconciliation

    s = get_settings()
    interval = getattr(s, "drover_reconcile_interval", 300)
    concurrency = getattr(s, "drover_reconcile_concurrency_per_project", 2)
    await asyncio.sleep(10)
    while True:
        try:
            await operations.recover_expired_callback_operations(timeout_seconds=1800)
            await reconciliation.schedule_worker_reconciliations(max_per_project=concurrency)
        except Exception:
            _logger.warning("Reconcile worker loop failed")
        await asyncio.sleep(interval)


async def _delegation_worker_loop():
    """Release idle operation trusts and record their Keystone expiry; never repeats cloud mutations."""
    from drover.services import delegation

    await asyncio.sleep(15)
    while True:
        try:
            await delegation.sweep()
        except Exception:
            _logger.warning("Delegation cleanup loop failed")
        await asyncio.sleep(300)


async def _jobs_worker_loop():
    from drover.services.jobs import claim_and_run_jobs

    await asyncio.sleep(5)
    while True:
        try:
            processed = await claim_and_run_jobs()
            _logger.debug("Jobs worker batch complete processed=%d", processed)
            if processed == 0:
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(1)
        except Exception:
            _logger.warning("Jobs worker loop failed")
            await asyncio.sleep(5)


async def _health_worker_loop():
    from drover.services import health

    s = get_settings()
    interval = s.k3s_health_interval
    await asyncio.sleep(10)
    while True:
        try:
            await health.check_all_active_clusters()
        except Exception:
            _logger.warning("Health worker loop failed")
        await asyncio.sleep(interval)


async def _stampede_worker_loop():
    from drover.services import stampede

    s = get_settings()
    interval = s.drover_stampede_interval or 60
    await asyncio.sleep(15)
    while True:
        try:
            if s.drover_stampede_enabled:
                await stampede.run_all()
        except Exception:
            _logger.warning("Stampede worker loop failed")
        await asyncio.sleep(interval)


async def _main_async():
    configure_logging()
    _logger.info("Starting Drover background worker...")

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

    def _sig_handler():
        _logger.info("Termination signal received. Shutting down worker...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass

    tasks = [
        asyncio.create_task(_reconcile_worker_loop()),
        asyncio.create_task(_jobs_worker_loop()),
        asyncio.create_task(_health_worker_loop()),
        asyncio.create_task(_stampede_worker_loop()),
        asyncio.create_task(_delegation_worker_loop()),
    ]

    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await close_db()
    _logger.info("Drover worker stopped cleanly.")


def main():
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
