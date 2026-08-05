"""Drover background worker process for health checks, Stampede reconciler, stale cleanup, and durable jobs."""

from __future__ import annotations

import asyncio
import logging
import signal

from drover.config import get_settings
from drover.db import close_db, init_db

_logger = logging.getLogger("drover.worker")


async def _stale_cleanup_loop():
    from drover.services import store

    await asyncio.sleep(120)
    while True:
        try:
            await store.check_stale_clusters(timeout_minutes=30)
        except Exception as exc:
            _logger.warning("Stale cluster cleanup error: %s", exc)
        await asyncio.sleep(300)


async def _jobs_worker_loop():
    from drover.services.jobs import claim_and_run_jobs

    await asyncio.sleep(5)
    while True:
        try:
            processed = await claim_and_run_jobs()
            if processed == 0:
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(1)
        except Exception as exc:
            _logger.warning("Jobs worker loop error: %s", exc)
            await asyncio.sleep(5)


async def _health_worker_loop():
    from drover.services import health

    s = get_settings()
    interval = getattr(s, "k3s_health_interval", 180)
    await asyncio.sleep(10)
    while True:
        try:
            await health.check_all_clusters_health()
        except Exception as exc:
            _logger.warning("Health worker loop error: %s", exc)
        await asyncio.sleep(interval)


async def _stampede_worker_loop():
    from drover.services import stampede

    s = get_settings()
    interval = s.drover_stampede_interval or s.drover_stampede_interval or 60
    await asyncio.sleep(15)
    while True:
        try:
            if s.drover_stampede_enabled or s.drover_stampede_enabled:
                await stampede.run_all()
        except Exception as exc:
            _logger.warning("Stampede worker loop error: %s", exc)
        await asyncio.sleep(interval)


async def _main_async():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    _logger.info("Starting Drover background worker...")

    settings = get_settings()
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
        asyncio.create_task(_stale_cleanup_loop()),
        asyncio.create_task(_jobs_worker_loop()),
        asyncio.create_task(_health_worker_loop()),
        asyncio.create_task(_stampede_worker_loop()),
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
