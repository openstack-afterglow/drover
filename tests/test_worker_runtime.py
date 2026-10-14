"""Native runtime/controller contracts; no verification run during implementation."""
from __future__ import annotations

import asyncio
import collections
import threading
import time

import fakeredis
import fakeredis.aioredis

from drover.services import worker_ownership as ownership
from drover.worker import WorkerRuntime


async def eventually(predicate, seconds=3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Native runtime condition did not become true")


async def test_three_native_runtimes_have_one_healthy_owner_per_target_and_role():
    server = fakeredis.FakeServer()
    targets = [f"{kind}:c{index}" for kind in ("health", "stampede") for index in range(6)]
    calls = collections.defaultdict(set)
    active = {}
    duplicates = []
    runtimes = []

    async def discover():
        await ownership.checkpoint()
        return targets + ["reconcile:c0"]

    for index in range(3):
        def factory():
            return ownership.Coordinator(
                fakeredis.aioredis.FakeRedis(server=server, decode_responses=True),
                lease_seconds=1, registry_seconds=1, startup_seconds=0.1)

        async def execute(key, registry, owner=index):
            await ownership.checkpoint()
            assert "reconcile:c0" in registry
            if key in active:
                duplicates.append(key)
            active[key] = owner
            calls[key].add(owner)
            try:
                await asyncio.sleep(0.025)
                await ownership.checkpoint()
            finally:
                active.pop(key, None)

        runtime = WorkerRuntime(coordinator_factory=factory, discover=discover,
                                execute=execute, interval=lambda key: 0.03,
                                control_interval=0.015)
        runtimes.append(runtime)
    try:
        for runtime in runtimes:
            await runtime.start()
        expected = set(targets) | {"recovery", "delegation", "reconciliation"}
        await eventually(lambda: set(calls) == expected)
        await asyncio.sleep(0.12)
        assert all(len(owners) == 1 for owners in calls.values())
        assert not duplicates
        assert set.union(*calls.values()) == {0, 1, 2}
    finally:
        await asyncio.gather(*(runtime.stop() for runtime in runtimes))


async def test_native_control_heartbeats_while_domain_thread_is_blocked():
    entered = threading.Event()
    coordinator_box = {}

    def factory():
        coordinator = ownership.Coordinator(
            fakeredis.aioredis.FakeRedis(decode_responses=True),
            lease_seconds=0.2, registry_seconds=1, startup_seconds=0)
        coordinator_box["value"] = coordinator
        return coordinator

    async def discover():
        await ownership.checkpoint()
        return ["health:c"]

    async def execute(key, registry):
        if key == "health:c" and not entered.is_set():
            entered.set()
            time.sleep(0.35)
            # If heartbeat shared this loop, this actual mutation fence would fail.
            await ownership.checkpoint()
            coordinator_box["survived"] = True

    runtime = WorkerRuntime(coordinator_factory=factory, discover=discover,
                            execute=execute, interval=lambda key: 0.02,
                            control_interval=0.01)
    try:
        await runtime.start()
        await eventually(lambda: coordinator_box.get("survived", False))
        assert entered.is_set()
        assert runtime.error is None
    finally:
        await runtime.stop()


async def test_native_shutdown_retains_ownership_until_actual_sdk_thread_drains():
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    after_revoke_fenced = threading.Event()
    second_mutation = threading.Event()
    server = fakeredis.FakeServer()

    def factory():
        return ownership.Coordinator(
            fakeredis.aioredis.FakeRedis(server=server, decode_responses=True),
            lease_seconds=0.2, registry_seconds=2, startup_seconds=0)

    async def discover():
        return ["health:c"]

    def sdk_request():
        ownership.checkpoint_sync()
        entered.set()
        assert release.wait(3)
        finished.set()
        try:
            ownership.checkpoint_sync()
        except ownership.OwnershipLost:
            after_revoke_fenced.set()
            raise
        second_mutation.set()

    async def execute(key, registry):
        if key == "health:c":
            await ownership.run_sync(sdk_request)

    runtime = WorkerRuntime(coordinator_factory=factory, discover=discover,
                            execute=execute, interval=lambda key: 0.02,
                            control_interval=0.01)
    stopping = None
    reader = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    try:
        await runtime.start()
        await eventually(entered.is_set)
        member = runtime.coordinator.member_id
        stopping = asyncio.create_task(runtime.stop())
        await asyncio.sleep(0.3)  # Longer than membership TTL; drain heartbeats must persist.
        assert not stopping.done()
        assert not finished.is_set()
        tm = await reader.time()
        now = tm[0] * 1000 + tm[1] / 1000
        assert await reader.zscore(runtime.coordinator._keys[0], member) > now
        assert await reader.sismember(runtime.coordinator._keys[5], member)
        release.set()
        await stopping
        assert finished.is_set() and after_revoke_fenced.is_set()
        assert not second_mutation.is_set()
        assert await reader.zscore(runtime.coordinator._keys[0], member) is None
        assert not runtime.domain_tasks
    finally:
        release.set()
        if stopping is not None:
            await stopping
        else:
            await runtime.stop()
        await reader.close()


async def test_native_rejoin_does_not_overlap_a_revoked_keys_draining_sdk_thread():
    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    starts = []
    overlaps = []
    server = fakeredis.FakeServer()

    def factory():
        return ownership.Coordinator(
            fakeredis.aioredis.FakeRedis(server=server, decode_responses=True),
            lease_seconds=0.3, registry_seconds=2, startup_seconds=0)

    async def discover():
        return ["health:c"]

    def held_send():
        ownership.checkpoint_sync()
        entered.set()
        try:
            assert release.wait(3)
        finally:
            exited.set()

    async def execute(key, registry):
        if key != "health:c":
            return
        starts.append(key)
        if len(starts) == 1:
            await ownership.run_sync(held_send)
        elif not exited.is_set():
            overlaps.append(key)

    runtime = WorkerRuntime(coordinator_factory=factory, discover=discover,
                            execute=execute, interval=lambda key: 0.02,
                            control_interval=0.01)
    try:
        await runtime.start()
        await eventually(entered.is_set)
        old_member = runtime.coordinator.member_id
        runtime.coordinator.revoke_local()
        await eventually(lambda: runtime.coordinator.member_id != old_member)
        await asyncio.sleep(0.15)
        assert len(starts) == 1
        assert not overlaps
        release.set()
        await eventually(lambda: len(starts) >= 2)
        assert not overlaps
    finally:
        release.set()
        await runtime.stop()


async def test_native_registry_expiry_does_not_overlap_a_draining_sdk_thread():
    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    republish = asyncio.Event()
    starts = []
    discoveries = []
    server = fakeredis.FakeServer()

    def factory():
        return ownership.Coordinator(
            fakeredis.aioredis.FakeRedis(server=server, decode_responses=True),
            lease_seconds=1, registry_seconds=0.12, startup_seconds=0)

    async def discover():
        if discoveries:
            await republish.wait()
        discoveries.append(True)
        return ["health:c"]

    def held_send():
        ownership.checkpoint_sync()
        entered.set()
        try:
            assert release.wait(3)
        finally:
            exited.set()

    async def execute(key, registry):
        if key != "health:c":
            return
        starts.append(key)
        if len(starts) == 1:
            await ownership.run_sync(held_send)
        else:
            assert exited.is_set(), "New target work overlapped the old SDK thread"

    runtime = WorkerRuntime(coordinator_factory=factory, discover=discover,
                            execute=execute, interval=lambda key: 0.02,
                            control_interval=0.01)
    try:
        await runtime.start()
        await eventually(entered.is_set)
        original = runtime.coordinator._leases["health:c"]
        await asyncio.sleep(0.2)  # Redis TIME registry expiry, no local manual revoke.
        assert original.revoked
        assert len(starts) == 1
        republish.set()
        await eventually(lambda: "health:c" in runtime.coordinator._leases)
        replacement = runtime.coordinator._leases["health:c"]
        assert replacement.token == original.token  # Reservation stays on this live process.
        assert replacement is not original
        assert original.revoked
        await asyncio.sleep(0.08)
        assert len(starts) == 1
        release.set()
        await eventually(lambda: len(starts) >= 2)
    finally:
        republish.set()
        release.set()
        await runtime.stop()
