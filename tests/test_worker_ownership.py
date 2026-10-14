"""Native Lua protocol contracts. Evidence remains test-defined until executed."""
from __future__ import annotations

import asyncio
import json
import threading
import time

import fakeredis
import fakeredis.aioredis
import pytest

from drover.services import worker_ownership as ownership


async def workers(count=3, *, lease_seconds=3, registry_seconds=3):
    server = fakeredis.FakeServer()
    result = [ownership.Coordinator(
        fakeredis.aioredis.FakeRedis(server=server, decode_responses=True),
        lease_seconds=lease_seconds, registry_seconds=registry_seconds, startup_seconds=0,
    ) for _ in range(count)]
    for coordinator in result:
        coordinator._test_server = server
        await coordinator.heartbeat()
    return result


async def test_staggered_initial_members_settle_before_balanced_role_assignment():
    server = fakeredis.FakeServer()
    coordinators = [ownership.Coordinator(
        fakeredis.aioredis.FakeRedis(server=server, decode_responses=True),
        startup_seconds=0.15,
    ) for _ in range(3)]
    await coordinators[0].heartbeat()
    assert await coordinators[0].assignments() == {}
    await asyncio.sleep(0.02)
    await coordinators[1].heartbeat()
    assert await coordinators[1].assignments() == {}
    await coordinators[2].heartbeat()
    await asyncio.sleep(0.16)
    assigned = await snapshot(coordinators)
    assert set(assigned) == set(ownership.ROLES)
    assert {owner for owner, _ in assigned.values()} == {0, 1, 2}


async def snapshot(coordinators):
    maps = [await coordinator.assignments() for coordinator in coordinators]
    flattened = {}
    for index, leases in enumerate(maps):
        for key, lease in leases.items():
            assert key not in flattened, f"duplicate owner for {key}"
            flattened[key] = (index, lease)
    return flattened


async def publish(coordinators, targets):
    initial = await snapshot(coordinators)
    leader_index, leader = initial["discovery"]
    await coordinators[leader_index].publish(targets, leader)
    return await snapshot(coordinators)


async def test_three_live_workers_balance_roles_and_targets_without_owner_churn():
    coordinators = await workers()
    keys = [f"{kind}:cluster-{n}" for kind in ("health", "stampede") for n in range(7)]
    before = await publish(coordinators, keys + ["reconcile:cluster-0"])
    assert set(before) == set(keys) | set(ownership.ROLES)
    counts = [sum(owner == index for owner, _ in before.values()) for index in range(3)]
    assert min(counts) > 0 and max(counts) - min(counts) <= 1
    assert "reconcile:cluster-0" in await coordinators[0].registry()
    newcomer = ownership.Coordinator(fakeredis.aioredis.FakeRedis(
        server=coordinators[0]._test_server, decode_responses=True), startup_seconds=0)
    await newcomer.heartbeat()
    after = await snapshot(coordinators + [newcomer])
    assert {key: lease.token for key, (_, lease) in before.items()} == {
        key: lease.token for key, (_, lease) in after.items()}
    assert await newcomer.assignments() == {}
    expanded = await publish(coordinators + [newcomer], keys + ["health:new-cluster"])
    assert expanded["health:new-cluster"][0] == 3


async def test_expired_owner_takeover_fences_stale_validation_and_release():
    coordinators = await workers()
    assigned = await publish(coordinators, [f"health:c{n}" for n in range(6)])
    owner, former = assigned["health:c0"]
    old = coordinators[owner]
    # Force server-side expiry, not local coroutine cancellation or mocked validate.
    await old.redis.zadd(old._keys[0], {old.member_id: 0})
    survivors = [item for index, item in enumerate(coordinators) if index != owner]
    current = await snapshot(survivors)
    replacement = current[former.key][1]
    assert replacement.token != former.token
    with pytest.raises(ownership.OwnershipLost):
        await former.checkpoint()
    assert not await old.release(former)
    await replacement.checkpoint()
    old_member = old.member_id
    with pytest.raises(ownership.OwnershipLost):
        await old.heartbeat()
    await old.heartbeat()
    assert old.member_id != old_member
    assert not await old.release(former)
    await replacement.checkpoint()


async def test_drain_revokes_local_work_and_hands_off_only_owned_incarnation():
    coordinators = await workers()
    before = await publish(coordinators, ["health:one", "stampede:one"])
    owner, former = before["health:one"]
    draining = coordinators[owner]
    draining.revoke_local()
    with ownership.bound(former), pytest.raises(ownership.OwnershipLost):
        await ownership.checkpoint()
    await draining.close()
    with pytest.raises(ownership.OwnershipLost):
        await draining.heartbeat()
    survivors = [item for index, item in enumerate(coordinators) if index != owner]
    after = await snapshot(survivors)
    assert after[former.key][1].token != former.token
    assert not await draining.release(former)


async def test_registry_requires_current_discovery_leader_and_expires_fail_closed():
    coordinators = await workers(registry_seconds=0.06)
    initial = await snapshot(coordinators)
    leader_index, leader = initial["discovery"]
    other_index, other = initial["recovery"]
    with pytest.raises(ownership.OwnershipLost):
        await coordinators[other_index].publish(["health:forbidden"], other)
    assert await coordinators[leader_index].registry() == []
    assigned = await publish(coordinators, ["health:one", "stampede:one"])
    old_target = assigned["health:one"][1]
    await asyncio.sleep(0.09)
    with pytest.raises(ownership.OwnershipLost):
        await old_target.checkpoint()
    assert await coordinators[leader_index].registry() == []
    assert set(await snapshot(coordinators)) == set(ownership.ROLES)
    await coordinators[leader_index].publish(["health:two"], leader)
    assert "health:two" in await snapshot(coordinators)
    await coordinators[leader_index].redis.zadd(
        coordinators[leader_index]._keys[0], {leader.member_id: 0})
    with pytest.raises(ownership.OwnershipLost):
        await coordinators[leader_index].publish(["health:late"], leader)
    assert "health:late" not in await coordinators[other_index].registry()


async def test_removing_target_immediately_fences_old_lease_before_next_assignment():
    coordinators = await workers()
    assigned = await publish(coordinators, ["health:one"])
    leader_index, leader = assigned["discovery"]
    target = assigned["health:one"][1]
    await coordinators[leader_index].publish([], leader)
    with pytest.raises(ownership.OwnershipLost):
        await target.checkpoint()
    assert set(await snapshot(coordinators)) == set(ownership.ROLES)


async def test_redis_outage_revokes_every_local_lease_and_rejoin_uses_new_incarnation():
    coordinator = (await workers(1))[0]
    leases = await coordinator.assignments()
    member = coordinator.member_id
    coordinator._test_server.connected = False
    with pytest.raises(ownership.OwnershipLost):
        await coordinator.heartbeat()
    assert all(lease.revoked for lease in leases.values())
    coordinator._test_server.connected = True
    await coordinator.heartbeat()
    assert coordinator.member_id != member
    for lease in leases.values():
        with pytest.raises(ownership.OwnershipLost):
            await lease.checkpoint()


async def test_overdue_local_guard_rejects_mutation_even_if_redis_owner_is_live(monkeypatch):
    coordinator = (await workers(1))[0]
    lease = (await coordinator.assignments())["discovery"]
    monkeypatch.setattr(ownership.time, "monotonic", lambda: lease._deadline + 0.001)
    with ownership.bound(lease), pytest.raises(ownership.OwnershipLost):
        ownership.checkpoint_sync()
    with ownership.bound(lease), pytest.raises(ownership.OwnershipLost):
        await ownership.checkpoint()


async def test_unbound_api_mutation_checkpoint_remains_noop():
    await ownership.checkpoint()
    ownership.checkpoint_sync()


async def test_domain_block_cannot_stall_independent_control_heartbeat():
    ready = threading.Event()
    stopping = threading.Event()
    state = {}

    def control():
        async def run():
            client = fakeredis.aioredis.FakeRedis(decode_responses=True)
            coordinator = ownership.Coordinator(client, lease_seconds=0.2, startup_seconds=0)
            state["coordinator"] = coordinator
            await coordinator.heartbeat()
            state["lease"] = (await coordinator.assignments())["discovery"]
            state["heartbeats"] = 0
            ready.set()
            try:
                while not stopping.is_set():
                    await coordinator.heartbeat()
                    await coordinator.assignments()
                    state["heartbeats"] += 1
                    await asyncio.sleep(0.01)
            finally:
                await coordinator.close()
                await client.close()
        asyncio.run(run())

    thread = threading.Thread(target=control, daemon=True)
    thread.start()
    assert await asyncio.to_thread(ready.wait, 2)
    try:
        lease = state["lease"]
        before = state["heartbeats"]
        # Deliberately stall this domain event loop beyond the membership TTL.
        time.sleep(0.3)
        assert state["heartbeats"] >= before + 5
        with ownership.bound(lease):
            await ownership.checkpoint()
            await asyncio.to_thread(ownership.checkpoint_sync)
        state["coordinator"].revoke_local()
        with ownership.bound(lease), pytest.raises(ownership.OwnershipLost):
            await asyncio.to_thread(ownership.checkpoint_sync)
    finally:
        stopping.set()
        await asyncio.to_thread(thread.join, 2)
        assert not thread.is_alive()


async def test_cancelled_sdk_thread_is_drained_even_after_repeat_cancellation():
    entered = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    def sdk_call():
        entered.set()
        assert release.wait(2)
        completed.set()

    task = asyncio.create_task(ownership.run_sync(sdk_call))
    assert await asyncio.to_thread(entered.wait, 1)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert completed.is_set()


async def test_registry_never_contains_cluster_credentials():
    coordinators = await workers(1)
    leader = (await coordinators[0].assignments())["discovery"]
    with pytest.raises(ValueError):
        await coordinators[0].publish([{"id": "c", "password": "secret"}], leader)
    await coordinators[0].publish(["health:c", "reconcile:c"], leader)
    raw = await coordinators[0].redis.get(coordinators[0]._keys[2])
    assert json.loads(raw)["targets"] == ["health:c", "reconcile:c"]


async def test_draining_member_keeps_active_leases_but_cannot_receive_new_work():
    coordinators = await workers(3, lease_seconds=0.2, registry_seconds=2)
    assigned = await publish(coordinators, ["health:one"])
    leader_index, leader = assigned["discovery"]
    owner, target = assigned["health:one"]
    if owner == leader_index:
        owner, target = assigned["recovery"]
    draining = coordinators[owner]
    member = draining.member_id
    await draining.begin_drain()
    try:
        await asyncio.sleep(0.25)
        survivors = [item for index, item in enumerate(coordinators) if index != owner]
        for coordinator in survivors:
            # Overdue local membership rejoins with a fresh incarnation.
            await coordinator.heartbeat()
        live = await snapshot(survivors)
        assert target.key not in live  # Existing draining lease is not stolen.
        new_leader_index, new_leader = live["discovery"]
        await survivors[new_leader_index].publish(["health:one", "health:new"], new_leader)
        newer = await snapshot(survivors)
        assert "health:new" in newer
        assert newer["health:new"][1].member_id != member
        with pytest.raises(ownership.OwnershipLost):
            await target.checkpoint()
    finally:
        await draining.close()


async def test_native_time_membership_expiry_takeover_without_manual_score_change():
    coordinators = await workers(3, lease_seconds=0.12, registry_seconds=2)
    before = await publish(coordinators, ["health:one", "stampede:one"])
    owner, former = before["health:one"]
    survivors = [item for index, item in enumerate(coordinators) if index != owner]
    for _ in range(8):
        await asyncio.sleep(0.02)
        for coordinator in survivors:
            await coordinator.heartbeat()
            await coordinator.assignments()
    after = await snapshot(survivors)
    assert after[former.key][1].token != former.token
    assert await coordinators[owner].redis.eval(
        ownership._PROTOCOL, len(coordinators[owner]._keys),
        *coordinators[owner]._keys, "validate", former.member_id,
        former.key, former.token) == 0
    assert not await coordinators[owner].release(former)
    await after[former.key][1].checkpoint()


async def test_same_member_stale_token_cannot_validate_or_release_replacement():
    coordinator = (await workers(1))[0]
    former = (await coordinator.assignments())["recovery"]
    assert await coordinator.release(former)
    replacement = (await coordinator.assignments())["recovery"]
    assert replacement.member_id == former.member_id
    assert replacement.token != former.token
    assert not await coordinator.release(former)
    assert await coordinator.redis.eval(
        ownership._PROTOCOL, len(coordinator._keys), *coordinator._keys,
        "validate", former.member_id, former.key, former.token) == 0
    await replacement.checkpoint()


async def test_two_workers_keep_draining_reservation_across_registry_expiry_and_republish():
    coordinators = await workers(2, lease_seconds=0.3, registry_seconds=0.05)
    targets = ["health:one", "health:two"]
    before = await publish(coordinators, targets)
    leader_index, leader = before["discovery"]
    key, (owner, former) = next((key, value) for key, value in before.items()
                               if key.startswith("health:") and value[0] != leader_index)
    draining = coordinators[owner]
    survivor = coordinators[leader_index]
    await draining.begin_drain()
    try:
        await asyncio.sleep(0.08)
        await survivor.heartbeat()
        assert key not in await survivor.assignments()
        retained = json.loads(await survivor.redis.hget(survivor._keys[1], key))
        assert retained == {"owner": former.member_id, "token": former.token}
        await survivor.publish([], leader)
        assert await survivor.redis.hget(survivor._keys[1], key) is not None
        await survivor.publish(targets, leader)
        assert key not in await survivor.assignments()
        with pytest.raises(ownership.OwnershipLost):
            await former.checkpoint()
    finally:
        await draining.close()
    replacement = (await survivor.assignments())[key]
    assert replacement.token != former.token
    assert not await draining.release(former)
    await replacement.checkpoint()


async def test_live_owner_reservation_survives_unpublication_without_lease_revival():
    coordinators = await workers(2, registry_seconds=0.05)
    targets = ["health:one", "health:two"]
    before = await publish(coordinators, targets)
    former_owner, former = before["health:two"]
    await asyncio.sleep(0.08)
    assert "health:two" not in await snapshot(coordinators)
    assert former.revoked
    leader_owner, leader = before["discovery"]
    await coordinators[leader_owner].publish(targets, leader)
    after = await snapshot(coordinators)
    current_owner, current = after["health:two"]
    assert current_owner == former_owner
    assert current.token == former.token
    assert current is not former
    with pytest.raises(ownership.OwnershipLost):
        await former.checkpoint()
    await current.checkpoint()
