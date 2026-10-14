"""Ephemeral periodic-work ownership, independent of the durable SQL job queue.

Only opaque process incarnations and cluster IDs enter Redis. Every operation uses
Redis TIME in one Lua transaction. The caller owns the control loop and its Redis
client; domain work never runs on that loop. A local monotonic deadline also fences
work when the control loop itself stops making progress.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import json
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Any


class OwnershipLost(RuntimeError):
    """The periodic-work incarnation can no longer begin a mutation."""


ROLES = ("discovery", "recovery", "reconciliation", "delegation")

# KEYS: members, assignments, registry, generation, cohort ready-at, draining.
# Assignment expiry is the owning member's server-time expiry, not a client clock.
_PROTOCOL = r"""
local tm = redis.call('TIME')
local now = tonumber(tm[1]) * 1000 + tonumber(tm[2]) / 1000
local member = ARGV[2]
local function live(owner)
    local expiry = redis.call('ZSCORE', KEYS[1], owner)
    return expiry and tonumber(expiry) > now
end
local function eligible(owner)
    return live(owner) and redis.call('SISMEMBER', KEYS[6], owner) == 0
end
local function registry()
    local raw = redis.call('GET', KEYS[3])
    if not raw then return nil end
    local value = cjson.decode(raw)
    if tonumber(value.expires) <= now then return nil end
    return value.targets
end
local function wanted(key, targets)
    if key == 'discovery' or key == 'recovery' or key == 'reconciliation'
       or key == 'delegation' then return true end
    if not targets then return false end
    for _, target in ipairs(targets) do
        if target == key then return true end
    end
    return false
end
local function valid(key, token)
    if not eligible(member) then return false end
    if key == 'reconciliation' and not registry() then return false end
    local raw = redis.call('HGET', KEYS[2], key)
    if not raw then return false end
    local value = cjson.decode(raw)
    return value.owner == member and value.token == token
       and wanted(key, registry())
end
local action = ARGV[1]
if action == 'heartbeat' then
    if ARGV[4] ~= 'join' and not live(member) then return 0 end
    if redis.call('ZCOUNT', KEYS[1], '(' .. tostring(now), '+inf') == 0 then
        redis.call('SET', KEYS[5], tostring(now + tonumber(ARGV[5])))
    end
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), member)
    return 1
elseif action == 'validate' then
    if valid(ARGV[3], ARGV[4]) then return 1 else return 0 end
elseif action == 'release' then
    local raw = redis.call('HGET', KEYS[2], ARGV[3])
    if raw then
        local value = cjson.decode(raw)
        if value.owner == member and value.token == ARGV[4] then
            return redis.call('HDEL', KEYS[2], ARGV[3])
        end
    end
    return 0
elseif action == 'drain' then
    if not live(member) then return 0 end
    redis.call('SADD', KEYS[6], member)
    return 1
elseif action == 'leave' then
    redis.call('ZREM', KEYS[1], member)
    redis.call('SREM', KEYS[6], member)
    return 1
elseif action == 'publish' then
    if not valid('discovery', ARGV[3]) then return 0 end
    local targets = cjson.decode(ARGV[4])
    redis.call('SET', KEYS[3], cjson.encode({targets=targets,
        expires=now + tonumber(ARGV[5])}), 'PX', ARGV[5])
    return 1
elseif action == 'registry' then
    if not live(member) then return false end
    local targets = registry()
    if not targets then return false end
    return cjson.encode(targets)
elseif action == 'assign' then
    if not eligible(member) then return false end
    local ready = tonumber(redis.call('GET', KEYS[5]) or '0')
    if now < ready then return '{}' end
    local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
    for _, owner in ipairs(expired) do redis.call('SREM', KEYS[6], owner) end
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
    local members = {}
    for _, owner in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
        if eligible(owner) then table.insert(members, owner) end
    end
    table.sort(members)
    local loads = {}
    for _, owner in ipairs(members) do loads[owner] = 0 end
    local targets = registry()
    local all = redis.call('HGETALL', KEYS[2])
    for i = 1, #all, 2 do
        local value = cjson.decode(all[i+1])
        if not live(value.owner) then
            redis.call('HDEL', KEYS[2], all[i])
        elseif loads[value.owner] ~= nil and wanted(all[i], targets) then
            loads[value.owner] = loads[value.owner] + 1
        end
    end
    local desired = {'discovery', 'recovery', 'reconciliation', 'delegation'}
    if targets then
        for _, target in ipairs(targets) do
            if string.sub(target, 1, 7) == 'health:' or
               string.sub(target, 1, 9) == 'stampede:' then
                table.insert(desired, target)
            end
        end
    end
    for _, key in ipairs(desired) do
        if redis.call('HEXISTS', KEYS[2], key) == 0 then
            local owner = members[1]
            for _, candidate in ipairs(members) do
                if loads[candidate] < loads[owner] then owner = candidate end
            end
            local token = tostring(redis.call('INCR', KEYS[4]))
            redis.call('HSET', KEYS[2], key, cjson.encode({owner=owner, token=token}))
            loads[owner] = loads[owner] + 1
        end
    end
    local result = {}
    all = redis.call('HGETALL', KEYS[2])
    for i = 1, #all, 2 do
        local value = cjson.decode(all[i+1])
        if value.owner == member and wanted(all[i], targets) then
            result[all[i]] = value.token
        end
    end
    if next(result) == nil then return '{}' end
    return cjson.encode(result)
end
return redis.error_reply('unknown ownership action')
"""


class Lease:
    def __init__(self, coordinator: Coordinator, key: str, token: str, deadline: float):
        self.coordinator = coordinator
        self.key = key
        self.token = token
        self.member_id = coordinator.member_id
        self._deadline = deadline
        self._lock = threading.Lock()
        self._revoked = threading.Event()

    @property
    def revoked(self) -> bool:
        with self._lock:
            overdue = time.monotonic() >= self._deadline
        return self._revoked.is_set() or overdue

    def revoke(self) -> None:
        self._revoked.set()

    def _remaining(self) -> float:
        with self._lock:
            remaining = self._deadline - time.monotonic()
        if self._revoked.is_set() or remaining <= 0:
            self.revoke()
            raise OwnershipLost(f"Periodic ownership lost: {self.key}")
        return remaining

    def _renew(self, deadline: float) -> None:
        with self._lock:
            # Never revive a coroutine's overdue lease object.
            if time.monotonic() >= self._deadline:
                self.revoke()
            elif not self._revoked.is_set():
                self._deadline = deadline

    async def checkpoint(self) -> None:
        remaining = self._remaining()
        loop = self.coordinator.loop
        if loop is None or not loop.is_running():
            self.revoke()
            raise OwnershipLost("Ownership control loop unavailable")
        if asyncio.get_running_loop() is loop:
            await self.coordinator._validate(self)
        else:
            future = asyncio.run_coroutine_threadsafe(self.coordinator._validate(self), loop)
            try:
                await asyncio.wait_for(asyncio.wrap_future(future), remaining)
            except (TimeoutError, concurrent.futures.CancelledError) as exc:
                future.cancel()
                self.revoke()
                raise OwnershipLost("Ownership checkpoint overdue") from exc
        self._remaining()

    def checkpoint_sync(self) -> None:
        remaining = self._remaining()
        loop = self.coordinator.loop
        if loop is None or not loop.is_running():
            self.revoke()
            raise OwnershipLost("Ownership control loop unavailable")
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if current_loop is loop:
            raise OwnershipLost("Synchronous checkpoint cannot block control loop")
        future = asyncio.run_coroutine_threadsafe(self.coordinator._validate(self), loop)
        try:
            future.result(timeout=remaining)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            self.revoke()
            raise OwnershipLost("Ownership checkpoint overdue") from exc
        self._remaining()


class Coordinator:
    """Control-loop-only Redis coordinator; SDK work belongs on another loop."""

    def __init__(self, redis: Any, *, namespace: str = "drover:worker:ownership",
                 lease_seconds: float = 15.0, registry_seconds: float = 45.0,
                 startup_seconds: float = 3.0,
                 close_client: Callable[[], Awaitable[None]] | None = None):
        if lease_seconds <= 0 or registry_seconds <= 0 or startup_seconds < 0:
            raise ValueError("Ownership lifetimes must be positive; startup nonnegative")
        self.redis = redis
        # The owner of a Sentinel-selected client also owns its Sentinel pools.
        self._close_client = close_client
        self.member_id = uuid.uuid4().hex
        self._first_join = True
        self.lease_seconds = lease_seconds
        self.registry_seconds = registry_seconds
        self.startup_seconds = startup_seconds
        self._keys = [f"{{{namespace}}}:{suffix}" for suffix in
                      ("members", "assignments", "registry", "generation", "ready", "draining")]
        self.loop: asyncio.AbstractEventLoop | None = None
        self._leases: dict[str, Lease] = {}
        self._issued: dict[str, Lease] = {}
        self._lock = threading.Lock()
        self._joined = False
        self._deadline = 0.0
        self._closed = threading.Event()
        self._drain_task: asyncio.Task | None = None

    def _control_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = loop
        elif self.loop is not loop:
            raise RuntimeError("Coordinator Redis methods require its control loop")

    async def _call(self, action: str, *args: Any) -> Any:
        self._control_loop()
        try:
            return await self.redis.eval(_PROTOCOL, len(self._keys), *self._keys,
                                         action, self.member_id, *args)
        except Exception as exc:
            self.revoke_local()
            raise OwnershipLost("Redis ownership unavailable") from exc

    def revoke_local(self) -> None:
        with self._lock:
            for lease in self._leases.values():
                lease.revoke()
            self._leases.clear()
        self._joined = False
        self._deadline = 0.0

    async def heartbeat(self) -> None:
        self._control_loop()
        if self._closed.is_set():
            raise OwnershipLost("Coordinator is draining")
        started = time.monotonic()
        if self._joined and started >= self._deadline:
            self.revoke_local()
        if not self._joined:
            # Each rejoin is a new incarnation: expired tokens never resurrect.
            if not self._first_join:
                self.member_id = uuid.uuid4().hex
            self._first_join = False
        joined = self._joined
        result = await self._call("heartbeat", int(self.lease_seconds * 1000),
                                  "renew" if joined else "join",
                                  int(self.startup_seconds * 1000))
        if not result:
            self.revoke_local()
            raise OwnershipLost("Ownership membership expired")
        self._joined = True
        self._deadline = started + self.lease_seconds
        if time.monotonic() >= self._deadline:
            self.revoke_local()
            raise OwnershipLost("Ownership heartbeat overdue")

    async def assignments(self) -> dict[str, Lease]:
        if self._closed.is_set() or not self._joined or time.monotonic() >= self._deadline:
            self.revoke_local()
            raise OwnershipLost("Ownership membership unavailable")
        raw = await self._call("assign")
        if not raw:
            self.revoke_local()
            raise OwnershipLost("Ownership membership expired")
        assigned = json.loads(raw)
        with self._lock:
            for key, lease in list(self._leases.items()):
                if assigned.get(key) != lease.token or lease.revoked:
                    lease.revoke()
                    del self._leases[key]
            for key, token in assigned.items():
                if key in self._leases:
                    self._leases[key]._renew(self._deadline)
                else:
                    self._leases[key] = Lease(self, key, token, self._deadline)
            self._issued = dict(self._leases)
            return dict(self._leases)

    async def publish(self, targets: list[str], leader: Lease) -> None:
        await self._validate(leader)
        if leader.key != "discovery":
            raise OwnershipLost("Only discovery may publish the registry")
        # Registry stores bounded non-secret identifiers, never credential payloads.
        if any(not isinstance(key, str) or not key.partition(":")[2]
               or key.partition(":")[0] not in {"health", "stampede", "reconcile"}
               for key in targets):
            raise ValueError("Registry requires native target keys")
        result = await self._call("publish", leader.token,
                                  json.dumps(sorted(set(targets))),
                                  int(self.registry_seconds * 1000))
        if not result:
            leader.revoke()
            raise OwnershipLost("Discovery publication ownership lost")

    async def registry(self) -> list[str]:
        raw = await self._call("registry")
        return list(json.loads(raw)) if raw else []

    async def _validate(self, lease: Lease) -> None:
        lease._remaining()
        if self._closed.is_set() or lease.member_id != self.member_id:
            lease.revoke()
            raise OwnershipLost("Ownership incarnation revoked")
        if not await self._call("validate", lease.key, lease.token):
            lease.revoke()
            raise OwnershipLost(f"Periodic ownership lost: {lease.key}")
        lease._remaining()

    async def release(self, lease: Lease) -> bool:
        lease.revoke()
        # Release uses the captured incarnation, not a possibly rejoined member.
        self._control_loop()
        try:
            result = await self.redis.eval(_PROTOCOL, len(self._keys), *self._keys,
                                           "release", lease.member_id,
                                           lease.key, lease.token)
        except Exception as exc:
            self.revoke_local()
            raise OwnershipLost("Redis ownership unavailable") from exc
        return bool(result)

    async def begin_drain(self) -> None:
        """Exclude new assignments but retain old ones until actual work drains."""
        self._closed.set()
        self.revoke_local()
        if not await self._call("drain"):
            return
        if self._drain_task is None:
            self._drain_task = asyncio.create_task(self._keep_draining())

    async def _keep_draining(self) -> None:
        try:
            while True:
                result = await self._call("heartbeat", int(self.lease_seconds * 1000),
                                          "renew", int(self.startup_seconds * 1000))
                if not result:
                    return
                await asyncio.sleep(self.lease_seconds / 3)
        except OwnershipLost:
            # Unknown Redis state stays revoked; surviving owners wait for expiry.
            return

    async def close(self) -> None:
        self._closed.set()
        with self._lock:
            issued = tuple(self._issued.values())
            self._issued.clear()
        self.revoke_local()
        if self._drain_task is not None:
            self._drain_task.cancel()
            try:
                await self._drain_task
            except asyncio.CancelledError:
                pass
            self._drain_task = None
        for lease in issued:
            await self.release(lease)
        await self._call("leave")

    async def close_client(self) -> None:
        """Close the control-thread Redis resources after ownership is released."""
        await (self._close_client or self.redis.close)()


_current: contextvars.ContextVar[Lease | None] = contextvars.ContextVar(
    "drover_periodic_ownership", default=None)


@contextmanager
def bound(lease: Lease) -> Iterator[None]:
    token = _current.set(lease)
    try:
        yield
    finally:
        _current.reset(token)


async def checkpoint() -> None:
    lease = _current.get()
    if lease is not None:
        await lease.checkpoint()


def checkpoint_sync() -> None:
    lease = _current.get()
    if lease is not None:
        lease.checkpoint_sync()


async def run_sync(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Drain the actual thread before relinquishing its mutation authority."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        from drover.services.execution_safety import revoke_current

        revoke_current()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        # Retrieve a drained failure without replacing the caller's cancellation.
        if task.done() and not task.cancelled():
            task.exception()
        raise
