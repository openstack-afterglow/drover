"""Three native worker processes, real MariaDB/Redis, synthetic HTTP provider.

Run only against an EMPTY disposable loopback MariaDB database named
'drover_ownership_smoke_<suffix>'. Supply its async SQLAlchemy URL in
DROVER_OWNERSHIP_SMOKE_DATABASE_URL, never on argv. This script starts its own
loopback redis-server, preserves SQL rows for inspection, and stops only its own
processes. It does not create a deployment service or contact a cloud.

Recipe (after creating the disposable database on an existing MariaDB service):
  uv run --extra service --extra dev python scripts/worker_ownership_smoke.py

The native SQL claims/heartbeat/non-replay guards and native WorkerRuntime run
unchanged. Only the domain/provider dispatch is synthetic; this is not evidence
of Keystone authorization, guest readiness, or production acceptance.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import contextlib
import json
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DATABASE_ENV = "DROVER_OWNERSHIP_SMOKE_DATABASE_URL"
LEASE_SECONDS = 3


def http(origin, path, body=None):
    request = Request(origin + path,
                      data=json.dumps(body).encode() if body is not None else None,
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=15) as response:
        return json.load(response)


class Provider:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = collections.Counter()
        self.job_counts = collections.Counter()
        self.owners = {}
        self.members = {}
        self.active = {}
        self.duplicates = []
        self.block = None
        self.block_entered = threading.Event()
        self.hold = None
        self.hold_entered = threading.Event()
        self.hold_release = threading.Event()
        self.crash_entered = threading.Event()
        self.crash_worker = None
        self.crash_release = threading.Event()
        self.slow_entered = threading.Event()
        self.slow_release = threading.Event()
        self.events = []

    def server(self):
        state = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def reply(self, value):
                encoded = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                    self.wfile.write(encoded)

            def do_GET(self):
                self.reply({"ready": True})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                worker, key = body["worker"], body["key"]
                if self.path == "/command":
                    with state.lock:
                        blocked = state.block == (worker, key)
                        held = state.hold == (worker, key)
                        if blocked:
                            state.block = None
                        if held:
                            state.hold = None
                    self.reply({"block": blocked, "hold": held})
                    return
                if self.path == "/blocked":
                    state.block_entered.set()
                    self.reply({"recorded": True})
                    return
                if self.path == "/job":
                    mode = body["mode"]
                    with state.lock:
                        state.job_counts[key] += 1
                        state.events.append((time.monotonic(), worker, key, "job"))
                    if mode == "unknown":
                        # Accepted side effect, then lost response; no retry-safe error.
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    if mode == "crash":
                        state.crash_worker = worker
                        state.crash_entered.set()
                        state.crash_release.wait(12)
                    if mode == "slow":
                        state.slow_entered.set()
                        state.slow_release.wait(12)
                    self.reply({"accepted": True})
                    return
                with state.lock:
                    if key in state.active and state.active[key] != worker:
                        state.duplicates.append((key, state.active[key], worker))
                    state.active[key] = worker
                    state.counts[(worker, key)] += 1
                    state.owners[key] = worker
                    state.members[body["member"]] = worker
                    state.events.append((time.monotonic(), worker, key, "periodic"))
                try:
                    if self.path == "/hold":
                        state.hold_entered.set()
                        state.hold_release.wait(12)
                    else:
                        time.sleep(0.035)
                    self.reply({"observed": True})
                finally:
                    with state.lock:
                        state.active.pop(key, None)

        class Server(ThreadingHTTPServer):
            # Three replicas poll many targets; the default backlog of 5 resets
            # loopback connections before the synthetic provider can accept them.
            request_queue_size = 256
            daemon_threads = True

        server = Server(("127.0.0.1", 0), Handler)
        return server


async def replica(args):
    import redis.asyncio as redis
    from keystoneauth1 import session as ks_session

    from drover import db
    from drover.services import execution_safety, jobs, worker_ownership
    from drover.worker import WorkerRuntime, discover_targets

    database_url = os.environ[DATABASE_ENV]
    os.environ["DATABASE_URL"] = database_url
    os.environ["REDIS_URL"] = args.redis_url
    os.environ["DROVER_STAMPEDE_ENABLED"] = "true"
    db.init_db(database_url)
    jobs._LEASE_SECONDS = LEASE_SECONDS
    stopped = asyncio.Event()
    runtime = None

    def coordinator():
        client = redis.from_url(args.redis_url, decode_responses=True,
                                socket_timeout=0.4, socket_connect_timeout=0.4)
        return worker_ownership.Coordinator(
            client, namespace=args.namespace, lease_seconds=LEASE_SECONDS,
            registry_seconds=2, startup_seconds=2)

    async def execute(key, registry):
        await worker_ownership.checkpoint()
        command = await worker_ownership.run_sync(http, args.provider, "/command",
                                                  {"worker": args.replica, "key": key})
        if command["block"]:
            await worker_ownership.run_sync(http, args.provider, "/blocked",
                                            {"worker": args.replica, "key": key})
            # Actual domain-loop blocking longer than the membership TTL.
            time.sleep(LEASE_SECONDS + 0.5)
        body = {"worker": args.replica, "key": key,
                "member": runtime.coordinator.member_id}

        def send():
            worker_ownership.checkpoint_sync()
            http(args.provider, "/hold" if command["hold"] else "/periodic", body)
            # A revoked thread must not begin a second mutation after its first reply.
            worker_ownership.checkpoint_sync()

        await worker_ownership.run_sync(send)

    async def dispatch(kind, payload, cluster_id, project_id, operation_id=None):
        # Keep _execute_job_direct and its authority binding, SQL claims, attempt
        # fence, execution-start marker and actual native transport send guard.
        if kind == "reconcile":
            session = ks_session.Session()
            try:
                execution_safety.install_session_guard(session)
                await worker_ownership.run_sync(
                    session.request, args.provider + "/ready", "GET", authenticated=False)
            finally:
                session.session.close()
            if payload.get("retry_read"):
                payload["retry_read"] = False
                # First attempt is selected from the native persisted attempt count.
                from sqlalchemy import select

                from drover.models.orm import DroverJob
                async with db.get_session_factory()() as sql:
                    row = (await sql.scalars(select(DroverJob).where(
                        DroverJob.cluster_id == cluster_id))).one()
                    if row.attempts == 1:
                        raise RuntimeError("Synthetic read-only transport interruption")
            return
        assert kind == "stampede_provision"
        session = ks_session.Session()
        try:
            execution_safety.install_session_guard(session)
            await worker_ownership.run_sync(
                session.request, args.provider + "/job", "POST", authenticated=False,
                json={"worker": args.replica, "key": payload["smoke_id"],
                      "mode": payload["mode"]})
        finally:
            session.session.close()

    jobs._dispatch_job = dispatch
    runtime = WorkerRuntime(coordinator_factory=coordinator, discover=discover_targets,
                            execute=execute, interval=lambda key: 0.15,
                            control_interval=0.08)
    loop = asyncio.get_running_loop()

    def request_stop(_signal, _frame):
        execution_safety.revoke_all_local()
        runtime.request_stop()
        loop.call_soon_threadsafe(stopped.set)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, request_stop)

    async def consume():
        while not stopped.is_set():
            try:
                await jobs.process_one_job()
            except Exception:
                # Like drover.worker._jobs_worker_loop, retry transient SQL
                # failures such as InnoDB deadlock 1213 instead of exiting.
                logging.getLogger("drover.smoke").warning("SQL job consumer retry", exc_info=True)
                await asyncio.sleep(0.2)
            await asyncio.sleep(0.04)

    await runtime.start()
    consumer = asyncio.create_task(consume())
    try:
        await stopped.wait()
    finally:
        consumer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consumer
        await runtime.stop()
        await db.close_db()


async def wait_until(predicate, label, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError(f"Timed out: {label}")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def smoke(args):
    import redis
    from sqlalchemy import inspect
    from sqlalchemy.engine import make_url

    from drover import db
    from drover.models.orm import DroverJob, K3sCluster
    from drover.services import jobs

    url = make_url(os.environ.get(DATABASE_ENV, ""))
    if (url.host not in {"127.0.0.1", "localhost", "::1"}
            or url.drivername not in {"mysql+aiomysql", "mysql+asyncmy"}
            or not re.fullmatch(r"drover_ownership_smoke_[A-Za-z0-9_]+", url.database or "")):
        raise ValueError("Require disposable loopback MariaDB URL in " + DATABASE_ENV)
    executable = shutil.which("redis-server")
    if not executable:
        raise RuntimeError("redis-server must already be installed for this smoke")
    db.init_db(os.environ[DATABASE_ENV])
    engine = db._engine
    async with engine.begin() as connection:
        tables = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        if tables:
            raise ValueError("Smoke requires an empty disposable database; preserves all resulting rows")
        await connection.run_sync(db.Base.metadata.create_all)
    factory = db.get_session_factory()
    clusters = [str(uuid.uuid4()) for _ in range(9)]
    async with factory() as session, session.begin():
        session.add_all([K3sCluster(id=cluster, project_id="synthetic-project",
                        name=f"ownership-{index}", status="ACTIVE", stampede_enabled=True)
                        for index, cluster in enumerate(clusters)])

    async def seed(mode, kind="stampede_provision"):
        cluster = str(uuid.uuid4())
        async with factory() as session, session.begin():
            session.add(K3sCluster(id=cluster, project_id="synthetic-project",
                                  name="job-" + mode, status="ACTIVE"))
        smoke_id = str(uuid.uuid4())
        job_id = await jobs.enqueue_job(cluster, "synthetic-project", kind,
                                       {"smoke_id": smoke_id, "mode": mode,
                                        "retry_read": kind == "reconcile"})
        return job_id, smoke_id

    async def row(job_id):
        async with factory() as session:
            result = await session.get(DroverJob, job_id)
            return {"status": result.status, "attempts": result.attempts,
                    "claimed_at": result.claimed_at, "payload": result.payload_json}

    successful = [await seed("success") for _ in range(9)]
    unknown = await seed("unknown")
    read_retry = await seed("read", "reconcile")
    provider = Provider()
    server = provider.server()
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    namespace = "drover:worker:smoke:" + uuid.uuid4().hex
    key_prefix = "{" + namespace + "}:"
    processes = {}
    redis_process = None
    redis_paused = False
    report = {"evidence": "native-loopback-synthetic", "production": False}
    with tempfile.TemporaryDirectory(prefix="drover-worker-smoke-") as directory, contextlib.ExitStack() as logs:
        try:
            port = free_port()
            redis_log = logs.enter_context(open(Path(directory) / "redis.log", "wb"))
            redis_process = subprocess.Popen(
                [executable, "--bind", "127.0.0.1", "--port", str(port),
                 "--save", "", "--appendonly", "no", "--dir", directory],
                stdout=redis_log, stderr=subprocess.STDOUT)
            redis_url = f"redis://127.0.0.1:{port}/0"
            client = redis.Redis.from_url(redis_url, decode_responses=True,
                                          socket_timeout=0.4, socket_connect_timeout=0.4)

            def ping():
                try:
                    return client.ping()
                except redis.RedisError:
                    return False
            await wait_until(ping, "loopback Redis readiness")
            for label in ("one", "two", "three"):
                log = logs.enter_context(open(Path(directory) / (label + ".log"), "wb"))
                processes[label] = subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve()), "--replica", label,
                     "--redis-url", redis_url, "--provider", origin, "--namespace", namespace],
                    cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)

            def assigned():
                return {key: json.loads(value) for key, value in
                        client.hgetall(key_prefix + "assignments").items()}

            await wait_until(lambda: len(assigned()) >= 22, "all roles and native SQL targets")
            initial = assigned()
            counts = collections.Counter(value["owner"] for value in initial.values())
            assert len(counts) == 3 and max(counts.values()) - min(counts.values()) <= 1
            await wait_until(lambda: all(label in provider.owners.values() for label in processes),
                             "all three runtime replicas execute")
            await asyncio.sleep(0.3)
            assert assigned() == initial, "Healthy assignment tokens or owners churned"
            report["initial_owner_counts"] = sorted(counts.values())
            report["sticky_assignment"] = True

            for job_id, smoke_id in successful:
                await wait_until(lambda job_id=job_id: terminal(job_id, row), "successful SQL job")
                assert (await row(job_id))["status"] == "completed"
                assert provider.job_counts[smoke_id] == 1
            await wait_until(lambda: terminal(unknown[0], row), "unknown accepted SQL job")
            unknown_row = await row(unknown[0])
            assert unknown_row["status"] == "failed" and unknown_row["attempts"] == 1
            assert provider.job_counts[unknown[1]] == 1
            await wait_until(lambda: terminal(read_retry[0], row), "read-only retry")
            assert (await row(read_retry[0]))["attempts"] == 2
            report["sql_successful_jobs"] = len(successful)
            report["accepted_lost_reply_no_replay"] = True
            report["read_only_retry"] = True

            slow = await seed("slow")
            await wait_until(provider.slow_entered.is_set, "native SQL heartbeat held provider")
            first_claim = (await row(slow[0]))["claimed_at"]
            await asyncio.sleep(1.4)
            later = await row(slow[0])
            assert later["status"] == "running" and later["attempts"] == 1
            assert later["claimed_at"] > first_claim
            provider.slow_release.set()
            await wait_until(lambda: terminal(slow[0], row), "slow job completion")
            assert provider.job_counts[slow[1]] == 1
            report["sql_heartbeat_during_sdk"] = True

            current = assigned()
            target = next(key for key, value in current.items()
                          if key.startswith("health:")
                          and value["owner"] != current["discovery"]["owner"])
            await wait_until(lambda: target in provider.owners, "target executed")
            blocked_worker = provider.owners[target]
            member = assigned()[target]["owner"]
            with provider.lock:
                provider.block = (blocked_worker, target)
            await wait_until(provider.block_entered.is_set, "domain loop blocking began")
            first_expiry = client.zscore(key_prefix + "members", member)
            await asyncio.sleep(0.6)
            assert client.zscore(key_prefix + "members", member) > first_expiry + 300
            await asyncio.sleep(LEASE_SECONDS + 0.2)
            assert assigned()[target] == initial[target]
            report["independent_control_heartbeat"] = True

            # Revoke and stop while an actual synchronous HTTP request is active.
            with provider.lock:
                provider.hold = (blocked_worker, target)
            await wait_until(provider.hold_entered.is_set, "drainable native runtime thread")
            processes[blocked_worker].send_signal(signal.SIGTERM)
            await asyncio.sleep(0.25)
            assert processes[blocked_worker].poll() is None, "Stopped before actual thread drained"
            assert client.zscore(key_prefix + "members", member) is not None
            provider.hold_release.set()
            await wait_until(lambda: processes[blocked_worker].poll() is not None, "true drain exit")
            assert processes[blocked_worker].returncode == 0
            await wait_until(lambda: assigned().get(target, {}).get("owner") != member,
                             "drained owner handoff")
            report["revocation_true_thread_drain"] = True

            # Kill after the native SQL marker and provider acceptance, before reply.
            crash = await seed("crash")
            await wait_until(provider.crash_entered.is_set, "accepted crash job")
            crash_worker = provider.crash_worker
            before_crash = assigned()
            processes[crash_worker].kill()
            await asyncio.to_thread(processes[crash_worker].wait, 3)
            provider.crash_release.set()
            await wait_until(lambda: terminal(crash[0], row), "ambiguous crash terminalized", 12)
            assert (await row(crash[0]))["status"] == "failed"
            assert (await row(crash[0]))["attempts"] == 1
            assert provider.job_counts[crash[1]] == 1
            await wait_until(lambda: len({value["owner"] for value in assigned().values()}) == 1,
                             "crash periodic-role takeover")
            assert any(assigned()[key]["token"] != value["token"]
                       for key, value in before_crash.items())
            report["hard_crash_takeover"] = True
            report["accepted_then_killed_no_replay"] = True

            # Actual Redis network outage, using only the Redis process we started.
            redis_process.send_signal(signal.SIGSTOP)
            redis_paused = True
            await asyncio.sleep(0.7)
            with provider.lock:
                frozen = len(provider.events)
            await asyncio.sleep(0.5)
            with provider.lock:
                assert len(provider.events) == frozen, "Mutation began during ownership outage"
            redis_process.send_signal(signal.SIGCONT)
            redis_paused = False
            await wait_until(lambda: len(provider.events) > frozen, "fail-closed rejoin recovery")
            report["redis_outage_fail_closed"] = True
            assert not provider.duplicates, provider.duplicates
            report["duplicate_healthy_target_polls"] = 0
            assert all(provider.job_counts[smoke_id] == 1 for _, smoke_id in successful + [unknown, crash])
            report["remaining_native_assignments"] = len(assigned())
            print(json.dumps(report, indent=2, sort_keys=True))
        finally:
            provider.hold_release.set()
            provider.crash_release.set()
            provider.slow_release.set()
            if redis_paused and redis_process is not None:
                redis_process.send_signal(signal.SIGCONT)
            for process in processes.values():
                if process.poll() is None:
                    process.terminate()
            for process in processes.values():
                try:
                    await asyncio.to_thread(process.wait, 8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait, 3)
            if redis_process is not None:
                redis_process.terminate()
                await asyncio.to_thread(redis_process.wait, 5)
            server.shutdown()
            server.server_close()
            server_thread.join(2)
            await db.close_db()


async def terminal(job_id, row):
    return (await row(job_id))["status"] in {"completed", "failed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replica", help=argparse.SUPPRESS)
    parser.add_argument("--redis-url", help=argparse.SUPPRESS)
    parser.add_argument("--provider", help=argparse.SUPPRESS)
    parser.add_argument("--namespace", help=argparse.SUPPRESS)
    args = parser.parse_args()
    asyncio.run(replica(args) if args.replica else smoke(args))


if __name__ == "__main__":
    main()
