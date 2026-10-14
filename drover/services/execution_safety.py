"""Revocable dispatch guards, independent of caller execution authority.

A checkpoint is a pre-send fence, not an atomic transaction with a remote cloud.
Already issued requests can finish after revocation; their outcome is never replayed.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from urllib.parse import urlsplit

from urllib3.util.retry import Retry

_ACTIVE: set[DispatchGuard] = set()


def revoke_all_local() -> None:
    """Signal-safe local revocation before cancellation reaches the domain loop."""
    # Snapshot iteration requires no lock: handlers execute on the main thread
    # and guard registration/unregistration occurs only on that same thread.
    for guard in tuple(_ACTIVE):
        guard.revoke()

class OwnershipLost(RuntimeError):
    """This execution may no longer issue provider mutations."""


class DispatchGuard:
    def __init__(
        self, checkpoint: Callable[[], Awaitable[float | None]], *,
        ownership_checkpoint: Callable[[], Awaitable[float | None]] | None = None,
        transaction_checkpoint: Callable[[object], Awaitable[float | None]] | None = None,
    ):
        self.loop = asyncio.get_running_loop()
        self.thread_id = threading.get_ident()
        self.started = False
        self._deadline: float | None = None
        self._checkpoint = checkpoint
        self._ownership_checkpoint = ownership_checkpoint or checkpoint
        self._transaction_checkpoint = transaction_checkpoint
        self._revoked = threading.Event()
        self._lock = threading.Lock()
        self._active = 0
        self._idle = asyncio.Event()
        self._idle.set()
        _ACTIVE.add(self)

    def close(self) -> None:
        self.revoke()
        _ACTIVE.discard(self)

    def revoke(self) -> None:
        self._revoked.set()

    def renewed(self, seconds: float) -> None:
        self._deadline = time.monotonic() + seconds

    def check(self) -> None:
        if self._revoked.is_set() or (self._deadline is not None and time.monotonic() >= self._deadline):
            self.revoke()
            raise OwnershipLost("Execution ownership was revoked or its local lease deadline expired")

    async def checkpoint(self) -> None:
        self.check()
        try:
            from drover.services import worker_ownership

            await worker_ownership.checkpoint()
            deadline = await self._checkpoint()
            if deadline is not None:
                self._deadline = deadline
            self.started = True
        except BaseException:
            self.revoke()
            raise
        self.check()

    async def ownership_checkpoint(self, session=None) -> None:
        self.check()
        try:
            from drover.services import worker_ownership

            await worker_ownership.checkpoint()
            if session is None:
                deadline = await self._ownership_checkpoint()
            elif self._transaction_checkpoint is not None:
                deadline = await self._transaction_checkpoint(session)
            else:
                raise OwnershipLost("SQL domain writes require transaction-bound attempt validation")
            if deadline is not None:
                self._deadline = deadline
        except BaseException:
            self.revoke()
            raise
        self.check()

    @contextlib.contextmanager
    def sending(self) -> Iterator[None]:
        # Never block the owning event loop waiting on its own SQL checkpoint.
        if threading.get_ident() == self.thread_id:
            raise OwnershipLost("Synchronous mutation must run in a worker thread")
        asyncio.run_coroutine_threadsafe(self.checkpoint(), self.loop).result()
        with self._lock:
            self.check()
            self._active += 1
            self.loop.call_soon_threadsafe(self._idle.clear)
        try:
            yield
        except BaseException:
            self.revoke()
            raise
        finally:
            with self._lock:
                self._active -= 1
                if not self._active:
                    self.loop.call_soon_threadsafe(self._idle.set)

    async def drain(self) -> None:
        while True:
            with self._lock:
                if not self._active:
                    return
            await self._idle.wait()
            await asyncio.sleep(0)


_CURRENT: contextvars.ContextVar[DispatchGuard | None] = contextvars.ContextVar("drover_dispatch_guard", default=None)
_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


@contextlib.contextmanager
def bound(guard: DispatchGuard) -> Iterator[DispatchGuard]:
    token = _CURRENT.set(guard)
    try:
        yield guard
    finally:
        _CURRENT.reset(token)


def revoke_current() -> None:
    """Stop later thread mutations immediately when its awaiting task cancels."""
    guard = _CURRENT.get()
    if guard is not None:
        guard.revoke()


async def checkpoint() -> None:
    """Fence job and periodic ownership without changing the bound caller identity."""
    guard = _CURRENT.get()
    if guard is not None:
        await guard.checkpoint()
    else:
        from drover.services import worker_ownership

        await worker_ownership.checkpoint()



async def ownership_checkpoint(session=None) -> None:
    """Fence domain SQL writes without marking a read-only job as mutating."""
    guard = _CURRENT.get()
    if guard is not None:
        await guard.ownership_checkpoint(session)
    else:
        from drover.services import worker_ownership

        await worker_ownership.checkpoint()


async def drain_connection(conn) -> None:
    """Wait for already-issued SDK sends before releasing its HTTP resources."""
    guard = getattr(conn.session, "_drover_dispatch_guard", None)
    if isinstance(guard, DispatchGuard):
        await guard.drain()


def _is_mutation(method: str, url: str) -> bool:
    if method.upper() in _READ_METHODS:
        return False
    # Keystone token authentication is a read-only authority exchange, not a
    # tenant resource mutation or a reason to mark the job non-replayable.
    return not (method.upper() == "POST" and urlsplit(str(url)).path.rstrip("/").endswith("/auth/tokens"))

def protect_httpx_client(client) -> None:
    """Fence Kubernetes mutation sends and revoke immediately on ambiguity."""
    guard = _CURRENT.get()
    send = client.send

    async def guarded_send(request, **kwargs):
        if not _is_mutation(request.method, str(request.url)):
            return await send(request, **kwargs)
        await checkpoint()
        kwargs["follow_redirects"] = False
        try:
            response = await send(request, **kwargs)
        except BaseException:
            if guard is not None:
                guard.revoke()
            raise
        if guard is not None and (response.status_code >= 500 or 300 <= response.status_code < 400):
            guard.revoke()
        return response

    client.send = guarded_send


def protect_connection(conn) -> None:
    """Protect this admitted connection without replacing its credentials."""
    install_session_guard(conn.session)


def install_session_guard(session) -> None:
    """Fence actual SDK sends, including to_thread and periodic trust cleanup.

    Capture the guard on the connection: a thread retaining this connection after
    task cancellation cannot escape revocation by losing its ContextVar context.
    Reads retain the SDK's ordinary retry policy. No credentials are replaced.
    """
    guard = _CURRENT.get()
    if getattr(session, "_drover_dispatch_protected", False):
        return
    session._drover_dispatch_protected = True
    session._drover_dispatch_guard = guard
    request = session.request
    send = session.session.send

    def guarded_request(url, method, **kwargs):
        if _is_mutation(method, url):
            kwargs.update(connect_retries=0, status_code_retries=0, redirect=False, allow_reauth=False)
        return request(url, method, **kwargs)

    def guarded_send(prepared, **kwargs):
        if not _is_mutation(prepared.method, prepared.url):
            return send(prepared, **kwargs)
        # urllib3 retries occur below Session.send, so disable those as well.
        adapter = session.session.get_adapter(prepared.url)
        previous_retries = adapter.max_retries
        adapter.max_retries = Retry(total=0, redirect=0)
        kwargs["allow_redirects"] = False
        try:
            if guard is None:
                from drover.services import worker_ownership

                worker_ownership.checkpoint_sync()
                return send(prepared, **kwargs)
            with guard.sending():
                response = send(prepared, **kwargs)
                if response.status_code >= 500 or 300 <= response.status_code < 400:
                    guard.revoke()
                return response
        finally:
            adapter.max_retries = previous_retries

    session.request = guarded_request
    session.session.send = guarded_send
