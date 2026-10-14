"""Explicit per-job OpenStack execution authority for Drover workers.

Every worker or callback path that touches tenant OpenStack resources must run inside exactly one bound authority:
an admitted operation delegation (requester-owned Keystone trust) or a cluster resource authority (user-owned
restricted application credential). There is no service-identity, manager or caller fallback.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass


class ExecutionAuthorityError(RuntimeError):
    """Terminal authorization failure: never retried and never replaced by another identity."""


class AuthorityRevoked(ExecutionAuthorityError):
    """The admitted actor, delegation or resource credential no longer authorizes this work."""


class ReauthorizationRequired(ExecutionAuthorityError):
    """The cluster has no active resource authority; an authorized operator must reauthorize it."""


class ExecutionAuthorityUnavailable(RuntimeError):
    """Keystone or the directory could not be reached; normal attempt-fenced retry applies."""


@dataclass(frozen=True)
class OperationAuthority:
    """A job runs under the requester's admitted trust for one operation."""

    delegation_id: str
    project_id: str
    cluster_id: str
    job_kind: str
    rollback_of_operation_id: str | None = None


@dataclass(frozen=True)
class ResourceAuthority:
    """Continuous cluster work runs under the active control credential's owner and this capability."""

    project_id: str
    cluster_id: str
    capability: str


Authority = OperationAuthority | ResourceAuthority

_CURRENT: contextvars.ContextVar[Authority | None] = contextvars.ContextVar("drover_execution_authority", default=None)


@contextlib.contextmanager
def bound(authority: Authority) -> Iterator[Authority]:
    """Bind one authority for the current task; nested code inherits it, siblings do not."""
    token = _CURRENT.set(authority)
    try:
        yield authority
    finally:
        _CURRENT.reset(token)


def current() -> Authority | None:
    return _CURRENT.get()


async def _open_connection(project_id: str):
    """Revalidate and construct the admitted SDK connection inside a retained task."""
    from drover.services import execution_safety, worker_ownership

    await worker_ownership.checkpoint()
    authority = _CURRENT.get()
    if authority is None:
        raise ExecutionAuthorityError("No admitted execution authority is bound to this OpenStack operation")
    if authority.project_id != project_id:
        raise AuthorityRevoked("Execution authority project does not match the requested project")
    if isinstance(authority, OperationAuthority):
        from drover.services import delegation

        conn = await delegation.open_trust_connection(authority)
    else:
        from drover.services import cluster_authority

        conn = await cluster_authority.open_control_connection(
            authority.project_id, authority.cluster_id, authority.capability
        )
    execution_safety.protect_connection(conn)
    return conn


async def _join(task: asyncio.Task) -> None:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except Exception:
            break


async def open_connection(project_id: str):
    """Drain authentication and close any produced connection on cancellation/loss."""
    from drover.services import execution_safety
    from drover.services.keystone import close_connection

    opening = asyncio.create_task(_open_connection(project_id))
    conn = None
    try:
        conn = await asyncio.shield(opening)
        await execution_safety.ownership_checkpoint()
        return conn
    except BaseException:
        # The connection factory may be blocked in synchronous authentication.
        # Preserve its result: joining only the SDK thread would leak the pool.
        await _join(opening)
        if conn is None and not opening.cancelled() and opening.exception() is None:
            conn = opening.result()
        if conn is not None:
            closing = asyncio.create_task(close_connection(conn))
            await _join(closing)
            closing.result()
        raise


@contextlib.asynccontextmanager
async def connection(project_id: str) -> AsyncIterator[object]:
    """Yield a revalidated connection from the bound authority and always release it."""
    from drover.services.keystone import close_connection

    conn = await open_connection(project_id)
    try:
        yield conn
    finally:
        await close_connection(conn)
