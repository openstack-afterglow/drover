"""Explicit per-job OpenStack execution authority for Drover workers.

Every worker or callback path that touches tenant OpenStack resources must run inside exactly one bound authority:
an admitted operation delegation (requester-owned Keystone trust) or a cluster resource authority (user-owned
restricted application credential). There is no service-identity, manager or caller fallback.
"""

from __future__ import annotations

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


async def open_connection(project_id: str):
    """Open a revalidated OpenStack connection from the bound authority; the caller must close it."""
    authority = _CURRENT.get()
    if authority is None:
        raise ExecutionAuthorityError("No admitted execution authority is bound to this OpenStack operation")
    if authority.project_id != project_id:
        raise AuthorityRevoked("Execution authority project does not match the requested project")
    if isinstance(authority, OperationAuthority):
        from drover.services import delegation

        return await delegation.open_trust_connection(authority)
    from drover.services import cluster_authority

    return await cluster_authority.open_control_connection(
        authority.project_id, authority.cluster_id, authority.capability
    )


@contextlib.asynccontextmanager
async def connection(project_id: str) -> AsyncIterator[object]:
    """Yield a revalidated connection from the bound authority and always release it."""
    from drover.services.keystone import close_connection

    conn = await open_connection(project_id)
    try:
        yield conn
    finally:
        await close_connection(conn)
