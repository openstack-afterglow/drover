"""Requester-owned Keystone trusts admitted for exactly one Drover operation.

Admission uses the caller's validated project token (trustor = caller, trustee = Drover service identity resolved
from its own service-project authentication, ``impersonation=True``) and delegates only the configured roles the caller
currently holds. Workers revalidate the trustor before every connection and verify the trust-scoped token. Requester
tokens and passwords are never persisted.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from keystoneauth1 import exceptions as ks_exc
from keystoneauth1 import session as ks_session
from keystoneauth1 import token_endpoint
from keystoneauth1.identity import v3
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from drover.config import get_settings
from drover.services.execution import AuthorityRevoked, ExecutionAuthorityUnavailable, OperationAuthority

_logger = logging.getLogger("drover.delegation")

ACTION_CREATE = "drover:clusters:create"
ACTION_SCALE = "drover:clusters:scale"
ACTION_DELETE = "drover:clusters:delete"

# Job kinds each admitted action may run. Callback continuations reuse the create operation's delegation.
ACTION_JOB_KINDS: dict[str, frozenset[str]] = {
    ACTION_CREATE: frozenset({"create", "bootstrap_ha", "provision_agents"}),
    ACTION_SCALE: frozenset({"scale", "nodegroup_reconcile"}),
    ACTION_DELETE: frozenset({"delete", "nodegroup_reconcile"}),
}
_FORBIDDEN_ROLES = frozenset({"admin", "manager"})
_ACTIVE_OPERATION_STATES = frozenset({"QUEUED", "RUNNING", "WAITING_CALLBACK"})
_EXPIRY_SKEW = timedelta(seconds=60)


class DelegationDenied(PermissionError):
    """The requester cannot delegate the roles this operation requires."""


class DelegationUnavailable(RuntimeError):
    """Keystone refused or could not complete trust admission for a non-authorization reason."""


@dataclass
class AdmittedDelegation:
    id: str
    project_id: str
    cluster_id: str
    action: str
    trust_id: str
    trustor_user_id: str
    trustee_user_id: str
    role_ids: list[str]
    role_names: list[str]
    expires_at: datetime
    bound: bool = False
    _caller_token: str = field(default="", repr=False)


@dataclass(frozen=True)
class _DelegationSnapshot:
    id: str
    project_id: str
    cluster_id: str
    operation_id: str | None
    action: str
    trust_id: str
    trustor_user_id: str
    trustee_user_id: str
    role_ids: tuple[str, ...]
    expires_at: datetime


def _now() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _parse_time(value) -> datetime | None:
    if isinstance(value, datetime):
        return _aware(value)
    if not isinstance(value, str) or not value:
        return None
    return _aware(datetime.fromisoformat(value.replace("Z", "+00:00")))


def select_delegated_roles(role_map: dict[str, str]) -> tuple[list[str], list[str]]:
    """Return (names, ids) of configured roles the principal currently holds; required roles must all be held."""
    settings = get_settings()
    required = list(settings.drover_delegated_required_roles)
    optional = list(settings.drover_delegated_optional_roles)
    if any(role.lower() in _FORBIDDEN_ROLES for role in [*required, *optional]):
        raise DelegationDenied("Drover never delegates admin or manager roles")
    missing = [role for role in required if role not in role_map]
    if missing:
        raise DelegationDenied("The requester lacks a project role required for delegated execution")
    names = list(dict.fromkeys(role for role in [*required, *optional] if role in role_map))
    return names, [role_map[name] for name in names]


def revalidate_principal_sync(user_id: str, project_id: str, action: str, delegated_role_ids) -> None:
    """Current enabled user/project, exact Drover capability and held delegated roles, or fail closed."""
    from drover import auth
    from drover.policy import authorize

    try:
        user_enabled, project_enabled = auth.current_principal_state(user_id, project_id)
        role_map = auth.current_project_role_map(user_id, project_id)
    except ks_exc.NotFound as exc:
        raise AuthorityRevoked("The delegating principal or project no longer exists") from exc
    except (ks_exc.ClientException, ValueError) as exc:
        raise ExecutionAuthorityUnavailable("Current principal authority could not be resolved") from exc
    if not user_enabled or not project_enabled:
        raise AuthorityRevoked("The delegating principal or project is disabled")
    if not set(delegated_role_ids or ()) <= set(role_map.values()):
        raise AuthorityRevoked("A delegated role is no longer held by the principal")
    creds = {
        "user_id": user_id,
        "project_id": project_id,
        "roles": sorted(role_map),
        "is_system_admin": auth._is_system_admin(user_id),
    }
    if not authorize(action, {"project_id": project_id}, creds, do_raise=False):
        raise AuthorityRevoked("The principal no longer holds the admitted Drover capability")


def _caller_keystone_client(token: str):
    from keystoneclient.v3 import client as ks_client

    from drover import auth

    endpoint = auth._resolve_internal_keystone_endpoint()
    session = ks_session.Session(
        auth=token_endpoint.Token(endpoint=endpoint, token=token), timeout=30, verify=get_settings().ssl_verify
    )
    return ks_client.Client(session=session, endpoint_override=endpoint)


def _field(obj, key, default=None):
    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)


def _verify_trust_record(trust, *, trustor: str, trustee: str, project_id: str, role_ids: list[str],
                         latest_expiry: datetime) -> datetime:
    expires_at = _parse_time(_field(trust, "expires_at"))
    trust_roles = {_field(role, "id") for role in (_field(trust, "roles") or [])}
    if (
        not _field(trust, "id")
        or _field(trust, "trustor_user_id") != trustor
        or _field(trust, "trustee_user_id") != trustee
        or _field(trust, "project_id") != project_id
        or _field(trust, "impersonation") is not True
        or expires_at is None
        or expires_at > latest_expiry + _EXPIRY_SKEW
        or not trust_roles
        or not trust_roles <= set(role_ids)
    ):
        raise DelegationUnavailable("Keystone returned a trust that does not match the admitted delegation")
    return expires_at


def _delete_trust_with_caller_token(token: str, trust_id: str) -> None:
    try:
        _caller_keystone_client(token).trusts.delete(trust_id)
    except ks_exc.NotFound:
        return


def _admit_sync(token_info: dict, project_id: str, cluster_id: str, action: str) -> AdmittedDelegation:
    from drover import auth

    if action not in ACTION_JOB_KINDS:
        raise ValueError("Unsupported delegated action")
    user_id = token_info.get("user_id") or ""
    token = token_info.get("token") or ""
    if not user_id or not token or token_info.get("project_id") != project_id:
        raise DelegationDenied("A project-scoped requester token for the target project is required")
    try:
        names, ids = select_delegated_roles(auth.current_project_role_map(user_id, project_id))
        trustee = auth.service_user_id()
    except DelegationDenied:
        raise
    except Exception as exc:
        raise DelegationUnavailable("Current requester roles could not be resolved") from exc
    if trustee == user_id:
        raise DelegationDenied("The Drover service identity cannot delegate to itself")
    latest_expiry = (_now() + timedelta(seconds=get_settings().drover_operation_trust_ttl_seconds)).replace(
        microsecond=0
    )
    try:
        trust = _caller_keystone_client(token).trusts.create(
            trustee_user=trustee,
            trustor_user=user_id,
            role_ids=ids,
            project=project_id,
            impersonation=True,
            expires_at=latest_expiry,
        )
    except (ks_exc.Forbidden, ks_exc.Unauthorized) as exc:
        raise DelegationDenied("Keystone refused requester delegation") from exc
    except Exception as exc:
        raise DelegationUnavailable("Keystone trust admission is unavailable") from exc
    try:
        expires_at = _verify_trust_record(
            trust, trustor=user_id, trustee=trustee, project_id=project_id, role_ids=ids, latest_expiry=latest_expiry
        )
    except DelegationUnavailable:
        with contextlib.suppress(Exception):
            _delete_trust_with_caller_token(token, _field(trust, "id"))
        raise
    return AdmittedDelegation(
        id=str(uuid.uuid4()),
        project_id=project_id,
        cluster_id=cluster_id,
        action=action,
        trust_id=_field(trust, "id"),
        trustor_user_id=user_id,
        trustee_user_id=trustee,
        role_ids=ids,
        role_names=names,
        expires_at=expires_at,
        _caller_token=token,
    )


async def _is_persisted(delegation_id: str) -> bool:
    from drover.db import get_session_factory
    from drover.models.orm import DroverDelegation

    factory = get_session_factory()
    if factory is None:
        return False
    async with factory() as session:
        return await session.get(DroverDelegation, delegation_id) is not None


async def _discard_unpersisted(admitted: AdmittedDelegation) -> None:
    """Delete a trust that no committed job references; finite expiry bounds any failure here."""
    try:
        if await _is_persisted(admitted.id):
            return
    except Exception:
        _logger.warning("Delegation persistence check failed; trust %s left to expire", admitted.id)
        return
    try:
        await asyncio.to_thread(_delete_trust_with_caller_token, admitted._caller_token, admitted.trust_id)
    except Exception:
        _logger.warning("Unused delegation %s could not be deleted; it expires at %s", admitted.id, admitted.expires_at)


@contextlib.asynccontextmanager
async def admission(token_info: dict, *, project_id: str, cluster_id: str, action: str) -> AsyncIterator[AdmittedDelegation]:
    """Admit a requester trust; it is deleted with the caller token unless a committed job references it."""
    admitted = await asyncio.to_thread(_admit_sync, token_info, project_id, cluster_id, action)
    try:
        yield admitted
    except BaseException:
        await _discard_unpersisted(admitted)
        raise
    if not admitted.bound:
        await _discard_unpersisted(admitted)


async def persist(session: AsyncSession, admitted: AdmittedDelegation, operation_id: str | None) -> str:
    """Add the admitted delegation in the caller's job transaction."""
    from drover.models.orm import DroverDelegation

    # The cluster and operation rows the delegation references must exist before its INSERT.
    await session.flush()
    now = _now()
    session.add(
        DroverDelegation(
            id=admitted.id,
            project_id=admitted.project_id,
            cluster_id=admitted.cluster_id,
            operation_id=operation_id,
            action=admitted.action,
            trust_id=admitted.trust_id,
            trustor_user_id=admitted.trustor_user_id,
            trustee_user_id=admitted.trustee_user_id,
            role_ids=list(admitted.role_ids),
            role_names=list(admitted.role_names),
            expires_at=admitted.expires_at,
            state="active",
            created_at=now,
            updated_at=now,
        )
    )
    await session.flush()
    admitted.bound = True
    return admitted.id


async def active_delegation_for_operation(session: AsyncSession | None, operation_id: str | None) -> str | None:
    """Return the active delegation admitted for an operation, used only by its callback continuations."""
    from drover.db import get_session_factory
    from drover.models.orm import DroverDelegation

    if not operation_id:
        return None
    stmt = (
        select(DroverDelegation.id)
        .where(DroverDelegation.operation_id == operation_id, DroverDelegation.state == "active")
        .order_by(DroverDelegation.created_at.desc())
        .limit(1)
    )
    if session is not None:
        return await session.scalar(stmt)
    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as owned:
        return await owned.scalar(stmt)


async def _load_snapshot(delegation_id: str) -> _DelegationSnapshot | None:
    from drover.db import get_session_factory
    from drover.models.orm import DroverDelegation

    factory = get_session_factory()
    if factory is None:
        raise ExecutionAuthorityUnavailable("Database unavailable for delegation lookup")
    async with factory() as session:
        row = await session.get(DroverDelegation, delegation_id)
        if row is None or row.state != "active":
            return None
        return _DelegationSnapshot(
            id=row.id,
            project_id=row.project_id,
            cluster_id=row.cluster_id,
            operation_id=row.operation_id,
            action=row.action,
            trust_id=row.trust_id,
            trustor_user_id=row.trustor_user_id,
            trustee_user_id=row.trustee_user_id,
            role_ids=tuple(row.role_ids or ()),
            expires_at=_aware(row.expires_at),
        )


async def _mark_state(delegation_id: str, state: str, reason: str) -> None:
    from drover.db import get_session_factory
    from drover.models.orm import DroverDelegation

    factory = get_session_factory()
    if factory is None:
        return
    async with factory() as session, session.begin():
        row = await session.get(DroverDelegation, delegation_id, with_for_update=True)
        if row is not None and row.state in {"active", "released"}:
            now = _now()
            row.state = state
            row.state_reason = reason[:255]
            row.released_at = row.released_at or now
            row.updated_at = now


def token_roles_within_delegation(access, delegated_role_ids) -> bool:
    """Keystone expands implied roles into trust and application-credential tokens, so the delegated IDs must be
    present (not equal) and the token must never carry admin or manager."""
    role_ids = set(access.role_ids or [])
    names = {str(name).lower() for name in (access.role_names or [])}
    return bool(delegated_role_ids) and set(delegated_role_ids) <= role_ids and not names & _FORBIDDEN_ROLES


def _verify_trust_access(access, snap: _DelegationSnapshot) -> None:
    expires = access.expires
    if (
        not access.trust_scoped
        or access.trust_id != snap.trust_id
        or access.project_id != snap.project_id
        or access.user_id != snap.trustor_user_id
        or access.trustor_user_id != snap.trustor_user_id
        or access.trustee_user_id != snap.trustee_user_id
        or not token_roles_within_delegation(access, snap.role_ids)
        or expires is None
        or _aware(expires) <= _now()
    ):
        raise AuthorityRevoked("The trust-scoped token does not match the admitted delegation")


class _TrustPassword(v3.Password):
    """Keep every authenticated request inside the delegation lifetime, including cached-token reuse."""

    def __init__(self, snap: _DelegationSnapshot):
        settings = get_settings()
        super().__init__(
            auth_url=settings.os_auth_url, user_id=snap.trustee_user_id,
            password=settings.os_password, trust_id=snap.trust_id,
        )
        self._delegation = snap

    def get_access(self, session, **kwargs):
        minimum = get_settings().drover_operation_trust_min_remaining_seconds
        if (self._delegation.expires_at - _now()).total_seconds() < minimum:
            raise AuthorityRevoked("The operation delegation is expired or too close to expiry")
        access = super().get_access(session, **kwargs)
        _verify_trust_access(access, self._delegation)
        if (self._delegation.expires_at - _now()).total_seconds() < minimum:
            raise AuthorityRevoked("The operation delegation is expired or too close to expiry")
        return access


def _connect_sync(snap: _DelegationSnapshot):
    import openstack.connection

    from drover import auth

    settings = get_settings()
    revalidate_principal_sync(snap.trustor_user_id, snap.project_id, snap.action, snap.role_ids)
    try:
        trustee = auth.service_user_id()
    except Exception as exc:
        raise ExecutionAuthorityUnavailable("Drover service identity could not authenticate") from exc
    if trustee != snap.trustee_user_id:
        raise AuthorityRevoked("The configured trustee is not the admitted trustee")
    # Trust-scoped password authentication carries no project selector: the service is never tenant-scoped.
    plugin = _TrustPassword(snap)
    session = ks_session.Session(auth=plugin, timeout=30, verify=settings.ssl_verify)
    try:
        plugin.get_access(session)
    except (ks_exc.Unauthorized, ks_exc.Forbidden, ks_exc.NotFound) as exc:
        raise AuthorityRevoked("Keystone no longer honors the admitted trust") from exc
    except ks_exc.ClientException as exc:
        raise ExecutionAuthorityUnavailable("Keystone trust authentication is unavailable") from exc
    return openstack.connection.Connection(
        session=session,
        region_name=settings.os_region_name,
        interface=settings.os_interface,
        api_timeout=30,
    )


async def open_trust_connection(authority: OperationAuthority):
    """Revalidate the trustor and open a verified trust-scoped connection for one admitted job."""
    snap = await _load_snapshot(authority.delegation_id)
    if snap is None:
        raise AuthorityRevoked("The operation delegation is missing, released or revoked")
    kind_allowed = authority.job_kind in ACTION_JOB_KINDS.get(snap.action, frozenset())
    rollback = (
        authority.job_kind == "delete"
        and snap.action == ACTION_CREATE
        and authority.rollback_of_operation_id is not None
        and authority.rollback_of_operation_id == snap.operation_id
    )
    if snap.project_id != authority.project_id or snap.cluster_id != authority.cluster_id or not (kind_allowed or rollback):
        await _mark_state(snap.id, "revoked", "scope mismatch")
        raise AuthorityRevoked("The operation delegation does not cover this job")
    remaining = (snap.expires_at - _now()).total_seconds()
    if remaining < get_settings().drover_operation_trust_min_remaining_seconds:
        await _mark_state(snap.id, "revoked", "lifetime exhausted")
        raise AuthorityRevoked("The operation delegation is expired or too close to expiry")
    try:
        return await asyncio.to_thread(_connect_sync, snap)
    except AuthorityRevoked as exc:
        await _mark_state(snap.id, "revoked", str(exc))
        raise


async def release_if_idle(session: AsyncSession, delegation_id: str | None, *, reason: str = "operation idle") -> None:
    """Release a delegation in the caller's transaction once no job or live operation still needs it."""
    from drover.models.orm import DroverDelegation, DroverJob, DroverOperation

    if not delegation_id:
        return
    row = await session.get(DroverDelegation, delegation_id, with_for_update=True)
    if row is None or row.state != "active":
        return
    busy = await session.scalar(
        select(DroverJob.id)
        .where(DroverJob.delegation_id == delegation_id, DroverJob.status.in_(["queued", "running"]))
        .limit(1)
    )
    if busy:
        return
    if row.operation_id:
        op = await session.get(DroverOperation, row.operation_id)
        if op is not None and op.status in _ACTIVE_OPERATION_STATES:
            return
    now = _now()
    row.state = "released"
    row.state_reason = reason[:255]
    row.released_at = now
    row.updated_at = now


def _trust_exists_sync(trust_id: str) -> bool:
    from drover import auth

    try:
        auth._get_admin_ks_client().trusts.get(trust_id)
    except ks_exc.NotFound:
        return False
    return True


def _delete_trust_with_trust_token_sync(trust_id: str, trustee_user_id: str) -> str:
    """Delete a trust through its own impersonating trust-scoped token.

    Keystone's ``identity:delete_trust`` allows the trustor and does not block trust-scoped tokens
    (keystone/api/trusts.py ``_check_delegated_token``); an impersonating token's user is the trustor. The service
    identity is never scoped to the tenant. Returns ``deleted``, ``gone`` (already absent) or ``inert`` (Keystone no
    longer issues or honors tokens for it; it stays unusable until its finite expiry).
    """
    from keystoneclient.v3 import client as ks_client

    from drover import auth

    settings = get_settings()
    plugin = v3.Password(
        auth_url=settings.os_auth_url, user_id=trustee_user_id, password=settings.os_password, trust_id=trust_id
    )
    session = ks_session.Session(auth=plugin, timeout=30, verify=settings.ssl_verify)
    try:
        endpoint = auth._resolve_internal_keystone_endpoint()
        ks_client.Client(session=session, endpoint_override=endpoint).trusts.delete(trust_id)
    except ks_exc.NotFound:
        return "gone"
    except (ks_exc.Unauthorized, ks_exc.Forbidden):
        return "inert"
    finally:
        session.session.close()
    return "deleted"


async def delete_released(delegation_id: str | None) -> str | None:
    """Delete the Keystone trust of a released or revoked delegation; transient failures retry on the next sweep."""
    from drover.db import get_session_factory
    from drover.models.orm import DroverDelegation

    factory = get_session_factory()
    if not delegation_id or factory is None:
        return None
    async with factory() as session:
        row = await session.get(DroverDelegation, delegation_id)
        if row is None or row.state not in {"released", "revoked"} or row.state_reason == "trust inert until expiry":
            return None
        trust_id, trustee = row.trust_id, row.trustee_user_id
    try:
        outcome = await asyncio.to_thread(_delete_trust_with_trust_token_sync, trust_id, trustee)
    except Exception:
        _logger.warning("Trust cleanup for delegation %s deferred to the next sweep", delegation_id)
        return None
    async with factory() as session, session.begin():
        row = await session.get(DroverDelegation, delegation_id, with_for_update=True)
        if row is not None and row.state in {"released", "revoked"}:
            now = _now()
            if outcome == "inert":
                row.state_reason = "trust inert until expiry"
            else:
                row.state = "deleted"
                row.state_reason = "Keystone trust deleted" if outcome == "deleted" else "Keystone trust already absent"
            row.released_at = row.released_at or now
            row.updated_at = now
    return outcome


async def sweep() -> dict[str, int]:
    """Release idle delegations, delete their trusts and record expiry; never repeats any cloud mutation."""
    from drover.db import get_session_factory
    from drover.models.orm import DroverDelegation

    factory = get_session_factory()
    if factory is None:
        return {"released": 0, "deleted": 0, "expired": 0}
    released = deleted = expired = 0
    async with factory() as session:
        active_ids = list((await session.scalars(
            select(DroverDelegation.id).where(DroverDelegation.state == "active")
        )).all())
    for delegation_id in active_ids:
        async with factory() as session, session.begin():
            await release_if_idle(session, delegation_id, reason="operation finished outside a job")
            row = await session.get(DroverDelegation, delegation_id)
            released += int(row is not None and row.state == "released")
    now = _now()
    async with factory() as session:
        cleanup_ids = list((await session.scalars(
            select(DroverDelegation.id).where(
                DroverDelegation.state.in_(["released", "revoked"]), DroverDelegation.expires_at >= now
            )
        )).all())
    for delegation_id in cleanup_ids:
        deleted += int(await delete_released(delegation_id) in {"deleted", "gone"})
    async with factory() as session:
        candidates = list((await session.execute(
            select(DroverDelegation.id, DroverDelegation.trust_id).where(
                DroverDelegation.state.in_(["active", "released", "revoked"]), DroverDelegation.expires_at < now
            )
        )).all())
    for delegation_id, trust_id in candidates:
        try:
            exists = await asyncio.to_thread(_trust_exists_sync, trust_id)
        except Exception:
            _logger.warning("Delegation expiry check deferred for %s", delegation_id)
            continue
        if exists:
            _logger.warning("Keystone still returns trust for expired delegation %s", delegation_id)
            continue
        async with factory() as session, session.begin():
            row = await session.get(DroverDelegation, delegation_id, with_for_update=True)
            if row is not None and row.state in {"active", "released", "revoked"}:
                row.state = "expired"
                row.state_reason = "Keystone trust expired"
                row.released_at = row.released_at or now
                row.updated_at = now
                expired += 1
    return {"released": released, "deleted": deleted, "expired": expired}
