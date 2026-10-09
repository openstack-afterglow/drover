"""User-owned restricted application credentials granting a cluster continuous authority.

Each credential generation has a ``control`` credential used only by Drover (Stampede, reconciliation, health) and,
when guest plugins need OpenStack access, a separate ``guest`` credential rendered into the cluster. Credentials are
created with the operator's own validated token (Keystone refuses delegated tokens), are restricted, carry only the
delegated role subset and are revalidated against their owner's current authority before every control-plane use.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from keystoneauth1 import exceptions as ks_exc
from keystoneauth1 import session as ks_session
from keystoneauth1.identity import v3
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from drover.config import get_settings
from drover.services.execution import AuthorityRevoked, ExecutionAuthorityUnavailable, ReauthorizationRequired

_logger = logging.getLogger("drover.cluster_authority")

CONTROL = "control"
GUEST = "guest"
GUEST_PLUGIN_NAMES = frozenset({"occm", "manila_csi", "octavia_ingress", "barbican_kms"})
ACTION_REAUTHORIZE = "drover:clusters:reauthorize"
CAPABILITY_SCALE = "drover:clusters:scale"
CAPABILITY_READ = "drover:clusters:get"
_SAFE_SECRET = re.compile(r"[A-Za-z0-9_\-]{16,512}")
_SAFE_ID = re.compile(r"[A-Za-z0-9_\-]{8,64}")


class CredentialIssueError(RuntimeError):
    """Keystone did not return a restricted credential matching the requested owner, project and roles."""


@dataclass
class IssuedCredential:
    purpose: str
    app_credential_id: str
    owner_user_id: str
    role_ids: list[str]
    role_names: list[str]
    secret: str = field(repr=False)


@dataclass(frozen=True)
class _CredentialSnapshot:
    id: str
    cluster_id: str
    project_id: str
    generation: int
    purpose: str
    owner_user_id: str
    app_credential_id: str
    role_ids: tuple[str, ...]
    secret_encrypted: str = field(repr=False)


def _now() -> datetime:
    return datetime.now(UTC)


def guest_plugin_names(settings) -> list[str]:
    """Active plugins that read OpenStack credentials inside the guest."""
    from drover.services import plugins as k3s_plugins

    return [plugin.name for plugin in k3s_plugins.get_active_plugins(settings) if plugin.name in GUEST_PLUGIN_NAMES]


def _verify_issued(created, *, owner_user_id: str, project_id: str,
                   required_role_ids: list[str], allowed_role_ids: set[str]) -> set[str]:
    roles = {(role.get("id") if isinstance(role, dict) else getattr(role, "id", None)) for role in (created.roles or [])}
    if (
        not created.id
        or not created.secret
        or created.unrestricted
        or (created.project_id and created.project_id != project_id)
        or (getattr(created, "user_id", None) and created.user_id != owner_user_id)
        or not roles
        or not set(required_role_ids) <= roles
        or not roles <= allowed_role_ids
    ):
        raise CredentialIssueError("Keystone returned an application credential outside the requested authority")
    return roles


def _issue_sync(conn, *, owner_user_id: str, project_id: str, cluster_id: str, generation: int,
                purposes: list[str], role_ids: list[str], allowed_role_map: dict[str, str]) -> list[IssuedCredential]:
    issued: list[IssuedCredential] = []
    try:
        for purpose in purposes:
            created = conn.identity.create_application_credential(
                user=owner_user_id,
                name=f"drover-{cluster_id}-{purpose}-g{generation}",
                description=f"Drover cluster {cluster_id} {purpose} authority (generation {generation})",
                roles=[{"id": role_id} for role_id in role_ids],
                unrestricted=False,
            )
            issued.append(IssuedCredential(
                purpose=purpose, app_credential_id=created.id, owner_user_id=owner_user_id,
                role_ids=list(role_ids), role_names=[], secret=created.secret or "",
            ))
            accepted = _verify_issued(
                created, owner_user_id=owner_user_id, project_id=project_id,
                required_role_ids=role_ids, allowed_role_ids=set(allowed_role_map.values()),
            )
            accepted_names = sorted(name for name, rid in allowed_role_map.items() if rid in accepted)
            issued[-1].role_names = accepted_names
            issued[-1].role_ids = [allowed_role_map[name] for name in accepted_names]
    except Exception:
        _delete_owned_sync(conn, owner_user_id, [item.app_credential_id for item in issued])
        raise
    return issued


def _delete_owned_sync(conn, owner_user_id: str, credential_ids: list[str]) -> list[str]:
    deleted = []
    for credential_id in credential_ids:
        try:
            conn.identity.delete_application_credential(owner_user_id, credential_id, ignore_missing=True)
            deleted.append(credential_id)
        except Exception:
            _logger.warning("Application credential %s could not be deleted by its owner", credential_id)
    return deleted


async def issue(conn, token_info: dict, *, project_id: str, cluster_id: str, generation: int,
                purposes: list[str]) -> list[IssuedCredential]:
    """Create restricted credentials owned by the validated caller with only the delegated role subset."""
    from drover import auth
    from drover.services.delegation import select_delegated_roles

    owner = token_info.get("user_id") or ""
    if not owner or token_info.get("project_id") != project_id:
        raise PermissionError("A project-scoped requester token for the cluster project is required")
    role_map, graph = await asyncio.to_thread(auth.current_project_role_state, owner, project_id)
    _, ids = select_delegated_roles(role_map)
    held_names = {rid: name for name, rid in role_map.items()}
    closure: set[str] = set()
    pending = list(ids)
    while pending:
        rid = pending.pop()
        if rid in closure:
            continue
        name = held_names.get(rid)
        if name is None or name.lower() in {"admin", "manager"}:
            raise CredentialIssueError("Delegated role inference exceeds the requester's safe global authority")
        closure.add(rid)
        pending.extend(graph[rid])
    allowed_role_map = {name: rid for name, rid in role_map.items() if rid in closure}
    return await asyncio.to_thread(
        _issue_sync, conn, owner_user_id=owner, project_id=project_id, cluster_id=cluster_id,
        generation=generation, purposes=purposes, role_ids=ids, allowed_role_map=allowed_role_map,
    )


async def discard_issued(conn, issued: list[IssuedCredential]) -> None:
    """Delete freshly issued credentials when their admission did not commit."""
    by_owner: dict[str, list[str]] = {}
    for item in issued:
        by_owner.setdefault(item.owner_user_id, []).append(item.app_credential_id)
    for owner, ids in by_owner.items():
        await asyncio.to_thread(_delete_owned_sync, conn, owner, ids)


def add_generation(session: AsyncSession, *, cluster_id: str, project_id: str, generation: int,
                   issued: list[IssuedCredential], state: str, operation_id: str | None,
                   guest_plugins: list[str] | None) -> None:
    from drover.crypto import encrypt_app_credential_secret
    from drover.models.orm import DroverClusterCredential

    now = _now()
    for item in issued:
        session.add(DroverClusterCredential(
            id=str(uuid.uuid4()),
            cluster_id=cluster_id,
            project_id=project_id,
            generation=generation,
            purpose=item.purpose,
            owner_user_id=item.owner_user_id,
            app_credential_id=item.app_credential_id,
            secret_encrypted=encrypt_app_credential_secret(item.secret),
            role_ids=list(item.role_ids),
            role_names=list(item.role_names),
            guest_plugins=list(guest_plugins) if item.purpose == GUEST and guest_plugins is not None else None,
            state=state,
            operation_id=operation_id,
            created_at=now,
            activated_at=now if state == "active" else None,
            updated_at=now,
        ))


async def next_generation(session: AsyncSession, cluster_id: str) -> int:
    from drover.models.orm import DroverClusterCredential

    current = await session.scalar(
        select(func.max(DroverClusterCredential.generation)).where(DroverClusterCredential.cluster_id == cluster_id)
    )
    return int(current or 0) + 1


async def has_active_control(session: AsyncSession, cluster_id: str) -> bool:
    from drover.models.orm import DroverClusterCredential

    return bool(await session.scalar(
        select(DroverClusterCredential.id).where(
            DroverClusterCredential.cluster_id == cluster_id,
            DroverClusterCredential.purpose == CONTROL,
            DroverClusterCredential.state == "active",
        ).limit(1)
    ))


async def authorized_cluster_ids(session: AsyncSession) -> set[str]:
    from drover.models.orm import DroverClusterCredential

    result = await session.execute(
        select(DroverClusterCredential.cluster_id).where(
            DroverClusterCredential.purpose == CONTROL, DroverClusterCredential.state == "active"
        )
    )
    return set(result.scalars().all())


async def active_credentials(cluster_id: str) -> list[dict]:
    """Active credential references for read-only inventory checks; empty when durable storage is unavailable."""
    from drover.db import get_session_factory

    if get_session_factory() is None:
        return []
    return [item for item in (await authority_status(cluster_id))["credentials"] if item["state"] == "active"]


async def _snapshot(cluster_id: str, purpose: str, *, state: str = "active",
                    generation: int | None = None) -> _CredentialSnapshot | None:
    from drover.db import get_session_factory
    from drover.models.orm import DroverClusterCredential

    factory = get_session_factory()
    if factory is None:
        raise ExecutionAuthorityUnavailable("Database unavailable for resource authority lookup")
    stmt = select(DroverClusterCredential).where(
        DroverClusterCredential.cluster_id == cluster_id,
        DroverClusterCredential.purpose == purpose,
        DroverClusterCredential.state == state,
    )
    if generation is not None:
        stmt = stmt.where(DroverClusterCredential.generation == generation)
    async with factory() as session:
        row = (await session.scalars(stmt.order_by(DroverClusterCredential.generation.desc()).limit(1))).first()
        if row is None or not row.secret_encrypted or not row.owner_user_id:
            return None
        return _CredentialSnapshot(
            id=row.id, cluster_id=row.cluster_id, project_id=row.project_id, generation=row.generation,
            purpose=row.purpose, owner_user_id=row.owner_user_id, app_credential_id=row.app_credential_id,
            role_ids=tuple(row.role_ids or ()), secret_encrypted=row.secret_encrypted,
        )


def _authenticate_sync(snap: _CredentialSnapshot, secret: str):
    settings = get_settings()
    plugin = v3.ApplicationCredential(
        auth_url=settings.os_auth_url,
        application_credential_id=snap.app_credential_id,
        application_credential_secret=secret,
    )
    session = ks_session.Session(auth=plugin, timeout=30, verify=settings.ssl_verify)
    try:
        access = plugin.get_access(session)
    except (ks_exc.Unauthorized, ks_exc.Forbidden, ks_exc.NotFound) as exc:
        raise AuthorityRevoked("Keystone no longer honors the cluster application credential") from exc
    except ks_exc.ClientException as exc:
        raise ExecutionAuthorityUnavailable("Keystone application credential authentication is unavailable") from exc
    credential = getattr(access, "_application_credential", None) or {}
    from drover.services.delegation import token_roles_within_delegation

    if (
        access.trust_scoped
        or access.project_id != snap.project_id
        or access.user_id != snap.owner_user_id
        or credential.get("id") != snap.app_credential_id
        or not token_roles_within_delegation(access, snap.role_ids)
    ):
        raise AuthorityRevoked("The application credential token does not match the recorded cluster authority")
    return session


def _connect_control_sync(snap: _CredentialSnapshot, capability: str):
    import openstack.connection

    from drover.crypto import decrypt_app_credential_secret
    from drover.services.delegation import revalidate_principal_sync

    revalidate_principal_sync(snap.owner_user_id, snap.project_id, capability, snap.role_ids)
    session = _authenticate_sync(snap, decrypt_app_credential_secret(snap.secret_encrypted))
    settings = get_settings()
    return openstack.connection.Connection(
        session=session, region_name=settings.os_region_name, interface=settings.os_interface, api_timeout=30
    )


async def open_control_connection(project_id: str, cluster_id: str, capability: str):
    """Revalidate the active control credential owner and open a verified connection."""
    snap = await _snapshot(cluster_id, CONTROL)
    if snap is None:
        raise ReauthorizationRequired("The cluster has no active resource authority; reauthorize it")
    if snap.project_id != project_id:
        raise AuthorityRevoked("The cluster resource authority belongs to another project")
    return await asyncio.to_thread(_connect_control_sync, snap, capability)


async def active_guest_credential(cluster_id: str) -> dict | None:
    """Return the active guest credential for guest rendering, or None when the cluster has none."""
    from drover.crypto import decrypt_app_credential_secret

    snap = await _snapshot(cluster_id, GUEST)
    if snap is None:
        return None
    return {"id": snap.app_credential_id, "secret": decrypt_app_credential_secret(snap.secret_encrypted),
            "user_id": snap.owner_user_id}


async def retire_owned(conn, *, cluster_id: str, owner_user_id: str, states: tuple[str, ...]) -> list[str]:
    """Delete credentials owned by the synchronous caller in the given states and record them deleted."""
    from drover.db import get_session_factory
    from drover.models.orm import DroverClusterCredential

    factory = get_session_factory()
    if factory is None:
        return []
    async with factory() as session:
        rows = (await session.scalars(select(DroverClusterCredential).where(
            DroverClusterCredential.cluster_id == cluster_id,
            DroverClusterCredential.owner_user_id == owner_user_id,
            DroverClusterCredential.state.in_(states),
        ))).all()
        ids = [row.app_credential_id for row in rows]
    if not ids:
        return []
    deleted = await asyncio.to_thread(_delete_owned_sync, conn, owner_user_id, ids)
    if deleted:
        async with factory() as session, session.begin():
            now = _now()
            for row in (await session.scalars(select(DroverClusterCredential).where(
                DroverClusterCredential.app_credential_id.in_(deleted)
            ).with_for_update())).all():
                row.state = "deleted"
                row.state_reason = "deleted by owner"
                row.secret_encrypted = None
                row.retired_at = row.retired_at or now
                row.deleted_at = now
                row.updated_at = now
    return deleted


async def retire_remaining_for_deleted_cluster(cluster_id: str, project_id: str, legacy_app_credential_id: str) -> int:
    """Erase every remaining secret of a deleted cluster and report credentials needing owner revocation."""
    from drover.db import get_session_factory
    from drover.models.orm import DroverClusterCredential

    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database unavailable for cluster credential retirement")
    async with factory() as session, session.begin():
        now = _now()
        rows = (await session.scalars(select(DroverClusterCredential).where(
            DroverClusterCredential.cluster_id == cluster_id,
            DroverClusterCredential.state.in_(["staged", "active", "retiring"]),
        ).with_for_update())).all()
        known = {row.app_credential_id for row in rows}
        for row in rows:
            row.state = "retiring"
            row.state_reason = "cluster deleted; owner revocation required"
            row.secret_encrypted = None
            row.retired_at = row.retired_at or now
            row.updated_at = now
        count = len(rows)
        if legacy_app_credential_id and legacy_app_credential_id not in known:
            tracked = await session.scalar(select(DroverClusterCredential.id).where(
                DroverClusterCredential.app_credential_id == legacy_app_credential_id
            ))
            if tracked is None:
                _add_legacy_row(session, cluster_id, project_id, legacy_app_credential_id, now,
                                "cluster deleted; legacy manager credential requires operator revocation")
                count += 1
        return count


def _add_legacy_row(session: AsyncSession, cluster_id: str, project_id: str, app_credential_id: str,
                    now: datetime, reason: str) -> None:
    from drover.models.orm import DroverClusterCredential

    session.add(DroverClusterCredential(
        id=str(uuid.uuid4()), cluster_id=cluster_id, project_id=project_id, generation=0, purpose=GUEST,
        owner_user_id=None, app_credential_id=app_credential_id, secret_encrypted=None, state="retiring",
        state_reason=reason, created_at=now, retired_at=now, updated_at=now,
    ))


async def authority_status(cluster_id: str) -> dict:
    """Credential references and states without secrets."""
    from drover.db import get_session_factory
    from drover.models.orm import DroverClusterCredential

    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database unavailable for resource authority lookup")
    async with factory() as session:
        rows = (await session.scalars(select(DroverClusterCredential).where(
            DroverClusterCredential.cluster_id == cluster_id
        ).order_by(DroverClusterCredential.generation.desc(), DroverClusterCredential.purpose))).all()
    items = [{
        "app_credential_id": row.app_credential_id,
        "purpose": row.purpose,
        "generation": row.generation,
        "owner_user_id": row.owner_user_id,
        "state": row.state,
        "state_reason": row.state_reason,
        "role_names": list(row.role_names or []),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "activated_at": row.activated_at.isoformat() if row.activated_at else None,
        "retired_at": row.retired_at.isoformat() if row.retired_at else None,
        "last_error": row.last_error,
    } for row in rows]
    active = [item for item in items if item["state"] == "active"]
    return {
        "cluster_id": cluster_id,
        "authorized": any(item["purpose"] == CONTROL for item in active),
        "active_generation": max((item["generation"] for item in active), default=None),
        "staged_generations": sorted({item["generation"] for item in items if item["state"] == "staged"}),
        "owner_revocation_required": [item for item in items if item["state"] == "retiring"],
        "credentials": items,
    }


async def plan_reauthorization(session: AsyncSession, cluster) -> tuple[int, list[str], list[str] | None]:
    """Decide generation, purposes and guest plugin metadata for a new generation of an ACTIVE cluster."""
    from drover.models.orm import DroverClusterCredential

    rows = (await session.scalars(select(DroverClusterCredential).where(
        DroverClusterCredential.cluster_id == cluster.id,
        DroverClusterCredential.state.in_(["active", "staged"]),
    ))).all()
    active_guest = next((row for row in rows if row.purpose == GUEST and row.state == "active"), None)
    has_active = any(row.state == "active" for row in rows)
    # A never-reauthorized legacy cluster still runs its manager-owned guest credential; failed staged generations
    # do not change that, so the replacement must include a guest credential.
    legacy_guest = bool(cluster.app_credential_id) and not has_active
    purposes = [CONTROL]
    guest_plugins: list[str] | None = None
    if active_guest is not None:
        purposes.append(GUEST)
        guest_plugins = list(active_guest.guest_plugins) if active_guest.guest_plugins is not None else None
    elif legacy_guest:
        purposes.append(GUEST)
    return await next_generation(session, cluster.id), purposes, guest_plugins


async def _record_staged_error(cluster_id: str, generation: int, error: str) -> None:
    from drover.db import get_session_factory
    from drover.models.orm import DroverClusterCredential

    factory = get_session_factory()
    if factory is None:
        return
    async with factory() as session, session.begin():
        for row in (await session.scalars(select(DroverClusterCredential).where(
            DroverClusterCredential.cluster_id == cluster_id,
            DroverClusterCredential.generation == generation,
            DroverClusterCredential.state == "staged",
        ).with_for_update())).all():
            row.last_error = error[:4096]
            row.updated_at = _now()


async def _activate(project_id: str, cluster_id: str, generation: int) -> None:
    """Atomically activate a fully rolled-out generation and retire everything it superseded."""
    from drover.db import get_session_factory
    from drover.models.orm import DroverClusterCredential, K3sCluster

    factory = get_session_factory()
    if factory is None:
        raise RuntimeError("Database unavailable for credential activation")
    async with factory() as session, session.begin():
        cluster = await session.get(K3sCluster, cluster_id, with_for_update=True)
        if cluster is None or cluster.project_id != project_id or cluster.deleted_at is not None:
            raise RuntimeError("Cluster disappeared before credential activation")
        rows = (await session.scalars(select(DroverClusterCredential).where(
            DroverClusterCredential.cluster_id == cluster_id,
            DroverClusterCredential.state.in_(["staged", "active"]),
        ).with_for_update())).all()
        now = _now()
        new_guest = None
        known = {row.app_credential_id for row in rows}
        for row in rows:
            if row.generation == generation and row.state == "staged":
                row.state = "active"
                row.activated_at = now
                row.last_error = None
                if row.purpose == GUEST:
                    new_guest = row.app_credential_id
            elif row.generation != generation:
                row.state = "retiring"
                row.state_reason = f"superseded by generation {generation}; owner revocation required"
                row.secret_encrypted = None
                row.retired_at = now
            row.updated_at = now
        legacy = cluster.app_credential_id
        if legacy and legacy not in known:
            _add_legacy_row(session, cluster_id, project_id, legacy, now,
                            "legacy manager credential replaced; operator revocation required")
        cluster.app_credential_id = new_guest
        cluster.updated_at = now


async def execute_reauthorization(project_id: str, cluster_id: str, payload: dict,
                                  operation_id: str | None = None) -> None:
    """Revalidate the operator, verify staged credentials, roll the guest credential out, then activate."""
    from drover.crypto import decrypt_app_credential_secret
    from drover.services import guest_rollout, operations
    from drover.services import store as k3s_store
    from drover.services.delegation import revalidate_principal_sync

    generation = int(payload["generation"])
    control = await _snapshot(cluster_id, CONTROL, state="staged", generation=generation)
    if control is None:
        if await _snapshot(cluster_id, CONTROL, state="active", generation=generation) is not None:
            return
        raise RuntimeError("Staged credential generation is missing")
    guest = await _snapshot(cluster_id, GUEST, state="staged", generation=generation)
    try:
        await asyncio.to_thread(
            revalidate_principal_sync, control.owner_user_id, project_id, ACTION_REAUTHORIZE, control.role_ids
        )
        for snap in (control, guest):
            if snap is not None:
                await asyncio.to_thread(_authenticate_sync, snap, decrypt_app_credential_secret(snap.secret_encrypted))
        if guest is not None:
            cluster = await k3s_store.get_cluster(project_id, cluster_id)
            if not cluster or cluster.get("deleted_at") or cluster.get("status") != "ACTIVE":
                raise RuntimeError("Guest credential rollout requires an ACTIVE cluster")
            secret = decrypt_app_credential_secret(guest.secret_encrypted)
            if not _SAFE_SECRET.fullmatch(secret) or not _SAFE_ID.fullmatch(guest.app_credential_id):
                raise RuntimeError("Guest credential contains characters unsafe for guest configuration rollout")
            plugins = await _guest_plugins(cluster_id, generation)
            summary = await guest_rollout.rollout_guest_credential(
                cluster_id,
                generation,
                {"id": guest.app_credential_id, "secret": secret},
                kms_required=plugins is not None and "barbican_kms" in plugins,
                kms_detect=plugins is None,
            )
            if operation_id:
                await operations.append_operation_event(
                    None, operation_id, phase="guest_credential_rolled_out",
                    message=f"Guest credential generation {generation} rolled out", payload_json=summary,
                )
        await _activate(project_id, cluster_id, generation)
    except Exception as exc:
        await _record_staged_error(cluster_id, generation, str(exc) or exc.__class__.__name__)
        raise
    if operation_id:
        await operations.append_operation_event(
            None, operation_id, phase="authority_activated",
            message=f"Cluster resource authority generation {generation} is active",
            payload_json={"generation": generation},
        )


async def _guest_plugins(cluster_id: str, generation: int) -> list[str] | None:
    from drover.db import get_session_factory
    from drover.models.orm import DroverClusterCredential

    async with get_session_factory()() as session:
        row = await session.scalar(select(DroverClusterCredential).where(
            DroverClusterCredential.cluster_id == cluster_id,
            DroverClusterCredential.generation == generation,
            DroverClusterCredential.purpose == GUEST,
        ))
        return None if row is None or row.guest_plugins is None else list(row.guest_plugins)
