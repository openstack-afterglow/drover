"""Keystone token validation and service-scoped OpenStack connections for Drover."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from functools import lru_cache

from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import APIKeyHeader
from keystoneauth1 import access as ks_access
from keystoneauth1 import session as ks_session
from keystoneauth1.identity import v3

from drover.config import get_settings

_logger = logging.getLogger(__name__)
_internal_keystone_endpoint_cache: str | None = None
keystone_token_scheme = APIKeyHeader(
    name="X-Auth-Token",
    auto_error=False,
    scheme_name="KeystoneToken",
    description="Keystone authentication token",
)


@dataclass
class CacheMode:
    enabled: bool = True
    refresh: bool = False


def cache_mode(
    no_cache: bool = False,
    refresh_cache: bool = False,
) -> CacheMode:
    return CacheMode(enabled=not no_cache, refresh=refresh_cache)


@lru_cache(maxsize=1)
def _get_admin_ks_session() -> ks_session.Session:
    settings = get_settings()
    auth = v3.Password(
        auth_url=settings.os_auth_url,
        username=settings.os_username,
        password=settings.os_password,
        project_name=settings.os_project_name,
        user_domain_name=settings.os_user_domain_name,
        project_domain_name=settings.os_project_domain_name,
    )
    return ks_session.Session(auth=auth, timeout=15, verify=settings.ssl_verify)


def _resolve_internal_keystone_endpoint(session: ks_session.Session | None = None) -> str:
    global _internal_keystone_endpoint_cache
    if _internal_keystone_endpoint_cache:
        return _internal_keystone_endpoint_cache

    settings = get_settings()
    session = session or _get_admin_ks_session()
    endpoint = session.get_endpoint(
        service_type="identity",
        interface="internal",
        region_name=settings.os_region_name,
    )
    if not endpoint:
        raise RuntimeError("Keystone internal endpoint is unavailable")

    # Kolla registers the identity catalog entry without a version path, so the
    # bare endpoint answers 404 for /auth/tokens and /roles.
    endpoint = endpoint.rstrip("/")
    if not endpoint.endswith("/v3"):
        endpoint += "/v3"
    _internal_keystone_endpoint_cache = endpoint
    return endpoint


def _get_admin_ks_client():
    from keystoneclient.v3 import client as ks_client

    session = _get_admin_ks_session()
    endpoint = _resolve_internal_keystone_endpoint(session)
    return ks_client.Client(session=session, endpoint_override=endpoint)


def _resolve_admin_role_id() -> str | None:
    try:
        roles = _get_admin_ks_client().roles.list(name="admin")
        global_roles = [role for role in roles if not getattr(role, "domain_id", None)]
        if len(global_roles) == 1:
            return global_roles[0].id
    except Exception:
        _logger.warning("Failed to resolve Keystone admin role")
    return None


def _is_system_admin(user_id: str) -> bool:
    """Fail closed unless the user has admin on Keystone system scope."""
    if not user_id:
        return False
    try:
        role_id = _resolve_admin_role_id()
        if not role_id:
            return False
        assignments = _get_admin_ks_client().role_assignments.list(
            user=user_id,
            role=role_id,
            system="all",
            effective=True,
        )
        for assignment in assignments:
            row = assignment if isinstance(assignment, dict) else assignment.to_dict()
            if (row.get("user", {}).get("id") == user_id
                    and row.get("role", {}).get("id") == role_id
                    and row.get("scope", {}).get("system", {}).get("all") is True):
                return True
        return False
    except Exception:
        _logger.warning("Keystone system-admin check failed")
        return False


def _current_project_roles(user_id: str, project_id: str) -> list[str]:
    """Resolve current assignments through Keystone's actual role-ID graph, uncached."""
    return sorted(current_project_role_map(user_id, project_id))


def current_project_role_map(user_id: str, project_id: str) -> dict[str, str]:
    """Return effective unique global role names mapped to their IDs for one current project assignment."""
    if not user_id or not project_id:
        raise ValueError("Project-scoped principal required")
    client = _get_admin_ks_client()

    def field(row, key, default=None):
        return row.get(key, default) if isinstance(row, dict) else getattr(row, key, default)

    catalog = {}
    global_names = {}
    for role in client.roles.list():
        rid, name = field(role, "id"), field(role, "name")
        if not isinstance(rid, str) or not rid or rid in catalog or not isinstance(name, str) or not name:
            raise ValueError("Invalid Keystone role catalog")
        catalog[rid] = role
        if not field(role, "domain_id"):
            if name in global_names:
                raise ValueError("Ambiguous global role name")
            global_names[name] = rid

    def is_global_role(rid):
        if not isinstance(rid, str) or not rid:
            raise ValueError("Invalid role identity")
        role = catalog.get(rid)
        if role is None:
            role = client.roles.get(rid)
            if field(role, "id") != rid:
                raise ValueError("Role identity mismatch")
            if not field(role, "domain_id"):
                raise ValueError("Global role missing from catalog")
            catalog[rid] = role
        return not field(role, "domain_id")
    graph = {rid: set() for rid in catalog}
    for inference in client.inference_rules.list_inference_roles():
        prior = field(field(inference, "prior_role"), "id")
        children = field(inference, "implies")
        if not isinstance(children, list):
            raise ValueError("Invalid Keystone role inference")
        if not is_global_role(prior):
            continue
        for child in children:
            rid = field(child, "id")
            if not is_global_role(rid):
                continue
            graph[prior].add(rid)
    pending = []
    for assignment in client.role_assignments.list(user=user_id, project=project_id, effective=True):
        scope = field(assignment, "scope", {})
        if field(field(scope, "project", {}), "id") != project_id:
            raise ValueError("Unexpected role assignment scope")
        subject = field(assignment, "user", {})
        if field(subject, "id") != user_id:
            raise ValueError("Unexpected effective assignment subject")
        rid = field(field(assignment, "role", {}), "id")
        if not is_global_role(rid):
            continue
        pending.append(rid)
    effective = set()
    while pending:
        rid = pending.pop()
        if rid not in effective:
            effective.add(rid)
            pending.extend(graph[rid])
    # Only unique global IDs can assert native names; domain aliases confer nothing.
    return {name: rid for name, rid in global_names.items() if rid in effective}


def current_principal_state(user_id: str, project_id: str) -> tuple[bool, bool]:
    """Return whether the user and project are currently enabled, read through the directory credential."""
    client = _get_admin_ks_client()
    user = client.users.get(user_id)
    project = client.projects.get(project_id)
    if getattr(user, "id", None) != user_id or getattr(project, "id", None) != project_id:
        raise ValueError("Directory identity mismatch")
    return bool(getattr(user, "enabled", False)), bool(getattr(project, "enabled", False))


def service_user_id() -> str:
    """Return the Drover service identity resolved from its own service-project authentication."""
    user_id = _get_admin_ks_session().get_user_id()
    if not isinstance(user_id, str) or not user_id:
        raise RuntimeError("Drover service identity is unavailable")
    return user_id


def validate_token(token: str, project_id: str = "") -> dict:
    settings = get_settings()
    auth_url = _resolve_internal_keystone_endpoint()
    if project_id:
        auth_plugin = v3.Token(auth_url=auth_url, token=token, project_id=project_id)
        session = ks_session.Session(auth=auth_plugin, timeout=30, verify=settings.ssl_verify)
        access = auth_plugin.get_access(session)
    else:
        # Token reauthentication without a target selects the user's default
        # project (or an unscoped token), not the submitted token's project.
        session = ks_session.Session(timeout=30, verify=settings.ssl_verify)
        response = session.get(
            f"{auth_url}/auth/tokens",
            headers={"X-Auth-Token": token, "X-Subject-Token": token},
            authenticated=False,
        )
        access = ks_access.create(resp=response, auth_token=token)
    roles = _current_project_roles(access.user_id or "", access.project_id or "")
    return {
        "token": access.auth_token,
        "project_id": access.project_id or "",
        "project_name": access.project_name or "",
        "user_id": access.user_id or "",
        "username": access.username or "",
        "expires_at": access.expires.isoformat() if access.expires else "",
        "roles": roles,
        "is_system_admin": _is_system_admin(access.user_id or ""),
    }


async def require_token(
    request: Request,
    x_auth_token: str | None = Depends(keystone_token_scheme),
    x_project_id: str | None = Header(default=None, alias="X-Project-Id"),
) -> dict:
    if not x_auth_token:
        raise HTTPException(status_code=401, detail="X-Auth-Token header is required")
    try:
        info = await asyncio.to_thread(validate_token, x_auth_token, x_project_id or "")
    except Exception:
        _logger.info("Keystone token validation failed")
        raise HTTPException(status_code=401, detail="Invalid or expired Keystone token") from None
    if not info.get("project_id"):
        raise HTTPException(status_code=401, detail="A project-scoped Keystone token is required")
    request.state.token_info = info
    return info


def get_token_info(token_info: dict = Depends(require_token)) -> dict:
    return token_info


def require_admin(token_info: dict = Depends(require_token)) -> dict:
    from drover.policy import authorize

    authorize("drover:admin", {"project_id": token_info.get("project_id", "")}, token_info)
    return token_info


async def get_os_conn(
    token_info: dict = Depends(require_token),
) -> AsyncGenerator[object, None]:
    """Yield a caller-token-scoped OpenStack connection and close it."""
    import openstack

    settings = get_settings()
    project_id = token_info["project_id"]
    scoped_token = token_info["token"]
    try:
        conn = openstack.connect(
            load_envvars=False,
            load_yaml_config=False,
            auth_url=settings.os_auth_url,
            auth_type="token",
            token=scoped_token,
            project_id=project_id,
            region_name=settings.os_region_name,
            interface=settings.os_interface,
            api_timeout=30,
            verify=settings.ssl_verify,
        )
        conn._afterglow_token = scoped_token
        conn._afterglow_project_id = project_id
        conn._afterglow_user_id = token_info.get("user_id", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid scoped Keystone token") from None

    try:
        yield conn
    finally:
        from drover.services.keystone import close_connection

        await close_connection(conn)
