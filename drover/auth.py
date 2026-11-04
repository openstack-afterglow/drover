"""Keystone token validation and service-scoped OpenStack connections for Drover."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import quote, urlencode

from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import APIKeyHeader
from keystoneauth1 import access as ks_access
from keystoneauth1 import exceptions as ks_exc
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


def _identity_collection(client, path: str, key: str) -> list[dict]:
    """Require a complete JSON envelope before SDK managers discard metadata."""
    try:
        response = client.get(path)
    except ks_exc.NotFound as exc:
        # A collection endpoint disappearing cannot confirm principal revocation.
        # Exact user/project/domain GETs retain their terminal NotFound semantics.
        raise ValueError("Keystone authority collection is unavailable") from exc
    if not isinstance(response, (tuple, list)) or len(response) != 2:
        raise ValueError("Invalid Keystone authority response")
    body = response[1]
    if not isinstance(body, dict):
        raise ValueError("Invalid Keystone authority envelope")
    rows, links, truncated = body.get(key), body.get("links", {}), body.get("truncated")
    if (not isinstance(rows, list) or any(not isinstance(row, dict) or not row for row in rows)
            or not isinstance(links, dict) or links.get("next") is not None
            or (truncated is not None and truncated is not False)):
        raise ValueError("Incomplete Keystone authority collection")
    return rows


def _field(row, key, default=None):
    return row.get(key, default) if isinstance(row, dict) else getattr(row, key, default)


def _directory_rows(rows):
    if not isinstance(rows, (list, tuple)):
        raise ValueError("Incomplete Keystone collection")
    return rows


def _current_role_graph(client):
    """Read the unique global catalog and actual role-ID DAG, without caching."""
    catalog = {}
    global_names = {}
    folded_names = set()
    for role in _identity_collection(client, "/roles", "roles"):
        rid, name = _field(role, "id"), _field(role, "name")
        domain = _field(role, "domain_id")
        if (not isinstance(rid, str) or not rid or rid in catalog
                or not isinstance(name, str) or not name
                or (domain is not None and (not isinstance(domain, str) or not domain))):
            raise ValueError("Invalid Keystone role catalog")
        catalog[rid] = role
        if domain is None:
            folded = name.casefold()
            if folded in folded_names:
                raise ValueError("Ambiguous case-insensitive global role name")
            folded_names.add(folded)
            global_names[name] = rid

    def is_global_role(rid):
        if not isinstance(rid, str) or not rid:
            raise ValueError("Invalid role identity")
        if rid not in catalog:
            # Keystone may omit domain-local roles from its global catalog. They
            # cannot assert a global name or connect global inference edges.
            try:
                role = client.roles.get(rid)
            except ks_exc.NotFound as exc:
                raise ValueError("Role missing from global catalog") from exc
            domain = _field(role, "domain_id")
            if (_field(role, "id") != rid or not isinstance(_field(role, "name"), str)
                    or not _field(role, "name") or not isinstance(domain, str) or not domain):
                raise ValueError("Role missing from global catalog")
            catalog[rid] = role
        return _field(catalog[rid], "domain_id") is None

    graph = {rid: set() for rid in catalog}
    for inference in _identity_collection(client, "/role_inferences", "role_inferences"):
        prior = _field(_field(inference, "prior_role"), "id")
        is_global_role(prior)
        children = _directory_rows(_field(inference, "implies"))
        edges = graph.setdefault(prior, set())
        for child in children:
            rid = _field(child, "id")
            is_global_role(rid)
            graph.setdefault(rid, set())
            edges.add(rid)
    # Validate the entire graph, not merely the branch of a positive grant.
    remaining = {rid: len(children) for rid, children in graph.items()}
    parents = {rid: set() for rid in graph}
    for prior, children in graph.items():
        for child in children:
            parents[child].add(prior)
    leaves = [rid for rid, count in remaining.items() if not count]
    visited = 0
    while leaves:
        visited += 1
        for prior in parents[leaves.pop()]:
            remaining[prior] -= 1
            if not remaining[prior]:
                leaves.append(prior)
    if visited != len(graph):
        raise ValueError("Cyclic Keystone role inference")
    return global_names, graph, is_global_role


def _expand_global_roles(pending, graph, is_global_role):
    effective = set()
    while pending:
        rid = pending.pop()
        if is_global_role(rid) and rid not in effective:
            effective.add(rid)
            pending.extend(graph.get(rid, ()))
    return effective


def _is_system_admin(user_id: str, *, client=None, role_graph=None) -> bool:
    """Resolve raw exact-user/group system grants through the current global DAG.

    Effective-system expansion is not supported by every Keystone deployment.
    Neither an empty effective query nor an outage is evidence of revocation.
    """
    from drover.services.execution import ExecutionAuthorityUnavailable

    try:
        if not isinstance(user_id, str) or not user_id:
            raise ValueError("Exact system principal required")
        client = client or _get_admin_ks_client()
        names, graph, is_global_role = role_graph if role_graph is not None else _current_role_graph(client)
        groups = set()
        for group in _identity_collection(client, f"/users/{quote(user_id, safe='')}/groups", "groups"):
            gid = _field(group, "id")
            if not isinstance(gid, str) or not gid or gid in groups:
                raise ValueError("Invalid Keystone group membership")
            groups.add(gid)
        pending = []

        def collect(subject, identity):
            query = urlencode({f"{subject}.id": identity, "scope.system": "all"})
            for row in _identity_collection(client, f"/role_assignments?{query}", "role_assignments"):
                scope = row.get("scope")
                other = "group" if subject == "user" else "user"
                if (not isinstance(scope, dict) or set(scope) != {"system"}
                        or not isinstance(scope["system"], dict) or set(scope["system"]) != {"all"}
                        or scope["system"]["all"] is not True
                        or _field(row.get(subject), "id") != identity or other in row):
                    raise ValueError("Unexpected raw system assignment subject or scope")
                rid = _field(row.get("role"), "id")
                is_global_role(rid)
                pending.append(rid)

        collect("user", user_id)
        for gid in sorted(groups):
            collect("group", gid)
        effective = _expand_global_roles(pending, graph, is_global_role)
        return "admin" in names and names["admin"] in effective
    except Exception as exc:
        raise ExecutionAuthorityUnavailable("Current system authority is unavailable") from exc


def _current_project_roles(user_id: str, project_id: str) -> list[str]:
    """Resolve current assignments through Keystone's actual role-ID graph, uncached."""
    return sorted(current_project_role_map(user_id, project_id))


def current_project_role_map(user_id: str, project_id: str, *, client=None, role_graph=None) -> dict[str, str]:
    """Return effective unique global roles from current native assignments."""
    if not user_id or not project_id:
        raise ValueError("Project-scoped principal required")
    client = client or _get_admin_ks_client()
    global_names, graph, is_global_role = role_graph if role_graph is not None else _current_role_graph(client)
    pending = []
    query = urlencode({"user.id": user_id, "scope.project.id": project_id, "effective": True})
    for assignment in _identity_collection(client, f"/role_assignments?{query}", "role_assignments"):
        scope = _field(assignment, "scope", {})
        if _field(_field(scope, "project", {}), "id") != project_id:
            raise ValueError("Unexpected role assignment scope")
        subject = _field(assignment, "user", {})
        if _field(subject, "id") != user_id:
            raise ValueError("Unexpected effective assignment subject")
        rid = _field(_field(assignment, "role", {}), "id")
        is_global_role(rid)
        pending.append(rid)
    effective = _expand_global_roles(pending, graph, is_global_role)
    # Only unique global IDs can assert native names; domain aliases confer nothing.
    roles = {name: rid for name, rid in global_names.items() if rid in effective}
    return roles


def current_project_role_state(user_id: str, project_id: str) -> tuple[dict[str, str], dict[str, set[str]]]:
    """Preserve the native role-ID graph for delegated-provider ceiling validation."""
    client = _get_admin_ks_client()
    role_graph = _current_role_graph(client)
    return current_project_role_map(user_id, project_id, client=client, role_graph=role_graph), role_graph[1]


def current_principal_state(user_id: str, project_id: str, *, client=None) -> tuple[bool, bool]:
    """Read exact enabled native user, project and owning domains."""
    client = client or _get_admin_ks_client()
    user = client.users.get(user_id)
    project = client.projects.get(project_id)
    if _field(user, "id") != user_id or _field(project, "id") != project_id:
        raise ValueError("Directory identity mismatch")
    user_domain = _field(user, "domain_id")
    project_domain = _field(project, "domain_id")
    if not isinstance(user_domain, str) or not user_domain or not isinstance(project_domain, str) or not project_domain:
        raise ValueError("Directory principal/project domain identity is missing")
    domains = {}
    for domain_id in dict.fromkeys((user_domain, project_domain)):
        domain = client.domains.get(domain_id)
        if _field(domain, "id") != domain_id:
            raise ValueError("Directory domain identity mismatch")
        domains[domain_id] = domain
    enabled = (_field(user, "enabled") is True and _field(domains[user_domain], "enabled") is True,
               _field(project, "enabled") is True and _field(domains[project_domain], "enabled") is True)
    return enabled


def current_project_authority(user_id: str, project_id: str) -> dict:
    """Read current native human authority once, without cached or personal-role bypasses."""
    client = _get_admin_ks_client()
    enabled = current_principal_state(user_id, project_id, client=client)
    role_graph = _current_role_graph(client)
    roles = current_project_role_map(user_id, project_id, client=client, role_graph=role_graph)
    system_admin = _is_system_admin(user_id, client=client, role_graph=role_graph)
    return {"enabled": enabled, "role_map": roles, "is_system_admin": system_admin}


def service_user_id() -> str:
    """Return the Drover service identity resolved from its own service-project authentication."""
    user_id = _get_admin_ks_session().get_user_id()
    if not isinstance(user_id, str) or not user_id:
        raise RuntimeError("Drover service identity is unavailable")
    return user_id


def validate_token(token: str, project_id: str = "") -> dict:
    """Inspect the submitted credential before any human rescope/current-role expansion."""
    settings = get_settings()
    auth_url = _resolve_internal_keystone_endpoint()
    session = ks_session.Session(timeout=30, verify=settings.ssl_verify)
    try:
        response = session.get(f"{auth_url}/auth/tokens",
                               headers={"X-Auth-Token": token, "X-Subject-Token": token}, authenticated=False)
        payload = response.json()["token"]
        original = ks_access.create(resp=response, auth_token=token)
    finally:
        session.session.close()
    methods = payload.get("methods") or []
    if not isinstance(methods, list) or any(not isinstance(method, str) for method in methods):
        raise ValueError("Invalid original token authentication methods")
    credential = payload.get("application_credential") or {}
    if not isinstance(credential, dict):
        raise ValueError("Invalid original application credential metadata")
    if payload.get("access_rules") or credential.get("access_rules") or payload.get("bind"):
        raise PermissionError("Bound or access-rule credentials require native policy support")
    human_methods = {"password", "totp", "oidc", "openid", "saml2", "mapped"}
    delegated = original.trust_scoped or any(key in payload for key in (
        "application_credential", "OS-TRUST:trust", "OS-OAUTH1:access_token", "OS-OAUTH2:access_token", "oauth1", "oauth2",
    ))
    human = (not delegated and bool(set(methods) & human_methods)
             and set(methods) <= human_methods | {"token"})
    access = original
    if project_id and project_id != (original.project_id or ""):
        if not human:
            raise PermissionError("Reduced credentials cannot change their issued project scope")
        plugin = v3.Token(auth_url=auth_url, token=token, project_id=project_id)
        scoped_session = ks_session.Session(auth=plugin, timeout=30, verify=settings.ssl_verify)
        try:
            access = plugin.get_access(scoped_session)
        finally:
            scoped_session.session.close()
        if access.user_id != original.user_id or access.project_id != project_id:
            raise PermissionError("Keystone rescope changed the original caller identity")
    from drover.services.execution import ExecutionAuthorityUnavailable

    issued_ids = list(original.role_ids or [])
    try:
        if access.project_id:
            if human:
                authority = current_project_authority(access.user_id or "", access.project_id)
                if not all(authority["enabled"]):
                    raise PermissionError("The original caller, project or owning domain is disabled")
                roles = sorted(authority["role_map"])
            else:
                if not all(current_principal_state(access.user_id or "", access.project_id)):
                    raise PermissionError("The original caller, project or owning domain is disabled")
                current = current_project_role_map(access.user_id or "", access.project_id)
                roles = sorted(name for name, role_id in current.items() if role_id in issued_ids)
        else:
            roles = []
    except ks_exc.NotFound:
        raise PermissionError("The original caller, project or authority no longer exists") from None
    except (ks_exc.ClientException, ValueError) as exc:
        raise ExecutionAuthorityUnavailable("Current project authority is unavailable") from exc
    expires = min(original.expires, access.expires) if original.expires and access.expires else original.expires
    return {
        "token": access.auth_token, "original_token": token,
        "project_id": access.project_id or "", "project_name": access.project_name or "",
        "user_id": access.user_id or "", "username": access.username or "",
        "expires_at": expires.isoformat() if expires else "", "roles": roles,
        "is_system_admin": human and (authority["is_system_admin"] if access.project_id else _is_system_admin(access.user_id or "")),
        "is_human": human, "credential_restricted": not human, "auth_methods": methods,
        "issued_role_ids": issued_ids, "trust_id": original.trust_id if original.trust_scoped else None,
        "application_credential_id": credential.get("id"),
    }


async def require_token(
    request: Request,
    x_auth_token: str | None = Depends(keystone_token_scheme),
    x_project_id: str | None = Header(default=None, alias="X-Project-Id"),
) -> dict:
    if not x_auth_token:
        raise HTTPException(status_code=401, detail="X-Auth-Token header is required")
    from drover.services.execution import ExecutionAuthorityUnavailable

    try:
        info = await asyncio.to_thread(validate_token, x_auth_token, x_project_id or "")
    except ExecutionAuthorityUnavailable:
        raise HTTPException(status_code=503, detail="Keystone authority is unavailable") from None
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
