"""Centralized oslo.policy enforcement and FastAPI dependencies for Drover."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastapi import Depends, HTTPException, Request
from oslo_config import cfg
from oslo_policy import policy

from drover.auth import require_token
from drover.config import get_settings

_logger = logging.getLogger(__name__)

_ENFORCER: policy.Enforcer | None = None

def _project_rule(name: str, roles: tuple[str, ...], *, base: str = "member") -> policy.RuleDefault:
    capability = " or ".join(f"role:{role}" for role in roles)
    baseline = "(role:reader or role:member)" if base == "reader" else "role:member"
    return policy.RuleDefault(
        name=name,
        check_str=f"rule:context_is_admin or (rule:owner and {baseline} and ({capability}))",
    )


DEFAULT_RULES: list[policy.RuleDefault] = [
    policy.RuleDefault("context_is_admin", "is_system_admin:True"),
    policy.RuleDefault("owner", "project_id:%(project_id)s"),
    _project_rule("drover:clusters:get", ("drover-inventory_reader",), base="reader"),
    _project_rule("drover:operations:get", ("drover-inventory_reader",), base="reader"),
    _project_rule("drover:clusters:create", ("drover-clusters_editor",)),
    _project_rule("drover:clusters:scale", ("drover-clusters_editor",)),
    _project_rule("drover:clusters:delete", ("drover-clusters_admin",)),
    # Creates the operator's own restricted cluster credentials and rolls them into the guest.
    _project_rule("drover:clusters:reauthorize", ("drover-clusters_admin",)),
    # Deleting one's own superseded credential only reduces authority; ownership is checked in code.
    policy.RuleDefault("drover:clusters:retire_credentials", "rule:context_is_admin or rule:owner"),
    _project_rule("drover:access:get", ("drover-access_user", "drover-workloads_editor", "drover-access_admin")),
    _project_rule("drover:access:read", ("drover-access_user", "drover-access_admin")),
    _project_rule("drover:access:admin", ("drover-access_admin",)),
    _project_rule("drover:workloads:write", ("drover-workloads_editor", "drover-access_admin")),
    _project_rule("drover:certificates:rotate", ("drover-clusters_admin",)),
    policy.RuleDefault("drover:templates:manage", "rule:context_is_admin"),
    policy.RuleDefault("drover:admin", "rule:context_is_admin"),
]


def reset_enforcer() -> None:
    """Reset global enforcer singleton (useful for testing policy reloads)."""
    global _ENFORCER
    _ENFORCER = None


def get_enforcer(policy_file: str | None = None, reload: bool = False) -> policy.Enforcer:
    """Initialize or return cached oslo.policy Enforcer."""
    global _ENFORCER
    if _ENFORCER is not None and not reload and policy_file is None:
        return _ENFORCER

    settings = get_settings()
    effective_file = policy_file if policy_file is not None else getattr(settings, "drover_policy_file", "/etc/drover/policy.yaml")

    conf = cfg.ConfigOpts()
    conf(args=[])

    if effective_file and Path(effective_file).is_file():
        enforcer = policy.Enforcer(conf, policy_file=str(effective_file))
    else:
        enforcer = policy.Enforcer(conf)

    enforcer.register_defaults(DEFAULT_RULES)
    enforcer.load_rules()

    if policy_file is None:
        _ENFORCER = enforcer
    return enforcer


def build_credentials(token_info: dict[str, Any]) -> dict[str, Any]:
    """Convert token_info into oslo.policy credentials dictionary."""
    roles = set(token_info.get("roles") or [])
    is_admin = token_info.get("is_system_admin") is True
    return {
        "user_id": token_info.get("user_id") or "",
        "project_id": token_info.get("project_id") or "",
        "roles": sorted(roles),
        "is_admin": is_admin,
        "is_system_admin": is_admin,
    }


def authorize(
    rule_name: str,
    target: dict[str, Any] | None,
    token_info: dict[str, Any],
    do_raise: bool = True,
    policy_file: str | None = None,
) -> bool:
    """Authorize an action using oslo.policy."""
    enforcer = get_enforcer(policy_file=policy_file)
    creds = build_credentials(token_info)
    target_dict = target if target is not None else {"project_id": creds["project_id"]}

    unsafe_roles = not creds["is_system_admin"] and bool({"admin", "manager"}.intersection(creds["roles"]))
    allowed = not unsafe_roles and enforcer.enforce(rule_name, target_dict, creds)
    if not allowed and do_raise:
        raise HTTPException(status_code=403, detail="Policy enforcement failed: access denied")
    return allowed


def require_policy(
    rule_name: str,
    target_provider: Callable[[Request, dict[str, Any]], dict[str, Any]] | None = None,
):
    """FastAPI dependency adapter for enforcing named policy rules."""
    async def _policy_dependency(
        request: Request,
        token_info: dict[str, Any] = Depends(require_token),
    ) -> dict[str, Any]:
        target = target_provider(request, token_info) if target_provider else {"project_id": token_info.get("project_id", "")}
        authorize(rule_name, target, token_info)
        return token_info

    return _policy_dependency


def authorize_workload_namespace(namespace: str, token_info: dict[str, Any]) -> None:
    """Workload editors operate only in their isolated principal namespace."""
    project_id = token_info.get("project_id", "")
    authorize("drover:workloads:write", {"project_id": project_id}, token_info)
    if authorize("drover:access:admin", {"project_id": project_id}, token_info, do_raise=False):
        return
    from drover.services.credentials import workload_namespace

    if not token_info.get("user_id") or namespace != workload_namespace(project_id, token_info["user_id"]):
        raise HTTPException(status_code=403, detail="Workload namespace access denied")


async def require_inventory(token_info: dict[str, Any] = Depends(require_token)) -> dict[str, Any]:
    authorize("drover:clusters:get", {"project_id": token_info.get("project_id", "")}, token_info)
    return token_info


async def require_workload_access(request: Request, token_info: dict[str, Any] = Depends(require_token)) -> dict[str, Any]:
    namespace = request.path_params.get("namespace") or request.query_params.get("namespace", "default")
    authorize_workload_namespace(namespace, token_info)
    return token_info


async def require_workload_or_inventory(request: Request, token_info: dict[str, Any] = Depends(require_token)) -> dict[str, Any]:
    if request.method in {"GET", "HEAD"}:
        authorize("drover:clusters:get", {"project_id": token_info.get("project_id", "")}, token_info)
    else:
        namespace = request.path_params.get("namespace") or request.query_params.get("namespace", "default")
        authorize_workload_namespace(namespace, token_info)
    return token_info
