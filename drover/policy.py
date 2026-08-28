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

DEFAULT_RULES: list[policy.RuleDefault] = [
    policy.RuleDefault(
        name="context_is_admin",
        check_str="role:admin or is_system_admin:True",
        description="System admin privileges check",
    ),
    policy.RuleDefault(
        name="owner",
        check_str="project_id:%(project_id)s",
        description="Resource owner check",
    ),
    policy.RuleDefault(
        name="admin_or_owner",
        check_str="rule:context_is_admin or rule:owner",
        description="Admin or owner check",
    ),
    policy.RuleDefault(
        name="drover:clusters:get",
        check_str="rule:admin_or_owner",
        description="List or retrieve cluster details",
    ),
    policy.RuleDefault(
        name="drover:clusters:create",
        check_str="rule:admin_or_owner",
        description="Create a cluster",
    ),
    policy.RuleDefault(
        name="drover:clusters:delete",
        check_str="rule:admin_or_owner",
        description="Delete a cluster",
    ),
    policy.RuleDefault(
        name="drover:clusters:scale",
        check_str="rule:admin_or_owner",
        description="Scale a cluster",
    ),
    policy.RuleDefault(
        name="drover:templates:manage",
        check_str="rule:context_is_admin",
        description="Create, update, or delete cluster templates",
    ),
    policy.RuleDefault(
        name="drover:operations:get",
        check_str="rule:admin_or_owner",
        description="Retrieve operation details or events",
    ),
    policy.RuleDefault(
        name="drover:admin",
        check_str="rule:context_is_admin",
        description="System administrative actions",
    ),
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
    roles = token_info.get("roles") or []
    is_admin = bool(token_info.get("is_system_admin")) or ("admin" in roles)
    return {
        "user_id": token_info.get("user_id") or "",
        "project_id": token_info.get("project_id") or "",
        "roles": roles,
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

    allowed = enforcer.enforce(rule_name, target_dict, creds)
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
