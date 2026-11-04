"""Centralized oslo.policy enforcement and FastAPI dependencies for Drover."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from threading import RLock, get_ident
from typing import Any

import yaml
from fastapi import Depends, HTTPException, Request
from oslo_config import cfg
from oslo_policy import _checks, _parser, policy

from drover.auth import require_token
from drover.config import get_settings

_logger = logging.getLogger(__name__)

_ENFORCER: policy.Enforcer | None = None
_ENFORCER_KEY: tuple[str, tuple[str, ...]] | None = None
_LOCK = RLock()


class _ParserFailureReporter:
    """Turn only this thread's native parser errors into load failure.

    Oslo reports invalid clauses as log errors plus FalseCheck, which can become
    True when negated. No expression grammar or evaluator is duplicated here.
    The wrapper does not depend on operator logging levels or logging.disable.
    """

    def __init__(self, logger):
        self.logger = logger
        self.thread = get_ident()

    def __getattr__(self, name):
        return getattr(self.logger, name)

    def error(self, *args, **kwargs):
        if get_ident() == self.thread:
            raise ValueError("Invalid oslo.policy expression")
        return self.logger.error(*args, **kwargs)

    def exception(self, *args, **kwargs):
        if get_ident() == self.thread:
            raise ValueError("Invalid oslo.policy expression")
        return self.logger.exception(*args, **kwargs)


def _validate_oslo_expression(expression: str) -> None:
    # Policy loads are serialized by _LOCK. Other threads retain ordinary native
    # logger behavior; restore the logger even when native parsing raises.
    with _LOCK:
        original = _parser.LOG
        try:
            _parser.LOG = _ParserFailureReporter(original)
            _parser.parse_rule(expression)
        finally:
            _parser.LOG = original


def _validate_native_rule_graph(engine: policy.Enforcer) -> None:
    """Walk parsed native references, including negation's singular child."""
    complete: set[str] = set()

    def visit_rule(name: str, active: frozenset[str]) -> None:
        if name not in engine.rules or name in active:
            raise ValueError("Undefined or cyclic native policy reference")
        if name in complete:
            return
        visit_check(engine.rules[name], active | {name})
        complete.add(name)

    def visit_check(check, active: frozenset[str]) -> None:
        if isinstance(check, _checks.RuleCheck):
            visit_rule(check.match, active)
        elif isinstance(check, _checks.NotCheck):
            visit_check(check.rule, active)
        else:
            for child in getattr(check, "rules", ()):
                visit_check(child, active)

    for name in engine.rules:
        visit_rule(name, frozenset())


class _Enforcer(policy.Enforcer):
    def load_rules(self, force_reload: bool = False) -> None:
        # Enforcer.authorize performs another reload internally: validate every
        # reload, not only the preceding capability/action wrapper.
        # self.policy_file falls back to Oslo's default at construction (the
        # conf override lands after), so judge the configured value instead.
        # "" is the explicit defaults-only convention; a missing default file
        # is normal (registered defaults apply).
        configured = self.conf.oslo_policy.policy_file
        if any(not Path(directory).is_dir() for directory in self.conf.oslo_policy.policy_dirs):
            raise ValueError("Configured policy directory is unavailable")
        if configured not in ("", "/etc/drover/policy.yaml") and self.conf.find_file(configured) is None:
            raise ValueError("Configured policy file is unavailable")
        super().load_rules(force_reload=force_reload)
        _validate_native_rule_graph(self)


    def _record_file_rules(self, data: str, overwrite: bool = False) -> None:
        # Oslo's legacy list syntax treats null/false/[] rules as unconditional
        # allow. Operator YAML must be a mapping of names to expression strings.
        # Expression parsing/evaluation and file reloads remain entirely Oslo's.
        document = yaml.safe_load(data)
        if document is not None and (not isinstance(document, dict) or any(
            not isinstance(name, str) or not name or not isinstance(check, str)
            for name, check in document.items()
        )):
            raise ValueError("Policy YAML must map rule names to expression strings")
        for expression in (document or {}).values():
            _validate_oslo_expression(expression)
        super()._record_file_rules(data, overwrite=overwrite)


def _project_rule(name: str, *, read_only: bool = False) -> policy.RuleDefault:
    roles = ("admin", "manager", "member", "reader") if read_only else ("admin", "manager", "member")
    checks = " or ".join(f"role:{role}" for role in roles)
    return policy.RuleDefault(name, f"rule:context_is_admin or (rule:owner and ({checks}))")


DEFAULT_RULES: list[policy.RuleDefault] = [
    policy.RuleDefault("context_is_admin", "is_system_admin:True"),
    policy.RuleDefault("owner", "project_id:%(project_id)s"),
    _project_rule("drover:clusters:list", read_only=True),
    _project_rule("drover:clusters:get", read_only=True),
    _project_rule("drover:operations:get", read_only=True),
    _project_rule("drover:clusters:create"),
    _project_rule("drover:clusters:scale"),
    _project_rule("drover:clusters:delete"),
    _project_rule("drover:clusters:reauthorize"),
    # Actual superseded-credential ownership is independently checked in code.
    _project_rule("drover:clusters:retire_credentials"),
    # These GETs disclose workload content or issue access credentials, not metadata.
    _project_rule("drover:access:get"),
    _project_rule("drover:access:read"),
    _project_rule("drover:access:admin"),
    _project_rule("drover:workloads:write"),
    _project_rule("drover:certificates:rotate"),
    policy.RuleDefault("drover:templates:manage", "rule:context_is_admin"),
    policy.RuleDefault("drover:admin", "rule:context_is_admin"),
]

_SYSTEM_ACTIONS = frozenset({"drover:admin", "drover:templates:manage"})
_READ_ACTIONS = frozenset({"drover:clusters:list", "drover:clusters:get", "drover:operations:get"})
_MANAGE_ROLES = frozenset({"admin", "manager", "member"})
_READ_ROLES = _MANAGE_ROLES | {"reader"}
_RESTRICTED_ACTIONS = _SYSTEM_ACTIONS | frozenset({
    "drover:access:admin", "drover:clusters:create", "drover:clusters:scale",
    "drover:clusters:delete", "drover:clusters:reauthorize", "drover:clusters:retire_credentials",
    # Guest/control application credentials carry provider member authority, not
    # permission to mint further access or mutate other clusters in the project.
    "drover:access:get", "drover:access:read", "drover:workloads:write", "drover:certificates:rotate",
})
_ACTIONS = tuple(sorted(rule.name for rule in DEFAULT_RULES if rule.name.startswith("drover:")))
# UI labels only: never inserted into credentials or delegated to Keystone.
_CAPABILITY_ACTIONS = {
    "drover-inventory_reader": ("drover:clusters:list", "drover:clusters:get", "drover:operations:get"),
    "drover-clusters_editor": ("drover:clusters:create", "drover:clusters:scale"),
    "drover-clusters_admin": ("drover:clusters:delete", "drover:clusters:reauthorize", "drover:certificates:rotate"),
    "drover-access_user": ("drover:access:read",),
    "drover-workloads_editor": ("drover:workloads:write",),
    "drover-access_admin": ("drover:access:admin",),
}


def list_rules() -> list[policy.RuleDefault]:
    """Standard oslo.policy sample-generator entry point."""
    return DEFAULT_RULES


def reset_enforcer() -> None:
    """Discard cached policy, including failed reloads."""
    global _ENFORCER, _ENFORCER_KEY
    with _LOCK:
        _ENFORCER = None
        _ENFORCER_KEY = None


def get_enforcer(policy_file: str | None = None, reload: bool = False) -> policy.Enforcer:
    """Standard enforcer entry point using the service TOML/environment bridge."""
    global _ENFORCER, _ENFORCER_KEY
    settings = get_settings()
    effective_file = policy_file if policy_file is not None else settings.oslo_policy_policy_file
    directories = tuple(settings.oslo_policy_policy_dirs)
    if any(not Path(directory).is_dir() for directory in directories):
        raise ValueError("Configured policy directory is unavailable")
    key = (effective_file, directories)
    with _LOCK:
        if _ENFORCER is None or reload or key != _ENFORCER_KEY:
            conf = cfg.ConfigOpts()
            conf(args=[], project="drover", default_config_files=[], default_config_dirs=[], use_env=False)
            enforcer = _Enforcer(conf, policy_file=effective_file)
            conf.set_override("policy_file", effective_file, group="oslo_policy")
            conf.set_override("policy_dirs", list(directories), group="oslo_policy")
            enforcer.register_defaults(list_rules())
            # Missing default policy.yaml is normal: oslo uses registered defaults.
            enforcer.load_rules()
            if effective_file not in ("", "/etc/drover/policy.yaml") and enforcer.policy_path is None:
                raise ValueError("Configured policy file is unavailable")
            enforcer.check_rules(raise_on_violation=True)
            _ENFORCER, _ENFORCER_KEY = enforcer, key
        return _ENFORCER


def build_credentials(token_info: dict[str, Any]) -> dict[str, Any]:
    """Convert token_info into oslo.policy credentials dictionary."""
    roles = set(token_info.get("roles") or [])
    is_admin = token_info.get("is_system_admin") is True and token_info.get("credential_restricted") is not True
    return {
        "user_id": token_info.get("user_id") or "",
        "project_id": token_info.get("connection_project_id", token_info.get("project_id")) or "",
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
    if rule_name not in _ACTIONS:
        allowed = False
    else:
        creds = build_credentials(token_info)
        target_dict = target if target is not None else {"project_id": token_info.get("project_id", "")}
        # These boundaries are not operator-overridable policy rules.
        same_project = bool(creds["project_id"]) and target_dict.get("project_id") == creds["project_id"]
        allowed = bool(creds["is_system_admin"] or same_project)
        required_roles = _READ_ROLES if rule_name in _READ_ACTIONS else _MANAGE_ROLES
        if not creds["is_system_admin"] and not required_roles.intersection(creds["roles"]):
            allowed = False
        if rule_name in _SYSTEM_ACTIONS and not creds["is_system_admin"]:
            allowed = False
        if token_info.get("credential_restricted") is True and rule_name in _RESTRICTED_ACTIONS:
            allowed = False
        if allowed:
            try:
                with _LOCK:
                    enforcer = get_enforcer(policy_file=policy_file)
                    enforcer.load_rules()
                    enforcer.check_rules(raise_on_violation=True)
                    allowed = bool(enforcer.authorize(rule_name, target_dict, creds))
            except Exception:
                # Never retain partially loaded/default rules after a bad operator update.
                reset_enforcer()
                _logger.error("Drover policy could not be loaded or evaluated", exc_info=False)
                raise HTTPException(status_code=503, detail="Policy configuration unavailable") from None
    if not allowed and do_raise:
        raise HTTPException(status_code=403, detail="Policy enforcement failed: access denied")
    return allowed


def capabilities(token_info: dict[str, Any]) -> dict[str, Any]:
    """Safe deterministic current-project projection of the actual action engine."""
    target = {"project_id": token_info.get("project_id", "")}
    actions = [action for action in _ACTIONS if authorize(action, target, token_info, do_raise=False)]
    allowed = set(actions)
    labels = sorted(label for label, checks in _CAPABILITY_ACTIONS.items() if allowed.intersection(checks))
    return {"service": "drover", "capabilities": labels, "actions": actions}


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
