"""The generated sandbox RBAC must satisfy Lumen's own RBAC preflight.

``lumen_check_rbac`` is a port of ``KubernetesSandbox._check_rbac`` in Lumen
(``lumen/services/infrastructure/kubernetes.py`` at b129ec0), applied to the rules a
SelfSubjectRulesReview would return for the issued ServiceAccount. It is a copy and can
drift from Lumen; it is a regression guard for the verbs/resources Lumen refuses, not
proof against a real cluster.
"""

from __future__ import annotations

import pytest

from drover.services.sandbox_connection import NAMESPACE_RULES, cluster_rules


def lumen_check_rbac(rules: list[dict]) -> None:
    required = {
        ("", "pods"): {"get", "list", "create", "delete"},
        ("", "secrets"): {"get", "create", "delete"},
        ("", "services"): {"get", "list", "create", "delete"},
        ("networking.k8s.io", "networkpolicies"): {"get", "list", "create", "delete"},
    }
    for (group, resource), verbs in required.items():
        allowed: set[str] = set()
        for rule in rules:
            if (group in rule.get("apiGroups", []) and resource in rule.get("resources", [])
                    and not rule.get("resourceNames")):
                allowed.update(rule.get("verbs", []))
        if not verbs <= allowed:
            raise ValueError("Kubernetes namespace RBAC insufficient")
    for rule in rules:
        resources, verbs = rule.get("resources", []), rule.get("verbs", [])
        if ("*" in rule.get("apiGroups", []) or "*" in resources or "*" in verbs
                or ("secrets" in resources and {"list", "watch"} & set(verbs))):
            raise ValueError("Kubernetes credential exceeds least-privilege resource scope")
        for group in rule.get("apiGroups", []):
            for resource in resources:
                permitted = required.get((group, resource), set())
                if (group, resource) == ("", "namespaces") or (group, resource) == ("node.k8s.io", "runtimeclasses"):
                    permitted = {"get"}
                elif (group, resource) in {
                    ("authorization.k8s.io", "selfsubjectaccessreviews"),
                    ("authorization.k8s.io", "selfsubjectrulesreviews"),
                    ("authentication.k8s.io", "selfsubjectreviews"),
                }:
                    permitted = {"create"}
                if not set(verbs) <= permitted:
                    raise ValueError("Kubernetes credential grants unrelated resource operations")


def _effective_rules() -> list[dict]:
    return NAMESPACE_RULES + cluster_rules("drover-sandbox-abc", "gvisor")


def test_generated_rules_satisfy_lumen_preflight():
    lumen_check_rbac(_effective_rules())


@pytest.mark.parametrize("extra", [
    {"apiGroups": [""], "resources": ["pods"], "verbs": ["watch"]},
    {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]},
    {"apiGroups": [""], "resources": ["secrets"], "verbs": ["list"]},
    {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["create"]},
    {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]},
])
def test_lumen_preflight_port_rejects_broader_rules(extra):
    with pytest.raises(ValueError):
        lumen_check_rbac(_effective_rules() + [extra])
