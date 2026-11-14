"""Least-privilege Kubernetes connection for an isolated sandbox controller.

``issue_sandbox_connection`` prepares one namespace per (cluster, deployment, pool)
and returns a short-lived bearer token whose RBAC is exactly what a sandbox
controller needs there: pods, services, secrets and network policies in that
namespace, ``get`` on that namespace and one named RuntimeClass, and the
self-review calls used to prove those limits. Nothing is cached or stored; every
call applies the same objects (server-side apply) and requests a fresh token.

The token lifetime is bounded by ``SANDBOX_TOKEN_TTL_SECONDS`` and by the caller's
Keystone token, like every other Drover-issued Kubernetes credential. Kubernetes
TokenRequest needs at least 600 seconds, so shorter sessions fail closed.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import yaml
from fastapi import HTTPException

from drover.policy import authorize
from drover.services import credentials, kube, store
from drover.services.errors import K3sApiError

SANDBOX_TOKEN_TTL_SECONDS = credentials.TOKEN_TTL_SECONDS
NETWORK_POLICY_NAME = "lumen-default-deny"
DEPLOYMENT_LABEL = "lumen.dev/deployment"
POOL_LABEL = "lumen.dev/pool"
DEFAULT_RUNTIME_CLASS = "gvisor"

_RBAC = "rbac.authorization.k8s.io/v1"
_LABEL_VALUE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_RUNTIME_CLASS = re.compile(r"^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$")

# Namespaced: exactly the operations Lumen's `_check_rbac` accepts (it refuses any other
# verb or resource, e.g. pods `watch` and `pods/log`). No list/watch on secrets, no
# wildcards, no pods/exec, no RBAC, no serviceaccounts, no deployments.
NAMESPACE_RULES = [
    {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "create", "delete"]},
    {"apiGroups": [""], "resources": ["services"], "verbs": ["get", "list", "create", "delete"]},
    {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get", "create", "delete"]},
    {"apiGroups": ["networking.k8s.io"], "resources": ["networkpolicies"], "verbs": ["get", "list", "create", "delete"]},
]


@dataclass(frozen=True)
class SandboxConnection:
    server: str
    ca_pem: str
    namespace: str
    namespace_uid: str
    service_account: str
    token: str
    expires_at: datetime
    runtime_class_name: str


def label_value(raw: str) -> str:
    """Kubernetes label value for ``raw``; anything not a valid value is hashed.

    The exact original text is always kept in the matching annotation.
    """
    if _LABEL_VALUE.fullmatch(raw):
        return raw
    return "h-" + hashlib.sha256(raw.encode()).hexdigest()[:60]


def _identity(project_id: str, cluster_id: str, deployment_id: str, pool_name: str) -> str:
    payload = json.dumps([project_id, cluster_id, deployment_id, pool_name], separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:24]


def sandbox_namespace(project_id: str, cluster_id: str, deployment_id: str, pool_name: str) -> str:
    return "drover-sandbox-" + _identity(project_id, cluster_id, deployment_id, pool_name)


def _validate(deployment_id: str, pool_name: str, runtime_class_name: str) -> None:
    for value in (deployment_id, pool_name):
        if not isinstance(value, str) or not _NAME.fullmatch(value):
            raise HTTPException(422, "deployment_id and pool_name must be 1-128 safe characters")
    if not isinstance(runtime_class_name, str) or not _RUNTIME_CLASS.fullmatch(runtime_class_name):
        raise HTTPException(422, "runtime_class_name must be a valid RuntimeClass name")


def cluster_rules(namespace: str, runtime_class_name: str) -> list[dict]:
    return [
        {"apiGroups": [""], "resources": ["namespaces"], "resourceNames": [namespace], "verbs": ["get"]},
        {"apiGroups": ["node.k8s.io"], "resources": ["runtimeclasses"],
         "resourceNames": [runtime_class_name], "verbs": ["get"]},
        {"apiGroups": ["authorization.k8s.io"], "resources": ["selfsubjectaccessreviews", "selfsubjectrulesreviews"],
         "verbs": ["create"]},
        {"apiGroups": ["authentication.k8s.io"], "resources": ["selfsubjectreviews"], "verbs": ["create"]},
    ]


def _network_policy() -> dict:
    return {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
            "metadata": {"name": NETWORK_POLICY_NAME},
            "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]}}


async def issue_sandbox_connection(
    cluster_id: str, token_info: dict, *, deployment_id: str, pool_name: str,
    runtime_class_name: str = DEFAULT_RUNTIME_CLASS,
) -> SandboxConnection:
    project_id, user_id = token_info.get("project_id"), token_info.get("user_id")
    credentials._principal(project_id, user_id)
    authorize("drover:sandbox:connect", {"project_id": project_id}, token_info)
    _validate(deployment_id, pool_name, runtime_class_name)
    keystone_expiry = credentials._expiry(token_info.get("expires_at"))
    deadline = min(keystone_expiry, datetime.now(UTC) + timedelta(seconds=SANDBOX_TOKEN_TTL_SECONDS))
    if math.floor((deadline - datetime.now(UTC)).total_seconds()) < 600:
        raise HTTPException(401, "Keystone token has insufficient remaining lifetime")
    namespace = sandbox_namespace(project_id, cluster_id, deployment_id, pool_name)
    account = "drover-sandbox-" + _identity(project_id, cluster_id, deployment_id, pool_name)
    try:
        cluster = await store.get_cluster(project_id, cluster_id)
        if not cluster or cluster.get("project_id") != project_id:
            raise HTTPException(404, "Cluster not found")
        if cluster.get("status") != "ACTIVE":
            raise HTTPException(409, "Cluster is not ACTIVE")
        stored = await store.get_kubeconfig(project_id=project_id, cluster_id=cluster_id)
        if not stored:
            raise K3sApiError(502, "Cluster credentials are not ready")
        stored_cluster = yaml.safe_load(stored)["clusters"][0]["cluster"]
        server_url = stored_cluster["server"]
        ca_pem = base64.b64decode(stored_cluster["certificate-authority-data"]).decode()
        subject = {"kind": "ServiceAccount", "name": account, "namespace": credentials.ISSUER_NAMESPACE}
        async with kube._kube_client(cluster_id, project_id=project_id, verify_server=True) as (client, server):
            apply = credentials._apply
            await apply(client, server, "/api/v1/namespaces", credentials._namespace(credentials.ISSUER_NAMESPACE))
            body = credentials._namespace(namespace)
            body["metadata"]["labels"].update({DEPLOYMENT_LABEL: label_value(deployment_id), POOL_LABEL: label_value(pool_name)})
            body["metadata"]["annotations"] = {DEPLOYMENT_LABEL: deployment_id, POOL_LABEL: pool_name}
            ns = await apply(client, server, "/api/v1/namespaces", body)
            await apply(client, server, f"/apis/networking.k8s.io/v1/namespaces/{namespace}/networkpolicies",
                        _network_policy())
            sa = await apply(client, server, f"/api/v1/namespaces/{credentials.ISSUER_NAMESPACE}/serviceaccounts", {
                "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": account},
                "automountServiceAccountToken": False,
            })
            base = f"/apis/{_RBAC}"
            for role_path, binding_path, kind, rules in (
                (f"{base}/namespaces/{namespace}/roles", f"{base}/namespaces/{namespace}/rolebindings",
                 "Role", NAMESPACE_RULES),
                (f"{base}/clusterroles", f"{base}/clusterrolebindings",
                 "ClusterRole", cluster_rules(namespace, runtime_class_name)),
            ):
                await apply(client, server, role_path, {
                    "apiVersion": _RBAC, "kind": kind, "metadata": {"name": account}, "rules": rules,
                })
                await apply(client, server, binding_path, {
                    "apiVersion": _RBAC, "kind": kind + "Binding", "metadata": {"name": account},
                    "subjects": [subject], "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": kind, "name": account},
                })
            token, expiry = await credentials._request_token(client, server, account, sa["metadata"]["uid"], deadline)
        return SandboxConnection(server_url, ca_pem, namespace, ns["metadata"]["uid"], account, token, expiry,
                                 runtime_class_name)
    except (HTTPException, K3sApiError):
        raise
    except Exception:
        raise K3sApiError(502, "Sandbox connection issuance failed") from None
