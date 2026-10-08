"""Fresh principal-bound Kubernetes credentials; stored certificates never leave the server.

``issue_kubeconfig(cluster_id, token_info, grade='user'|'editor')`` returns
``IssuedKubeconfig``. Shell callers always request a restricted grade, including
admins. ``namespace`` is the private workload namespace, not the shell runtime.
Kubernetes TokenRequest requires at least 600 seconds; shorter Keystone sessions
fail closed. Editor issuance requires v1 ValidatingAdmissionPolicy support.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import yaml
from fastapi import HTTPException

from drover.policy import authorize, build_credentials
from drover.services import kube, store
from drover.services.errors import K3sApiError

TOKEN_TTL_SECONDS = 900
ISSUER_NAMESPACE = "drover-credentials"
ADMISSION_PROBE_ATTEMPTS = 20
ADMISSION_PROBE_INTERVAL_SECONDS = 0.5
READ_RULES = [
    {"apiGroups": [""], "resources": ["namespaces", "nodes", "pods", "pods/log", "services", "endpoints", "events"],
     "verbs": ["get", "list", "watch"]},
    {"apiGroups": ["apps"], "resources": ["deployments", "replicasets", "statefulsets", "daemonsets"],
     "verbs": ["get", "list", "watch"]},
    {"apiGroups": ["batch"], "resources": ["jobs", "cronjobs"], "verbs": ["get", "list", "watch"]},
]
EDITOR_RULES = [
    {"apiGroups": [""], "resources": ["pods", "configmaps"],
     "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
    {"apiGroups": [""], "resources": ["pods/log", "events"], "verbs": ["get", "list", "watch"]},
    {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["get", "create"]},
    {"apiGroups": ["apps"], "resources": ["deployments", "replicasets", "statefulsets", "daemonsets", "deployments/scale", "statefulsets/scale", "replicasets/scale"],
     "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
    {"apiGroups": ["batch"], "resources": ["jobs", "cronjobs"],
     "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
]


@dataclass(frozen=True)
class IssuedKubeconfig:
    kubeconfig: str
    expires_at: datetime
    namespace: str
    service_account: str


def _principal(project_id: str, user_id: str) -> str:
    if not all(isinstance(value, str) and value.strip() and value == value.strip() for value in (project_id, user_id)):
        raise HTTPException(401, "A project-scoped user principal is required")
    return hashlib.sha256(json.dumps([project_id, user_id], separators=(",", ":")).encode()).hexdigest()[:32]


def workload_namespace(project_id: str, user_id: str) -> str:
    """Stable DNS-safe namespace bound to both Keystone project and user IDs."""
    return "drover-workload-" + _principal(project_id, user_id)


def _expiry(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("timezone required")
        return result.astimezone(UTC)
    except (AttributeError, TypeError, ValueError):
        raise HTTPException(401, "A valid token expiration is required") from None


async def _apply(client: httpx.AsyncClient, server: str, collection: str, body: dict) -> dict:
    """Server-side apply reconciles only issuance-owned fields; no permissive fallback."""
    response = await client.patch(
        f"{server}{collection}/{body['metadata']['name']}",
        params={"fieldManager": "drover-credentials", "force": "true"},
        headers={"Content-Type": "application/apply-patch+yaml"},
        content=yaml.safe_dump(body),
    )
    if response.status_code not in (200, 201):
        # Do not echo provider responses, which can contain bearer credentials.
        raise K3sApiError(502, "Kubernetes credential provisioning failed")
    return response.json()


def _namespace(name: str, *, workload: bool = False) -> dict:
    labels = {"pod-security.kubernetes.io/enforce": "restricted", "pod-security.kubernetes.io/enforce-version": "latest"}
    if workload:
        labels["drover.io/restricted-workload"] = "true"
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name, "labels": labels}}


async def _admission(client: httpx.AsyncClient, server: str, namespace: str) -> None:
    """Pods must not turn workload creation into credential/secret extraction."""
    name = "drover-restricted-workload"
    expressions = [
        "!has(object.spec.serviceAccountName) || object.spec.serviceAccountName == 'default'",
        "!has(object.spec.automountServiceAccountToken) || object.spec.automountServiceAccountToken == false",
        "!has(object.spec.volumes) || object.spec.volumes.all(v, has(v.emptyDir) || has(v.configMap) || has(v.downwardAPI))",
        "!has(object.spec.imagePullSecrets) || size(object.spec.imagePullSecrets) == 0",
    ]
    for field in ("containers", "initContainers", "ephemeralContainers"):
        expressions.append(
            f"!has(object.spec.{field}) || object.spec.{field}.all(c, "
            "(!has(c.env) || c.env.all(e, !has(e.valueFrom) || !has(e.valueFrom.secretKeyRef))) && "
            "(!has(c.envFrom) || c.envFrom.all(e, !has(e.secretRef))))"
        )
    await _apply(client, server, "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies", {
        "apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": name},
        "spec": {"failurePolicy": "Fail", "matchConstraints": {"resourceRules": [{
            "apiGroups": [""], "apiVersions": ["v1"], "operations": ["CREATE", "UPDATE"],
            "resources": ["pods", "pods/ephemeralcontainers"],
        }]}, "validations": [{"expression": expr, "message": "Restricted workload credential isolation"} for expr in expressions]},
    })
    await _apply(client, server, "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings", {
        "apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": name}, "spec": {"policyName": name, "validationActions": ["Deny"],
        "matchResources": {"namespaceSelector": {"matchLabels": {"drover.io/restricted-workload": "true"}}}},
    })
    # Status/typeChecking is asynchronous and not evidence admission is enabled.
    # A dry-run pod must be rejected by this specific binding before any token
    # can grant workload creation. Dry-run never persists or executes a pod.
    # A freshly applied policy/binding reaches the apiserver's admission cache
    # asynchronously (observed ~1 s on k3s v1.31), so poll for a bounded time
    # and fail closed if the probe is still admitted.
    probe = {"apiVersion": "v1", "kind": "Pod", "metadata": {"generateName": "drover-admission-probe-"},
             "spec": {"serviceAccountName": "default", "automountServiceAccountToken": True,
                      "securityContext": {"runAsNonRoot": True, "runAsUser": 65534,
                                          "seccompProfile": {"type": "RuntimeDefault"}},
                      "containers": [{"name": "probe", "image": "registry.k8s.io/pause:3.10",
                                      "securityContext": {"allowPrivilegeEscalation": False,
                                                          "capabilities": {"drop": ["ALL"]}}}]}}
    for attempt in range(ADMISSION_PROBE_ATTEMPTS):
        if attempt:
            await asyncio.sleep(ADMISSION_PROBE_INTERVAL_SECONDS)
        response = await client.post(f"{server}/api/v1/namespaces/{namespace}/pods", params={"dryRun": "All"}, json=probe)
        if response.status_code in (403, 422):
            message = response.json().get("message", "")
            if "drover-restricted-workload" in message and "Restricted workload credential isolation" in message:
                return
    raise K3sApiError(502, "Workload admission is not enforcing credential isolation")


async def _prepare_workload_namespace(client: httpx.AsyncClient, server: str, namespace: str) -> None:
    await _apply(client, server, "/api/v1/namespaces", _namespace(namespace, workload=True))
    await _apply(client, server, f"/api/v1/namespaces/{namespace}/serviceaccounts", {
        "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "default"},
        "automountServiceAccountToken": False,
    })
    await _admission(client, server, namespace)


async def ensure_workload_namespace(cluster_id: str, token_info: dict) -> str:
    """Prepare the editor namespace without issuing or discarding a bearer token."""
    project_id, user_id = token_info.get("project_id"), token_info.get("user_id")
    namespace = workload_namespace(project_id, user_id)
    authorize("drover:workloads:write", {"project_id": project_id}, token_info)
    if _expiry(token_info.get("expires_at")) <= datetime.now(UTC):
        raise HTTPException(401, "Token expired")
    cluster = await store.get_cluster(project_id, cluster_id)
    if not cluster or cluster.get("project_id") != project_id:
        raise HTTPException(404, "Cluster not found")
    try:
        async with kube._kube_client(cluster_id, project_id=project_id, verify_server=True) as (client, server):
            await _prepare_workload_namespace(client, server, namespace)
    except K3sApiError:
        raise
    except Exception:
        raise K3sApiError(502, "Workload namespace preparation failed") from None
    return namespace


async def issue_kubeconfig(cluster_id: str, token_info: dict, *, grade: str) -> IssuedKubeconfig:
    """Issue fresh RBAC/TokenRequest credentials, never cache or fall back to admin."""
    project_id, user_id = token_info.get("project_id"), token_info.get("user_id")
    principal = _principal(project_id, user_id)
    if grade not in ("user", "editor"):
        raise HTTPException(403, "Only restricted credential grades can be issued")
    credentials = build_credentials(token_info)
    required_role = "drover-workloads_editor" if grade == "editor" else "drover-access_user"
    if not credentials["is_system_admin"] and not ({required_role, "drover-access_admin"} & set(credentials["roles"])):
        raise HTTPException(403, "Credential grade access denied")
    authorize("drover:workloads:write" if grade == "editor" else "drover:access:get", {"project_id": project_id}, token_info)
    keystone_expiry = _expiry(token_info.get("expires_at"))
    deadline = min(keystone_expiry, datetime.now(UTC) + timedelta(seconds=TOKEN_TTL_SECONDS))
    if math.floor((deadline - datetime.now(UTC)).total_seconds()) < 600:
        raise HTTPException(401, "Keystone token has insufficient remaining lifetime")
    namespace = workload_namespace(project_id, user_id)
    account = f"drover-{grade}-{principal}"
    try:
        cluster = await store.get_cluster(project_id, cluster_id)
        if not cluster or cluster.get("project_id") != project_id:
            raise HTTPException(404, "Cluster not found")
        stored = await store.get_kubeconfig(project_id=project_id, cluster_id=cluster_id)
        if not stored:
            raise K3sApiError(502, "Cluster credentials are not ready")
        stored_cluster = yaml.safe_load(stored)["clusters"][0]["cluster"]
        # Whitelist public connection material; do not copy users, exec, or auth-provider.
        public_cluster = {key: stored_cluster[key] for key in ("server", "certificate-authority-data")}
        async with kube._kube_client(cluster_id, project_id=project_id, verify_server=True) as (client, server):
            await _apply(client, server, "/api/v1/namespaces", _namespace(ISSUER_NAMESPACE))
            sa = await _apply(client, server, f"/api/v1/namespaces/{ISSUER_NAMESPACE}/serviceaccounts", {
                "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": account},
                "automountServiceAccountToken": False,
            })
            subject = {"kind": "ServiceAccount", "name": account, "namespace": ISSUER_NAMESPACE}
            rbac = "/apis/rbac.authorization.k8s.io/v1"
            if grade == "editor":
                await _prepare_workload_namespace(client, server, namespace)
                role_path, binding_path, kind = f"{rbac}/namespaces/{namespace}/roles", f"{rbac}/namespaces/{namespace}/rolebindings", "Role"
                rules = EDITOR_RULES
            else:
                role_path, binding_path, kind = f"{rbac}/clusterroles", f"{rbac}/clusterrolebindings", "ClusterRole"
                rules = READ_RULES
            await _apply(client, server, role_path, {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": kind,
                "metadata": {"name": account}, "rules": rules,
            })
            await _apply(client, server, binding_path, {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": kind + "Binding",
                "metadata": {"name": account}, "subjects": [subject],
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": kind, "name": account},
            })
            ttl = math.floor((deadline - datetime.now(UTC)).total_seconds())
            if ttl < 600:
                raise HTTPException(401, "Keystone token has insufficient remaining lifetime")
            response = await client.post(f"{server}/api/v1/namespaces/{ISSUER_NAMESPACE}/serviceaccounts/{account}/token", json={
                "apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest", "spec": {"expirationSeconds": ttl},
            })
            if response.status_code not in (200, 201):
                raise K3sApiError(502, "Kubernetes token issuance failed")
            status = response.json()["status"]
            token = status["token"]
            try:
                expiry = _expiry(status["expirationTimestamp"])
            except HTTPException:
                raise K3sApiError(502, "Kubernetes token expiration is invalid") from None
            # Provider-authenticated JWT claims must agree with the requested identity and expiry.
            payload = token.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            identity = claims["kubernetes.io"]
            jwt_expiry = datetime.fromtimestamp(claims["exp"], UTC)
            if (claims["sub"] != f"system:serviceaccount:{ISSUER_NAMESPACE}:{account}"
                    or identity["namespace"] != ISSUER_NAMESPACE
                    or identity["serviceaccount"]["name"] != account
                    or identity["serviceaccount"]["uid"] != sa["metadata"]["uid"]
                    or not datetime.now(UTC) < expiry <= deadline
                    or not datetime.now(UTC) < jwt_expiry <= deadline
                    or jwt_expiry != expiry):
                raise K3sApiError(502, "Kubernetes token identity or lifetime mismatch")
        config = {"apiVersion": "v1", "kind": "Config", "clusters": [{"name": cluster_id, "cluster": public_cluster}],
                  "users": [{"name": account, "user": {"token": token}}],
                  "contexts": [{"name": account, "context": {"cluster": cluster_id, "user": account, "namespace": namespace}}],
                  "current-context": account}
        return IssuedKubeconfig(yaml.safe_dump(config), expiry, namespace, account)
    except HTTPException:
        raise
    except K3sApiError:
        raise
    except Exception:
        raise K3sApiError(502, "Kubernetes credential issuance failed") from None
