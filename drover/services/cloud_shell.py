"""Isolated Cloud Shell pods with expiring, principal-bound workload credentials."""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import time
from datetime import UTC, datetime

from fastapi import HTTPException

from drover.policy import authorize_workload_namespace
from drover.services import credentials
from drover.services import kube as k3s_kube
from drover.services.errors import K3sApiError

SHELL_NAMESPACE = "afterglow-shell"
SHELL_IMAGE = "bitnami/kubectl:1.31"
IDLE_TIMEOUT_SECONDS = 15 * 60
SESSION_MAX_SECONDS = 60 * 60


def _user_hash(user_id: str) -> str:
    return hashlib.sha256(user_id.encode()).hexdigest()[:12]


def pod_name(user_id: str) -> str:
    return f"cloud-shell-v2-{_user_hash(user_id)}"


def pvc_name(user_id: str) -> str:
    # Never reuse a legacy home: it may contain copied administrator keys.
    return f"cloud-shell-home-v2-{_user_hash(user_id)}"


def kubeconfig_secret_name(pod: str) -> str:
    return f"{pod}-kc"


def session_expires_at(token_info: dict) -> float:
    try:
        expires = datetime.fromisoformat(token_info["expires_at"].replace("Z", "+00:00"))
        if expires.tzinfo is None:
            raise ValueError("unscoped expiry")
        expiry = expires.astimezone(UTC).timestamp()
    except (KeyError, TypeError, AttributeError, ValueError):
        raise HTTPException(401, "A Keystone token with expiration is required") from None
    if expiry <= time.time():
        raise HTTPException(401, "Keystone token expired")
    return min(expiry, time.time() + SESSION_MAX_SECONDS)


def build_pod_manifest(
    user_id: str, kc_secret_name: str, pvc_name_: str, *, name: str, project_id: str,
    workload_namespace: str, expires_at: float,
) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": SHELL_NAMESPACE,
            "labels": {"app": "cloud-shell", "afterglow-user": _user_hash(user_id)},
            "annotations": {
                "drover.io/user-id": user_id,
                "drover.io/project-id": project_id,
                "drover.io/workload-namespace": workload_namespace,
                "drover.io/credential-expiry": str(expires_at),
            },
        },
        "spec": {
            "automountServiceAccountToken": False,
            "activeDeadlineSeconds": max(1, int(expires_at - time.time())),
            "restartPolicy": "Never",
            "securityContext": {"runAsNonRoot": True, "runAsUser": 1001, "fsGroup": 1001},
            "containers": [{
                "name": "shell",
                "image": SHELL_IMAGE,
                "command": ["sh", "-c", "sleep infinity"],
                "stdin": True,
                "tty": True,
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "env": [
                    {"name": "HOME", "value": "/home/afterglow"},
                    {"name": "KUBECONFIG", "value": "/etc/drover/config"},
                ],
                "volumeMounts": [
                    {"name": "home", "mountPath": "/home/afterglow"},
                    {"name": "kubeconfig", "mountPath": "/etc/drover", "readOnly": True},
                ],
                "resources": {
                    "limits": {"memory": "256Mi", "cpu": "200m"},
                    "requests": {"memory": "64Mi", "cpu": "50m"},
                },
            }],
            "volumes": [
                {"name": "home", "persistentVolumeClaim": {"claimName": pvc_name_}},
                {"name": "kubeconfig", "secret": {
                    "secretName": kc_secret_name, "defaultMode": 0o440,
                    "items": [{"key": "config", "path": "config"}],
                }},
            ],
        },
    }


async def _delete_resource(client, url: str) -> None:
    response = await client.delete(url)
    if response.status_code not in (200, 202, 404):
        k3s_kube._raise_k8s_error(response, "remove obsolete shell resource")
    # A 202 is not deletion: do not admit a replacement while old pods run.
    deadline = time.monotonic() + 90
    while response.status_code != 404:
        response = await client.get(url)
        if response.status_code == 404:
            return
        if response.status_code != 200:
            k3s_kube._raise_k8s_error(response, "observe shell resource deletion")
        if time.monotonic() >= deadline:
            raise K3sApiError(504, "Obsolete shell resource deletion timed out")
        await asyncio.sleep(1)


async def retire_legacy_sessions(cluster_id: str, *, project_id: str) -> None:
    """Retire admin-bearing pods/Secrets/bindings; quarantine old homes by non-use."""
    paths = (
        (f"/api/v1/namespaces/{SHELL_NAMESPACE}/pods", r"cloud-shell-[0-9a-f]{12}"),
        ("/apis/rbac.authorization.k8s.io/v1/clusterrolebindings", r"afterglow-shell-afterglow-user-[0-9a-f]{12}"),
        (f"/api/v1/namespaces/{SHELL_NAMESPACE}/secrets", r"cloud-shell-kc-[0-9a-f]{12}"),
    )
    async with k3s_kube._kube_client(cluster_id, project_id=project_id) as (client, server):
        for path, pattern in paths:
            response = await client.get(server + path)
            if response.status_code == 404:
                continue
            if response.status_code != 200:
                k3s_kube._raise_k8s_error(response, "inventory obsolete shell resources")
            for item in response.json().get("items", []):
                name = item.get("metadata", {}).get("name", "")
                if re.fullmatch(pattern, name):
                    await _delete_resource(client, f"{server}{path}/{name}")


async def delete_session(cluster_id: str, pod: str, *, project_id: str) -> None:
    async with k3s_kube._kube_client(cluster_id, project_id=project_id) as (client, server):
        base = f"{server}/api/v1/namespaces/{SHELL_NAMESPACE}"
        await _delete_resource(client, f"{base}/pods/{pod}")
        await _delete_resource(client, f"{base}/secrets/{kubeconfig_secret_name(pod)}")


async def ensure_session(cluster_id: str, token_info: dict, *, project_id: str) -> str:
    """Issue a fresh restricted credential and pod, without sharing mutable Secrets."""
    user_id = token_info.get("user_id")
    if not user_id or token_info.get("project_id") != project_id:
        raise HTTPException(403, "Shell principal/project mismatch")
    namespace = credentials.workload_namespace(project_id, user_id)
    authorize_workload_namespace(namespace, token_info)
    expires_at = session_expires_at(token_info)
    await k3s_kube.ensure_namespace(cluster_id, SHELL_NAMESPACE, project_id=project_id)
    await retire_legacy_sessions(cluster_id, project_id=project_id)
    issued = await credentials.issue_kubeconfig(cluster_id, token_info, grade="editor")
    if issued.namespace != namespace:
        raise HTTPException(403, "Issued workload namespace mismatch")
    expires_at = min(expires_at, issued.expires_at.timestamp())
    if expires_at <= time.time():
        raise HTTPException(401, "Shell credential expired")
    pod = f"{pod_name(user_id)}-{secrets.token_hex(6)}"
    secret = kubeconfig_secret_name(pod)
    pvc = pvc_name(user_id)
    await k3s_kube.ensure_pvc(cluster_id, SHELL_NAMESPACE, pvc, project_id=project_id)
    try:
        await k3s_kube.create_k8s_secret(
            cluster_id, SHELL_NAMESPACE, secret, {"config": issued.kubeconfig}, project_id=project_id,
        )
        manifest = build_pod_manifest(
            user_id, secret, pvc, name=pod, project_id=project_id,
            workload_namespace=namespace, expires_at=expires_at,
        )
        await k3s_kube.create_pod(cluster_id, SHELL_NAMESPACE, manifest, project_id=project_id)
        ready = await k3s_kube.wait_pod_ready(
            cluster_id, SHELL_NAMESPACE, pod, timeout=120.0, project_id=project_id,
        )
        if not ready:
            raise HTTPException(504, "Shell pod readiness timed out")
        if expires_at <= time.time():
            raise HTTPException(401, "Shell credential expired during provisioning")
    except BaseException:
        await asyncio.shield(delete_session(cluster_id, pod, project_id=project_id))
        raise
    return pod
