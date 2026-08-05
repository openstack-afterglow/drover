"""k3s Cloud Shell — ephemeral pod 세션 관리."""

from __future__ import annotations

import hashlib
import logging

import yaml
from fastapi import HTTPException

from drover.services import kube as k3s_kube
from drover.services import store as k3s_db

_logger = logging.getLogger(__name__)

SHELL_NAMESPACE = "afterglow-shell"
SHELL_IMAGE = "bitnami/kubectl:1.31"
IDLE_TIMEOUT_SECONDS = 15 * 60


def _user_hash(user_id: str) -> str:
    """deterministic 12자 lowercase hex — K8s 이름 호환."""
    return hashlib.sha256(user_id.encode()).hexdigest()[:12]


def pod_name(user_id: str) -> str:
    return f"cloud-shell-{_user_hash(user_id)}"


def pvc_name(user_id: str) -> str:
    return f"cloud-shell-home-{_user_hash(user_id)}"


def kubeconfig_secret_name(user_id: str) -> str:
    return f"cloud-shell-kc-{_user_hash(user_id)}"


def k8s_user_name(user_id: str) -> str:
    return f"afterglow-user-{_user_hash(user_id)}"


def build_shell_kubeconfig(admin_kubeconfig_yaml: str, user_id: str) -> str:
    """admin kubeconfig 에 impersonation 필드 추가하여 반환."""
    kc = yaml.safe_load(admin_kubeconfig_yaml)
    kc["users"][0]["user"]["as"] = k8s_user_name(user_id)
    return yaml.safe_dump(kc, default_flow_style=False)


def build_pod_manifest(user_id: str, kc_secret_name: str, pvc_name_: str) -> dict:
    """sleep infinity 로 유지되는 shell pod 매니페스트."""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name(user_id),
            "namespace": SHELL_NAMESPACE,
            "labels": {"app": "cloud-shell", "afterglow-user": _user_hash(user_id)},
        },
        "spec": {
            "automountServiceAccountToken": False,
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "shell",
                    "image": SHELL_IMAGE,
                    "command": ["sh", "-c", "sleep infinity"],
                    "stdin": True,
                    "tty": True,
                    "env": [
                        {"name": "HOME", "value": "/home/afterglow"},
                        {"name": "KUBECONFIG", "value": "/root/.kube/config"},
                    ],
                    "volumeMounts": [
                        {"name": "home", "mountPath": "/home/afterglow"},
                        {"name": "kubeconfig", "mountPath": "/root/.kube", "readOnly": True},
                    ],
                    "resources": {
                        "limits": {"memory": "256Mi", "cpu": "200m"},
                        "requests": {"memory": "64Mi", "cpu": "50m"},
                    },
                }
            ],
            "volumes": [
                {
                    "name": "home",
                    "persistentVolumeClaim": {"claimName": pvc_name_},
                },
                {
                    "name": "kubeconfig",
                    "secret": {
                        "secretName": kc_secret_name,
                        "items": [{"key": "config", "path": "config"}],
                    },
                },
            ],
        },
    }


async def ensure_session(cluster_id: str, user_id: str, *, project_id: str) -> str:
    """Cloud Shell 세션 보장 — 없으면 생성, 있으면 Running 확인.

    반환값: pod_name (str)
    """
    k8s_user = k8s_user_name(user_id)
    p_name = pod_name(user_id)
    pvc = pvc_name(user_id)
    kc_secret = kubeconfig_secret_name(user_id)

    # 1. 네임스페이스
    await k3s_kube.ensure_namespace(cluster_id, SHELL_NAMESPACE, project_id=project_id)

    # 2. ClusterRoleBinding (idempotent)
    await k3s_kube.ensure_cluster_role_binding_for_user(cluster_id, k8s_user, project_id=project_id)

    # 3. PVC (영속 홈 디렉터리)
    await k3s_kube.ensure_pvc(cluster_id, SHELL_NAMESPACE, pvc, project_id=project_id)

    # 4. kubeconfig Secret (매 세션마다 최신 kubeconfig 반영)
    admin_kc_yaml = await k3s_db.get_kubeconfig(project_id=project_id, cluster_id=cluster_id)
    if not admin_kc_yaml:
        admin_kc_yaml = await k3s_db.get_kubeconfig_admin(cluster_id)
    if not admin_kc_yaml:
        raise HTTPException(502, "kubeconfig 없음 — 클러스터가 아직 준비되지 않았습니다")
    shell_kc = build_shell_kubeconfig(admin_kc_yaml, user_id)
    await k3s_kube.create_k8s_secret(
        cluster_id, SHELL_NAMESPACE, kc_secret, {"config": shell_kc}, project_id=project_id
    )

    # 5. Pod 존재 확인
    existing = await k3s_kube.get_pod(cluster_id, SHELL_NAMESPACE, p_name, project_id=project_id)
    if existing:
        phase = existing.get("status", {}).get("phase", "")
        if phase == "Running":
            return p_name
        if phase == "Pending":
            ready = await k3s_kube.wait_pod_ready(
                cluster_id, SHELL_NAMESPACE, p_name, timeout=90.0, project_id=project_id
            )
            if ready:
                return p_name
        # Failed / Succeeded / Unknown 또는 timeout → 재생성
        await k3s_kube.delete_pod(cluster_id, SHELL_NAMESPACE, p_name, project_id=project_id)

    manifest = build_pod_manifest(user_id, kc_secret, pvc)
    await k3s_kube.create_pod(cluster_id, SHELL_NAMESPACE, manifest, project_id=project_id)
    ready = await k3s_kube.wait_pod_ready(cluster_id, SHELL_NAMESPACE, p_name, timeout=120.0, project_id=project_id)
    if not ready:
        raise HTTPException(504, "Shell pod 가 준비되지 않았습니다 (timeout)")
    return p_name
