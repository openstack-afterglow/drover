"""NVIDIA worker runtime bootstrap and the cluster device-plugin boundary."""

import json
import re

import httpx

from drover.services import kube, store

_TOOLKIT_VERSION = "1.20.1-1"
_DEVICE_PLUGIN_IMAGE = "nvcr.io/nvidia/k8s-device-plugin:v0.17.3"
_DEVICE_PLUGIN_NAME = "afterglow-nvidia-device-plugin"
_NON_GPU_PCI_ALIAS_TOKENS = frozenset(
    {"audio", "crypto", "fpga", "infiniband", "network", "nic", "nvme", "qat", "rdma", "sriov"}
)


def _gpu_alias_counts(extra_specs: dict) -> dict[str, int]:
    aliases: dict[str, int] = {}
    for entry in str(extra_specs.get("pci_passthrough:alias", "")).split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            name, _, count_text = entry.rpartition(":")
            try:
                count = max(1, int(count_text.strip()))
            except ValueError:
                count = 1
        else:
            name, count = entry, 1
        name = name.strip()
        tokens = {token for token in re.split(r"[^a-z0-9]+", name.lower()) if token}
        if "audio" in name.lower() or tokens & _NON_GPU_PCI_ALIAS_TOKENS:
            continue
        aliases[name] = aliases.get(name, 0) + count
    return aliases


def flavor_gpu_count(extra_specs: dict) -> int:
    raw = extra_specs.get("gpu_count")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    return sum(_gpu_alias_counts(extra_specs).values())


def node_labels(labels: dict | None, extra_specs: dict, *, gpu_required: bool) -> dict:
    """Use the same flavor-derived GPU identity for planning and kubelet registration."""
    effective = {key: value for key, value in (labels or {}).items()
                 if key not in {"afterglow.io/gpu", "afterglow.io/gpu-type"}}
    if not gpu_required:
        return effective
    effective["afterglow.io/gpu"] = "true"
    # Match Afterglow GPU quota/inventory tokens before checking uniqueness.
    aliases = {re.sub(r"[^A-Z0-9]", "", alias.upper()) for alias in _gpu_alias_counts(extra_specs)}
    if len(aliases) == 1:
        gpu_type = next(iter(aliases))
        if gpu_type and len(gpu_type) <= 63:
            effective["afterglow.io/gpu-type"] = gpu_type
            # Kubernetes' conventional user label: derive it, but keep an explicit nodegroup value.
            effective.setdefault("accelerator", gpu_type)
    return effective


def agent_args(args: list[str]) -> list[str]:
    """Return GPU agent arguments without mutating the caller's list."""
    result = [*args, "--default-runtime=nvidia"]
    if "--node-label=afterglow.io/gpu=true" not in result:
        result.append("--node-label=afterglow.io/gpu=true")
    return result


def bootstrap_script(os_type: str) -> str:
    """Drivers are baked into the image; Ubuntu can install the signed toolkit."""
    install = ""
    if os_type == "ubuntu":
        install = f"""
if ! command -v nvidia-container-runtime >/dev/null; then
  export DEBIAN_FRONTEND=noninteractive
  timeout 300 apt-get update
  timeout 300 apt-get install -y --no-install-recommends curl ca-certificates gnupg
  curl --fail --show-error --location --connect-timeout 10 --max-time 60 \\
    https://nvidia.github.io/libnvidia-container/gpgkey \\
    | gpg --batch --yes --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
  printf '%s\\n' 'deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://nvidia.github.io/libnvidia-container/stable/deb/$(ARCH) /' \\
    > /etc/apt/sources.list.d/nvidia-container-toolkit.list
  timeout 300 apt-get update
  timeout 600 apt-get install -y --no-install-recommends \\
    nvidia-container-toolkit={_TOOLKIT_VERSION} nvidia-container-toolkit-base={_TOOLKIT_VERSION} \\
    libnvidia-container-tools={_TOOLKIT_VERSION} libnvidia-container1={_TOOLKIT_VERSION}
fi
"""
    return f"""# GPU workers require a matching, preinstalled NVIDIA kernel driver.
command -v nvidia-smi >/dev/null || {{ echo 'GPU image is missing the NVIDIA driver' >&2; exit 1; }}
timeout 30 nvidia-smi -L
{install}
command -v nvidia-container-runtime >/dev/null || {{ echo 'GPU image is missing the NVIDIA container runtime' >&2; exit 1; }}
# `nvidia-container-runtime --version` exits 1 until a low-level runtime (runc/crun) is on PATH,
# which K3s never provides before it starts; libnvidia-container checks the driver stack itself.
timeout 30 nvidia-container-cli info
"""


def _device_plugin_manifest() -> dict:
    labels = {"app.kubernetes.io/name": _DEVICE_PLUGIN_NAME}
    return {
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {
            "name": _DEVICE_PLUGIN_NAME, "namespace": "kube-system",
            "labels": {**labels, "app.kubernetes.io/managed-by": "drover"},
        },
        "spec": {
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "nodeSelector": {"afterglow.io/gpu": "true"},
                    "tolerations": [{"operator": "Exists"}],
                    "priorityClassName": "system-node-critical",
                    "runtimeClassName": "nvidia",
                    "containers": [{
                        "name": "device-plugin", "image": _DEVICE_PLUGIN_IMAGE,
                        "env": [
                            {"name": "FAIL_ON_INIT_ERROR", "value": "true"},
                            {"name": "NVIDIA_VISIBLE_DEVICES", "value": "all"},
                            {"name": "NVIDIA_DRIVER_CAPABILITIES", "value": "utility"},
                        ],
                        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
                        "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                        "volumeMounts": [{"name": "device-plugin", "mountPath": "/var/lib/kubelet/device-plugins"}],
                    }],
                    "volumes": [{"name": "device-plugin", "hostPath": {"path": "/var/lib/kubelet/device-plugins"}}],
                },
            },
        },
    }


async def ensure_device_plugin(cluster_id: str) -> None:
    """Reuse an operator's plugin; otherwise apply Drover's GPU-only DaemonSet."""
    kubeconfig = await store.get_kubeconfig_admin(cluster_id)
    if not kubeconfig:
        raise RuntimeError("GPU bootstrap requires an admin kubeconfig")
    cert, key, server = kube._parse_kubeconfig(kubeconfig)
    context = kube._make_ssl_context(cert, key)
    endpoint = f"{server}/apis/apps/v1/namespaces/kube-system/daemonsets"
    async with httpx.AsyncClient(verify=context, timeout=30) as client:
        response = await client.get(endpoint)
        if response.status_code != 200:
            raise RuntimeError(f"GPU device-plugin discovery failed HTTP {response.status_code}")
        for daemonset in response.json().get("items", []):
            metadata = daemonset.get("metadata") or {}
            if metadata.get("name") == _DEVICE_PLUGIN_NAME:
                if (metadata.get("labels") or {}).get("app.kubernetes.io/managed-by") != "drover":
                    raise RuntimeError("GPU device-plugin name belongs to another controller")
                continue
            containers = (((daemonset.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
            if any("k8s-device-plugin" in container.get("image", "") or "nvidia-device-plugin" in container.get("image", "") for container in containers):
                return
        response = await client.patch(
            f"{endpoint}/{_DEVICE_PLUGIN_NAME}",
            params={"fieldManager": "drover-stampede"},
            headers={"Content-Type": "application/apply-patch+yaml"},
            content=json.dumps(_device_plugin_manifest()),
        )
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"GPU device-plugin apply failed HTTP {response.status_code}")
