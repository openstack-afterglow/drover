"""Roll a new guest application credential into a running K3s cluster and verify it took effect.

Only application-credential keys are replaced in place, so creation-time plugin configuration is never re-rendered.
Consumers are restarted and must finish rolling out; every control-plane host rewrites its Barbican KMS configuration
and restarts the KMS service; a Secret write then exercises the encryption path. Any failure raises before the
caller activates the new generation, leaving previously active credentials valid.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
import uuid

from drover.config import get_settings
from drover.services import kube as k3s_kube

_logger = logging.getLogger("drover.guest_rollout")

NAMESPACE = "kube-system"
CLOUD_CONFIG_SECRET = "cloud-config"
MANILA_SECRET = "manila-cloud-secret"
OCTAVIA_INGRESS_CONFIGMAP = "octavia-ingress-controller-config"
GENERATION_ANNOTATION = "drover.io/guest-credential-generation"
_WORKLOAD_KINDS = ("deployments", "daemonsets", "statefulsets")
_POLL_SECONDS = 3.0

# Runs in the host mount/PID namespaces through nsenter; values arrive only through the environment from a Secret.
_HOST_SCRIPT = r"""
set -eu
mode="$1"
rewrite() {
  f="$1"
  [ -f "$f" ] || return 2
  tmp="$(mktemp "${f}.drover.XXXXXX")"
  if ! awk -v id="$DROVER_AC_ID" -v s="$DROVER_AC_SECRET" '
      /^\[/ { section=$0 }
      section=="[Global]" && /^application-credential-id[ \t]*=/ { print "application-credential-id=" id; n++; next }
      section=="[Global]" && /^application-credential-secret[ \t]*=/ { print "application-credential-secret=" s; m++; next }
      { print }
      END { if (n != 1 || m != 1) exit 3 }' "$f" > "$tmp"; then
    rm -f "$tmp"
    return 3
  fi
  chmod 0600 "$tmp"
  mv -f "$tmp" "$f"
}
rc=0; rewrite /etc/kubernetes/cloud.conf || rc=$?
[ "$rc" -eq 0 ] || [ "$rc" -eq 2 ] || exit 10
rc=0; rewrite /etc/kubernetes/barbican-cloud.conf || rc=$?
if [ "$rc" -eq 2 ]; then
  [ "$mode" = "detect" ] && exit 0
  exit 11
fi
[ "$rc" -eq 0 ] || exit 12
systemctl restart barbican-kms.service
i=0
while [ "$i" -lt 60 ]; do
  if systemctl is-active --quiet barbican-kms.service && [ -S /var/lib/kms/kms.sock ]; then exit 0; fi
  i=$((i + 1))
  sleep 2
done
exit 13
"""


class GuestRolloutError(RuntimeError):
    """The guest credential could not be fully rolled out and verified."""


def replace_ini_credential(text: str, credential: dict) -> str:
    """Replace exactly one id and one secret line inside [Global]; any other shape fails closed."""
    section = ""
    seen = {"id": 0, "secret": 0}
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            section = stripped
        if section == "[Global]" and re.match(r"application-credential-id\s*=", stripped):
            out.append(f"application-credential-id={credential['id']}")
            seen["id"] += 1
            continue
        if section == "[Global]" and re.match(r"application-credential-secret\s*=", stripped):
            out.append(f"application-credential-secret={credential['secret']}")
            seen["secret"] += 1
            continue
        out.append(line)
    if seen != {"id": 1, "secret": 1}:
        raise GuestRolloutError("cloud.conf does not contain exactly one application credential in [Global]")
    return "\n".join(out) + "\n"


def replace_yaml_credential(text: str, credential: dict) -> str:
    """Replace the Octavia Ingress controller's application credential keys without re-rendering its config."""
    seen = {"id": 0, "secret": 0}
    out = []
    for line in text.splitlines():
        match = re.match(r"^(\s*)application-credential-(id|secret):", line)
        if match:
            key = match.group(2)
            out.append(f"{match.group(1)}application-credential-{key}: {credential[key]}")
            seen[key] += 1
            continue
        out.append(line)
    if seen != {"id": 1, "secret": 1}:
        raise GuestRolloutError("Octavia Ingress config does not contain exactly one application credential")
    return "\n".join(out) + "\n"


def _b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _unb64(value: str) -> str:
    return base64.b64decode(value, validate=True).decode("utf-8")


async def _get(client, url: str) -> dict | None:
    resp = await client.get(url, headers={"Accept": "application/json"})
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise GuestRolloutError(f"Kubernetes read failed ({resp.status_code})")
    return resp.json()


async def _put(client, url: str, body: dict) -> None:
    resp = await client.put(url, json=body, headers={"Accept": "application/json"})
    if resp.status_code not in (200, 201):
        raise GuestRolloutError(f"Kubernetes update failed ({resp.status_code})")


async def _update_objects(client, server: str, credential: dict) -> dict[str, set[str]]:
    base = f"{server}/api/v1/namespaces/{NAMESPACE}"
    updated: dict[str, set[str]] = {"secret": set(), "configmap": set()}

    cloud = await _get(client, f"{base}/secrets/{CLOUD_CONFIG_SECRET}")
    if cloud is not None:
        data = dict(cloud.get("data") or {})
        if "cloud.conf" not in data:
            raise GuestRolloutError("cloud-config Secret has no cloud.conf")
        data["cloud.conf"] = _b64(replace_ini_credential(_unb64(data["cloud.conf"]), credential))
        await _put(client, f"{base}/secrets/{CLOUD_CONFIG_SECRET}", {**cloud, "data": data})
        updated["secret"].add(CLOUD_CONFIG_SECRET)

    manila = await _get(client, f"{base}/secrets/{MANILA_SECRET}")
    if manila is not None:
        data = dict(manila.get("data") or {})
        if "os-applicationCredentialID" not in data or "os-applicationCredentialSecret" not in data:
            raise GuestRolloutError("Manila CSI Secret has no application credential")
        data["os-applicationCredentialID"] = _b64(credential["id"])
        data["os-applicationCredentialSecret"] = _b64(credential["secret"])
        await _put(client, f"{base}/secrets/{MANILA_SECRET}", {**manila, "data": data})
        updated["secret"].add(MANILA_SECRET)

    ingress = await _get(client, f"{base}/configmaps/{OCTAVIA_INGRESS_CONFIGMAP}")
    if ingress is not None:
        data = dict(ingress.get("data") or {})
        if "config.yaml" not in data:
            raise GuestRolloutError("Octavia Ingress ConfigMap has no config.yaml")
        data["config.yaml"] = replace_yaml_credential(data["config.yaml"], credential)
        await _put(client, f"{base}/configmaps/{OCTAVIA_INGRESS_CONFIGMAP}", {**ingress, "data": data})
        updated["configmap"].add(OCTAVIA_INGRESS_CONFIGMAP)
    return updated


def _references(template_spec: dict, updated: dict[str, set[str]]) -> bool:
    for volume in template_spec.get("volumes") or []:
        if (volume.get("secret") or {}).get("secretName") in updated["secret"]:
            return True
        if (volume.get("configMap") or {}).get("name") in updated["configmap"]:
            return True
        for source in (volume.get("projected") or {}).get("sources") or []:
            if (source.get("secret") or {}).get("name") in updated["secret"]:
                return True
            if (source.get("configMap") or {}).get("name") in updated["configmap"]:
                return True
    for container in [*(template_spec.get("containers") or []), *(template_spec.get("initContainers") or [])]:
        for env in container.get("env") or []:
            ref = env.get("valueFrom") or {}
            if (ref.get("secretKeyRef") or {}).get("name") in updated["secret"]:
                return True
            if (ref.get("configMapKeyRef") or {}).get("name") in updated["configmap"]:
                return True
        for env_from in container.get("envFrom") or []:
            if (env_from.get("secretRef") or {}).get("name") in updated["secret"]:
                return True
            if (env_from.get("configMapRef") or {}).get("name") in updated["configmap"]:
                return True
    return False


async def _restart_consumers(client, server: str, updated: dict[str, set[str]], generation: int) -> list[tuple[str, str]]:
    restarted: list[tuple[str, str]] = []
    if not updated["secret"] and not updated["configmap"]:
        return restarted
    patch = {"spec": {"template": {"metadata": {"annotations": {GENERATION_ANNOTATION: str(generation)}}}}}
    for kind in _WORKLOAD_KINDS:
        listing = await _get(client, f"{server}/apis/apps/v1/namespaces/{NAMESPACE}/{kind}")
        for item in (listing or {}).get("items") or []:
            name = (item.get("metadata") or {}).get("name")
            template = ((item.get("spec") or {}).get("template") or {}).get("spec") or {}
            if not name or not _references(template, updated):
                continue
            resp = await client.patch(
                f"{server}/apis/apps/v1/namespaces/{NAMESPACE}/{kind}/{name}",
                json=patch,
                headers={"Content-Type": "application/strategic-merge-patch+json", "Accept": "application/json"},
            )
            if resp.status_code != 200:
                raise GuestRolloutError(f"Restarting {kind}/{name} failed ({resp.status_code})")
            restarted.append((kind, name))
    return restarted


def _rolled_out(kind: str, item: dict) -> bool:
    meta, spec, status = item.get("metadata") or {}, item.get("spec") or {}, item.get("status") or {}
    if int(status.get("observedGeneration") or 0) < int(meta.get("generation") or 0):
        return False
    if kind == "daemonsets":
        desired = int(status.get("desiredNumberScheduled") or 0)
        return int(status.get("updatedNumberScheduled") or 0) == desired and int(status.get("numberAvailable") or 0) == desired
    replicas = int(spec.get("replicas") if spec.get("replicas") is not None else 1)
    if kind == "statefulsets":
        return (
            int(status.get("updatedReplicas") or 0) == replicas
            and int(status.get("readyReplicas") or 0) == replicas
            and status.get("currentRevision") == status.get("updateRevision")
        )
    return (
        int(status.get("updatedReplicas") or 0) == replicas
        and int(status.get("availableReplicas") or 0) == replicas
        and int(status.get("replicas") or 0) == replicas
    )


async def _wait_rollouts(client, server: str, restarted: list[tuple[str, str]], deadline: float) -> None:
    pending = list(restarted)
    while pending:
        remaining = []
        for kind, name in pending:
            item = await _get(client, f"{server}/apis/apps/v1/namespaces/{NAMESPACE}/{kind}/{name}")
            if item is None:
                raise GuestRolloutError(f"{kind}/{name} disappeared during rollout")
            if not _rolled_out(kind, item):
                remaining.append((kind, name))
        if not remaining:
            return
        if time.monotonic() >= deadline:
            raise GuestRolloutError("Guest credential consumers did not finish rolling out: " + ", ".join(
                f"{kind}/{name}" for kind, name in remaining
            ))
        pending = remaining
        await asyncio.sleep(_POLL_SECONDS)


def _host_job(name: str, node: str, secret_name: str, mode: str, image: str) -> dict:
    def env(key: str, var: str) -> dict:
        return {"name": var, "valueFrom": {"secretKeyRef": {"name": secret_name, "key": key}}}

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": NAMESPACE, "labels": {"app": "drover-guest-credential-rollout"}},
        "spec": {
            "ttlSecondsAfterFinished": 600,
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": {"app": "drover-guest-credential-rollout"}},
                "spec": {
                    "hostPID": True,
                    "restartPolicy": "Never",
                    "nodeSelector": {"kubernetes.io/hostname": node},
                    "tolerations": [{"operator": "Exists"}],
                    "containers": [{
                        "name": "rewrite",
                        "image": image,
                        "command": [
                            "nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--",
                            "/bin/sh", "-c", _HOST_SCRIPT, "drover-guest-credential", mode,
                        ],
                        "env": [env("id", "DROVER_AC_ID"), env("secret", "DROVER_AC_SECRET")],
                        "securityContext": {"privileged": True},
                    }],
                },
            },
        },
    }


async def _wait_job(client, server: str, name: str, deadline: float) -> None:
    url = f"{server}/apis/batch/v1/namespaces/{NAMESPACE}/jobs/{name}"
    while True:
        job = await _get(client, url)
        status = (job or {}).get("status") or {}
        if int(status.get("succeeded") or 0) >= 1:
            return
        if job is None or int(status.get("failed") or 0) >= 1:
            raise GuestRolloutError(f"Host credential job {name} failed")
        if time.monotonic() >= deadline:
            raise GuestRolloutError(f"Host credential job {name} timed out")
        await asyncio.sleep(_POLL_SECONDS)


async def _list_control_plane_nodes(client, server: str) -> list[str]:
    listing = await _get(client, f"{server}/api/v1/nodes?labelSelector=node-role.kubernetes.io%2Fcontrol-plane")
    names = [(item.get("metadata") or {}).get("name") for item in (listing or {}).get("items") or []]
    return [name for name in names if name]


async def _rewrite_hosts(client, server: str, generation: int, credential: dict, *, mode: str, deadline: float) -> int:
    nodes = await _list_control_plane_nodes(client, server)
    if not nodes:
        raise GuestRolloutError("No control-plane node is available for host credential rollout")
    suffix = uuid.uuid4().hex[:8]
    secret_name = f"drover-guest-credential-g{generation}-{suffix}"
    base = f"{server}/api/v1/namespaces/{NAMESPACE}"
    resp = await client.post(f"{base}/secrets", json={
        "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
        "metadata": {"name": secret_name, "namespace": NAMESPACE, "labels": {"app": "drover-guest-credential-rollout"}},
        "data": {"id": _b64(credential["id"]), "secret": _b64(credential["secret"])},
    })
    if resp.status_code not in (200, 201):
        raise GuestRolloutError(f"Creating the host credential Secret failed ({resp.status_code})")
    image = get_settings().drover_cert_rotation_job_image
    try:
        for index, node in enumerate(nodes):
            name = f"drover-guest-cred-g{generation}-{suffix}-{index}"
            created = await client.post(
                f"{server}/apis/batch/v1/namespaces/{NAMESPACE}/jobs",
                json=_host_job(name, node, secret_name, mode, image),
                headers={"Accept": "application/json"},
            )
            if created.status_code not in (200, 201):
                raise GuestRolloutError(f"Creating host credential job for {node} failed ({created.status_code})")
            await _wait_job(client, server, name, deadline)
    finally:
        try:
            await client.delete(f"{base}/secrets/{secret_name}")
        except Exception:
            _logger.warning("Transient guest credential Secret %s cleanup failed", secret_name)
    return len(nodes)


async def _probe_secret_write(client, server: str) -> None:
    """A Secret write must succeed through the (possibly KMS-backed) encryption provider."""
    base = f"{server}/api/v1/namespaces/{NAMESPACE}/secrets"
    name = f"drover-credential-probe-{uuid.uuid4().hex[:10]}"
    resp = await client.post(base, json={
        "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
        "metadata": {"name": name, "namespace": NAMESPACE}, "data": {"probe": _b64("ok")},
    })
    if resp.status_code not in (200, 201):
        raise GuestRolloutError(f"Secret write probe failed after credential rollout ({resp.status_code})")
    await client.delete(f"{base}/{name}")


async def rollout_guest_credential(cluster_id: str, generation: int, credential: dict, *,
                                   kms_required: bool, kms_detect: bool) -> dict:
    """Replace the guest credential everywhere the cluster reads it and verify the rollout."""
    deadline = time.monotonic() + get_settings().drover_guest_rollout_timeout_seconds
    async with k3s_kube._kube_client(cluster_id, verify_server=True) as (client, server):
        updated = await _update_objects(client, server, credential)
        hosts = 0
        if kms_required or kms_detect:
            hosts = await _rewrite_hosts(
                client, server, generation, credential, mode="required" if kms_required else "detect", deadline=deadline
            )
        restarted = await _restart_consumers(client, server, updated, generation)
        await _wait_rollouts(client, server, restarted, deadline)
        await _probe_secret_write(client, server)
    return {
        "generation": generation,
        "secrets": sorted(updated["secret"]),
        "configmaps": sorted(updated["configmap"]),
        "restarted": [f"{kind}/{name}" for kind, name in restarted],
        "control_plane_hosts": hosts,
    }
