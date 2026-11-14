## Verified starting facts (Drover 0.4.19, dc8dbe5 lineage; source read 2026-11-13)

- `_cluster_security_group_rules` (provisioner.py:105-181) opens tcp 22 and 6443 only to `allowed_cidrs`, self-group 2379-2380, 10250, udp 8472 and 51820, and 80/443/30000-32767 to `0.0.0.0/0`. No self-group 6443 rule exists; a cluster created with restricted CIDRs leaves its agent unable to reach the API (observed on DMS Lab 2026-11-13; fixed there by an operator-added self-group rule, reconcile drift stayed 0).
- Reconciliation marks `ERROR` for a missing recorded resource; an operator that replaced the recorded world-open NodePort rule made the previous cluster `ERROR`. Untracked extra rules do not produce `ERROR`.
- `policy.py:153-159,245` denies cluster create/scale/delete/reauthorize, `drover:access:*`, workloads write and certificate rotation to credential-restricted tokens; `credentials.issue_kubeconfig` issues short-lived RBAC/TokenRequest credentials (deadline min(Keystone token expiry, 900 s), refuse under 600 s) and never the admin kubeconfig.
- Resource policy (admin-only `drover:admin`) already pins default network, flavors, image, LB subnet and floating networks.

## Phase 1 design

1. **Self-group 6443.** Add `tcp/6443 remote_group_id=<cluster sg>` to the plan for all clusters. Recorded like the other rules. Existing clusters are unchanged; operators may add the rule by hand (untracked).
2. **Agent admission check (recoverable; implemented in reconciliation).** Reconciliation (`reconciliation.py:_observe_agent_join`) compares the recorded agent VM rows with the Kubernetes node list (reusing the health client) on every pass; `provision_agents` itself does not block. An agent that is not a Ready node is `agent_join_pending` in `drift_status` for a bounded window (900 s; the DMS Lab agent needed about 150 s before `k3s-agent` even started). The create operation is NOT failed: `provision_agents` shares the `create` operation kind, and reconciliation turns any FAILED/CANCELLED create operation into a state mismatch on every pass (reconciliation.py:430-436), which would make a late-joining agent a permanent, delete-only `ERROR`. After the window, an agent that is still not Ready makes the cluster `ERROR` with a reconciler-owned reason (`Reconciliation failed: Required Kubernetes agent node '<name>' is not Ready`) through the missing-required-resource path, which reconciliation already clears automatically (reconciliation.py:627-633) once a later pass observes the node Ready. If the node list cannot be observed nothing new is reported and an earlier agent-node `ERROR` is carried forward. A re-check is also available through `POST .../health/check`. No mutation is retried automatically.
3. **Cluster network profile.** New admin-only policy `k3s.cluster_profiles` (name to profile): `network_id`, `ssh_cidrs`, `api_cidrs`, `nodeport_cidrs`, `http_cidrs` (empty to omit 80/443), self-group rules fixed. A request may carry `profile` (name). With a profile, request-level `allowed_cidrs` and `network_id` are rejected with 400; without a profile the current behaviour stays for backward compatibility. Profile rules are recorded like any rule, so removing them out of band is still drift. The profile is admin-managed; its rule detail and security-group/network identifiers are returned only to `drover:admin`.
4. **Visibility.** Cluster and operation responses for non-admin callers omit `security_group_id`, rule details and network identifiers for profile clusters; the sandbox project has no human members and its groups are not shared.

## Phase 2 design (contract draft, to be agreed with the Lumen session)

`POST /v1/clusters/{id}/sandbox-connection` with body `{ "deployment_id": str, "pool_name": str }` (labels bind the namespace to a Lumen pool). Human-token action (new `drover:sandbox:connect` in the restricted action set for credential tokens). Response:

```json
{
  "server": "https://<api>:6443",
  "ca_pem": "-----BEGIN CERTIFICATE-----...",
  "namespace": "lumen-<id>",
  "namespace_uid": "<uid>",
  "bearer_token": "<expiring TokenRequest token>",
  "expires_at": "<RFC3339>",
  "runtime_class": {"name": "gvisor", "handler": "runsc", "uid": "<uid>", "spec_sha256": "<hex>"}
}
```

Preconditions Drover provisions per cluster, idempotently: namespace with `lumen.dev/deployment`/`lumen.dev/pool` label and annotation, `pod-security.kubernetes.io/enforce=restricted`, `lumen-default-deny` NetworkPolicy. RBAC: a namespaced Role for the ServiceAccount (Pods and Services get/list/create/delete; Secrets get/create/delete; NetworkPolicies get/list/create/delete) plus a ClusterRole/ClusterRoleBinding for the cluster-scoped checks, because Roles cannot grant them: `get` on the single Namespace and the single RuntimeClass restricted with `resourceNames`, and `create` on `selfsubjectaccessreviews`, `selfsubjectrulesreviews` and `authentication.k8s.io` `selfsubjectreviews` (create-only resources take no `resourceNames`). No wildcard, no namespace-admin, no Secret list/watch. Token lifetime: the default path is bounded by the caller's Keystone token and the 900 s ceiling of `issue_kubeconfig`; the consumer refreshes before expiry and swaps clients atomically. Because refreshing needs a live human-token exchange with Keystone and Drover, an outage of about 15 minutes ends the consumer's authority over running sandboxes. This contract therefore proposes a separate, longer lifetime for the namespace-scoped sandbox token (value chosen by the owners, for example one to a few hours), issued only to this dedicated ServiceAccount and independently revocable; the 900 s ceiling stays for user/editor kubeconfigs.

## Phase 3 design (open)

gVisor install and RuntimeClass are cluster-node concerns Drover must own (cloud-init or an applied manifest); the operator's 2026-10 manual procedure and artefacts exist (`/home/pieroot/.local/share/lumen-sandbox`). Evidence: Drover measures runtime/CNI/PID facts on the real cluster and returns them with provenance. The consumer binds them to its own pool digest; Drover does not know Lumen's pool configuration. Open question for the owners: who signs the evidence and what the consumer trusts (Drover service signing key, Keystone-authenticated response over TLS, or operator-approved policy digest).

## Risks

- Changing the provisioner SG plan changes the recorded-rule count (quota admission already counts planned rules and must stay consistent).
- Profile rules recorded in the inventory make hardening by deletion a drift; the profile makes rule changes an administrator policy change, which has to flow through Drover rather than out-of-band.
- Agent check needs a reachable API and a working node-list client during provisioning and reconciliation; a slow image pull must not be reported as failure before the bounded window, and a late join must clear the degraded state without operator action.
- Short-lived sandbox tokens couple sandbox control to Keystone/Drover availability (about 15 minutes at 900 s). A longer-lived, namespace-scoped, revocable token trades that outage tolerance for a larger leak window; the choice is an owner decision.
- Cluster-scoped RBAC (ClusterRole for Namespace/RuntimeClass `get` and self-review `create`) is broader in kind than a Role even with `resourceNames`; it must be created per cluster and reviewed against Lumen's preflight, which rejects wildcards and extra grants.
