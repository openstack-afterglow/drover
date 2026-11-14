## Why

Lumen's controller will request its isolated-sandbox Kubernetes cluster from Drover as part of its own workflow (Lumen change `drover-managed-sandbox-workflow`). Lumen names the Drover side as an unmet external contract: a sandbox-ready, least-privilege connection and trusted qualification evidence. The 2026-11 operations on DMS Lab also exposed Drover defects that make that workflow unsafe: restricted `allowed_cidrs` clusters never admit their agent node although `provision_agents` reports completed; network and security-group choices are per-request instead of administrator-managed and visible to project members; NodePort, 80 and 443 are always world-open and recorded, so operator hardening turns the cluster `ERROR`.

## What Changes

- Phase 1 (Drover-only defects): always open tcp/6443 from the cluster's own security group; make agent provisioning complete only after every requested agent node is a Ready Kubernetes node, otherwise fail the operation and mark the cluster `ERROR` with a precise reason; administrator-managed cluster network profile (network, ingress CIDR sets for SSH/API/NodePort/HTTP(S), self-group rules) selected by name, ignoring request-level CIDRs for profile clusters, hiding security-group and network identifiers from non-administrator responses.
- Phase 2 (sandbox connection): a profile may declare a Lumen-style sandbox namespace; Drover creates the namespace (labels/annotations, Pod Security `restricted`, default-deny NetworkPolicy), a least-privilege ServiceAccount and Role matching the Lumen controller preflight, and exposes `POST /v1/clusters/{id}/sandbox-connection` returning an explicit HTTPS server, verified CA, namespace and an expiring bearer token. The call is a human-token action, never available to application-credential or trust tokens.
- Phase 3 (runtime and evidence): sandbox-capable runtime installation (gVisor `runsc` RuntimeClass) and Drover-measured runtime/CNI/PID evidence with provenance, bound to cluster, namespace and runtime UIDs. Trust model for consumer-side qualification is agreed with Lumen before implementation.

## Non-goals

No change to who may create clusters (human tokens only), no Lumen-side logic, no production deployment or Kolla change, no weakening of requester-owned trusts, no deletion of operator-recorded rules to "re-harden" existing clusters.

## Impact

Provisioner security-group plan, post-provision verification, reconciliation drift semantics for profile rules, resource policy admin API, cluster API schemas (field visibility), new connection endpoint and RBAC, docs/ARCHITECTURE, tests. Schema impact to be established; any migration must be additive with its manifest checksum. Live acceptance requires a Drover-provisioned cluster on DMS Lab; synthetic and source tests do not substitute.
