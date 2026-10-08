## Context
Native Drover uses Keystone project tokens and internally stored cluster-admin kubeconfigs. Ownership is a boundary, not a service entitlement.

## Goals / Non-Goals
Goals: enforce exact effective Drover leaves from the current role-ID DAG; genuinely restricted credentials; protect namespace/system boundaries; regression coverage.
Non-goals: live cloud mutation, deployment, certificate rotation, adding tenant OpenStack admin/manager roles.

## Decisions
- Fetch current effective project assignments, global role catalog and inference graph from Keystone on each authentication. Resolve assigned IDs through actual edges and accept only unique global leaf names. Never hardcode parent expansion: removing a hierarchy edge must revoke the feature. Reader leaves require base reader/member and other leaves base member; nonverified raw admin/manager fails closed. Directory failures never fall back to stale scoped-token roles.
- Separate inventory, cluster editor/admin, access user/admin and workload editor rules; secrets are not inventory. Apply native HTTP/WebSocket authorization, not just BFF gating.
- Use Kubernetes ServiceAccounts, explicit RBAC and TokenRequest for reduced-grade downloads and shell. Read-only permissions omit secrets, RBAC and mutation. Editor permissions are limited to a deterministic per-principal workload namespace; no administrator cert is returned or mounted.
- Revalidate current Keystone principal per request; cap issued credentials to principal expiration. Fail closed when issuance is unavailable; no admin fallback. Avoid caching user credentials.

## Risks / Trade-offs
- Historic downloaded administrator certificates cannot be recalled by API policy changes; operators must explicitly rotate/rebuild affected cluster trust after inventorying exposure. No live rotation here.
- Bearer tokens remain usable until expiration even after Keystone downgrade; short lifetimes and bounded issuance limit exposure.
- Namespace workload writers can execute workload code: use isolated namespaces and no elevated service accounts, RBAC, privileged pods or host mounts. Internal shell namespace must not be an editor workload namespace.
- Synthetic smoke exercises native routes with real issuance HTTP calls against a fake Kubernetes boundary, not a claim of production cluster verification. A separate disposable k3s v1.31 smoke (synthetic Keystone only) exercised real RBAC, PSA and ValidatingAdmissionPolicy semantics; it is still not production evidence.
- Admission objects become enforcing asynchronously after server-side apply, so editor issuance polls a dry-run probe for a bounded time (20×0.5 s) before failing closed; slower apiservers add issuance latency instead of a permissive path.
