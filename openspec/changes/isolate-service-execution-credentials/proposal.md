## Why
Drover workers execute tenant OpenStack mutations as a per-project `afterglow-cluster-mgr-*` user that the service account creates, assigns `member`/`load-balancer_member` in every tenant project and authenticates with a stored password. That identity outlives the requester's authority, is visible to and removable by project members, and lets a revoked user's queued or continuous work keep running. The user approved replacing it with requester-owned, bounded delegation for operations and explicitly reauthorized, user-owned resource credentials for continuous cluster work.

## What Changes
- **BREAKING** Remove manager-user creation, tenant role assignment, stored manager-password authentication and every caller of the manager connection; there is no service-identity or caller fallback.
- Admit create, delete, manual scale and nodegroup jobs with a bounded, impersonating Keystone Trust created from the caller's validated project token (trustee = Drover service user, roles = least native subset the caller currently holds, never admin/manager). Callback continuations (HA bootstrap, agents, joiner LB member, timeout rollback) reuse only the create operation's admitted delegation.
- Revalidate the delegating actor's enabled user/project and exact Drover capability before each worker connection, verify the trust-scoped token and fail terminally on revocation, scope mismatch or expiry while preserving lease/attempt fences and transient-outage retries. Release delegations durably when their operation is idle and record Keystone expiry.
- Create user-owned, restricted (non-`unrestricted`) application credentials per cluster: a `control` credential used only by Drover for Stampede, reconciliation and health, and a separate `guest` credential for OCCM/CSI/Ingress/Barbican KMS. Owners are revalidated before every control-plane use.
- **BREAKING** Existing clusters have no resource authority until a currently authorized operator reauthorizes them. New `POST/GET /v1/clusters/{id}/authorization` and `POST /v1/clusters/{id}/authorization/retire` create staged credentials, roll them into the guest `cloud-config`/plugin Secrets and every control-plane host's Barbican KMS configuration, verify workload rollout, activate atomically, and retire superseded credentials without breaking a partially rolled-out cluster.
- Fresh authorized deletion uses the current actor's delegation; credentials owned by another user are zeroized in Drover and reported for owner revocation instead of blocking deletion.
- Additive migration `004_execution_authority.sql`; settings, Kolla template/defaults/precheck, SDK, docs and tests migrated together. Unrelated native Kubernetes RBAC/TokenRequest work is unchanged.

## Capabilities
### New Capabilities
- `delegated-execution-authority`: operation Trust admission/verification/release and user-owned resource credential reauthorization, rollout and retirement for Drover.
### Modified Capabilities
None.

## Impact
`drover/services/{delegation,cluster_authority,execution,guest_rollout,keystone,jobs,provisioner,autoscale,stampede,deletion,reconciliation,health,barbican,operations,nodegroup}.py`, `drover/api/{clusters,nodegroups,admin,callback,authorization}.py`, `drover/auth.py`, `drover/policy.py`, `drover/crypto.py`, `drover/config.py`, ORM/migration 004, `sdk/drover_sdk/proxy.py`, Kolla role, ARCHITECTURE/docs, tests. Operators must drain queued pre-upgrade mutation jobs, reauthorize every existing cluster, then remove legacy manager users out of band. No production role change, live rotation or deployment is performed by this change.
