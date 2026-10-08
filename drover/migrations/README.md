# Drover Database Migrations

## Base Migration Notice

- `001_baseline.sql` contains the historical baseline schema including the `gpu_quotas` table definition.
- Baseline migrations are immutable and must not be retroactively modified.

## `gpu_quotas` Table Retirement Prerequisite

Active GPU quota management has cut over to **Afterglow** (`app.services.gpu_quota`). The `GpuQuota` ORM model and Drover quota APIs have been removed.

- **DO NOT** auto-drop or rewrite the historical `gpu_quotas` table in baseline.
- Future source-table retirement (`DROP TABLE gpu_quotas;`) is permitted ONLY after:
  1. Complete and audited import of existing GPU quota data into Afterglow.
  2. Full rollout of Afterglow as the sole GPU quota authority across all environments.
  3. Verification that no components query Drover's legacy `gpu_quotas` table.

For detailed audit procedures and retirement steps, refer to [docs/gpu-quota-table-retirement-runbook.md](../../docs/gpu-quota-table-retirement-runbook.md).

## Execution authority upgrade (004)

**Evidence: local only.** This is an operator runbook. On 2026-10-07 a disposable MariaDB 11.4 applied ledger 001–004, every 004 statement re-ran as a no-op and a second `drover-migrate --apply` reported nothing pending (OpenSpec `isolate-service-execution-credentials` task 4.4). No production migration, deployment or live Keystone/Kubernetes verification has been performed.

`004_execution_authority.sql` and its manifest checksum add:

- `drover_delegations`: requester/trustee/project/action/roles/expiry and `active|released|revoked|deleted|expired` state; no requester token/password.
- `drover_cluster_credentials`: user-owned `control|guest` generations, encrypted secrets, roles/plugin metadata, operation reference and `staged|active|retiring|deleted` state.
- `drover_jobs.delegation_id` and its FK; `reauthorize` in the operation-kind constraint.

It does **not** backfill operation trusts, turn old manager passwords into authority, drop `project_manager_credentials`, or reauthorize existing clusters. The historical table and its encrypted manager-password rows remain byte-for-byte; `cutover.py` preserves them only as operator cleanup inventory and never decrypts or uses them for execution. Keep 001–003 immutable. Migration 004 uses idempotent `ADD KEY IF NOT EXISTS`, `FOREIGN KEY IF NOT EXISTS`, and constraint `DROP ... IF EXISTS`/`ADD ... IF NOT EXISTS` forms in addition to additive columns/tables; run it through the existing ledger, not by rewriting old migrations.

### Ordered cutover

1. **Apply 004 before new API/Worker processes start.** Back up the DB and retain the kubeconfig/app-credential encryption key securely. Use the migration image/package containing the matching manifest:

   ```bash
   drover-migrate --apply
   ```

   Inspect the migration ledger/readiness using the normal deployment procedure. This document has not run that command. Kolla's migration bootstrap must precede new process start.
2. **Drain pre-upgrade mutations with the old worker, not the new worker.** Stop accepting mutations and disable automatic sizing during the drain. Wait for queued/running create, scale, delete, manual nodegroup and Stampede mutations and `WAITING_CALLBACK` create operations (including HA/agent continuations and rollback) to settle. Old jobs have no admitted delegation; the new worker deliberately terminalizes delegated mutation kinds without one. Do not fabricate delegation IDs, substitute a service/manager identity, or replay cloud mutations from a migration. If a job cannot settle, inventory its partial resources and resolve it explicitly before cutover.
3. **Switch API and Worker together to the new authority code/config.** Restore intended traffic only after schema/readiness and the drain boundary are satisfied. New durable mutations admit requester-owned impersonating trusts with finite TTL. Continuous work requires a user-owned active control credential; no tenant-manager fallback exists.
4. **Reauthorize legacy clusters before retiring old identities.** An authorized operator with a normal token scoped to the cluster project calls `POST /v1/clusters/{id}/authorization` on an `ACTIVE` cluster with no competing mutation. Poll the returned operation ID and `GET .../authorization`. Legacy guest credentials trigger guest replacement, including legacy KMS detection. Confirm `SUCCEEDED`, the new active generation and guest rollout before any old credential is revoked. Legacy clusters without active control cannot enable Stampede; periodic reconciliation excludes them and explicit reconciliation reports `reauthorization_required`. Non-ACTIVE legacy clusters must be resolved to ACTIVE or deleted by a currently authorized delete actor; do not bypass the reauthorization state guard.
5. **Only then remove `afterglow-cluster-mgr-*` users and old application credentials out of band.** Inventory exact user IDs, project assignments and credential references; confirm no cluster/guest still depends on them before operator revocation. New code does not use or automatically remove these legacy users/password rows. New generation activation erases superseded secrets and records `retiring` references, including ownerless legacy entries. `POST .../authorization/retire` uses the caller's token to delete only that caller's retiring credentials; other owners must revoke theirs and ownerless legacy credentials require an operator. Never put secret values in cleanup evidence.

### Rollout and retirement boundaries

The worker revalidates current reauthorize capability, authenticates the staged restricted credentials, updates `kube-system/cloud-config` and `manila-cloud-secret` Secrets and Octavia Ingress `octavia-ingress-controller-config` ConfigMap in place, and restarts referencing Deployments/DaemonSets/StatefulSets with rollout completion waits. When KMS is required (or detected for legacy clusters), privileged hostPID Jobs on each control-plane host use a temporary Secret and `nsenter` to rewrite `/etc/kubernetes/cloud.conf` if present and `/etc/kubernetes/barbican-cloud.conf`, restart `barbican-kms.service`, and confirm service/socket readiness. A final Secret-write probe must succeed before atomic activation. Partial failure records staged `last_error` and leaves the previous active generation valid; it does not guarantee rollback of already changed guest objects. Do not revoke the previous credential merely because POST returned 202.

Deletion is independent of the creator: durable deletion uses the current delete actor's operation trust. Tenant DELETE attempts caller-owned credential revocation synchronously using the caller token; direct tenant delete-async uses the current caller connection. Completion erases remaining secrets and reports other-owner/legacy credentials for owner revocation. Deleting the cluster is not proof that every remote credential was revoked.

Jobs/live operations retain a delegation; idle rows become `released`. After each job and on the 300s sweep, `delete_released` authenticates with the trust's own impersonating password token (`user_id=trustee`, `trust_id`, service password; no project selector) and DELETEs released/revoked trusts. Success/404 records `deleted`; Unauthorized/Forbidden keeps released/revoked with `state_reason="trust inert until expiry"`; outages retry on the next sweep. At expiry, Keystone trust GET 404 is required before recording `expired`. TTL bounds grants when immediate deletion is unavailable and cleanup never replays mutations. [Keystone master `api/trusts.py`](https://github.com/openstack/keystone/blob/master/keystone/api/trusts.py) `_check_delegated_token` blocks app-credential/OAuth/EC2 trust management but not ordinary trust-scoped tokens. `identity:delete_trust` permits the trustor represented by impersonation, which Drover uses for trust DELETE. [Master `api/users.py`](https://github.com/openstack/keystone/blob/master/keystone/api/users.py) `_block_delegated_token_app_creds` still blocks trust/OAuth/EC2 app-credential management, and `_check_unrestricted_application_credential` blocks restricted app credentials from managing additional credentials. Resource-credential retirement therefore needs the owner's non-delegated token; secret erasure/backlog is not proof of remote revocation. These are source-reviewed facts, not a deployed Keystone test. `tests/test_native_trust_loopback.py` defines real SDK HTTP trust create/project-less OS-TRUST auth/DELETE/revoked-role boundaries against a synthetic provider (test-defined, not run by this documentation slice).

### Configuration cutover

TOML keys under `[drover]` (Settings/Kolla use the corresponding `drover_` prefix; environment uses uppercase):

| Key | Default | Runtime constraints |
|---|---|---|
| `operation_trust_ttl_seconds` | `14400` | 900–86400 seconds |
| `operation_trust_min_remaining_seconds` | `300` | 60–3600; strictly less than TTL |
| `delegated_required_roles` | `["member"]` | Nonempty; every role must currently be held |
| `delegated_optional_roles` | `["load-balancer_member"]` | Only currently held roles are included |
| `guest_rollout_timeout_seconds` | `600` | 60–3600 seconds |

Role configuration rejects `admin` and `manager`. Barbican `member` authorization and Octavia `load-balancer_member` policy sufficiency are **[INFERENCE]**, not live-verified defaults; qualify the deployed service policies/KEK ACL before rollout.

Remove retired Settings/Kolla variables `drover_afterglow_provisioning_url`, `drover_afterglow_provisioning_token`, `drover_afterglow_provisioning_token_file`, TOML `afterglow_provisioning_url/token/token_file` and environment `DROVER_AFTERGLOW_PROVISIONING_TOKEN_FILE`. Afterglow provisioning intents are gone. Kolla `tasks/config.yml` removes the `afterglow_k3s_provisioning_token` file. Keep GPU admission URL/token/file settings and the separate admission token file: native GPU worker creates re-check admission before each new create when that URL is configured; admission is a live-usage check, not a capacity reservation.

Sources: [`delegation.py`](../services/delegation.py), [`execution.py`](../services/execution.py), [`cluster_authority.py`](../services/cluster_authority.py), [`guest_rollout.py`](../services/guest_rollout.py), [`jobs.py`](../services/jobs.py), [`authorization.py`](../api/authorization.py), [`Kolla role`](../../deploy/kolla/ansible/roles/drover/). See the [API reference](../../docs/drover-api-v1-reference.md) for responses and SDK helpers.
