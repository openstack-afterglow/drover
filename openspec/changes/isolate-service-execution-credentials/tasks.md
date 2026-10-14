## 1. Schema and configuration

- [x] 1.1 Add additive `004_execution_authority.sql` (delegations, cluster credentials, `drover_jobs.delegation_id`, widened operation kind) with manifest checksum and ORM models.
- [x] 1.2 Add trust TTL/minimum-remaining/delegated-role settings with admin/manager rejection, rollout timeout, and synchronize Kolla template, defaults and precheck.

## 2. Operation delegation

- [x] 2.1 Implement trust admission, verification, abandoned-admission deletion, persistence with jobs, release and expiry sweep.
- [x] 2.2 Implement per-connection actor revalidation and trust-token verification with terminal versus transient failure classes.
- [x] 2.3 Bind execution authority per job kind and migrate every worker/callback OpenStack caller; terminal authorization failures skip retries.
- [x] 2.4 Admit trusts in create, scale, delete, nodegroup and admin mutation routes; reuse create delegation for callbacks and rollback. Tenant `POST /clusters/{id}/delete-async` stays a synchronous request executed with the current requester's own validated token (no stored authority, no background job), which design decision 7 permits and which can delete that requester's own application credentials.

## 3. Resource authority

- [x] 3.1 Create restricted control/guest application credentials with the caller token at create admission; render guest plugins from stored guest credentials (including HA joiner KMS files).
- [x] 3.2 Use revalidated control credentials for Stampede, reconciliation and health; report reauthorization required and block Stampede enable without authority.
- [x] 3.3 Implement reauthorization API, staged generation, guest Secret/ConfigMap/KMS host rollout with verification, atomic activation, retirement and owner retire endpoint.
- [x] 3.4 Durable deletion uses the current actor's operation trust, attempts caller-owned credential revocation synchronously, erases remaining secrets and reports owner revocation backlog. The design-decision-7 exception remains explicit: tenant `POST /clusters/{id}/delete-async` executes on the current requester's validated caller-token connection (no stored authority/background job), not a creator or manager identity.

## 4. Removal and contracts

- [x] 4.1 Remove manager identity creation, role assignment, stored password use, tenant-admin connection helper and manager crypto/cutover decryption.
- [x] 4.2 Update SDK proxy, API reference, ARCHITECTURE, feature coverage, Kolla docs and upgrade runbook.
- [x] 4.3 Update broken consumer tests; add boundary regressions for scope substitution, revocation, least roles, release, rollout partial failure, legacy reauthorization and deletion after creator departure. `tests/test_execution_authority.py` and `tests/test_native_trust_loopback.py` cover current authority/rollout/GPU/deletion and real-SDK loopback trust boundaries; consumer tests patch `require_gpu_admission`.
- [x] 4.4 Parent verification (2026-10-07, local): `uv run --frozen --extra service --extra dev pytest -q -p no:cacheprovider tests --ignore=tests/integration` 1123 passed, 1 skipped; SDK `tests/test_proxy.py` 114 passed; `tests/test_native_trust_loopback.py` 2 passed (real keystoneauth1/python-keystoneclient/openstacksdk over loopback synthetic Keystone); disposable MariaDB 11.4 applied ledger 001–004, re-ran every 004 statement as a no-op and reported nothing pending on a second `migrate --apply`; `openspec validate` passed. Root ruff reports only two findings in pre-existing scoped-access work (`drover/api/configmaps.py` SIM102, `drover/services/cloud_shell.py` UP017).

Not exercised: live Keystone trust/app-credential policy, OpenStack/K3s provisioning, guest Secret/ConfigMap/KMS rollout, Barbican/Octavia role-default sufficiency (`[INFERENCE]`), image builds or deployment. Keystone master citations distinguish blocked delegated auth methods from ordinary trust-scoped tokens.

## 5. Native worker ownership extension

- [x] 5.1 Trace periodic recovery/reconciliation, delegation sweep and all-cluster health/Stampede scans; retain SQL job competing claims and the existing authority/source/owner boundaries.
- [x] 5.2 Add service-local Redis server-time membership and sticky role/target ownership, leader-only registry publication, token-only release, revocation/drain and crash takeover. Independent control scheduling/heartbeat runs outside blocked SDK domain work.
- [x] 5.3 Replace all-cluster health/Stampede loops with assigned target point lookups; separate recovery/reconciliation/delegation roles; fence periodic writes and cleanup after lock/auth waits. POSIX stop revokes, cancels and drains every owned task.
- [x] 5.4 Define native Lua/multi-replica/runtime/target and SQL ambiguity tests and synthetic-provider/network smoke hooks. Include native Sentinel factory selection, cancellation remaining nonclaimable until actual drain, operation-ID propagation to every job kind, offloaded reconciliation recorded/orphan reads keeping SQL heartbeat runnable, exact-attempt validation before dispatch and same-transaction zero-agent activation fencing after takeover. Native coverage is **test-passed locally** in the parent verification recorded in 5.6, not live-verified.
- [x] 5.5 Update own architecture/design/specs/feature docs with ownership, SQL accepted/unknown non-replay, authority preservation and smoke limits.
- [x] 5.6 Parent verification (2026-10-14, local): focused ownership/job lane **374 passed**; full service `pytest -p no:cacheprovider tests --ignore=tests/integration` **1323 passed, 1 skipped**; three-replica native smoke with disposable Redis/DB and loopback synthetic provider **exit 0**, initial owners **[11, 11, 11]**, all seven flags true. `ruff check .` retains only **2 pre-existing scoped-access findings outside this extension** (`drover/api/configmaps.py` SIM102, `drover/services/cloud_shell.py` UP017); all 15 ownership-introduced findings were fixed. Architecture stamp/check is recorded in the parent receipt from an isolated clone with the shared review marker/digest/UTC block untouched. Smoke failures/fixes, exact flags and not-exercised boundaries are recorded in `docs/drover-feature-coverage.md`. No deployment or live cloud verification was performed or authorized by this extension.


**Integration boundary (0.4.15, outcome updated 2026-10-14):** The dated results above qualify the preserved original WIP only, not the latest-dev integration. Subsequent qualification and rollout are recorded separately in [0.4.15 release notes](../../../docs/release-0.4.0.md#0415-patch--latest-dev-wip-integration-and-distributed-worker-cutover): source `314c7a6a` + pin `977a1cad`, exact-SHA CI and tag Release/Docker success, and completed DMS Lab whole-cohort cutover with pinned replicas/readiness. This does not establish a new guest/credential-rotation or Sentinel failover acceptance run. Latest native role-inference, Sentinel and GPU/plugin protections are retained.

