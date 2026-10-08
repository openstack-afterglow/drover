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
