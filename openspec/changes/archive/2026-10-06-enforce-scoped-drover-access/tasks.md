## 1. Authorization

- [x] 1.1 Replace ownership-only mutation rules with exact current role-ID DAG capabilities; remove raw admin promotion.
- [x] 1.2 Apply rules and namespace restrictions to native HTTP/WebSocket routes without changing callback authentication.

## 2. Credentials

- [x] 2.1 Implement RBAC and TokenRequest issuance with principal-bounded expiration and no admin fallback.
- [x] 2.2 Integrate kubeconfig download and shell with actual current principal credentials; eliminate impersonation-admin mounts.

## 3. Acceptance

- [x] 3.1 Update behavioral tests and add native synthetic HTTP Kubernetes issuance smoke coverage (test-passed: focused 177 tests after the admission fix, `uv run --frozen pytest tests/test_scoped_credentials.py tests/test_k3s_cloud_shell_service.py tests/test_k3s_shell.py tests/test_scoped_api.py tests/test_policy.py tests/test_auth.py tests/test_current_role_directory.py tests/test_k3s_kubeconfig_audit.py`; full suite remains the integration gate).
- [x] 3.2 Document runtime smoke prerequisites and historic certificate limitations without rotating live credentials.
- [x] 3.3 Build `drover-api`/`drover-worker` for linux/amd64 and linux/arm64. Smoke the uvicorn image against a disposable k3s v1.31, MariaDB and Redis with synthetic Keystone: 119/119 checks on each architecture across grades, aliases, system scope, revocation, real RBAC/admission and the Cloud Shell. No production cluster or deployment.
- [x] 3.4 Fix the real-apiserver defect found by 3.3: first editor issuance returned 502 because a freshly applied admission binding admitted the dry-run probe for about 1 s. Poll for a bounded time and stay fail-closed; covered by `test_editor_waits_for_new_admission_binding_to_enforce`.
