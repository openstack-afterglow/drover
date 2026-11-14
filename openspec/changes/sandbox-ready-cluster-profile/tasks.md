## Phase 1

- [x] Add self-group tcp/6443 to the cluster security-group plan; update rule count, quota admission and inventory tests (`test_k3s_master_ha`, `test_native_security_group_quota_http`, `test_durable_create`, `test_cluster_network_profile`; test-passed locally).
- [x] Observe agent nodes through reconciliation with a bounded window (900 s); never fail the create operation; `agent_join_pending`, then a reconciler-owned `ERROR` that clears automatically on a later Ready observation; tests with fake node lists including a late join and an unobservable node list (`test_reconciliation`; test-passed locally).
- [x] Add admin-only `k3s.cluster_profiles` runtime setting, `profile` request field, request-level CIDR/network rejection for profile clusters, backward-compatible default (`test_durable_create`, `test_cluster_network_profile`; test-passed locally).
- [x] Hide network, security-group-rule identifiers and drift details from non-admin responses for profile clusters (`test_cluster_network_profile`; test-passed locally).
- [x] Update ARCHITECTURE, docs, CHANGELOG; stamp and run the architecture guard; run `uv run pytest tests` and `uv run ruff check .` (done in the Phase 1 commits).

## Phase 2 (contract implemented as defined here; Lumen has not yet confirmed it)

- [x] Namespace, default-deny policy, namespaced Role and per-cluster ClusterRole (Namespace and RuntimeClass `get` with `resourceNames`, self-review `create`), ServiceAccount provisioning per call; `POST /v1/clusters/{id}/sandbox-connection`; restricted-action policy entry (`test_scoped_credentials` sandbox cases, `test_policy`; test-passed locally against the synthetic Kubernetes boundary).
- [ ] Decide the sandbox token lifetime with the owners. Implemented default: 900 s, bounded by the caller's Keystone token (`SANDBOX_TOKEN_TTL_SECONDS`); a longer namespace-scoped revocable token needs a setting and an owner decision. Add revocation tests if a longer lifetime is chosen.
- [ ] Verify the generated RBAC and the response contract against the Lumen preflight in an isolated real cluster (the synthetic boundary does not evaluate `resourceNames`).

## Phase 3 (open)

- [ ] Decide evidence signing/trust; implement gVisor RuntimeClass provisioning and measured evidence.
- [ ] Live acceptance on DMS Lab after owner approval; do not claim it from fixtures.
