## Phase 1

- [x] Add self-group tcp/6443 to the cluster security-group plan; update rule count, quota admission and inventory tests (`test_k3s_master_ha`, `test_native_security_group_quota_http`, `test_durable_create`, `test_cluster_network_profile`; test-passed locally).
- [x] Observe agent nodes through reconciliation with a bounded window (900 s); never fail the create operation; `agent_join_pending`, then a reconciler-owned `ERROR` that clears automatically on a later Ready observation; tests with fake node lists including a late join and an unobservable node list (`test_reconciliation`; test-passed locally).
- [x] Add admin-only `k3s.cluster_profiles` runtime setting, `profile` request field, request-level CIDR/network rejection for profile clusters, backward-compatible default (`test_durable_create`, `test_cluster_network_profile`; test-passed locally).
- [x] Hide network, security-group-rule identifiers and drift details from non-admin responses for profile clusters (`test_cluster_network_profile`; test-passed locally).
- [ ] Update ARCHITECTURE, docs, CHANGELOG; stamp and run the architecture guard; run `uv run pytest tests` and `uv run ruff check .`.

## Phase 2 (after the contract is agreed with the Lumen session)

- [ ] Namespace, default-deny policy, namespaced Role and per-cluster ClusterRole (Namespace and RuntimeClass `get` with `resourceNames`, self-review `create`), ServiceAccount provisioning per profile; `POST /v1/clusters/{id}/sandbox-connection`; restricted-action policy entry.
- [ ] Decide the sandbox token lifetime (900 s default path versus a longer namespace-scoped revocable token) with the owners; add expiry and revocation tests.
- [ ] Verify the generated RBAC against the Lumen preflight rules in an isolated cluster.

## Phase 3 (open)

- [ ] Decide evidence signing/trust; implement gVisor RuntimeClass provisioning and measured evidence.
- [ ] Live acceptance on DMS Lab after owner approval; do not claim it from fixtures.
