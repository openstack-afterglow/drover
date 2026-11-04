## Why

K3s creation currently accepts and queues work without checking the target project's native Neutron security-group budget. Existing Drover-owned groups consume the same project quota as user groups; accepting a known shortage wastes delegated credentials and starts a predictably failing operation.

## What Changes

- Check fresh native detailed group/rule quota, including reserved usage and provider-generated rules, before create admission and again before the worker's first resource write.
- Return an actionable shortage or unavailable result instead of pretending system-owned groups are quota-exempt, increasing quotas, sharing cluster isolation groups, or reserving cloud resources during preflight.
- Preserve native final enforcement, tracked partial resources, and no-replay handling for concurrent/unknown write outcomes.
- Parent-approved correctness extensions: expose an unkeyed-only pre-admission refusal marker with structured quota data; withhold it for overlapping keyed requests; fix the observed coordinator heartbeat/revocation race using the existing lock and a local generation without changing SDK drain ordering.

## Capabilities

### New Capabilities

- `cluster-security-group-admission`: authoritative project-scoped security-group/rule budget admission.
- `worker-heartbeat-revocation-fence`: reject stale renewal completion after local ownership revocation.

### Modified Capabilities

None.

## Impact

Native cluster create API, provisioner, Neutron read helper and ownership coordinator; focused endpoint, worker and actual installed-SDK loopback HTTP coverage. Structured refusal detail is an intentional error-wire extension. No schema, quota policy, credential scope, scheduling topology, SDK drain ordering or deployment change. Afterglow remains a forwarding BFF.
