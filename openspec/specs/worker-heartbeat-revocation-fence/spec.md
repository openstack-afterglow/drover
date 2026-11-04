# worker-heartbeat-revocation-fence Specification

## Purpose
Prevent in-flight Redis renewal completion from restoring locally revoked worker ownership while preserving real SDK drain, incarnation and mutation-replay boundaries.
## Requirements
### Requirement: Local revocation fences in-flight renewal commit

The coordinator SHALL capture a local generation under its existing thread lock before awaiting heartbeat I/O. Local revocation SHALL revoke/clear leases, invalidate joined/deadline state and advance that generation under the same lock. A successful Redis response SHALL NOT restore local admission if that generation changed or drain began while awaiting it.

#### Scenario: Revoked during a successful renewal
- **WHEN** Redis renewal succeeds but local revocation happens before heartbeat admission commits
- **THEN** the stale heartbeat raises OwnershipLost, joined remains false, deadline remains invalid, and an already-held consumer lease cannot validate or dispatch

#### Scenario: Fresh incarnation after revocation
- **WHEN** the next heartbeat joins after the stale renewal was rejected
- **THEN** it uses a new member ID and former consumer leases remain revoked

### Requirement: Preserve draining boundaries

The correction SHALL NOT alter active SDK coroutine/thread drain, per-key no-overlap behavior, token-release ordering, tenant authority, native resource execution or mutation replay policy.

#### Scenario: Revocation while native work drains
- **WHEN** an old incarnation has already-sent SDK work draining
- **THEN** rejoining cannot revive its lease or permit overlapping execution, and existing drain-before-release safety remains in effect

