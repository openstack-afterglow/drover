## Context

Drover creates a separate cluster isolation group in the actual execution project. Native Neutron quotas count project-owned groups regardless of the UI's system-managed label. `get_network_quota` has permissive display fallbacks and must not be reused for admission.

## Goals / Non-Goals

**Goals:** reject known shortages before delegating/queueing, include native reservations and generated rule cost, recheck with the actual worker authority, preserve authoritative provider failures and unknown outcomes.

**Non-Goals:** increasing project limits, borrowing another project, sharing isolation groups, reserving resources during GET/preflight, changing credential authority, automatic retry/cleanup of ambiguous writes.

## Decisions

- Use native `get_quota(project_id, details=True)` and strictly require integer `limit`, `used`, `reserved` for groups and rules. Missing/malformed/unavailable usage is not unlimited/zero.
- Count the exact existing cluster rule plan. Read native rule templates when `security-group-default-rules` is supported; for explicitly unsupported legacy providers use Neutron's standard two non-default egress/four default rules. An initially empty group inventory (`used == 0`) needs the implicit default group plus the cluster group. No group inventory GET is used to manufacture a default group during admission.
- API preflight precedes trust, credential issuance, cluster records and jobs. Worker preflight precedes its first SG write and uses the newly verified trust connection, not admission's stale snapshot.
- This is advisory read-only admission, not a reservation. Concurrent native writers remain governed by Neutron's atomic quota enforcement; accepted/unknown writes retain the existing inventory and mutation replay fences. Existing partial resources require explicit native owner-checked deletion, not name-based adoption or unsafe replay.
- Authoritative no-operation markers apply only to unkeyed requests. Keyed requests retain structured quota information but withhold the marker, because another same-key request may have accepted concurrently; a second unlocked read, expiring Redis mutex or node-local SQL advisory lock cannot prove global absence. No schema or synthetic lock is added.
- Parent-required ownership prerequisite: fence heartbeat local commit with a generation under the existing coordinator lock so concurrent revocation cannot be lost during await. Leave worker scheduling, real SDK drain and token release order unchanged.

## Risks / Trade-offs

- Provider policy may deny quota details/templates; create fails unavailable rather than using admin credentials or inventing a budget.
- An administrator can change templates/quota or another writer can consume capacity after preflight. Final native rejection remains possible, and no atomic multi-service reservation is claimed.
- No schema or data migration. Production version, policy and quota data are not qualified by synthetic local HTTP coverage.
