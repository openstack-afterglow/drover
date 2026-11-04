## Context
The primary checkout is stale0.4.3 with extensive unrelated dirty work. Candidate starts clean at remote dev ea0bc07 (published0.4.15 code plus deployment documentation).

## Goals / Non-Goals
Goals: replace service-leaf requirements end to end, preserve native role-ID graph and provider delegation attenuation, reader nonsecret access, hard ownership/system/human/original-token boundaries.
Non-goals: administrator executor runtime, worker/network cutovers, credential cleanup redesign, IAM mutation or deployment. Exact existing005 is recognized only for additive schema compatibility.

## Decisions
Use native Keystone effective project assignments and actual global role-ID inference DAG, not aliases or a retired-role denylist. Retain current_project_role_state for existing provider-token inference ceilings. Reject partial/ambiguous/cyclic identity data. Inspect submitted tokens before rescope; delegated/unknown-method credentials retain issued IDs and metadata-only REST access. Human password/federation authority is freshly read; directory failure is503.
Core role floors and system/credential/ownership boundaries remain outside operator YAML. Operator policy may attenuate but not widen them. Capability discovery evaluates the same action engine. Use standard oslo_policy TOML keys and shared read-only Kolla mounts; no operator file overwrite.

## Risks / Trade-offs
Manager uses actual native manager→member implication (parent observed this deployed edge read-only); never fabricate provider grants. Custom YAML requiring retired leaves must be reviewed. Package exact already-applied005, preserve001–004 and refuse unknown/drifted/pending ledgers, nonNULL requester rows or nonempty cleanup journal at migration/API/worker startup/readiness. No row rewrite or executor fallback is allowed. Retain existing whole-worker drain. Synthetic native HTTP is not live provider/guest acceptance.
