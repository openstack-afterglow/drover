## ADDED Requirements

### Requirement: Native project security budget admission

Cluster creation SHALL use current native detailed security-group and rule quota for its actual execution project, including used and reserved counts, the cluster rule plan and provider-generated rules. It SHALL NOT exempt system-managed groups, raise quotas, use another project's credentials, or reserve native resources during preflight.

#### Scenario: Exhausted project
- **WHEN** current used plus reserved plus required groups or rules exceeds a finite project limit
- **THEN** new creation returns HTTP 409 naming the project, exhausted resource and required/available counts before issuing delegated credentials or storing cluster/job state

#### Scenario: Unknown native budget
- **WHEN** quota/template lookup fails or required integer fields are absent or invalid
- **THEN** new creation returns HTTP 503 without a quota fallback or resource mutation

#### Scenario: First project group
- **WHEN** the native group used count is zero
- **THEN** admission includes the implicit default group and its provider-generated rules as well as the cluster group

### Requirement: Explicit pre-admission refusal marker

Only an **unkeyed** quota preflight rejection before trust, credential, cluster/job or provider-resource mutation SHALL return nested `detail.code=drover_pre_admission_refused` and a nonempty sanitized message. A shortage SHALL carry `reason=security_group_quota_exceeded` and verified `quota` project/resource/required/available/limit/used/reserved fields; unavailable data SHALL carry `reason=security_group_quota_unavailable`, project_id and no invented quota counts.

#### Scenario: No operation exists
- **WHEN** API quota preflight rejects an unkeyed request with 409 or 503 before admission
- **THEN** the explicit marker allows clients to render a localized terminal refusal and administrator-review guidance without inventing an operation ID

#### Scenario: Other failure or later worker failure
- **WHEN** idempotency conflict, generic503, transport ambiguity, worker quota failure or native write race occurs
- **THEN** no pre-admission marker is synthesized, and existing operation/unknown-outcome semantics remain authoritative

#### Scenario: Overlapping keyed admissions
- **WHEN** requests with the same Idempotency-Key both miss initial lookup and one queues while another rejects quota
- **THEN** the refusal preserves structured quota information but exposes no authoritative absence marker or no-operation message, and readback of the exact key finds the accepted operation without replay

### Requirement: Worker revalidation and native final enforcement

The worker SHALL freshly revalidate security budget with its verified execution connection before the first security-group write. Admission SHALL NOT claim an atomic reservation or retry mutations with unknown outcomes.

#### Scenario: Capacity consumed after acceptance
- **WHEN** a fresh worker read observes a shortage after API admission
- **THEN** the operation fails before any SG/volume/server write, guarded cluster status settles before terminalizing the operation, and the original budget details survive in job/operation/cluster/events without requeue or retry

#### Scenario: Native quota becomes unknown after acceptance
- **WHEN** the worker cannot establish the fresh budget because native reads fail or quota/template data is malformed or incomplete
- **THEN** a sanitized quota-unavailable message naming the project survives in job/operation/cluster/events, the attempt terminalizes without requeue, and no native resource write is attempted

#### Scenario: Concurrent native conflict or unknown write
- **WHEN** a native mutation rejects after preflight or has an uncertain outcome
- **THEN** the operation remains failed/uncertain under existing mutation fences and tracked resources remain available for explicit ownership-checked recovery, without automatic replay
