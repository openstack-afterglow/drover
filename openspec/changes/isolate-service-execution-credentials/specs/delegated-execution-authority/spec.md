## ADDED Requirements

### Requirement: No tenant service identities
Drover SHALL NOT create tenant users, assign tenant roles, store or use tenant manager passwords, or password-scope its service identity to a caller-selected tenant project. Its service identity SHALL authenticate only in its configured service project for directory reads and as trust trustee.

#### Scenario: Cluster creation
- **WHEN** a clusters editor creates a cluster in a project without prior Drover activity
- **THEN** no Keystone user or role assignment is created and every OpenStack call uses the editor's delegation or the editor's own restricted application credentials

#### Scenario: Missing authority
- **WHEN** a worker job or continuous task has no admitted delegation or active resource credential
- **THEN** it fails closed without falling back to a manager, service or caller identity

### Requirement: Bounded operation delegation
Create, delete, manual scale and nodegroup mutations SHALL be admitted with a Keystone trust created from the requester's validated project token, impersonating the requester, delegating only configured roles the requester currently holds (never admin or manager) to the Drover service user for the validated project, with finite expiry. The trust reference and verified metadata SHALL be stored with the job; requester tokens and passwords SHALL NOT be stored.

#### Scenario: Least role subset
- **WHEN** a requester holds member but not load-balancer_member
- **THEN** the trust delegates only member and admission fails if member is absent

#### Scenario: Abandoned admission
- **WHEN** a trust is created but no job is persisted
- **THEN** Drover deletes the trust with the requester's token and otherwise relies on its finite expiry

### Requirement: Revalidated worker execution
Before every worker connection Drover SHALL revalidate the delegating user and project as enabled, the stored Drover action through current role assignments, and the delegated role subset, then verify the trust-scoped token's trust, project, user, trustor, trustee, roles and expiry.

#### Scenario: Revoked actor
- **WHEN** the requester loses the action's Drover role after admission
- **THEN** the next job attempt fails terminally, the delegation becomes revoked and no OpenStack mutation is attempted

#### Scenario: Scope substitution
- **WHEN** a trust token's project, trustee or impersonated user differs from the stored delegation
- **THEN** execution is refused as a terminal authorization failure

#### Scenario: Provider outage
- **WHEN** Keystone or the directory is temporarily unreachable
- **THEN** the job keeps its normal attempt-fenced retry semantics

#### Scenario: Continuation
- **WHEN** the server callback enqueues HA bootstrap or agents, or the callback timeout enqueues rollback deletion
- **THEN** only the create operation's admitted delegation is reused

### Requirement: Durable delegation release
Delegations SHALL be released when no queued or running job references them and their operation is terminal, and SHALL be recorded as expired after Keystone no longer returns them past expiry, independently of job success.

#### Scenario: Failed create
- **WHEN** a create operation fails after its callback
- **THEN** its delegation is released and never used again

### Requirement: User-owned continuous resource authority
Continuous Stampede, reconciliation and health work SHALL use a restricted user-owned control application credential, and guest plugins SHALL use a separate restricted user-owned guest credential, each limited to the delegated role subset. The owner's enabled user, project and current capability SHALL be revalidated before each control-plane use.

#### Scenario: Owner loses capability
- **WHEN** the control credential owner no longer holds clusters editor
- **THEN** Stampede records authority revoked and does not scale

#### Scenario: Legacy cluster
- **WHEN** a cluster created before this change has no resource authority
- **THEN** Stampede and reconciliation report reauthorization required, enabling Stampede returns 409, and no credential is minted from stored creator IDs

### Requirement: Reauthorization with guest rollout
A currently authorized clusters admin SHALL be able to create a new credential generation for any ACTIVE cluster, roll the guest credential into existing plugin Secrets/ConfigMaps and every control-plane host's Barbican KMS configuration, verify workload rollout, then atomically activate it and retire superseded credentials. Failure before activation SHALL leave previously active credentials valid.

#### Scenario: Partial rollout
- **WHEN** the KMS host job fails after cloud-config was updated
- **THEN** the previous generation stays active and undeleted, the staged generation stays staged, and a retry or newer generation can complete

#### Scenario: Legacy manager credential
- **WHEN** a legacy cluster is reauthorized
- **THEN** its manager-owned application credential is replaced in the guest and recorded as retiring for owner revocation

### Requirement: Deletion independent of creator
Cluster deletion SHALL use only the current authorized actor's delegation. Credentials owned by other users SHALL be erased from Drover storage and reported for owner revocation without blocking deletion.

#### Scenario: Original creator left
- **WHEN** a current clusters admin deletes a cluster created by a user who has left the project
- **THEN** deletion runs under the admin's trust and completes

### Requirement: Service-local periodic ownership
Workers sharing Drover's Redis and MariaDB SHALL register independent Redis-server-time membership. Only the discovery owner SHALL discover the shared cluster registry. Live eligible replicas SHALL divide recovery, reconciliation and delegation roles and health/Stampede targets, retaining healthy current owners rather than shuffling them on every heartbeat. Ownership SHALL NOT replace durable SQL job competing claims, attempt fences, operation authority, caller trusts, resource-owner/source validation or accepted/unknown non-replay.

#### Scenario: Three native replicas
- **WHEN** three worker processes share the same service Redis and DB
- **THEN** a single discovery owner publishes targets, roles/targets are divided among eligible processes, and healthy target owners do not duplicate polling

#### Scenario: Blocked SDK and revocation
- **WHEN** cluster SDK work blocks a domain event loop or an assignment is revoked
- **THEN** membership and assignment scheduling continue independently, the old token cannot release a newer assignment, and the former owner cannot begin subsequent mutations after the revocation fence

#### Scenario: POSIX stop and crash
- **WHEN** a worker receives SIGTERM/SIGINT or crashes
- **THEN** graceful stop revokes and cancels every owned task and drains before token-only release; crash takeover waits for server-time membership expiry

#### Scenario: Ambiguous external mutation
- **WHEN** a physical mutation was accepted or its result is unknown after error, cancellation or worker loss
- **THEN** the SQL job is not automatically replayed, existing operation/caller/resource authority remains required, and operators inspect durable status and provider inventory rather than blindly retrying


**Integration boundary (0.4.15):** The dated results above qualify the preserved original WIP only, not the latest-dev integration. This integration has not run tests, lint, builds, architecture stamping, native smoke or deployment; the parent owns those gates. Latest native role-inference, Sentinel and GPU/plugin protections are retained.

