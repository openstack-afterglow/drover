## ADDED Requirements

### Requirement: Native project authority
Drover SHALL permit effective global project admin, manager and member to manage owned resources and reader to read nonsecret metadata without a separate service role. Native effective group/domain/custom implications SHALL remain authoritative; names alone SHALL NOT become aliases.

#### Scenario: Member without service leaves
- **WHEN** a member manages an owned cluster or workload
- **THEN** core policy permits the action subject to native provider credentials and resource state

#### Scenario: Reader content ceiling
- **WHEN** a reader requests kubeconfig, secrets, logs, shell, workload content or mutation
- **THEN** access is denied before content or mutation

### Requirement: Hard authorization boundaries
Ownership, human system administration, original reduced-credential ceilings and native key/credential ownership SHALL NOT be overridden by permissive operator policy.

#### Scenario: Reduced administrator credential
- **WHEN** an administrator-owned reduced token has issued reader or member roles
- **THEN** REST access remains issued-role-limited nonsecret metadata and cannot become human/system or mutation authority

#### Scenario: Legacy implication
- **WHEN** an existing service/custom assignment actually implies native member through Keystone IDs
- **THEN** native effective member authority remains usable without rewriting IAM

### Requirement: Exact release scope
Version0.4.16 SHALL preserve published001–004 checksums, recognize the exact already-applied additive005 file and SHALL NOT import unpublished executor runtime. Unknown/checksum-drift/pending ledgers, any executor-owned requester rows or cleanup references SHALL fail canonical migration/startup/readiness rather than be ignored.

#### Scenario: Upgrade from0.4.15
- **WHEN** matching API and worker replace0.4.15 with valid001–005 and only caller-owned NULL requester rows
- **THEN** no further schema migration, IAM grant, row rewrite or credential expansion is required
