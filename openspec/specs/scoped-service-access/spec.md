# Scoped service access

## Purpose
Authorize Drover project actions by current service capabilities and issue genuinely restricted, expiring Kubernetes credentials without exposing stored administrative credentials to reduced-grade callers.

## Requirements

### Requirement: Native service capability authorization
Drover SHALL require exact effective service leaves and project ownership. Current effective assignments and the actual role-ID inference graph SHALL be queried at native authentication, without hardcoded parent expansion or stale token-role fallback. Only unique global role IDs SHALL assert built-in role authority. Reader leaves require base reader/member, other leaves base member. Plain member/reader/project roles SHALL NOT imply service mutations. System global settings/templates SHALL require verified system administration.

#### Scenario: Grade boundaries
- **WHEN** a project member has drover_user with current hierarchy links to inventory and access-user leaves
- **THEN** inventory and restricted read-only credentials are available but create, scale, delete and shell are denied

#### Scenario: Editor and admin
- **WHEN** a project member has drover_editor with the preset's current hierarchy links
- **THEN** cluster create/update/scale and namespace-scoped workload execution are allowed while cluster deletion, rotation and full administrator credential download are denied

#### Scenario: Service admin isolation
- **WHEN** a user has drover_admin or raw admin/manager on a project token without verified system administration
- **THEN** no OpenStack/system administration is inferred and other projects remain inaccessible

#### Scenario: Editable hierarchy revocation
- **WHEN** the user retains a service parent assignment but its link to an access leaf is removed
- **THEN** the next native request resolves the changed graph and denies that leaf's capability

#### Scenario: Directory failure or alias
- **WHEN** current directory resolution fails or an assignment uses a same-name domain role instead of the unique global leaf ID
- **THEN** no built-in capability is inferred from the token's role label

### Requirement: Genuine restricted credentials
Reduced-grade kubeconfigs SHALL contain only a real expiring RBAC-bound Kubernetes bearer token, server and CA data, never the stored administrator client key/certificate plus impersonation fields. Issuance SHALL use the current principal and fail closed on provider errors.

#### Scenario: Explicit safe credential variant
- **WHEN** an access administrator downloads without a grade query
- **THEN** the default is read-only user credentials; `grade=admin` is required for full certificates, unauthorized variants return 403 and unknown variants return 422 without fallback

#### Scenario: Usable editor namespace discovery
- **WHEN** an editor without access-admin requests namespace metadata
- **THEN** its isolated namespace is prepared with enforcing workload admission and returned without issuing a discarded credential

#### Scenario: User download
- **WHEN** drover_user downloads a kubeconfig against the synthetic Kubernetes smoke boundary
- **THEN** native RBAC and TokenRequest issuance calls occur and its effective permissions allow reads but not creates/deletes

#### Scenario: Shell
- **WHEN** an editor starts shell
- **THEN** its mounted config has namespace-limited workload credentials and cannot copy administrator credentials

### Requirement: Credential lifecycle disclosure
Documentation SHALL identify the historic downloaded-certificate revocation limit and native runtime smoke setup.

#### Scenario: Historic exposure
- **WHEN** operators upgrade native authorization
- **THEN** documentation explains that old administrator certificates remain effective until explicit cluster trust remediation and no rotation is performed automatically
