## ADDED Requirements

### Requirement: Agent nodes admitted, recoverably
Drover SHALL observe every requested agent node until it is a Ready Kubernetes node or a bounded window passes. It SHALL NOT fail the create operation for a slow join. After the window an agent that is not Ready SHALL make the cluster `ERROR` with a reconciler-owned reason naming the agent, and a later reconciliation that observes the node Ready SHALL restore `ACTIVE` without operator action.

#### Scenario: Agent cannot reach the API
- **WHEN** an agent VM is ACTIVE but does not join within the bounded window
- **THEN** the cluster is ERROR with a reconciler-owned reason, the create operation is not FAILED, and no success is reported for the agent

#### Scenario: Agent joins late
- **WHEN** the agent becomes Ready after the window
- **THEN** the next reconciliation clears the ERROR and the cluster returns to ACTIVE

### Requirement: Cluster-internal API access
Drover SHALL open tcp/6443 from the cluster's own security group for every cluster so agents can reach the API regardless of administrator CIDR restrictions.

#### Scenario: Restricted administrator CIDRs
- **WHEN** a cluster is created with operator-only ingress CIDRs
- **THEN** its agents can join without any out-of-band rule

### Requirement: Administrator-managed network profile
A named, administrator-managed profile SHALL define the network and ingress CIDR sets of clusters created with it. Request-level CIDRs and network SHALL be rejected for those clusters, and non-administrators SHALL NOT receive security-group, rule or network identifiers.

#### Scenario: Profile request with CIDRs
- **WHEN** a request names a profile and also supplies allowed CIDRs
- **THEN** the request is rejected with HTTP 400 before any resource mutation

### Requirement: Sandbox connection (Phase 2)
Drover SHALL provide an explicit HTTPS server, verified CA, namespace and expiring least-privilege bearer token to a human-token caller of the cluster project, and SHALL NOT expose it to credential-restricted tokens or issue broader credentials.

#### Scenario: Application credential caller
- **WHEN** an application-credential or trust token requests the connection
- **THEN** the request is denied
