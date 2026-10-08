## Why
Project ownership currently admits privileged cluster operations and credential downloads expose administrator certificates. Native Drover must enforce service grades without promoting tenant roles into system/OpenStack administration.

## What Changes
- **BREAKING** Require Drover capabilities for native inventory, cluster mutations, workloads, credentials and shell.
- Preserve project isolation, verified system-only global settings/templates and independent provisioning callbacks.
- Issue real expiring Kubernetes RBAC-bound TokenRequest credentials for users/editors; eliminate impersonation-marker administrator credentials and shell administrator fallback.
- Document historic downloaded-certificate limits and native synthetic runtime smoke setup.

## Capabilities
### New Capabilities
- `scoped-service-access`: Native service grade authorization and restricted Kubernetes credential issuance.
### Modified Capabilities
None.

## Impact
Drover policy/auth, HTTP/WebSocket routes, Kubernetes credential helpers, shell sessions, tests and operational documentation. No production role mutation or live rotation.
