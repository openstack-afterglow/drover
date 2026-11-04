## Why
Published Drover 0.4.15 requires dedicated service leaves and rejects native project manager/admin. This contradicts the operator's standard project-role model.

## What Changes
- **BREAKING**: core project admin/manager/member manage owned clusters; reader reads nonsecret metadata. Dedicated service roles are no longer required or sufficient.
- Preserve native effective custom/group/domain implications, resource ownership, original credential ceilings and human system administration.
- Add native capability discovery and fail-closed operator policy reload; ship 0.4.16 without unrelated shared-checkout work.

## Capabilities
### New Capabilities
- `standard-project-authority`: native core-role floors, reduced-token ceilings and nonsecret capability discovery.
### Modified Capabilities
None.

## Impact
Authentication, action policy, Kolla policy mounts/configuration, tests and release metadata. Preserve001–004; package exact already-applied additive005 with explicit unsafe executor/cleanup-data refusal. No IAM writes, automatic role upgrades, executor runtime import or operational changes.
