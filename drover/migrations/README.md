# Drover Database Migrations

## Base Migration Notice

- `001_baseline.sql` contains the historical baseline schema including the `gpu_quotas` table definition.
- Baseline migrations are immutable and must not be retroactively modified.

## `gpu_quotas` Table Retirement Prerequisite

Active GPU quota management has cut over to **Afterglow** (`app.services.gpu_quota`). The `GpuQuota` ORM model and Drover quota APIs have been removed.

- **DO NOT** auto-drop or rewrite the historical `gpu_quotas` table in baseline.
- Future source-table retirement (`DROP TABLE gpu_quotas;`) is permitted ONLY after:
  1. Complete and audited import of existing GPU quota data into Afterglow.
  2. Full rollout of Afterglow as the sole GPU quota authority across all environments.
  3. Verification that no components query Drover's legacy `gpu_quotas` table.

For detailed audit procedures and retirement steps, refer to [docs/gpu-quota-table-retirement-runbook.md](../../docs/gpu-quota-table-retirement-runbook.md).
