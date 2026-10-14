## Context

The failed container is the canonical `python -m drover.scripts.migrate --apply` target from Afterglow `docker-compose.dev.yml`. Its role-list environment keys are initially absent; `_load_toml()` supplies typed lists from the mounted Afterglow TOML/defaults, and `get_settings()` incorrectly converts them with `str()`. MariaDB is healthy; its ledger contains unchanged migrations001–003. Failure happens before SQL connection.

## Decision

Keep the existing settings bridge and priority semantics. Encode list/dict fallback values with `json.dumps`; keep existing scalar `str()` encoding. Do not disable Pydantic decoding, replace operator inputs, relax role validation or bypass configuration in the migration CLI. This is a serialization defect, not a corrupt database or unsafe role-policy exception.

## Verification

Use file-backed public `get_settings()` regressions with isolated process environment/caches, including actual delegation role selection, populated CIDR lists, JSON environment overrides and denial of empty/privileged required roles. Keep current source/configured volumes and secrets intact. Execute both canonical image targets on arm64/amd64 and verify settings/native imports. Recover only the affected existing local stack; inspect jobs before worker replacement, apply the existing manifest, retain old ledger identities/timestamps, observe API readiness and backend/frontend HTTP. Runtime receipts are added after execution.

## Non-goals

No new configuration syntax, authority changes, retries/fallbacks, schema repair, data reset, dependency upgrade, production rollout, VM creation or global privilege assignment.
