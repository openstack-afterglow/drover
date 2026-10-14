## Why

The user's current Afterglow local Compose build stops at `drover-migrate` with `SettingsError` for `drover_delegated_required_roles`, before opening MariaDB. The native `get_settings()` bridge writes typed TOML/default arrays into `os.environ` with Python `str()`, yielding `['member']` rather than a JSON array. Pydantic's environment source decodes complex fields as JSON before model validators. Direct `Settings(...)` tests did not exercise this configured-file startup path.

## What Changes

- JSON-serialize structured TOML fallback values when bridging them to environment settings; retain scalar encoding, explicit nonempty environment precedence and all validation.
- Exercise real file-backed `get_settings()` startup, configured arrays, environment precedence and privileged/empty required-role rejection.
- Build and execute canonical API/worker targets on both supported architectures, then recover the existing local Compose migration/API/backend/frontend without deleting volumes or changing secrets.
- Record actual schema/readiness results and limits after runtime proof.

## Capabilities

### New Capabilities
- `typed-toml-settings`: Typed list fallbacks load consistently through the native settings entrypoint without losing precedence or validation.

### Modified Capabilities

None.

## Impact

Native `drover/config.py`, existing configuration regression tests, canonical Drover API/worker images and settings/operator documentation. No SQL migration identity, application authority, role grant, key, dependency or Compose topology change. Existing local migration 004 may be applied by the user's requested startup; inspect and preserve 001–003 ledger/records first. No production/cloud mutation, commit or push.
