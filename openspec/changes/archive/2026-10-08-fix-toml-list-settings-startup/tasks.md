## Investigation
- [x] Inspect reported migration logs, exact native input/source path, healthy MariaDB and existing001–003 ledger without restarting failed startup.

## Implementation and verification
- [x] Add file-backed settings startup/precedence/authority regressions; the new public-entrypoint startup regression failed before correction with the same EnvSettingsSource role-list SettingsError as the local `drover-migrate` container.
- [x] JSON-serialize structured TOML fallback values while preserving scalar encoding, explicit environment precedence and validation.
- [x] Build/execute canonical API and worker targets on linux/arm64 and linux/amd64: all four images ran typed TOML settings, delegation selection and API/worker imports with the host `config.py` SHA-256.
- [x] Recover existing local Compose migration and affected services, retaining DB/volume/key identity: 0600 dump backup, unchanged config/secret hashes and 001–003 ledger rows, 004 applied once (second run no-op), readiness/backend/frontend 200, Chromium login without console/page errors. No cloud resources were created.
- [x] Run affected configuration/authority/migration regressions and repository gates: config 26, selected 143, full service suite 1135 passed/1 skipped, changed-file Ruff passed. Full `ruff check .` fails on out-of-scope working-tree SIM102 (`drover/api/configmaps.py`) and UP017 (`drover/services/cloud_shell.py`); their HEAD versions pass.
- [x] After runtime proof, update architecture/settings/runbook documentation, sync main requirements and archive the completed change.
