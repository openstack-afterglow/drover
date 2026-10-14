## ADDED Requirements

### Requirement: File-backed structured settings load through the native entrypoint
Native `get_settings()` SHALL preserve typed TOML/default list values when bridging them into Pydantic environment settings. Configured delegation roles and callback CIDRs SHALL load through the same path used by migration, API and worker startup. Scalar settings retain their current encoding. The implementation SHALL NOT disable complex-value decoding or bypass validation to hide a startup exception.

#### Scenario: Afterglow TOML with default or explicit arrays
- **WHEN** a nonempty Afterglow/Drover TOML file provides native settings, including default or explicit role arrays
- **THEN** public settings construction succeeds, delegation selects only configured held roles and configured callback CIDRs remain individual entries.

### Requirement: Environment precedence and role validation remain intact
A nonempty explicit environment value SHALL retain precedence over its TOML fallback. Empty environment values retain the existing fallback behavior. Structured JSON environment values SHALL continue to be validated. Required delegation roles SHALL remain nonempty and either required or optional role sets containing admin/manager SHALL be rejected before execution authority is granted.

#### Scenario: Explicit environment overrides a file array
- **WHEN** the same list field has a TOML array and a nonempty JSON environment array
- **THEN** the explicit environment array governs native role selection without merging the file array.

#### Scenario: Privileged or empty required delegation is configured
- **WHEN** file or environment configuration provides admin/manager roles or an empty required set
- **THEN** native startup rejects that configuration rather than coercing it to a safe-looking default.
