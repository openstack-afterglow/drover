# Drover architecture and evidence governance

## Purpose

This is a contributor contract for Drover, not a claim that an OpenStack deployment or release has been verified. The [source-linked architecture](../../../ARCHITECTURE.md), [feature/security detail](../../../docs/drover-feature-coverage.md), [API contract](../../../docs/drover-api-v1-reference.md), and [release notes](../../../docs/release-0.2.25.md) describe their respective implementation/evidence boundaries. Actual source outranks plans and old documentation. No architecture, runtime, or deployment change is made by this documentation migration.

## Requirements

### Requirement: Source-backed architecture review

Contributors SHALL read `ARCHITECTURE.md` and affected `docs/` detail before changing code, config, schema, dependencies, deployment, or tests. They SHALL update affected architecture sections and source-linked detail in the same change. A topology-neutral bugfix/refactor SHALL still explain why its topology/data contract is unchanged in the latest architecture review summary. They SHALL review actual source before stamping with `python3 scripts/check_architecture.py --stamp --summary "변경 경로와 영향"`; for a staged-only review they SHALL use `--stamp --staged --summary "..."`. Before completion or commit they SHALL pass `python3 scripts/check_architecture.py` (or `--staged` for the reviewed staged range). The pre-commit hook uses `python3 scripts/check_architecture.py --staged`. A documentation-only change SHALL NOT restamp unrelated, unreviewed source as reviewed to conceal a stale guard.

#### Scenario: A code change alters a contract
- **WHEN** source changes affect a runtime boundary or a test definition
- **THEN** the corresponding architecture/detail descriptions and source-backed review marker are updated in that change and the applicable guard is run before completion

#### Scenario: A topology-neutral change or unrelated dirty tree
- **WHEN** a bugfix changes no topology, or unrelated unreviewed source leaves the architecture guard stale
- **THEN** the summary records the topology-neutral reasoning for reviewed source; the contributor does not stamp unrelated dirty files as reviewed or claim the guard passed

### Requirement: Honest verification status

Only an actually executed `uv run pytest ...` SHALL be called `test-passed`, and only an actually exercised live OpenStack call SHALL be called `live-verified`. Existence of a test defines `test-defined`, not a pass. Plans, projections, source-preparation notes, tag defaults, and readiness/liveness do not prove artifact publication, deployment, or a working K3s cluster. Common entry points are `uv run pytest tests`, `uv run ruff check .`, and `uv run drover-migrate --apply`; migration/readiness requires the relevant DB, Redis, and Keystone prerequisites. SDK tests have a separate `uv --directory sdk run pytest` entry point; live integration has separate prerequisites and evidence.

#### Scenario: Reporting a release or test result
- **WHEN** a contributor reports verification or a deployment/release outcome
- **THEN** they state the actually executed command/environment/date and limit the claim to observed results; skipped integration tests and unverified wheel/image tags remain unverified

### Requirement: Keep credentials and ownership boundaries intact

Contributors SHALL NOT put credentials, raw tokens, passwords, or encryption keys in docs, logs, or examples. Deployment secrets come from appropriate environment/secret-file boundaries, not committed literal values. Drover's API validates Keystone project/token ownership; the Worker uses tenant-project manager identity, not caller keypair names; callback authentication uses one-time Redis token and configured source CIDR restriction before consumption. Kubeconfig is encrypted in MariaDB. Cluster plugins use minimum-privilege application credentials, not a disclosed OpenStack service password. Admin managed-resource responses must mask secrets. These contracts and the current implementation details are maintained in [architecture security boundaries](../../../ARCHITECTURE.md#security-boundaries) and [feature coverage](../../../docs/drover-feature-coverage.md#41-보안-아키텍처-security-architecture).

#### Scenario: Documenting or modifying a credential-handling path
- **WHEN** a change touches API/Worker/callback/plugin authentication or adds an example
- **THEN** project/CIDR/token/least-privilege and secret-redaction boundaries remain explicit and no actual secret value is published

### Requirement: Preserve certification and deployment caveats

Certificate rotation has no fixed inter-node settle wait; the Ready-first-poll can be stale and etcd member health is not checked. Removing the old implicit 10-second floor is an operational safety change whose owner confirmation is **still pending**; upstream master `Type=notify` rationale is not validation against the pinned K3s version or a live HA cluster. The known SSE-disconnect Redis rotation-lock release defect remains unresolved. A contributor SHALL NOT represent this path as owner-approved or risk-free on the strength of tests alone. The Kolla `drover_image_tag` default is not proof that either image exists; publication/digests and authorized staging/live behavior need their own evidence. Consult [Certificate rotation](../../../ARCHITECTURE.md#certificate-rotation), [current release boundary](../../../docs/release-0.2.25.md), and the [manual staging workflow](../../../.github/workflows/staging.yml).

#### Scenario: Cert rotation or release readiness is discussed
- **WHEN** review or release text refers to safety or rollout completion
- **THEN** pending owner confirmation, the known lock failure, and the difference between source/test evidence and live/published evidence are stated without promoting a plan to a fact
