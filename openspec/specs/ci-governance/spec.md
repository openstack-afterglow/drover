# Drover CI critical-path and publication contract

## Purpose

Applies to `.github/workflows/`, tests executed in CI, and `docker/Dockerfile`. This is the normative contract migrated from the original root guidance; historical measurements, projections and dated access observations are [separate evidence](evidence.md). Current wiring: [CI](../../../.github/workflows/ci.yml), [image publication](../../../.github/workflows/docker-build.yml), [wheel release](../../../.github/workflows/release.yml), [workflow contract tests](../../../tests/test_ci_workflows.py), [architecture](../../../ARCHITECTURE.md#development-and-verification). “SHALL” describes change obligations, not an assertion that the current release workflow is already compliant.

## Requirements

### Requirement: Measure the real critical path before claiming savings (original rules 1, 2, 12)

Before and after CI changes, contributors SHALL collect at least 20 recent runs' job/step timings using `gh run list --workflow ci.yml`, `gh run list --workflow docker-build.yml`, and `gh api repos/openstack-afterglow/drover/actions/runs/<id>/jobs`. Record event-matched median and p90 from run creation to final required job completion in the commit body or PR and this spec's [evidence](evidence.md) once measured. Compare `CI` PR with PR and Docker push/tag suite through last `test / *` job with push suite, not simply a mixed-event baseline. Start with the longest job (`docker-build-and-scan` in the historical sample); savings from independent jobs do not add. Mark log-derived numbers *projected* until post-change Actions runs actually measure them. Drover is public and uses hosted runners, so optimize wall-clock first; if private or paid runners are introduced, also track runner-minutes. If the latest same-event median regresses ≥20% (historical triggers: `CI` PR >125s, Docker push suite >118s), test count rises substantially, or a new test layer is introduced, remeasure and improve the longest job first. Every CI change SHALL attach before/after measurements or explicitly state that post-change measurements are not yet available; do not call a projection a speedup.

#### Scenario: Reviewing a CI speedup
- **WHEN** a workflow/test-runtime/Dockerfile change claims an improvement
- **THEN** a same-event ≥20-run before/after critical-path median/p90 comparison supports it, or the claim is explicitly labeled unmeasured/projection

### Requirement: Parallel validation with fail-closed artifact publication (original rules 3, 11)

`python3 scripts/check_architecture.py` SHALL run as the first `service` step after checkout, not as a `needs:` prerequisite for other validation jobs. CI jobs and steps run in parallel/unconditionally without job dependencies or `if:` and without `continue-on-error`. `service` runs exactly `uv run pytest tests` and `uv run ruff check .`; SDK runs its full suite. A non-publishing PR validation image build MAY run alongside tests. Image/package publication and deployment SHALL wait for the *whole* suite before publishing; image `build-and-push` does so with `needs: test`. For every `docker-build.yml` publishing job (job/workflow permission `packages: write` or `write-all`, `docker/login-action`, or `docker/build-push-action` with `push` other than literal `false`), `needs` SHALL contain `test`; publishing and `test` jobs SHALL NOT have job-level `if` or `continue-on-error`. Skipped/failed/cancelled suite results must never appear as successful publication gates. **Known unresolved exception, not an achieved gate:** `release.yml` publishes a GitHub Release wheel on `v*` tag independently of the same tag's `Docker Build & Push / test`; it does not wait for suite success. This documentation migration does not change that workflow; closing the gap requires a separate release-workflow change. Contract tests do not enforce that wheel gate today.

#### Scenario: A test fails before GHCR image publication
- **WHEN** any required reusable `test` job fails, skips, or is cancelled
- **THEN** image publication does not run; a tag-triggered wheel release is currently **not** guaranteed to wait and must not be described as gated

### Requirement: Measure setup and preserve scan coverage (original rules 4, 11)

Measure checkout, `setup-uv`/`uv sync`, Buildx setup, and service-container initialization before changing fixed costs. Remove cache only if restoration is slower than reinstalling; compare `prune-cache: false` empirically if considering it. Service health checks SHALL use an interval at most 2s with start-period + interval × retries ≥30s; Docker start-period failures do not count against retries. Runner readiness polling backoff can quantize any savings, so interval changes alone do not prove faster initialization. For scan-only Docker image builds use default docker driver with `load: true`, no image push and no unsupported docker-driver build-cache export; benchmark any proposed layer-cache/driver replacement. Trivy SHALL scan **every** locally built `drover-api` and `drover-worker` image using the immutable known-safe `aquasecurity/trivy-action@57a97c7e7821a5776cebc9bb87c984fa69cba8f1` (0.35.0); maintain the scanning contract in `tests/test_ci_workflows.py`. Current Trivy `exit-code: '0'` is a report, **not** a vulnerability-blocking gate; do not claim HIGH/CRITICAL findings prevent publication or silently weaken build-and-scan coverage. Buildx metadata/labels/publishing steps are not exercised by PR; first PR scan and first post-merge dev push are separate path checks. Their exact step times belong in evidence. If Buildx-only regression repeats, measure a PR Buildx dry build's cost before adding one.

The health-window contract test counts start-period only when expressed in whole seconds as `--health-start-period=Ns`; changes SHALL preserve that supported representation when relying on it to satisfy the required window.

#### Scenario: A second image target or scanner change is proposed
- **WHEN** an image is built for PR validation or Trivy action/scan driver changes
- **THEN** every built target is scanned by a pinned Trivy action, local scans do not publish, and any changed caching/driver claim is supported by measurements

### Requirement: Only useful, isolated test parallelism (original rules 5, 6, 7)

Shard only when test duration dominates fixed setup cost. Partition by pytest file/item and call the runner directly, not by appending shard arguments after a wrapper script; verify per-shard test counts in CI. The historical local `service` pytest 11–18s and **projected**, not measured post-change CI 15–20s do not justify sharding. Isolation relaxation is per-file opt-in only after at least two shuffled runs identify no shared-state leaks; monkeypatch/global state changes are restored. Unit tests SHALL NOT contact actual Keystone, OpenStack APIs, K3s API servers or arbitrary external TLS IPs. Patch failure boundaries such as `socket.create_connection`. Keepalive SHALL stop delaying completion once work ends; remaining operational settle/backoff/retry waits SHALL be named module constants patched to zero in tests. Changing production safety waits needs independent operational justification, not a CI speed rationale. In particular cert rotation's former implicit 10s inter-restart floor is gone **without owner confirmation**; see [architecture certificate rotation](../../../ARCHITECTURE.md#certificate-rotation). A future pytest-xdist change SHALL explicitly cap workers to CI vCPU (`-n auto` prohibited).

Unit-test results or timings that depend on whether a local configuration file exists SHALL be treated as a defect, not as an acceptable CI/local environment difference.

#### Scenario: A fast test depends on live networking or a production sleep
- **WHEN** a unit test touches an external socket or a safety wait slows the suite
- **THEN** the test patches the connection/wait boundary without changing production safety behavior merely to speed CI

### Requirement: Accurate change detection and no identity-based skips (original rules 8, 9)

Current CI has no path filter/change detection. If introduced, push diff SHALL use `github.event.before..github.sha`, treating zero SHA, forced push, and fetch failure as full-scope; PR diff SHALL use base..head. `HEAD^1..HEAD` is insufficient. Assess published artifacts against the actual published revision. The same event/SHA/suite SHALL run once: PRs targeting main/dev, including forks and dependabot, run `CI`; dev/main push and `v*` tags run `Docker Build & Push / test` via reusable `ci.yml`. `CI` has no push trigger; `docker-build.yml` push filters are only `branches` and `tags`, with no PR trigger. PR tests MAY skip only for a same-repository branch whose merge tree is proven identical to an already-tested head tree; forks and dependabot SHALL always run. Never skip based only on actor, event, branch name (including fork names), or `head.repo` without tree equivalence. Because skipped jobs report success, introducing this exception requires simultaneous contract-test changes and proof of identity; current `ci.yml` has no job/step `if:`.

#### Scenario: A fork uses the same branch name as a repository branch
- **WHEN** a fork/dependabot PR targets dev/main
- **THEN** all CI jobs and steps execute; branch-name/head-repository-only shortcuts cannot bypass validation

### Requirement: Public PR code has no privileged runner or secret (original rules 10, 11)

PR-triggered workflows and *transitively called local reusable workflows* SHALL execute PR code only on GitHub-hosted `ubuntu-*` runners, with top-level `permissions: contents: read`, no job-level permissions/environment/secrets forwarding, and no secret reference other than `secrets.GITHUB_TOKEN` (case-insensitive, including bracket access/`toJSON(secrets)`). Do not run public-repo PR code on self-hosted runners or through unchecked remote reusable workflows. Contract tests parse all reachable workflow YAML, but a PR can modify both YAML and tests; GitHub settings are an additional control. The recorded fork approval setting `first_time_contributors` approves first-time contributors only; changing to `all_external_contributors` is an owner decision. The org owner SHALL verify that organization runner-group `Repository access` and `Allow public repositories` do not expose self-hosted runners to Drover; the historical audit could not inspect org runner groups (HTTP 403). Introducing self-hosted runners requires verifying and updating both settings in the same change, **without** opening them to public PR code. See [dated security observations](evidence.md#repository-and-organization-security-observations).

#### Scenario: A workflow runs fork PR code
- **WHEN** a PR workflow invokes a local reusable workflow
- **THEN** every reachable job uses `ubuntu-*`, read-only top-level token, no environment or forwarded secrets, and org runner-group exposure is not falsely assumed verified

### Requirement: Maintain the executable CI contract (original rule 11)

When modifying workflows, contributors SHALL update `tests/test_ci_workflows.py` in the same change to preserve/explicitly revise trigger sets, job/step unconditional execution and error propagation, architecture-first/full-suite commands, all-image pinned Trivy scan with docker-driver `load: true`, adequate health windows, hosted runner/read-only PR token/no other secrets, and image publication's suite dependency. The current test explicitly does **not** enforce the unresolved `release.yml` wheel gate or organization-level GitHub settings. The image build also skips publishing Buildx/metadata/labels on PR; post-merge detection is accepted because `needs: test` protects GHCR publication.

#### Scenario: A workflow optimization changes a pinned invariant
- **WHEN** a contributor changes trigger, scan, permission, dependency, or skip behavior
- **THEN** the contract test changes with an explicit justification, and no failed or skipped suite can silently satisfy an artifact publication gate
