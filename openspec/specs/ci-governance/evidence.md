# Drover CI evidence and unresolved verification

Historical evidence migrated from the former root `AGENTS.md` CI section. It is **not** a new benchmark, a measurement of this documentation change, or evidence that a configured workflow ran successfully. Normative change obligations and scenarios live in [the CI spec](spec.md). Sources: [CI](../../../.github/workflows/ci.yml), [image publisher](../../../.github/workflows/docker-build.yml), [release workflow](../../../.github/workflows/release.yml), [architecture](../../../ARCHITECTURE.md#development-and-verification), [0.2.23 release notes](../../../docs/release-0.2.23.md).

## Baselines (pre-2026-09 improvement)

Runs from 2026-08-30 through 2026-09-19, critical path = run creation → final job completion (unless otherwise specified):

| Cohort | Median | p90 | Size, run IDs / qualification |
|---|---:|---:|---|
| `CI` PR + push mixed | 100s | 151s | n=40, push 30 + PR 10, 33339073898..35421775251 |
| `Docker Build & Push` all | 160s | 231s | n=40, 33358781597..35421775411 |
| `CI` PR only, recent 20 (event-matched baseline) | 104.5s (rounded 105s) | 148.1s (rounded 148s) | n=20, 33299439644..35405417634; 2026-08-30 07:32Z → 2026-09-18; ten PRs were from 2026-08-30 |
| `CI` push only | 98s | 143.5s (rounded 144s) | n=30, 33339073898..35421775251 |
| `Docker Build & Push` push suite only, creation → last `test / *` job (event-matched baseline) | 98.5s (rounded 99s) | 122.6s (rounded 123s) | n=32, 33358781597..35421775411 |

Earlier PR n=10 estimate was 105.5s / 152.9s; the PR n=20 cohort supersedes it for comparisons. Of those 20 PRs, `docker-build-and-scan` ended last 16 times and `service` 4; `ci.yml` retained the same five-job shape in the period except a package-job rename, which never ended last. Overall last job: `docker-build-and-scan` 25/40, `service` 15/40. Job median/p90: scan 94/148s (slow apt-get two-step path 50–110s), service 90s (pytest step 78s), DB migration/readiness 39s (container initialization 23s), SDK 10s, package-wheel 9s. Median job queue wait 2s.

## Fixed cost measurements and projections, not a post-change CI result

Baseline 40-run step medians at one-second resolution: `setup-uv` 2s; `uv sync --all-extras --frozen`: service 3s, DB job 4s, SDK 1s; DB job `Initialize containers` 23s, p90 30s, including service image pull. Logs for cache hits jobs 105794073559, 105291927609, 105794061515 and miss job 105291927868 showed `prune-cache: true` preserving only ~93–97KB. Hits and miss both logged `Prepared 81 packages` (2.2–4.5s). Restore cost 0.1–0.6s was not slower than reinstall; `enable-cache: true` was retained. Comparing `prune-cache: false` is a future measurement, not a recorded benefit.

2026-09 changes: remove duplicate push + PR suite execution; remove a fixed 10-second cert-rotation sleep (local macOS arm64 pytest 78s → 11–18s); switch scan to docker driver; set container health interval to 2s. **Projected, not measured after:** mixed `CI` median 100s → ~82s; p90 may remain ~150s due to Debian apt mirror delays. No event-specific post-change projection was calculated. The local 11–18s suite is not a CI timing. Predicted CI pytest step ~15–20s comes from historical 78s minus ~60s of sleeps and up to ~3.5s TLS connection timeout; post-change CI pytest is unmeasured. DB initialization was never last in the baseline (0/40), so the health interval change was excluded from the projected critical-path gain.

Docker `start-period` and retries provide additive probe-failure windows. GitHub runner readiness polling backs off independently: observed `Waiting for all services to be ready` checks at 2→4→7s (job 105291927609) and 2→3→9s (job 105794061515), both using the *old* 10s health interval. Approximate check times after waiting began were 0/2/6/14s, next ~30s **projected**. A healthy state reached before the old ~14s check could be observed at ~6s or ~2s instead (8s or 12s projected savings), or 0s saving if it lands in the same runner check. Both observed MariaDB-healthy states were detected at the ~14s check. The new 2s interval's effect on `Initialize containers` remains unverified until actual before/after step timing.

Current measurement method: `CI` runs on PR and `workflow_dispatch`, not push; `Docker Build & Push` runs on main/dev push and `v*` tag and invokes the suite via `test`. Collect ≥20 **new** PR runs and ≥20 Docker push/tag runs separately and add event-matched medians/p90 here; do not forecast when samples will accrue. The previous PR sample was clustered (10/20 on Aug 30; only 3 over the 17 days after Sep 2). Use `CI` PR 104.5/148s and Docker push suite 99/123s as matching comparators; mixed 100/151s only for context. Historical 20% median regression references are >125s PR, >118s push suite (mixed >120s informational only).

## Accepted gaps and pending safety checks

- PR scans build `drover-api` and `drover-worker` without `setup-buildx-action`, default docker driver, `load: true`; PRs do **not** run publishing Buildx docker-container builds or metadata/labels. This can reveal publish-only breakage first on dev/main merge. Because `build-and-push` depends on the whole `test` result, publication is fail-closed *for tests*, not guaranteed to succeed after merge. The original validation plan called for recording `Build drover-api image target` at first PR and `Set up Docker Buildx`, `Build and push API`, `Build and push Worker` at first dev push. These two paths were not both proven by a local scan. Repeated Buildx-only regression would justify measuring a PR dry build before adding it.
- `release.yml` already built/uploaded the root wheel to GitHub Release on `v*` independently of the image publisher's suite. **Unresolved:** it has no suite wait/`needs: test` gate; the docs/spec migration does not fix it. The workflow checks tag/version lockstep and wheel/Kolla role packaging, not overall suite success. `tests/test_ci_workflows.py` checks wheel shape, not this gate.
- Current CI scans two image targets with Trivy SHA `57a97c7e7821a5776cebc9bb87c984fa69cba8f1` (0.35.0) and severity `CRITICAL,HIGH`, but `exit-code: '0'` means findings are **non-blocking**; do not report that vulnerabilities fail the job. Duplicate worker scan cost was historically median 18s, retained for both-target coverage. Deferred candidates: Dockerfile apt layer cache (p90 tail; conflicts with docker driver, measure on an experimental branch), pytest-xdist (service expected no longer critical), skipping main-merge/tag retests (publication gate risk).
- Cert rotation: removing the old implicit ≥10s floor after Job completion is an operational safety change; no owner confirmation. Stale Ready/unchecked etcd and the SSE disconnect lock-release defect remain described under [Certificate rotation](../../../ARCHITECTURE.md#certificate-rotation). Do not infer live safety from a locally faster suite.
- 2026-09-24 non-loopback `socket.connect` recorder during the full suite observed zero connections; it is a dated local observation, not proof for future test additions or live OpenStack tests.

## Repository and organization security observations

Read-only `gh api` observations on 2026-09-24: fork PR approval value `first_time_contributors` at `repos/openstack-afterglow/drover/actions/permissions/fork-pr-contributor-approval` ([repository Actions settings](https://github.com/openstack-afterglow/drover/settings/actions)); first-time contributors' fork PRs require approval, **not** all external contributors. Owner decision pending on stricter `all_external_contributors`. Repository-level self-hosted runners returned zero at `repos/openstack-afterglow/drover/actions/runners`. Organization runner-group listing failed HTTP 403 (missing `admin:org`), so whether any self-hosted group is exposed to this public repo was **not confirmed**. Org owner must check each group's `Repository access` and `Allow public repositories` in [organization runner-group settings](https://github.com/organizations/openstack-afterglow/settings/actions/runner-groups); update these observations if settings change or new runners are introduced. CI YAML/contract tests alone are mutable by a PR and do not constitute an organization-level control.

## Prior architecture review provenance

Before this documentation migration, the architecture marker recorded source SHA-256 `325e70a5501cfbe6656b9a098a5652b4f62be2f08f3bf9a4fab2b1442cd0df7c` at `2026-09-26T18:00:26Z`, with summary: "0.2.25: cluster deletion reads every Service's annotations before touching nodes/VMs (kube.list_service_annotations) and the OCCM LB cleanup keeps floating IPs whose creator or load-balancer-id sharer set keep-floatingip, or whose intent is unknown; release notes and versions; no schema/public API change".

This prior snapshot's review scope remains historical evidence; the later documentation-only stamp neither reruns those paths nor supersedes their implementation or verification status.
