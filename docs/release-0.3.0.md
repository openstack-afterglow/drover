# Drover 0.3.0 release notes

Changes since `v0.2.25`. Minor release: it adds request/job lifecycle logging and an opt-in `LOG_LEVEL=DEBUG` setting. There is no schema, migration, or `/v1` API change. This release has not been deployed; record live evidence here after the Kolla rollout.

## Added

- **HTTP completion log.** `CorrelationMiddleware` writes one INFO `API completed` line per HTTP response: method, route template, status, `success`/`error` outcome, duration, and the validated request ID. Routes from `include_router` resolve to the full prefixed template (`/v1/clusters/{cluster_id}`) through `fastapi.routing.iter_route_contexts`; FastAPI 0.141 puts the router-local route (`""` for `GET /v1/clusters`) in the ASGI scope. Unmatched paths log `(unmatched)`. A failure before the response starts still returns 500 and logs `error`.
- **Durable job outcome log.** The worker logs `Drover job completion kind=… job_id=… attempt=… outcome=success|retry|error|deferred` only after the attempt fence accepts the transition. A worker that lost its lease logs nothing. Unknown kinds log as `unknown` and non-canonical IDs as `untrusted`.
- **`LOG_LEVEL=DEBUG`.** `drover/logging.py:configure_logging` sets up API and worker logging with a shared format that includes `request_id`. Drover loggers default to INFO; `LOG_LEVEL=DEBUG` raises only the `drover` logger hierarchy, not HTTP/DB/OpenStack libraries. DEBUG lines summarise query/state/result with `safe_metadata`: counts and allowlisted field names only, never values.

## Changed

- Callback, auth and worker-loop logs no longer include exception strings, tracebacks, callback source/server IPs, plugin names or plugin error text. Durable job failures keep the error in `DroverJob.last_error` and operation events, but the log no longer carries a traceback.
- `AGENTS.md` CI rules and baselines moved to `openspec/specs/ci-governance/spec.md` and `evidence.md`. The architecture maintenance contract lives in `openspec/specs/architecture-maintenance/spec.md`.

## Known limits

- Docker and Kolla run `uvicorn` with the access log enabled, so the uvicorn access line still includes the raw path and query string. Do not put credentials in query strings.
- Readiness probe failures (`drover/main.py`, `drover/db.py`) still log tracebacks.
- The Kolla role has no `LOG_LEVEL` variable. To use DEBUG, set it in both container environments.

## Verification (2026-10-02, local)

- `uv run ruff check .` passed. `python3 scripts/check_architecture.py` passed after the source-reviewed stamp.
- `uv run pytest tests`: 689 passed, 3 skipped. `uv --directory sdk run pytest`: 111 passed. `uv build --wheel` produced `drover-0.3.0-py3-none-any.whl`, which ships the Kolla role with `drover_image_tag: "v0.3.0"`.
- `tests/test_correlation_middleware.py::test_api_completion_logs_prefixed_template_for_included_routers` covers prefixed templates, including one router included under two prefixes. Before the fix, the live server logged `route=` for `GET /v1/clusters`.
- Local `uvicorn drover.main:app` with `LOG_LEVEL=DEBUG`, placeholder config and unreachable loopback MariaDB/Redis/Keystone returned `/v1/health/live` 200, `/v1/health/ready` 503, `/v1/clusters` and `/v1/clusters/{id}` 401, and an unknown path 404. The Drover log showed `route=/v1/clusters`, `route=/v1/clusters/{cluster_id}` and `route=(unmatched)` with no path or query values.
- Both Dockerfile targets were built locally (`linux/arm64`) and run with the same placeholder config. `drover-api` reported version `0.3.0`, returned the same status codes, and logged prefixed templates. `drover-worker` started, logged the DEBUG job-query summary, and logged `Jobs worker loop failed` with no exception text.

Not verified: a real MariaDB/Redis/Keystone stack, live OpenStack, `linux/amd64` images, GHCR publication, and the Kolla rollout.

## Release metadata

`pyproject.toml`, `drover/__init__.py`, the root package in `uv.lock`, and the Kolla role's `drover_image_tag` declare `0.3.0`. `drover-sdk` stays `0.2.21`. Afterglow's Kolla installer pins `drover==0.2.25` (`deploy/kolla/README.md`) and needs its own update to adopt this release.

## 0.3.1 patch candidate (2026-10-05)

Prepared on `dev` at `8afc434eeb29b245d6077c3795da28cab65d9e88`, with `origin/main` at `a571fea33eeaf0bbf14847cbdbde158b35c8e7fd`. `git ls-remote --heads --tags origin` contained no `v0.3.1` or `0.3.1` tag. No tag or push has been performed.

This patch updates root `pyproject.toml`, `drover/__init__.py`, the root package record in `uv.lock`, and the shared Kolla role's `drover_image_tag` to `0.3.1` / `v0.3.1`. `drover-sdk` remains `0.2.21`; ordinary dependencies and `drover_source_version` remain unchanged. The only function changes fix an observed Redis shutdown error: the pinned `redis==5.0.0` exposes asynchronous `close()`, not `aclose()`. API cache shutdown (`drover/cache.py`) and cutover source/destination shutdown (`drover/scripts/cutover.py`) now call that supported method. No schema, migration, API or topology change is included. The 0.3.0 evidence above remains historical rather than being relabelled as 0.3.1 evidence.

### Finalized branch coverage

Read-only ancestry/history review found no finalized release work to merge:

| Branch / ref | Reviewed tip | Disposition |
| --- | --- | --- |
| `ci-perf` | `f9ceea0f00ce30aa96f79926f894859a17c0bd75` | Already an ancestor of dev; no branch-only commits. |
| `claude/frosty-rhodes-0fbe38` | `bf6ec22b2443d51dd63b22d3bcf083b9e350a32a` | Admin TLS/cert-rotation signature fix already an ancestor of dev. |
| `claude/vigorous-taussig-ebd2f3` | `aea117045fad9731cbacc5d4a511d6fd0d0bf42d` | FastAPI security update already an ancestor of dev; PR #24 merged. |
| `origin/fix/sdk-catalog-relative-v1` | `0e8bc4407a06ff7aad82be2dba785c353320781a` | Stale, unmerged branch, no PR found. Its two commits are not patch-equivalent, but current dev supersedes their behavior; do not merge. |
| `origin/main` | `a571fea33eeaf0bbf14847cbdbde158b35c8e7fd` | Branch-only commits are release merge commits #26–29; no unique non-merge patch (`git cherry` empty). PR #29 promoted 0.3.0. |

The stale SDK branch blindly strips `/v1`; dev's `sdk/drover_sdk/proxy.py:Proxy.request` instead inspects the effective catalog version, preserves absolute URLs, and rejects malformed endpoints (`sdk/tests/test_proxy.py` catalog/version tests). Its flavor change is superseded by `d9aeeb793e908ea34790ccbf66a957a272289993` and current `FlavorInfo`, which retains `ram`/`disk`, legacy field synchronization and `extra_specs`. No backup/stale/unrelated branch was merged or cherry-picked; SDK stays unchanged.

### Patch runtime and packaging evidence

- Frozen Python 3.12 environments installed with `uv sync --python 3.12 --all-extras --frozen` and `uv --directory sdk sync --python 3.12 --all-extras --frozen`.
- `uv build --wheel` produced `dist/drover-0.3.1-py3-none-any.whl`. A separate Python 3.12 venv installed it with `uv pip install --no-deps`; an isolated `python -I` import outside the source tree confirmed package and runtime version `0.3.1`. All 20 installed role files matched the source bytes, including image tag `v0.3.1`. The wheel declares Python `>=3.11`, exposes the `service` extra, and does not require Kolla-Ansible. Uninstall removed all role files.
- Actual loopback Uvicorn process, with placeholder configuration and intentionally unreachable MariaDB/Redis/Keystone, returned `/v1/health/live` **200**, `/v1/health/ready` **503** with all four checks unavailable, and cluster requests **401** both without a token and with an invalid synthetic token. This is real HTTP failure-path evidence, not a mocked ASGI client or successful cloud authentication.
- Actual `drover-worker` entrypoint stayed running, attempted jobs, logged dependency-unavailable job/reconciliation failures, and handled SIGTERM with `Drover worker stopped cleanly` and exit 0. The internal health service still logs a DB traceback on this failure path; successful job execution against OpenStack is not established.
- Two regression tests use the actual pinned Redis client with only the network boundary mocked: `tests/test_redis_backend.py::test_close_cache_disconnects_pinned_redis_and_clears_client` and `tests/test_cutover.py::test_redis_migration_closes_both_pinned_clients`. Both failed with `AttributeError` before the three shutdown callsites were corrected. No dependency upgrade or compatibility shim was added.
- Final post-fix Python 3.12 gates: `uv run --frozen pytest tests` **691 passed, 3 skipped**; `uv --directory sdk run --frozen pytest` **111 passed**; both root and SDK `ruff check .` passed. `python3 scripts/check_architecture.py` passed for source snapshot `0b0bfd402de00927f1976ff7416824e37ac4e898905487be3e47970513b578b1`. The final rebuilt wheel SHA-256 is `620b4ce87d330fe2f1a29658f1e25d1973ec3cc6857df55b1f1d238acbf01ab9`; isolated installation/uninstallation and all role bytes were checked again after the shutdown fix.
- Disposable canonical CI dependencies (`mariadb:11.4`, `redis:7-alpine`) passed `uv run --frozen drover-migrate --apply`. Real checks reported database/Redis/migrations **ok** and Keystone **unavailable**. Actual loopback HTTP remained live **200**, ready **503**, and unauthorized cluster access **401**. API shutdown completed without the Redis `AttributeError`; cutover Redis dry-run exited 0 without mutation. The worker queried the migrated empty queue (`processed=0`) and shut down cleanly. This proves local SQL/Redis paths, not successful cloud provisioning or ready 200 with valid Keystone credentials.
- Final Dockerfile builds used `docker build --platform linux/<arch> --file docker/Dockerfile --target <target> --tag drover-release-031-<api|worker>:<arch> .` for **both targets on linux/arm64 and linux/amd64**. Changed source stages executed after the shutdown fix. All four images reported runtime `0.3.1`, correct machine architecture (`aarch64` / `x86_64`), and the final cache-source checksum. Each API ran the canonical Uvicorn CMD and returned live **200**, ready **503**, and missing/invalid-token **401** over loopback; each worker ran the canonical `python -m drover.worker` CMD, attempted jobs with intentionally unavailable dependencies, and stopped cleanly with exit 0. arm64 ran natively; amd64 ran under local Docker emulation. These are local target builds, not a GHCR multi-architecture publication or Kolla rollout.

### Publication policy and remaining boundaries

For **both** `drover-api` and `drover-worker`, `.github/workflows/docker-build.yml` runs the reusable CI suite and requires `needs: test` before publication. Version tags produce the original `v*` image tag, a SHA tag, **and `latest`**: both metadata steps use `docker/metadata-action@v5` with `type=ref,event=tag` and no `flavor` override, so the action's default `latest=auto` adds `latest` for tag events ([upstream policy](https://github.com/docker/metadata-action/tree/v5#latest-tag)). Dev produces `dev` plus SHA; main explicitly produces `latest` plus SHA. Thus a version-tag push **does move `latest`**; the explicit main-only raw rule does not disable the automatic tag rule. The publication workflow uses the runner's default platform, not a declared multi-architecture manifest.

The separate `.github/workflows/release.yml` checks tag/runtime version lockstep, builds the expected root wheel and checks isolated role install/uninstall, but does not wait for the tag suite. Trivy's existing `exit-code: '0'` reports findings without blocking publication. These existing policy gaps are unchanged.

The image publisher does not itself compare the tag with the runtime package version; that check lives only in the separate wheel workflow. Before eventual tagging, the candidate commit must retain matching root/runtime/lock/role metadata and built runtime `0.3.1`. CI's named API/worker readiness step only directly calls `readiness_checks()` and asserts database/Redis/migrations; it does not launch either runtime or require Keystone success. The actual process/container evidence above is separate from that CI helper.

Live Keystone/OpenStack provisioning, GHCR publication, a published multi-architecture manifest, and Kolla rollout are not verified by local failure-path smoke. No production deployment is claimed.

