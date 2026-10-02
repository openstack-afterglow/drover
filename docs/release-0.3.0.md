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
