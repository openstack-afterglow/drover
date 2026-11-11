# Drover 0.4.18 — Kolla Ansible 2026.1 auth-cloud selector

## 한국어

- 버전: **0.4.18** (로컬 릴리스 후보).
- 릴리스 일자: **미발행**; 준비일 **2026-11-11 Asia/Seoul (UTC+09:00)**.
- 배포 일자: **미배포**. 발행과 운영 배포를 구분합니다.
- 변경/수정: Kolla role이 `openstack.cloud.project_info`로 `drover-service` project ID를 조회할 때 기존 `openstack_auth`와 함께 `openstack_auth_cloud`를 명시합니다. Kolla Ansible 2026.1 toolbox에서 auth mapping만 전달하면 `auth_url` 누락으로 실패하는 호환성 문제를 해결합니다.
- 폐기/제거: **없음**. 서비스 계정/프로젝트, 런타임 인증, 의존성, DB schema/migration, Kubernetes guest 동작은 바뀌지 않습니다.

## English

- Version: **0.4.18** (local release candidate).
- Release date: **Not published**; prepared **2026-11-11 Asia/Seoul (UTC+09:00)**.
- Deployment date: **Not deployed**. Publication and production rollout are separate.
- Changes/fixes: the Kolla role explicitly passes `openstack_auth_cloud` alongside `openstack_auth` when `openstack.cloud.project_info` resolves the `drover-service` project ID. This fixes Kolla Ansible 2026.1 toolbox authentication, where an auth mapping alone fails with missing `auth_url`.
- Deprecated/removed: **None**. Service identity/project, runtime authentication, dependencies, database schema/migrations, and Kubernetes guest behavior are unchanged.

## Scope and compatibility

This patch changes the Drover Kolla role's project lookup only. It reuses Kolla's selected auth cloud and does not add credentials, elevate roles, or alter the Drover service account. The regression test asserts the explicit `cloud: "{{ openstack_auth_cloud }}"` module argument. No application runtime code, image contents beyond version metadata, dependency, or migration changes are intended.

The wheel contains the Kolla role as shared data. An operator must install the 0.4.18 wheel for the task fix to reach Kolla; upgrading only the API/Worker containers leaves an installed 0.4.17 role unchanged. Registry mode selects numeric image tag `0.4.18`. Source-build mode uses a final metadata commit that pins the implementation commit's immutable full SHA; the release tag identifies that metadata commit, never a commit that tries to embed its own SHA.

## Qualification and publication receipts

- Implementation commit `a31b9fa` and source-pin metadata commit `5f7e9679cb23bbd731564d5f68ad9d4ca8928caf` (tag `0.4.18`, annotated object `aefe52c`). Exact-SHA dev CI for `5f7e967` succeeded: service, SDK, MariaDB smoke, wheel, Docker scan, and Build & Push.
- Tag workflows succeeded on `0.4.18`: Release uploaded `drover-0.4.18-py3-none-any.whl`; Docker published `drover-api` digest `sha256:974c237366f5a3492bd68e9d42de804b8ac028976933f761101ca0041cdc8ad3` and `drover-worker` digest `sha256:436cf1c3fe8b58f60762e4671424a5dbb5d090bf3634d3ec23fba1333e71084a`. Both report revision `5f7e9679cb23bbd731564d5f68ad9d4ca8928caf`, `x86_64`, and version `0.4.18`.
- Local Python 3.12 gates on the implementation: `uv run --frozen --all-extras pytest tests` (1807 passed, 3 skipped), SDK pytest (114 passed), Ruff, wheel build, and architecture check.

## DMS Lab Drover-only rollout (2026-11-11)

- Operator `/etc/kolla/pyproject.toml` moved drover from `0.4.17` to `tag = "0.4.18"`; `uv lock --upgrade-package drover` changed only the drover entry. Pre-change files were backed up under `/etc/kolla/.afterglow-drover-0418-backup-20261111T103652Z`. The installed role contains the Kolla `openstack_auth_cloud` selector.
- Drover-only `genconfig --tags drover` exited 0 with `changed=0` on controllers 1–3.
- Digest pins in `globals.d/zzz-authority-three-20261104.yml` moved from the 0.4.17 images to the 0.4.18 digests above. Rolling `reconfigure --tags drover --skip-tags haproxy --limit <host>` ran for controller3, controller2, then controller1; each exited 0 with `failed=0`.
- Post-rollout gates: on every controller, `drover_api` runs the 0.4.18 digest, `/v1/health/ready` returned 200, and `drover_worker` is Up on the 0.4.18 digest.

Not claimed: Palimpsest's Kolla precondition has the same missing-`cloud` pattern and was not changed by this release; Lumen/Waygate/Palimpsest reconfigure and the full five-service control play were not run; authenticated end-to-end cluster operations and guest acceptance were not exercised.
- Container identity comparison against the pre-rollout baseline (627 containers in the snapshot) changed exactly the six `drover_api`/`drover_worker` containers; the other 621 kept their IDs, including HAProxy, ProxySQL and keepalived.
- Worker and durable-job gate (read-only, 2026-11-11 ~10:52Z): all three `drover_worker` containers were Up with 0 restarts after cutover. Each log contains no Traceback. Controller3 logged one `reconcile` job completion with `outcome=error` at 10:46:53Z (job `6c6087df…`, `last_error` "Keystone no longer honors the cluster application credential"). That exact error recurs about 12 times per hour since at least 2026-11-10 00:00Z, so it predates this rollout. Three reconcile jobs failed in the 10:40–10:55Z window; only the one above was checked individually. `drover_jobs` and `drover_operations` had zero non-terminal rows at the probe time, so no queued or running work was left stranded.

## Deployment boundary

The production error occurred in the Drover role's `Config | Resolve Drover service project ID` task before config rendering. Use a Drover-only Kolla reconfigure so unrelated service roles do not execute. Before replacement, inspect current operator package/role path, image references, controller/container baseline, effective Kolla config, and migration ledger. Verify the new wheel/role on the operator, all three controller APIs, matching API/Worker image versions, health/readiness, and unchanged non-Drover containers. No live deployment, database migration, or production acceptance is claimed until separately recorded.