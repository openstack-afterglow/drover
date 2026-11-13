# Drover 0.4.18 — Kolla Ansible 2026.1 auth-cloud selector

## 한국어

- 버전: **0.4.18** (발행 완료).
- 릴리스 일자: **2026-11-11**; tag Release와 Docker workflow 모두 성공했습니다.
- 배포 일자: **2026-11-11**; DMS Lab 세 controller에 Drover-only로 배포하고 readiness 200을 확인했습니다.
- 변경/수정: Kolla role이 `openstack.cloud.project_info`로 `drover-service` project ID를 조회할 때 기존 `openstack_auth`와 함께 `openstack_auth_cloud`를 명시합니다. Kolla Ansible 2026.1 toolbox에서 auth mapping만 전달하면 `auth_url` 누락으로 실패하는 호환성 문제를 해결합니다.
- 폐기/제거: **없음**. 서비스 계정/프로젝트, 런타임 인증, 의존성, DB schema/migration, Kubernetes guest 동작은 바뀌지 않습니다.

## English

- Version: **0.4.18** (published).
- Release date: **2026-11-11**; both tag Release and Docker workflows succeeded.
- Deployment date: **2026-11-11**; Drover-only rollout reached all three DMS Lab controllers with readiness 200.
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
- Container identity comparison against the pre-rollout baseline (627 containers in the snapshot) changed exactly the six `drover_api`/`drover_worker` containers; the other 621 kept their IDs, including HAProxy, ProxySQL and keepalived.
- Worker/database gate (read-only): all three `drover_worker` containers remained Up with zero restarts. One controller3 reconcile job reported the pre-existing Keystone application-credential failure; the same error recurred before rollout. `drover_jobs` and `drover_operations` had zero non-terminal rows at probe time.

## Additional deployment limits

Palimpsest's Kolla precondition has the same missing-`cloud` pattern and was not changed in this release. Lumen/Waygate/Palimpsest reconfigure, the full five-service control play, authenticated end-to-end cluster operations, and guest acceptance were not exercised.

## Deployment boundary

The original production failure occurred in `Config | Resolve Drover service project ID`. The Drover-only rollout above updated the installed role and all three controller APIs/Workers. It does not claim a full five-service control play, an end-to-end cluster lifecycle, or guest acceptance.