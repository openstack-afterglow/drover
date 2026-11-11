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

## Qualification status

Local Python 3.12.13 qualification passed: `uv lock --check`; `uv run --frozen --all-extras pytest tests` (**1807 passed, 3 skipped**); root Ruff; SDK pytest (**114 passed**) and Ruff; `uv build --wheel`; and `python3 scripts/check_architecture.py`. This was the implementation/version candidate before the separate source-pin metadata commit. Final exact-SHA CI, final wheel/image artifacts, publication and deployment remain pending. The release workflow builds the wheel on the numeric tag but does not independently wait for the reusable CI suite; publish only after the final metadata commit's full CI suite passes.

## Deployment boundary

The production error occurred in the Drover role's `Config | Resolve Drover service project ID` task before config rendering. Use a Drover-only Kolla reconfigure so unrelated service roles do not execute. Before replacement, inspect current operator package/role path, image references, controller/container baseline, effective Kolla config, and migration ledger. Verify the new wheel/role on the operator, all three controller APIs, matching API/Worker image versions, health/readiness, and unchanged non-Drover containers. No live deployment, database migration, or production acceptance is claimed until separately recorded.