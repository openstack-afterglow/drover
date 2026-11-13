# Changelog

## 0.4.19 — durable tenant cluster deletion

### 한국어

- 버전: **0.4.19**.
- 릴리스 일자: **미발행**; 준비일 **2026-11-13 Asia/Seoul (UTC+09:00)**.
- 배포 일자: **미배포**; 발행과 운영 배포는 구분합니다.
- 변경/수정: Afterglow UI가 호출하는 `POST /v1/clusters/{id}/delete-async`가 requester trust 기반 durable delete job을 enqueue/reuse하고 저장된 operation event를 SSE로 재생합니다. 연결이 끊겨도 Worker 정리는 계속됩니다.
- 폐기/제거: **없음**. API 경로, progress schema, dependency, DB schema/migration은 유지합니다.

### English

- Version: **0.4.19**.
- Release date: **Not published**; prepared **2026-11-13 Asia/Seoul (UTC+09:00)**.
- Deployment date: **Not deployed**; publication and rollout are separate.
- Changes/fixes: Afterglow's existing `POST /v1/clusters/{id}/delete-async` now enqueues or reuses a requester-trusted durable delete job and streams persisted operation events. Worker cleanup continues when the client disconnects.
- Deprecated/removed: **None**. The API path, progress schema, dependencies, database schema and migrations remain unchanged.

See [0.4.19 release notes](docs/release-0.4.19.md) for qualification and rollout boundaries.

## 0.4.18 — Kolla Ansible 2026.1 auth-cloud selector

### 한국어

- 버전: **0.4.18**.
- 릴리스 일자: **2026-11-11**; tag Release와 Docker workflow 모두 성공했습니다.
- 배포 일자: **2026-11-11**; DMS Lab 세 controller에 Drover-only로 배포하고 readiness 200을 확인했습니다.
- 변경/수정: Kolla Ansible 2026.1에서 Drover service-project lookup이 `auth_url` 누락으로 실패하지 않도록 `openstack.cloud.project_info`에 `openstack_auth_cloud`를 명시합니다. 기존 Kolla 인증 설정을 사용하며 Drover runtime 동작은 변경하지 않습니다.
- 폐기/제거: **없음**. 의존성, DB schema/migration, 서비스 authority 변경이 없습니다.

### English

- Version: **0.4.18**.
- Release date: **2026-11-11**; both tag Release and Docker workflows succeeded.
- Deployment date: **2026-11-11**; Drover-only rollout reached all three DMS Lab controllers with readiness 200.
- Changes/fixes: explicitly select `openstack_auth_cloud` for Drover's `openstack.cloud.project_info` service-project lookup, fixing Kolla Ansible 2026.1 toolbox authentication when an auth mapping alone lacks `auth_url`. Reuses existing Kolla auth configuration; Drover runtime behavior is unchanged.
- Deprecated/removed: **None**. No dependency, schema/migration, or service-authority changes.

See [0.4.18 release notes](docs/release-0.4.18.md) for exact-SHA qualification and deployment boundaries.

## 0.4.17 — local release candidate

### 한국어

- 버전: **0.4.17**.
- 릴리스 일자: **미발행**; 준비일 **2026-11-04 Asia/Seoul (UTC+09:00)**.
- 배포 일자: **미배포**; 발행과 운영 배포를 구분합니다.
- 변경/수정: 실제 Neutron 프로젝트 보안 그룹·규칙 quota와 예약 사용량·기본 규칙을 생성 전 및 worker 쓰기 전에 검사합니다. 구조화된 부족/불명 오류, 키 없는 요청만의 권위 있는 사전 거부 marker, 같은 멱등성 키 동시 요청의 receipt 보존, worker quota 오류의 원본 정보 유지·무재시도 terminal 처리, heartbeat 갱신/revoke 경쟁의 generation fence를 추가합니다. 기존0.4.16 GPU 사전 검사·Sentinel client 종료·표준 프로젝트 권한을 보존합니다. 의존성/스키마 변경은 없습니다.
- 폐기/제거: **없음**. 기존 quota 자동 확대·IAM 변경·불명 쓰기 재실행은 없습니다.

### English

- Version: **0.4.17**.
- Release date: **Not published**; prepared **2026-11-04 Asia/Seoul (UTC+09:00)**.
- Deployment date: **Not deployed**; artifact publication is separate from operational rollout.
- Changes/fixes: fresh native project security-group/rule quota admission includes reserved usage and provider defaults before creation and worker writes. Adds structured shortage/unavailable detail, unkeyed-only authoritative refusal markers, overlapping-key receipt preservation, terminal worker quota settlement with original errors/no replay, and generation-fenced heartbeat renewal after local revocation. Preserves0.4.16 GPU preflight, Sentinel client close lifecycle and standard project authority. No dependency/schema change.
- Deprecated/removed: **None**. No automatic quota increase, IAM change or replay of uncertain writes.

See [0.4.17 release notes](docs/release-0.4.17.md) for exact-source qualification, immutable source-pin handling and rollout boundaries. Earlier release history remains in the dated `docs/release-*.md` records.
