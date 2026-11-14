# Changelog

## 0.4.20 — sandbox-ready cluster profile and sandbox connection

### 한국어

- 상태: **발행·배포됨**(2026-11-14; tag `0.4.20`, 이미지·wheel 발행, DMS Lab 세 controller Drover-only reconfigure; 증거와 미검증 범위는 [릴리스 노트](docs/release-0.4.20.md)).
- 변경/수정: 모든 클러스터가 자기 security group에서 오는 tcp/6443을 연다(제한 CIDR 클러스터에서 agent가 join하지 못하던 결함). 관리자 `k3s.cluster_profiles` 런타임 설정과 `profile` 요청 필드로 네트워크·SSH/API/NodePort/HTTP(S) CIDR을 관리자가 고정한다(요청의 `network_id`·`allowed_cidrs`는 `400`; 비관리자 응답과 operation 이벤트는 profile cluster의 네트워크·규칙 식별자·drift 상세를 숨김). reconcile이 agent의 Kubernetes node Ready를 관찰한다: Nova 방식으로 정규화한 이름으로 맞추고, 한 번도 Ready가 아니었던 agent만 합류 창(900초)과 연속 2회 관찰 실패 뒤 자동 회복되는 reconciler 소유 `ERROR`로 표시한다. `profile`이 없는 요청의 request hash는 이전과 같아 기존 `Idempotency-Key` 재생이 유지된다.
- 추가: `POST /v1/clusters/{id}/sandbox-connection`(새 정책 action `drover:sandbox:connect`, human 토큰 전용)이 `deployment_id`·`pool_name`별 격리 namespace(restricted Pod Security, `lumen-default-deny` NetworkPolicy)를 멱등 준비하고 해당 namespace 전용 Role과 Namespace·RuntimeClass `get`만 가진 ClusterRole에 묶인 900초 이하 bearer token, API server 주소·CA를 반환한다. token은 저장·캐시하지 않는다.
- 영향: 클러스터당 security group 규칙이 1개 늘어 quota admission 계산도 1 증가한다. DB schema/migration 변경 **없음**. profile 없는 요청의 보안 그룹 동작은 같다(NodePort·80·443 `0.0.0.0/0` 기본 유지). **배포 직후 agent가 있는 기존 모든 클러스터가 첫 reconcile부터 노드 관찰(reconcile당 최대 약 15초 추가)을 받는다.** 일시적인 NotReady는 한 번의 관찰로 `ERROR`가 되지 않으며, 한 번이라도 Ready였던 agent는 `ERROR`로 승격되지 않는다.
- 폐기/제거: **없음**.

### English

- Status: **published and deployed** (2026-11-14; tag `0.4.20`, images and wheel published, Drover-only reconfigure on all three DMS Lab controllers; evidence and unverified scope in the [release note](docs/release-0.4.20.md)).
- Changes/fixes: every cluster opens tcp/6443 from its own security group (restricted-CIDR clusters could never admit an agent). An administrator `k3s.cluster_profiles` runtime setting and a `profile` request field pin the network and SSH/API/NodePort/HTTP(S) CIDRs (request `network_id`/`allowed_cidrs` get `400`; non-admin responses and operation events hide a profile cluster's network, rule identifiers and drift details). Reconciliation observes agent Kubernetes node readiness: agents are matched by the Nova-normalised name, and only an agent never observed Ready becomes a reconciler-owned `ERROR` (self-clearing) after the 900 s window and two consecutive failed observations. The request hash of a request without `profile` is unchanged, so replaying an existing `Idempotency-Key` still works.
- Added: `POST /v1/clusters/{id}/sandbox-connection` (new policy action `drover:sandbox:connect`, human tokens only) idempotently prepares an isolated namespace per `deployment_id`/`pool_name` (restricted Pod Security, `lumen-default-deny` NetworkPolicy) and returns the API server address, CA and a bearer token of at most 900 s bound to a namespace-only Role plus a ClusterRole limited to `get` on that Namespace and RuntimeClass. The token is never stored or cached.
- Impact: one more security-group rule per cluster, so quota admission counts one more rule. No database schema or migration change. The security-group behaviour of requests without a profile is unchanged (NodePort, 80 and 443 stay `0.0.0.0/0` by default). **After rollout every existing cluster that has agents gets the node observation from its first reconcile (up to about 15 s extra per reconcile).** A transient NotReady does not become `ERROR` on one observation, and an agent that was ever Ready is never escalated.
- Deprecated/removed: **None**.

## 0.4.19 — durable tenant cluster deletion

### 한국어

- 버전: **0.4.19**.
- 릴리스 일자: **2026-11-13**; tag `0.4.19`와 wheel을 발행했습니다.
- 배포 일자: **2026-11-13**; DMS Lab 세 컨트롤러에 배포했습니다. `test-k3s` 삭제 재시도는 owner 인증 확보 전까지 대기합니다.
- 변경/수정: Afterglow UI가 호출하는 `POST /v1/clusters/{id}/delete-async`가 requester trust 기반 durable delete job을 enqueue/reuse하고 저장된 operation event를 SSE로 재생합니다. 연결이 끊겨도 Worker 정리는 계속됩니다.
- 폐기/제거: **없음**. API 경로, progress schema, dependency, DB schema/migration은 유지합니다.

### English

- Version: **0.4.19**.
- Release date: **2026-11-13**; published tag `0.4.19` and wheel.
- Deployment date: **2026-11-13**; deployed to all three DMS Lab controllers. `test-k3s` deletion retry remains pending owner authentication.
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
