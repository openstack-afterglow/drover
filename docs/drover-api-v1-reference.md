# Drover Native v1 API 기술 Reference

Drover 서비스의 네이티브 REST, SSE(Server-Sent Events) 및 WebSocket API에 대한 완전한 사양서입니다. 본 API 사양서는 OpenStack Keystone 인증 기반의 프로젝트 격리 멀티테넌트 환경을 전제로 설계되었습니다.

**Execution-authority 변경의 증거는 source-reviewed only입니다.** 기존 테스트 계약은 test-defined로 구분하며 이번 갱신에서는 테스트·live Keystone/OpenStack/K3s 검증을 실행하지 않았습니다. 역사적 릴리스 결과는 새 trust/credential rollout의 검증 증거가 아닙니다.

> **기계 읽기용 OpenAPI 스키마**
> 실행 중인 Drover API 서버의 최신 기계 읽기 표준 OpenAPI 스키마는 **`/openapi.json`**, 대화형 UI는 **`/docs`**에서 동적으로 조회할 수 있습니다.

---

## 1. 공통 규격 및 HTTP 헤더 (Common Standards & Headers)

### 1.1 HTTP 인증 및 콘텍스트 헤더
* **`X-Auth-Token`** *(필수)*: OpenStack Keystone 프로젝트 스코프 인증 토큰. OpenAPI scheme 명칭: `KeystoneToken`.
* **`X-Project-Id`** *(선택)*: Keystone 인증 시 명시적으로 타겟 프로젝트 ID를 지정할 때 사용.
  생략하면 `GET /v3/auth/tokens`로 제출된 토큰 자체를 검증하고 원래 프로젝트 범위를 보존합니다. 사용자의 default project로 재인증하지 않습니다. 명시하면 Keystone이 승인하는 해당 프로젝트로의 rescope를 수행합니다. Drover는 서비스 자격으로 카탈로그의 `identity` **internal** 인터페이스를 먼저 해석하고, 토큰 검증과 관리자 역할 조회를 그 URL로만 보냅니다. internal identity endpoint가 없거나 조회에 실패하면 external/public URL로 우회하지 않고 인증을 거부합니다. 미스코프·폐기·만료 토큰과 검증 실패도 계속 거부합니다.
* **`X-Openstack-Request-Id`** *(자동 생성/전달)*: 시스템 전반의 상관관계(Correlation) 추적용 요청 식별자. API 응답 헤더 및 로그/이벤트 페이로드에 포함됨.
* **`Idempotency-Key`** *(생성 API에서 선택·권장)*: `POST /v1/clusters/async`의 재전송을 같은 오퍼레이션으로 귀속시키는 유니크 키입니다. 현재 스케일·삭제·노드그룹 변경에는 외부 멱동성 계약이 없습니다.
* **`Content-Type`** *(JSON body 요청에서 필수)*: JSON body를 보내는 요청은 `application/json` 계열 값을 보내야 합니다. FastAPI strict content-type 검사로 헤더 없이 보낸 JSON body도 `422`로 거부됩니다(FastAPI `0.132` 이전에는 허용). JSON 계열이 아닌 값은 이전에도 `422`였습니다.

### 1.1.1 요청 및 작업 로그
API는 정상(2xx/3xx) 및 오류(4xx/5xx) HTTP 응답 완료 시 INFO로 method, 라우트 템플릿, status, success/error, 경과 시간과 검증된 request ID를 남깁니다. SSE는 스트림 종료까지 완료가 지연될 수 있습니다. Worker의 내구성 job도 완료·재시도·실패·보류를 INFO로 남깁니다. `LOG_LEVEL=DEBUG`를 **API 및 Worker 각 프로세스**에 지정하면 query/state/result의 제한된 구조 요약(허용된 키 이름과 개수, 값 없음)을 DEBUG로 추가합니다. 쿼리 값, 요청/응답 바디, 동적 URL 경로, 인증 헤더, kubeconfig, node token, Keystone credential, 예외 문자열/traceback은 이 공통 로그에 넣지 않습니다.

```bash
LOG_LEVEL=DEBUG drover-api
LOG_LEVEL=DEBUG drover-worker
```

기본값은 INFO이며 `LOG_LEVEL=DEBUG`는 Drover logger에만 적용되어 HTTP/DB/OpenStack 라이브러리 전체의 DEBUG를 켜지 않습니다. 라우트 템플릿은 router prefix를 포함한 경로입니다(예: `/v1/clusters/{cluster_id}`, 일치하는 라우트가 없으면 `(unmatched)`). 로그에서 볼 수 없는 상세 operation 상태는 `/v1/operations/{operation_id}`와 이벤트에서 조회합니다.

이 보장은 Drover logger에 한정됩니다. 컨테이너 이미지와 Kolla role의 `uvicorn` 명령은 access log를 끄지 않으므로 uvicorn access log(`INFO: ... "GET /path?query HTTP/1.1" 401`)에는 원본 경로와 쿼리 문자열이 남습니다. 쿼리 문자열에 credential을 넣지 마십시오. Kolla role은 `LOG_LEVEL` 변수를 제공하지 않으므로 DEBUG를 쓰려면 운영자가 두 컨테이너 환경에 직접 지정해야 합니다.

### 1.2 표준 HTTP 상태 코드
* `200 OK` / `201 Created` / `204 No Content`: 성공적인 처리
* `400 Bad Request`: 요청 파라미터 검증 실패 (Pydantic 모델 검증 오류 포함)
* `401 Unauthorized`: Keystone 토큰 누락, 만료 또는 프로젝트 스코프 미지정
* `403 Forbidden`: `oslo.policy` 정책 거부 또는 시스템 관리자 권한 부족
* `404 Not Found`: 리소스 미존재 또는 타 테넌트 자원에 대한 요청 (프로젝트 격리 보안)
* `409 Conflict`: 멱동성 키 불일치 중복 요청 또는 리소스 상태 충돌
* `429 Too Many Requests`: Rate Limit(초당/분당 요청 제한) 초과
* `503 Service Unavailable`: 백엔드 서비스(Redis, DB, Keystone 등) 오류

### 1.3 Durable execution authority

- create/scale/delete, mutation nodegroup 및 admin scale/delete는 target project-scoped requester token으로 per-operation Keystone trust를 admit합니다. trustor=requester, trustee=service-project에서 resolve한 Drover identity, `impersonation=True`입니다. required roles는 모두 현재 보유해야 하며 optional은 보유한 것만 위임합니다. `admin`/`manager` 위임과 service/tenant-manager fallback은 없습니다. admin API도 target project에 scope된 caller token이 필요합니다.
- trust record의 scope/impersonation/role subset/expiry를 검증하고 각 connection에서 enabled principal/project, 현재 held role IDs 및 create/scale/delete capability와 token identity/project/trustee/expiry를 재검증합니다. trust/app-credential token은 admitted role IDs를 모두 포함해야 합니다. Keystone implied-role 확장은 허용하지만 `admin`/`manager` token roles는 거부합니다. `AuthorityRevoked`/`ReauthorizationRequired` 등 `ExecutionAuthorityError`는 terminal이고 retry하지 않으며 directory/Keystone 통신 장애는 attempt-fenced retry입니다. HTTP admission denial은 403, unavailable은 503입니다.
- callback continuation과 해당 create operation의 rollback delete는 create delegation을 재사용합니다. queued/running job 또는 live operation이 있으면 유지하고 idle이면 `released`로 전환합니다. job 종료 후와 300초 sweep에서 `delete_released`가 released/revoked trust를 자신의 impersonating trust-scoped password token으로 DELETE합니다(project selector 없음). 성공/404는 `deleted`, Unauthorized/Forbidden은 released/revoked와 `state_reason="trust inert until expiry"`, 통신 장애는 다음 sweep 재시도입니다. expiry 이후 Keystone GET 404가 확인되면 `expired`입니다. TTL은 원격 DELETE 불가/장애의 fallback bound입니다. caller token/password는 persist하지 않으며 미커밋 admission은 일시적 caller token으로 정리합니다.
- [Keystone master trusts.py](https://github.com/openstack/keystone/blob/master/keystone/api/trusts.py)의 `_check_delegated_token`은 app-credential/OAuth/EC2 token을 차단하지만 ordinary trust-scoped token은 차단하지 않습니다. `identity:delete_trust`는 admin/trustor를 허용하고 impersonation token의 user는 trustor이므로 Drover는 이 token으로 trust를 삭제합니다. [Master users.py](https://github.com/openstack/keystone/blob/master/keystone/api/users.py)의 `_block_delegated_token_app_creds`와 `_check_unrestricted_application_credential`은 trust/OAuth/EC2와 restricted app-credential token의 추가 app-credential 관리를 차단하므로 resource credential 원격 폐기는 owner의 non-delegated token을 사용합니다. 이는 source-reviewed이며 배포 Keystone 검증이 아닙니다. `tests/test_native_trust_loopback.py`는 실제 keystoneauth1/keystoneclient HTTP trust create, project-less OS-TRUST auth/verification, trust-token DELETE와 revoked-role terminal 경계를 synthetic provider에서 정의합니다(test-defined, 이 문서 slice에서는 실행하지 않음).
- create admission은 caller-owned `unrestricted=False` control과 플러그인용 별도 guest credential을 held delegated-role subset으로 발급·암호화 저장합니다. control은 guest에 노출하지 않습니다. Stampede는 owner의 현재 scale, reconcile/health는 get capability 및 credential token scope/roles를 매 connection에 검증합니다. legacy active control 없음은 재인가 필요입니다. reconcile authority 실패나 credential missing(non-required drift)만으로 cluster를 ERROR로 바꾸지 않아 ACTIVE-only 재인가가 가능합니다.
- app-credential create는 configured delegated roots만 요청하지만 [Keystone stable/2025.2 `users.py:_get_roles`](https://github.com/openstack/keystone/blob/stable/2025.2/keystone/api/users.py)는 그 roots의 implied roles도 반환합니다. `auth.current_project_role_state`는 기존 current-project role map과 동일한 검증된 directory graph를 제공하며 `cluster_authority.issue`는 configured roots의 current global role-ID closure만 허용합니다. 반환 ID에는 요청한 roots가 모두 있어야 하고 closure 밖의 역할, `admin`/`manager`, unknown/domain/ambiguous 역할, owner가 현재 보유하지 않은 역할은 거부합니다. caller가 별도로 보유한 역할은 whitelist가 아닙니다. accepted full role IDs/names를 암호화 credential row와 함께 snapshot하며 잘못된 응답은 owner-token cleanup합니다. control/guest 분리와 guest rollout→activation→legacy retirement 순서는 그대로입니다.
- 설정(`[drover]` TOML, Settings/Kolla는 `drover_` prefix): `operation_trust_ttl_seconds=14400`(900–86400), `operation_trust_min_remaining_seconds=300`(60–3600, TTL보다 작음), `delegated_required_roles=["member"]`(nonempty), `delegated_optional_roles=["load-balancer_member"]`(보유한 것만), `guest_rollout_timeout_seconds=600`(60–3600). Barbican member/Octavia member-role defaults의 실제 cloud policy 충족은 **[INFERENCE]**입니다.
- `drover/config.py:get_settings`는 TOML list/dict 값을 표준 JSON으로 환경에 전달합니다. 명시적 환경 키가 있으면 빈 값도 TOML로 덮어쓰지 않습니다. role-array 환경값은 JSON 배열이어야 하며 빈 문자열은 Settings validation 오류입니다(예: `DROVER_DELEGATED_REQUIRED_ROLES='["member"]'`, `DROVER_DELEGATED_OPTIONAL_ROLES='["load-balancer_member"]'` 또는 `[]`). JSON 전달과 key-presence precedence는 configured delegated roles나 authority ceiling을 넓히지 않습니다.


---

## 2. 디스커버리 및 헬스 체크 API (Discovery & Health)

### `GET /`
- **설명**: Root 서비스 버전 디스커버리 정보 반환.
- **인증**: 필요 없음 (Unauthenticated).
- **응답 (200 OK)**: `RootDiscoveryResponse` (`versions: list[VersionDocument]`)

### `GET /v1/`
- **설명**: v1 API 버전 상세 디스커버리 정보 반환.
- **인증**: 필요 없음 (Unauthenticated).
- **응답 (200 OK)**: `VersionDiscoveryResponse` (`version: VersionDocument`)

### `GET /v1/health`
- **설명**: 레거시/하위 호환 프로세스 Liveness 체크.
- **인증**: 필요 없음.
- **응답 (200 OK)**: `{"status": "ok"}`

### `GET /v1/health/live`
- **설명**: 프로세스 생존 여부(Liveness) 검증.
- **인증**: 필요 없음.
- **응답 (200 OK)**: `{"status": "ok"}`

### `GET /v1/health/ready`
- **설명**: MariaDB, Redis, Migration Ledger, Keystone Service Credentials 종속성 점검(Readiness).
- **인증**: 필요 없음.
- **응답 (200 OK / 503 Service Unavailable)**: `ReadinessResponse`
  ```json
  {
    "status": "ok",
    "checks": {
      "database": "ok",
      "redis": "ok",
      "migrations": "ok",
      "keystone": "ok"
    }
  }
  ```

### `GET /v1/clusters/health`
- **설명**: 호출자 테넌트 소유 전체 클러스터의 K3s APIReachability 및 Node 헬스 종합 조회.
- **인증**: `X-Auth-Token` 필수.
- **응답 (200 OK)**: `list[K3sClusterHealth]`

### `GET /v1/clusters/{cluster_id}/health`
- **설명**: 단일 클러스터 상세 헬스 상태 조회.
- **인증**: `X-Auth-Token` 필수.
- **응답 (200 OK)**: `K3sClusterHealth`

### `POST /v1/clusters/{cluster_id}/health/check`
- **설명**: 단일 클러스터에 대해 즉시 헬스 체크 재검증을 트리거 (Rate limit: 3/min).
- **인증**: `X-Auth-Token` 필수.
- **응답 (200 OK)**: `K3sClusterHealth`

---

## 3. 테넌트 클러스터 라이프사이클 API (Tenant Clusters)

### `GET /v1/clusters`
- **설명**: 프로젝트 내 K3s 클러스터 목록 조회.
- **쿼리 파라미터**: `include_deleted` (bool, 기본값: `false`)
- **응답 (200 OK)**: `list[K3sClusterInfo]`

### `GET /v1/clusters/{cluster_id}`
- **설명**: 지정 클러스터 상세 정보 조회.
- **응답 (200 OK)**: `K3sClusterInfo`

### `GET /v1/clusters/{cluster_id}/kubeconfig?grade=user|editor|admin`
- **설명**: K3s 클러스터 접근용 Kubeconfig YAML 다운로드. 기본 `grade=user`이며 요청한 grade만 발급하고 상위·하위 grade로 대체하지 않는다.
  - `user`: `drover-access_user` 또는 `drover-access_admin`. 읽기 전용 ClusterRole에 묶인 ServiceAccount TokenRequest(최대 900초, Keystone 토큰 만료 이내). secret·변경·exec 권한 없음.
  - `editor`: `drover-workloads_editor` 또는 `drover-access_admin`. 사용자별 격리 namespace의 Role과 admission 정책에 묶인 TokenRequest. 새 ValidatingAdmissionPolicy/binding이 dry-run probe를 거부할 때까지 최대 20회(0.5초 간격) 확인하고, 끝내 거부하지 않으면 토큰 없이 `502`를 반환한다.
  - `admin`: `drover-access_admin`(또는 검증된 system admin)만. 저장된 전체 kubeconfig.
  - 모든 grade에 base `member`가 필요하고, system admin이 아닌 raw `admin`/`manager` 역할은 거부한다.
- **응답 (200 OK)**: `application/yaml`, `Cache-Control: no-store`. 제한 grade는 `X-Credential-Expires-At`, `X-Workload-Namespace` 헤더를 포함한다.
- **오류**: 권한 없는 grade `403`, 알 수 없는 grade `422`, 다른 프로젝트 cluster `404`, 발급·admission 실패 `502`(관리자 자격으로 대체하지 않음). `HEAD`는 권한만 확인하고 자격을 발급하지 않는다.

### `POST /v1/clusters/async`
- **설명**: 비동기 K3s 클러스터 생성 (SSE 스트림 반환). `Idempotency-Key` 헤더 지원. (Rate limit: 5/min)
- **요청 바디 (`CreateK3sClusterRequest`)**:
  ```json
  {
    "name": "k3s-demo",
    "agent_count": 2,
    "agent_flavor_id": "flavor-uuid",
    "network_id": "net-uuid",
    "key_name": "my-key",
    "os_type": "ubuntu",
    "allowed_cidrs": ["10.0.0.0/8"],
    "template_id": "template-uuid",
    "master_count": 1,
    "stampede_enabled": false
  }
  ```
- **클러스터 네트워크**: `network_id`는 Neutron 외부 네트워크의 ID만 받습니다. 생략하면 관리자 `k3s.default_network` 정책의 외부 네트워크 ID를 조회·재검증합니다. 공유 네트워크라도 외부가 아니면 명시 요청과 기본 정책 모두 사용할 수 없습니다. 명시한 네트워크가 없거나 내부이면 기록 전에 `400`, 기본 정책이 누락·만료되면 `503`, 네트워크 조회 서비스가 불가하면 `503`으로 거부하며 Nova 자동 네트워크 선택으로 우회하지 않습니다. 선택한 ID와 이름은 cluster/job의 `resource_policy_snapshot["k3s.default_network"]`에, ID는 `network_id`에도 저장되고 primary·HA·agent·nodegroup 프로비저닝과 후속 scale에 사용됩니다. [요청 처리](../drover/api/clusters.py), [정책 제약과 조회](../drover/services/resource_policies.py), [정책 스냅샷](../drover/services/resource_policy_store.py), [직접 VM 생성](../drover/services/provisioner.py), [노드그룹 VM 생성](../drover/services/autoscale.py).
- **응답 (200 OK)**: `text/event-stream` (SSE 스트림)
  - 이벤트 라인 형식 (`K3sProgressMessage`):
    `data: {"step": "security_group", "progress": 10, "message": "...", "cluster_id": "...", "operation_id": "op-123"}`

- **SSH 키 소유권**: `key_name`은 요청자 Nova 키페어 이름입니다. API가 공개키를 조회·검증해 `ssh_public_key` cluster/job snapshot에 저장하고 Worker는 requester trust로 실행합니다. Nova에 키페어 이름을 넘기지 않고 서버/HA/agent userdata에 공개키를 설치합니다. private key/caller token은 job에 저장하지 않습니다.
- **실패 및 upgrade 경계**: 키 조회/검증 실패는 cluster/job 전에 거부합니다. 이미 승인된 idempotent 요청은 기존 operation을 재사용합니다. named-key 구 job/cluster에 공개키 snapshot이 없거나 mutation job에 delegation이 없으면 fail closed 하며 다른 identity/keypair로 추정하지 않습니다. [004 upgrade runbook](../drover/migrations/README.md#execution-authority-upgrade-004)에 따라 구 mutation/callback jobs를 drain한 뒤 새 API/Worker를 적용합니다.

### `PATCH /v1/clusters/{cluster_id}/scale`
- **설명**: 클러스터 워커(Agent) 노드 수 변경. (Rate limit: 10/min)
- **요청 바디 (`ScaleK3sClusterRequest`)**: `{"agent_count": 4}`
- **응답 (200 OK)**: `{"message": "...", "target_count": 4}`

### `DELETE /v1/clusters/{cluster_id}`
- **설명**: 내구성 delete job을 enqueue합니다. 현재 delete 권한 actor의 trust를 사용하며 creator의 account/credential이 필요하지 않습니다. caller 소유 credential은 admission에서 caller token으로 동기 회수 시도합니다. worker 완료 시 남은 secret을 지우고 다른 owner/legacy credential은 owner revocation backlog로 보고합니다. 204는 cloud 삭제 완료/모든 원격 credential 폐기의 증거가 아닙니다.
- **응답 (204 No Content)**

### `POST /v1/clusters/{cluster_id}/delete-async`
- **설명**: 현재 caller connection으로 `delete_cluster_progress`를 직접 실행하는 SSE 삭제 경로입니다. creator credential은 필요 없고 caller 소유 credential을 이 connection으로 회수합니다. durable delete job/trust admission 경로가 아니므로 disconnect 후 계속 실행된다는 계약은 없습니다. tenant `DELETE` 및 admin deletion과 구분합니다. (Rate limit: 5/min)
- **응답 (200 OK)**: `text/event-stream` (SSE 스트림)

### `GET /v1/clusters/{cluster_id}/authorization`
- **Policy**: `drover:clusters:get`; 다른 프로젝트/없는 cluster는 404.
- **응답 (200, `ClusterAuthorizationStatus`)**: `cluster_id`, `authorized`(active control row 존재), `active_generation`, `staged_generations`, `credentials`, `owner_revocation_required`(retiring 항목). credential reference는 ID/purpose/generation/owner/state/reason/role names/timestamps/last_error이며 secret은 없습니다. `authorized=true`는 현재 Keystone 권한/authentication이 live 검증됐다는 뜻이 아닙니다.
- **SDK**: `conn.drover.cluster_authorization(cluster_id)`.

### `POST /v1/clusters/{cluster_id}/authorization`
- **Policy**: `drover:clusters:reauthorize`; caller의 target project token으로 restricted credentials를 발급합니다. body는 없습니다.
- **선행 조건**: `ACTIVE` cluster, 다른 queued/running mutation 없음(reconcile 제외). legacy cluster도 새 generation으로 재인가해야 합니다. 기존 active guest 또는 legacy app credential이 있으면 guest도 대체하고 기존 plugin metadata를 유지/legacy detect합니다.
- **응답 (202, `ClusterReauthorizationResponse`)**: `cluster_id`, `generation`, `operation_id`, `job_id`, staged `credentials` reference, `retired_credential_ids`(이번 admission에서 caller 소유의 이미 retiring인 항목을 회수한 ID). **202는 activation 완료가 아닙니다.** operation과 authorization 상태를 poll합니다.
- **Worker rollout**: 현재 reauthorize capability와 staged token을 검증합니다. `kube-system/cloud-config`·`manila-cloud-secret` Secret, Octavia Ingress `octavia-ingress-controller-config` ConfigMap의 credential keys를 바꾸고 참조 Deployment/DaemonSet/StatefulSet을 restart/rollout wait합니다. KMS required/legacy detect이면 control-plane host별 privileged hostPID Job이 temporary Secret env와 `nsenter`로 `/etc/kubernetes/cloud.conf`(있으면), `/etc/kubernetes/barbican-cloud.conf`를 rewrite하고 `barbican-kms.service` restart/active/socket을 확인합니다. 마지막 Secret-write probe 뒤에만 atomic activation합니다.
- **실패/retirement**: 부분 실패는 staged `last_error`와 이전 active generation을 유지합니다; guest 객체의 자동 원복은 보장하지 않습니다. 성공 시 이전 active/다른 staged generation을 retiring으로 바꾸고 secret을 지웁니다. 원격 credential 폐기는 owner token 또는 legacy operator의 out-of-band 절차가 필요합니다.
- **오류**: 프로젝트/cluster 404, non-ACTIVE/busy/concurrent generation 409, issuance denial 403, DB/Keystone issuance unavailable 503. durable rollout 실패는 operation에서 확인합니다.
- **503 진단 경계(source-reviewed)**: `api/delegated.py:issue_credentials`는 delegation/permission/SDK Forbidden 거부만 403으로 변환하고 나머지 예외는 같은 issuance 503으로 변환합니다. SDK3.3.0의 create/delete signature와 resource attributes는 맞습니다. 이전 `_verify_issued`의 `returned roles <= requested roots` 검사는 정상 implied-role 응답도 거부했습니다. 부모가 제공한 기존 요청 access log의 POST 201→DELETE 204는 생성 후 cleanup을 확인하며 blanket Keystone availability/auth 실패가 아닙니다. 위 closure 수정과 `tests/test_native_app_credentials.py`는 이 source defect를 다룹니다(`test-defined`, 실행하지 않음). 실제 deployed graph/기존 실패의 redacted 응답 metadata와 예외 원인을 확인하기 전 live 재시도·성공·guest activation을 주장하지 않습니다.
- **SDK**: `conn.drover.reauthorize_cluster(cluster_id)`.

### `POST /v1/clusters/{cluster_id}/authorization/retire`
- **Policy**: `drover:clusters:retire_credentials`; body 없음. caller token으로 **caller 소유 retiring** credential만 삭제하고 삭제된 row의 secret을 지웁니다. active credential이나 다른 owner 항목을 삭제하지 않습니다.
- **응답 (200, `ClusterCredentialRetireResponse`)**: `cluster_id`, `deleted_credential_ids`, 남은 `owner_revocation_required`. 개별 원격 DELETE 실패는 backlog에 남을 수 있으므로 200을 전체 회수 성공으로 해석하지 않습니다. endpoint-level 회수 장애는 503.
- **SDK**: `conn.drover.retire_cluster_credentials(cluster_id)`.


### `GET /v1/clusters/{cluster_id}/nodes/{vm_id}/interfaces`
- **설명**: 특정 노드 VM에 연결된 Neutron 네트워크 인터페이스 목록 조회.
- **응답 (200 OK)**: `list[K3sInterfaceInfo]`

### `POST /v1/clusters/{cluster_id}/nodes/{vm_id}/interfaces`
- **설명**: 특정 노드 VM에 추가 Neutron 포트/네트워크 바인딩.
- **요청 바디 (`K3sAttachInterfaceRequest`)**: `{"net_id": "net-uuid"}`
- **응답 (201 Created)**: `K3sInterfaceInfo`

### `DELETE /v1/clusters/{cluster_id}/nodes/{vm_id}/interfaces/{port_id}`
- **설명**: 특정 노드 VM의 Neutron 인터페이스 연결 해제.
- **응답 (204 No Content)**

### `POST /v1/clusters/{cluster_id}/stampede/enable`
- **설명**: 클러스터의 Stampede 오토스케일링 모드를 활성화합니다.
- **선행 조건**: 클러스터 상태가 `ACTIVE`여야 하며, `stampede_enabled=true`인 agent 노드그룹(유효한 flavor_id 및 min_size <= node_count <= max_size)이 최소 1개 이상 존재해야 합니다. 노드그룹 `flavor_id`는 요청 프로젝트 scope의 Nova 조회로 검증하므로 프로젝트에 공유된 private flavor(예: GPU passthrough)도 허용되고, 보이지 않는 flavor는 `422`입니다. `image_id`는 `k3s.server_image` 정책(public/community image)을 따릅니다. 노드그룹 생성·수정(`POST`/`PATCH /v1/clusters/{id}/nodegroups`)도 같은 규칙을 적용합니다.
- **Resource authority**: active control credential이 없으면 409로 거부합니다(legacy cluster는 먼저 재인가). planner와 job은 credential owner의 현재 scale capability를 재검증하며 `reauthorization_required`/`authority_revoked` 차단은 다른 identity로 우회하지 않습니다.
- **응답 (200 OK - `StampedeMutationResponse`)**:
  ```json
  {
    "message": "Stampede 모드가 활성화되었습니다",
    "cluster_id": "cluster-uuid",
    "stampede_enabled": true
  }
  ```
- **오류 응답**:
  - `400 Bad Request`: 서버 전역 `drover_stampede_enabled` 비활성화 상태
  - `404 Not Found`: 클러스터 없음 또는 프로젝트 불일치
  - `409 Conflict`: 클러스터가 `ACTIVE` 상태가 아님
  - `422 Unprocessable Entity`: 활성화 가능한 agent 노드그룹 없음 또는 sizing 불변식 위반
  - `503 Service Unavailable`: MariaDB 정본 접근 불가

### `POST /v1/clusters/{cluster_id}/stampede/disable`
- **설명**: 클러스터의 Stampede 오토스케일링 모드를 비활성화합니다. 이미 큐에 등록된 내구성 작업은 완료까지 유지됩니다.
- **응답 (200 OK - `StampedeMutationResponse`)**:
  ```json
  {
    "message": "Stampede 모드가 비활성화되었습니다",
    "cluster_id": "cluster-uuid",
    "stampede_enabled": false
  }
  ```

### `GET /v1/clusters/{cluster_id}/stampede` 및 `GET /v1/clusters/{cluster_id}/stampede/status`
- **설명**: MariaDB에 저장한 마지막 Kubernetes 관측과 내구성 작업 상태를 조회합니다. GET 자체는 Kubernetes/Nova를 새로 조회하지 않습니다. `observed_at`을 확인하고, 미관측 `ready_count=null`을 0 또는 GPU-ready로 해석하지 않습니다.
- **응답 (200 OK - `StampedeStatusResponse`)**:
  ```json
  {
    "cluster_id": "cluster-uuid",
    "stampede_enabled": true,
    "global_stampede_enabled": true,
    "policy": {
      "interval": 60,
      "scale_down_window": 600,
      "scale_up_cooldown": 120,
      "scale_down_cooldown": 300,
      "scale_down_threshold": 0.5,
      "resource_headroom_factor": 0.3
    },
    "active_operation_ids": [],
    "nodegroups": [
      {
        "id": "ng-uuid",
        "name": "gpu-workers",
        "role": "agent",
        "flavor_id": "gpu-flavor-uuid",
        "stampede_enabled": true,
        "min_size": 1,
        "max_size": 5,
        "node_count": 2,
        "desired_count": 2,
        "tracked_count": 2,
        "ready_count": 2,
        "in_flight": 0,
        "observed_at": 1728244800.0,
        "last_operation_id": "op-uuid-1",
        "last_job_id": "job-uuid-1",
        "active_operation_ids": [],
        "capacity": {
          "allocatable": {"cpu_m": 4000, "memory_bytes": 16777216000, "gpu": 2, "pods": 220},
          "requested": {"cpu_m": 1200, "memory_bytes": 4194304000, "gpu": 1, "pods": 12},
          "free": {"cpu_m": 2800, "memory_bytes": 12582912000, "gpu": 1, "pods": 208},
          "nodes": []
        },
        "pending_assignments": [],
        "blocked_reasons": [],
        "last_decision": "within_capacity",
        "last_blocked_reason": "",
        "flavor_summary": {
          "id": "gpu-flavor-uuid",
          "name": "m1.gpu",
          "vcpus_m": 4000,
          "ram_bytes": 16777216000,
          "gpu": 1,
          "estimated_allocatable": {"cpu_m": 2800, "memory_bytes": 11744051200, "gpu": 1, "pods": 110}
        },
        "quota_state": {"allowed": true},
        "stampede_state": {}
      }
    ]
  }
  ```
- `ready_count`는 K3s Ready 수이고 GPU-ready 수가 아닙니다. `capacity.allocatable.gpu`, `stampede_state.ready_nodes`/`failed_nodes` 및 최종 operation 상태를 함께 확인합니다. `tracked_count`는 DB 추적 row 수입니다.
- `quota_state.allowed`는 admission 결과일 뿐, 현재 Nova quota/GPU 호스트 여유를 보장하지 않습니다. `blocked_reasons`는 Pending Pod 분류이고 flavor/cooldown/min-max 등 결정 차단은 `last_blocked_reason`에 기록합니다.
- GPU admission URL이 설정되면 planner 판정과 별도로 **각 새 native GPU worker create 전** admission을 재검사합니다. Afterglow provisioning intents는 제거됐고 Nova/Cinder 생성은 Drover가 bound authority로 직접 실행합니다. live-count admission은 capacity reservation이 아니며 denial/unavailable은 fail closed 합니다. URL 미설정이면 native Nova quota를 따릅니다.
- 증설 실패 사유는 Node가 Ready가 되지 않았으면 `node_not_ready`, Ready 이후 GPU allocatable이 요청보다 부족하면 `gpu_not_allocatable`입니다. 부분 VM 생성은 `provision_failed`가 우선하고, 여러 worker 중 join 실패가 있으면 GPU 부족보다 우선합니다.

### `GET /v1/clusters/{cluster_id}/stampede/events`
- **설명**: Redis 기반 Stampede 스케일링 이벤트 최신 이력을 역순(최신순)으로 조회합니다 (내구성 저널이 아닌 보조적 Activity 피드).
- **쿼리 파라미터**: `limit` (int, 1~200, 기본값: 50)
- **응답 (200 OK)**: `list[dict]` (타임스탬프, action `scale_up`/`scale_down`/`blocked`, status `success`/`failed`/`started`/`skipped`, 메타데이터)
- Redis 장애 시 빈 목록이 반환될 수 있습니다. 내구성 진행/오류의 정본은 `last_operation_id` 또는 `active_operation_ids`로 조회하는 `/v1/operations/{operation_id}` 및 `/events`입니다.

---

## 4. 노드그룹 및 클러스터 템플릿 API (Nodegroups & Templates)

### 4.1 노드그룹 API (`/v1/clusters/{cluster_id}/nodegroups`)
* **`GET /v1/clusters/{cluster_id}/nodegroups`**: 클러스터 노드그룹 목록 조회 (`list[K3sNodegroupInfo]`)
* **`GET /v1/clusters/{cluster_id}/nodegroups/{nodegroup_id}`**: 노드그룹 단건 조회 (`K3sNodegroupInfo`)
* **`POST /v1/clusters/{cluster_id}/nodegroups`**: 신규 노드그룹 생성 (`CreateK3sNodegroupRequest`, `201 Created`)
* **`PATCH /v1/clusters/{cluster_id}/nodegroups/{nodegroup_id}`**: 노드그룹 설정/노드 수 수정 (`UpdateK3sNodegroupRequest`)
* **`DELETE /v1/clusters/{cluster_id}/nodegroups/{nodegroup_id}`**: 노드그룹 삭제 (`204 No Content`)

### 4.2 클러스터 템플릿 API (`/v1/cluster-templates`)
* **`GET /v1/cluster-templates`**: 공개 및 본인 소유 클러스터 템플릿 목록 조회 (`list[K3sClusterTemplateInfo]`)
* **`GET /v1/cluster-templates/{template_id}`**: 템플릿 상세 조회 (`K3sClusterTemplateInfo`)
* **`POST /v1/cluster-templates`**: 템플릿 생성 (Policy: `drover:templates:manage`, `201 Created`)
* **`PATCH /v1/cluster-templates/{template_id}`**: 템플릿 수정 (Policy: `drover:templates:manage`)
* **`DELETE /v1/cluster-templates/{template_id}`**: 템플릿 소프트 삭제 (Policy: `drover:templates:manage`, `204 No Content`)

---

## 5. Kubernetes 리소스 프록시 API (Kubernetes Resources)

Drover API는 K3s 클러스터 내부 Control Plane과 통신하여 테넌트용 K8s 리소스를 REST 프록시로 제공합니다.

* **Namespaces**:
  - `GET /v1/clusters/{cluster_id}/namespaces`: 네임스페이스 목록
* **ConfigMaps**:
  - `GET /v1/clusters/{cluster_id}/configmaps`: ConfigMap 전체 목록
  - `GET /v1/clusters/{cluster_id}/namespaces/{namespace}/configmaps/{name}`: 단건 조회
  - `POST /v1/clusters/{cluster_id}/namespaces/{namespace}/configmaps`: 생성 (`201 Created`)
  - `PUT /v1/clusters/{cluster_id}/namespaces/{namespace}/configmaps/{name}`: 수정
  - `DELETE /v1/clusters/{cluster_id}/namespaces/{namespace}/configmaps/{name}`: 삭제 (`204 No Content`)
* **Secrets**:
  - `GET /v1/clusters/{cluster_id}/secrets`: Secret 전체 목록
  - `GET /v1/clusters/{cluster_id}/namespaces/{namespace}/secrets/{name}`: 단건 조회
  - `POST /v1/clusters/{cluster_id}/namespaces/{namespace}/secrets`: 생성 (`201 Created`)
  - `PUT /v1/clusters/{cluster_id}/namespaces/{namespace}/secrets/{name}`: 수정
  - `DELETE /v1/clusters/{cluster_id}/namespaces/{namespace}/secrets/{name}`: 삭제 (`204 No Content`)
* **Pods**:
  - `GET /v1/clusters/{cluster_id}/namespaces/{namespace}/pods`: Pod 목록
  - `DELETE /v1/clusters/{cluster_id}/namespaces/{namespace}/pods/{name}`: Pod 삭제 (`204 No Content`)
  - `GET /v1/clusters/{cluster_id}/namespaces/{namespace}/pods/{name}/log`: Pod 로그 조회 (`PodLogResponse`)
* **Services**:
  - `GET /v1/clusters/{cluster_id}/namespaces/{namespace}/services`: Service 목록
  - `DELETE /v1/clusters/{cluster_id}/namespaces/{namespace}/services/{name}`: Service 삭제 (`204 No Content`)
* **Workloads (Deployments & ReplicaSets)**:
  - `GET /v1/clusters/{cluster_id}/namespaces/{namespace}/deployments`: Deployment 목록
  - `GET /v1/clusters/{cluster_id}/namespaces/{namespace}/replicasets`: ReplicaSet 목록
  - `POST /v1/clusters/{cluster_id}/namespaces/{namespace}/deployments/{name}/restart`: Deployment 재시작
  - `PATCH /v1/clusters/{cluster_id}/namespaces/{namespace}/deployments/{name}/scale`: Deployment 스케일링

---

## 6. 인증서 및 클라우드 셸 API (Certificates & Shell)

### 6.1 인증서 관리 API
* **`GET /v1/clusters/{cluster_id}/ca-certificate`**: 클러스터 CA 인증서 다운로드 (`text/plain`)
* **`GET /v1/clusters/{cluster_id}/certificate-expiry`**: CA 및 클라이언트/서버 TLS 인증서 만료일 조회 (`CertificateExpiryResponse`)
* **`POST /v1/clusters/{cluster_id}/rotate-certs`**: K3s 클러스터 TLS 인증서 자동 순환 트리거 (SSE 스트림 반환)

### 6.2 클라우드 셸 API (WebSocket 구분)
일반 REST API와 구분되는 디버깅/터미널 전용 엔드포인트입니다.
* **`POST /v1/clusters/{cluster_id}/shell-ticket`**: 셸 접속용 30초 유효 일회성 티켓 생성 (`201 Created`, 응답: `{"ticket": "...", "expires_in": 30}`)
* **`WebSocket /v1/clusters/{cluster_id}/shell?ticket={ticket}`**:
  - **설명**: K3s 노드 대화형 터미널(PTY) 연결을 위한 WebSocket 이중 통신 채널.
  - **프로토콜**: WebSocket (`ws://` 또는 `wss://`). Ticket 파라미터 인증 방식.

---

## 7. 내구성 오퍼레이션 및 이벤트 API (Durable Operations)

클러스터 수명주기 관련 모든 비동기 작업은 `DroverOperation` 객체로 DB에 원자적 보장됩니다.

### `GET /v1/operations/{operation_id}`
- **설명**: 오퍼레이션 단건 상세 조회. 테넌트 간 격리(Cross-tenant 요청 시 404 반환).
- **인증**: `X-Auth-Token` 필수 (Policy: `drover:operations:get`).
- **응답 (200 OK)**: `DroverOperationInfo`
  ```json
  {
    "id": "op-uuid",
    "project_id": "proj-uuid",
    "cluster_id": "cluster-uuid",
    "kind": "create",
    "status": "RUNNING",
    "request_id": "req-uuid",
    "idempotency_key": "afterglow-key-01",
    "error": null,
    "created_at": "2026-08-28T10:00:00Z",
    "started_at": "2026-08-28T10:00:01Z",
    "finished_at": null
  }
  ```

#### 오퍼레이션 상태(Status) 및 종류(Kinds) 정의
- **Status**: `QUEUED`, `RUNNING`, `WAITING_CALLBACK`, `SUCCEEDED`, `FAILED`, `CANCELLED`
- **Kinds**: `create`, `scale`, `nodegroup_reconcile`, `delete`, `rotate_certificates`, `reconcile`, `reauthorize`

> 생성은 외부 멱동성 키와 operation ID를 반환하는 SSE 계약을 제공합니다. durable 스케일·DELETE·노드그룹 변경의 현재 HTTP 응답은 operation ID를 반환하지 않으므로 후속 상태로 완료를 확인합니다. 재인가는 202에 operation ID를 반환합니다. 현재 tenant delete-async는 직접 caller-connection SSE이며 durable operation 계약의 예외입니다.

### `GET /v1/operations/{operation_id}/events`
- **설명**: 오퍼레이션의 시퀀스별 상세 이벤트 로그 조회.
- **쿼리 파라미터**: `since_sequence` (int, 기본값: `0`)
- **응답 (200 OK)**: `list[DroverOperationEventInfo]`

---

## 8. 시스템 관리자 및 자원 관리 API (Admin APIs)

Policy `drover:admin` (시스템 관리자 전용) 인증이 요구되는 관리 엔드포인트입니다.

* **`GET /v1/admin/clusters`**: 전체 테넌트 클러스터 통합 조회 (`status` 필터링 지원)
* **`GET /v1/admin/clusters/{cluster_id}`**: 타 테넌트 클러스터 강제 조회
* **`GET /v1/admin/clusters/{cluster_id}/kubeconfig`**: 관리자용 Kubeconfig 다운로드
* **`PATCH /v1/admin/clusters/{cluster_id}/scale`**: 강제 스케일링
* **`DELETE /v1/admin/clusters/{cluster_id}`**: 강제 동기 삭제
* **`POST /v1/admin/clusters/{cluster_id}/delete-async`**: 강제 비동기 삭제
* **`GET /v1/admin/clusters/{cluster_id}/ca-certificate`**: CA 다운로드
* **`GET /v1/admin/clusters/{cluster_id}/certificate-expiry`**: 인증서 만료 조회 (kubeconfig CA/클라이언트 + `api_address` URL의 host/port, 없으면 `server_ip:6443` TLS 프로브; 프로브 실패 시 `server_via_tls=[]`)
* **`POST /v1/admin/clusters/{cluster_id}/rotate-certs`**: 인증서 강제 순환 (SSE 스트림). 사용자 경로와 같은 클러스터별 Redis 회전 락을 사용하므로 진행 중인 회전이 있으면 `409`를 반환하며, 사용자 경로의 상태·`master_count≥3` 제한은 적용하지 않음. `last_rotation_initiated_by`는 `system-admin`으로 기록
- admin scale/delete도 requester trust가 필요하며 caller token을 target cluster project에 scope해야 합니다. system-admin 인증이 tenant-manager/service fallback을 허용하지 않습니다.
* **`GET /v1/admin/cluster-templates`**: 전체 템플릿 관리자 조회
* **`GET /v1/admin/managed-resources`**:
  - Drover가 생성하고 관리 중인 OpenStack 클라우드 실제 자원(`ManagedOpenStackResource`) 목록 조회.
  - 비밀번호, Kubeconfig, 토큰 등 민감한 데이터는 자동 마스킹 및 제거되어 안전하게 반환됨.
* **Resource Policies & Runtime Settings**:
  - `GET /v1/admin/resource-policies`: 자원 정책 규격 조회
  - `GET /v1/admin/resource-policies/catalog/{policy_key}`: 카탈로그 옵션 디스커버리
  - `k3s.default_network` 카탈로그에는 외부 Neutron 네트워크만 표시됩니다. 정책 변경 시에도 ID와 외부 속성을 확인하며 공유 전용 네트워크는 선택할 수 없습니다. [정책 정의](../drover/services/resource_policies.py).
  - `PUT /v1/admin/resource-policies/{policy_key}`: 자원 정책 동적 업데이트
  - `GET /v1/admin/runtime-settings`: 런타임 설정 조회
  - `PUT /v1/admin/runtime-settings/{setting_key}`: 런타임 설정 동적 업데이트

---

## 9. 통계 API (Stats API)

### 9.1 테넌트 통계 API
* **`GET /v1/stats/clusters`**: 현재 프로젝트 소유의 클러스터 개수 및 상태별 통계 반환.

> **참고 (GPU 쿼터 이관)**: `/v1/gpu-quotas` 및 `/v1/admin/gpu-quotas` API는 Afterglow 서비스(`app.services.gpu_quota`)로 완전히 이관 및 이관 완료되어 Drover API에서 제거되었습니다. GPU 쿼터 관련 모든 조회 및 설정은 Afterglow API (`/api/v1/admin/gpu-quotas`)를 사용합니다.
---

## 10. 인프라 전용 Guest Callback API (System Callback)

### `POST /v1/callback`
- **설명**: K3s Server VM의 cloud-init 부트스트랩 스크립트가 실행 완료 후 Kubeconfig 및 Node Token을 Drover API로 콜백 전달하는 인프라 전용 엔드포인트.
- **인증**: 토큰 기반 일회성 비인증 (Unauthenticated HTTP POST; 30분 유효기간 Redis 1-Time Token `token` 필수).
- **보안 제한**:
  - Kolla Reverse Proxy 및 API 백엔드 레벨에서 `drover_callback_allowed_cidrs` (CIDR 허용목록) 외부의 소스 IP 요청을 즉시 거부 (403 Forbidden).
  - 30분 만료 또는 1회 콜백 성공 후 Redis 토큰 즉시 삭제(`GETDEL`).
  - callback은 create operation의 active delegation을 후속 HA/agent job과 HA Octavia connection에 연결하며 새 authority를 부여하지 않습니다. delegation 없는/revoked/만료된 worker mutation은 terminal입니다. HA callback의 LB-member 예외는 현재 warning으로 기록하고 join-count/후속 enqueue를 계속할 수 있으므로 callback HTTP/operation을 즉시 실패 처리한다고 보장하지 않습니다; 다른 identity connection으로 우회하지 않습니다.
- **요청 바디 (`K3sCallbackRequest`)**:
  ```json
  {
    "token": "redis-one-time-token-string",
    "success": true,
    "kubeconfig": "apiVersion: v1...",
    "node_token": "K10...",
    "server_ip": "10.0.0.15",
    "error": null
  }
  ```

---

## 상호 문서 참조
* [Drover 기술 문서 인덱스](README.md)
* [Afterglow 서비스 통합 가이드](afterglow-service-integration.md)
* [Drover 레거시 기능 커버리지 및 오픈스택 통합 사양서](drover-feature-coverage.md)
