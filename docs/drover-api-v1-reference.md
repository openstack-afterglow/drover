# Drover Native v1 API 기술 Reference

Drover 서비스의 네이티브 REST, SSE(Server-Sent Events) 및 WebSocket API에 대한 완전한 사양서입니다. 본 API 사양서는 OpenStack Keystone 인증 기반의 프로젝트 격리 멀티테넌트 환경을 전제로 설계되었습니다.

> **기계 읽기용 OpenAPI 스키마**
> 실행 중인 Drover API 서버의 최신 기계 읽기 표준 OpenAPI 스키마는 **`/openapi.json`**, 대화형 UI는 **`/docs`**에서 동적으로 조회할 수 있습니다.

---

## 1. 공통 규격 및 HTTP 헤더 (Common Standards & Headers)

### 1.1 HTTP 인증 및 콘텍스트 헤더
* **`X-Auth-Token`** *(필수)*: OpenStack Keystone 프로젝트 스코프 인증 토큰. OpenAPI scheme 명칭: `KeystoneToken`.
* **`X-Project-Id`** *(선택)*: Keystone 인증 시 명시적으로 타겟 프로젝트 ID를 지정할 때 사용.
* **`X-Openstack-Request-Id`** *(자동 생성/전달)*: 시스템 전반의 상관관계(Correlation) 추적용 요청 식별자. API 응답 헤더 및 로그/이벤트 페이로드에 포함됨.
* **`Idempotency-Key`** *(생성 API에서 선택·권장)*: `POST /v1/clusters/async`의 재전송을 같은 오퍼레이션으로 귀속시키는 유니크 키입니다. 현재 스케일·삭제·노드그룹 변경에는 외부 멱동성 계약이 없습니다.

### 1.2 표준 HTTP 상태 코드
* `200 OK` / `201 Created` / `204 No Content`: 성공적인 처리
* `400 Bad Request`: 요청 파라미터 검증 실패 (Pydantic 모델 검증 오류 포함)
* `401 Unauthorized`: Keystone 토큰 누락, 만료 또는 프로젝트 스코프 미지정
* `403 Forbidden`: `oslo.policy` 정책 거부 또는 시스템 관리자 권한 부족
* `404 Not Found`: 리소스 미존재 또는 타 테넌트 자원에 대한 요청 (프로젝트 격리 보안)
* `409 Conflict`: 멱동성 키 불일치 중복 요청 또는 리소스 상태 충돌
* `429 Too Many Requests`: Rate Limit(초당/분당 요청 제한) 초과
* `503 Service Unavailable`: 백엔드 서비스(Redis, DB, Keystone 등) 오류

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

### `GET /v1/clusters/{cluster_id}/kubeconfig`
- **설명**: K3s 클러스터 접근용 Kubeconfig YAML 파일 다운로드.
- **응답 (200 OK)**: `text/yaml` / `application/x-yaml`

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
- **응답 (200 OK)**: `text/event-stream` (SSE 스트림)
  - 이벤트 라인 형식 (`K3sProgressMessage`):
    `data: {"step": "security_group", "progress": 10, "message": "...", "cluster_id": "...", "operation_id": "op-123"}`

### `PATCH /v1/clusters/{cluster_id}/scale`
- **설명**: 클러스터 워커(Agent) 노드 수 변경. (Rate limit: 10/min)
- **요청 바디 (`ScaleK3sClusterRequest`)**: `{"agent_count": 4}`
- **응답 (200 OK)**: `{"message": "...", "target_count": 4}`

### `DELETE /v1/clusters/{cluster_id}`
- **설명**: 클러스터 동기 삭제. (Rate limit: 5/min)
- **응답 (204 No Content)**

### `POST /v1/clusters/{cluster_id}/delete-async`
- **설명**: 클러스터 비동기 삭제 (SSE 스트림 반환). (Rate limit: 5/min)
- **응답 (200 OK)**: `text/event-stream` (SSE 스트림)

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
- **설명**: Stampede 동적 오토스케일링 활성화.
- **응답 (200 OK)**: `{"message": "Stampede가 활성화되었습니다", ...}`

### `POST /v1/clusters/{cluster_id}/stampede/disable`
- **설명**: Stampede 동적 오토스케일링 비활성화.
- **응답 (200 OK)**: `{"message": "Stampede가 비활성화되었습니다", ...}`

### `GET /v1/clusters/{cluster_id}/stampede`
- **설명**: Stampede 오토스케일링 상태 조회.
- **응답 (200 OK)**: Stampede 상태 객체

### `GET /v1/clusters/{cluster_id}/stampede/events`
- **설명**: Stampede 스케일링 이벤트 이력 조회.
- **쿼리 파라미터**: `limit` (int, 1~200, 기본값: 50)
- **응답 (200 OK)**: Stampede 이벤트 목록

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
- **Kinds**: `create`, `scale`, `nodegroup_reconcile`, `delete`, `rotate_certificates`, `reconcile`

> 생성은 외부 멱동성 키와 operation ID를 반환하는 SSE 계약을 제공합니다. 스케일·삭제·노드그룹 변경도 Worker Job으로 큐잉되지만, 현재 해당 HTTP 응답에서 operation ID를 반환하지 않습니다. 이들은 클러스터/노드그룹의 후속 상태 조회로 완료를 확인해야 합니다.

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
* **`GET /v1/admin/clusters/{cluster_id}/certificate-expiry`**: 인증서 만료 조회
* **`POST /v1/admin/clusters/{cluster_id}/rotate-certs`**: 인증서 강제 순환
* **`GET /v1/admin/cluster-templates`**: 전체 템플릿 관리자 조회
* **`GET /v1/admin/managed-resources`**:
  - Drover가 생성하고 관리 중인 OpenStack 클라우드 실제 자원(`ManagedOpenStackResource`) 목록 조회.
  - 비밀번호, Kubeconfig, 토큰 등 민감한 데이터는 자동 마스킹 및 제거되어 안전하게 반환됨.
* **Resource Policies & Runtime Settings**:
  - `GET /v1/admin/resource-policies`: 자원 정책 규격 조회
  - `GET /v1/admin/resource-policies/catalog/{policy_key}`: 카탈로그 옵션 디스커버리
  - `PUT /v1/admin/resource-policies/{policy_key}`: 자원 정책 동적 업데이트
  - `GET /v1/admin/runtime-settings`: 런타임 설정 조회
  - `PUT /v1/admin/runtime-settings/{setting_key}`: 런타임 설정 동적 업데이트

---

## 9. 통계 및 GPU 쿼터 API (Stats & GPU Quotas)

### 9.1 테넌트 통계 API
* **`GET /v1/stats/clusters`**: 현재 프로젝트 소유의 클러스터 개수 및 상태별 통계 반환.

### 9.2 테넌트 GPU 쿼터 API (`/v1/gpu-quotas`)
* **`GET /v1/gpu-quotas/effective`**: 적용된 실효 GPU 쿼터 한도 조회
* **`GET /v1/gpu-quotas/status`**: GPU 타입별 한도(`limit`), 사용량(`in_use`), 잔여량(`available`) 조회
* **`POST /v1/gpu-quotas/check`**: 요청 Flavor의 `extra_specs` 기반 GPU 쿼터 충족 여부 사전 검증 (`GpuQuotaCheckRequest`)

### 9.3 관리자 GPU 쿼터 API (`/v1/admin/gpu-quotas`)
* **`GET /v1/admin/gpu-quotas/defaults`**: 기본 GPU 쿼터 조회
* **`PUT /v1/admin/gpu-quotas/defaults`**: 기본 GPU 쿼터 설정 (`GpuQuotaRequest`)
* **`DELETE /v1/admin/gpu-quotas/defaults/{gpu_type}`**: 기본 GPU 쿼터 삭제
* **`GET /v1/admin/gpu-quotas/{project_id}`**: 특정 프로젝트 GPU 쿼터 및 사용량 조회
* **`PUT /v1/admin/gpu-quotas/{project_id}`**: 특정 프로젝트 GPU 쿼터 지정
* **`DELETE /v1/admin/gpu-quotas/{project_id}/{gpu_type}`**: 특정 프로젝트 GPU 쿼터 삭제

---

## 10. 인프라 전용 Guest Callback API (System Callback)

### `POST /v1/callback`
- **설명**: K3s Server VM의 cloud-init 부트스트랩 스크립트가 실행 완료 후 Kubeconfig 및 Node Token을 Drover API로 콜백 전달하는 인프라 전용 엔드포인트.
- **인증**: 토큰 기반 일회성 비인증 (Unauthenticated HTTP POST; 30분 유효기간 Redis 1-Time Token `token` 필수).
- **보안 제한**:
  - Kolla Reverse Proxy 및 API 백엔드 레벨에서 `drover_callback_allowed_cidrs` (CIDR 허용목록) 외부의 소스 IP 요청을 즉시 거부 (403 Forbidden).
  - 30분 만료 또는 1회 콜백 성공 후 Redis 토큰 즉시 삭제(`GETDEL`).
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
