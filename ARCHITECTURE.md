# Drover Architecture

## Overview

Drover는 OpenStack 프로젝트 단위로 K3s 클러스터와 노드그룹의 생성·운영·삭제를 담당하는 독립적인 control plane 서비스다. Afterglow가 화면과 외부 BFF를 소유한다면 Drover는 `/v1` API, 내구성 작업 큐, OpenStack 자원 inventory, VM callback 이후의 K3s 조정을 소유한다.

- Repository: https://github.com/openstack-afterglow/drover
- 분석 기준: `dev` 브랜치, 작업 트리의 소스와 테스트
- 패키지: `drover==0.2.22`, `drover-sdk==0.2.21`
- 주요 런타임: Python `>=3.11`(root package `requires-python`; CI·container image는 3.12), FastAPI `0.125.0`, Uvicorn `0.39.0`, openstacksdk `3.3.0`, SQLAlchemy `>=2.0`, Redis client `5.0.0`

1분 요약: FastAPI API가 MariaDB에 cluster/operation/job을 함께 기록하고, 독립 Worker가 lease를 얻어 OpenStack 작업을 실행한다. 서버 VM의 일회성 cloud-init callback은 K3s bootstrap 결과를 전달하고, Worker가 agent/HA 후속 작업을 수행한다. MariaDB는 내구성 상태와 queue의 정본이며 Redis는 callback token·짧은 상태/헬스 캐시·분산 잠금·stampede 이벤트 같은 보조 저장소다.

## Development status

| 기능 | Implementation | Verification evidence | Current limit | Source |
|---|---|---|---|---|
| Native `/v1` API와 Keystone 프로젝트 격리 | implemented | source-reviewed, test-defined | Magnum wire 호환 API가 아니다 | `drover/main.py:127-143`, `drover/auth.py`, `tests/test_auth.py`, `tests/test_openapi_contract.py` |
| 내구성 cluster create와 operation ID | implemented | source-reviewed, test-defined | 외부 operation ID/idempotency 계약은 create API에만 있다 | `drover/api/clusters.py:create_k3s_cluster_async`, `drover/services/jobs.py:enqueue_job`, `tests/test_durable_create.py` |
| lease worker와 retry/attempt fence | implemented | source-reviewed, test-defined | 최대 3회 시도 후 실패하며 원격 API의 모든 작업을 되돌린다고 보장하지 않는다 | `drover/services/jobs.py:_claim_one`, `process_one_job`, `tests/test_jobs.py` |
| cloud-init callback 및 HA/agent handoff | implemented | source-reviewed, test-defined | callback 만료/실패는 cluster를 `ERROR`로 만들며 callback token은 Redis 일회성이다 | `drover/api/callback.py:k3s_callback`, `drover/services/provisioner.py`, `tests/test_k3s_callback.py` |
| nodegroup·Stampede autoscale | implemented | source-reviewed, test-defined | `min_size`/`max_size`와 cooldown 범위 안에서만 동작하며 외부 GPU admission에 의존한다 | `drover/services/autoscale.py`, `drover/services/stampede.py`, `tests/test_k3s_stampede.py` |
| OpenStack drift reconciliation | implemented | source-reviewed, test-defined | orphan은 보고만 하고 자동 삭제하지 않는다 | `drover/services/reconciliation.py:reconcile_cluster`, `tests/test_reconciliation.py` |
| Afterglow provisioning intent/GPU admission 연동 | partial | source-reviewed, test-defined | 일반 create는 Drover가 직접 Nova/Cinder 등을 호출하고 intent/admission은 특정 Stampede 경로다 | `drover/services/afterglow.py`, `drover/services/stampede.py`, `tests/test_afterglow_admission.py`, `tests/test_afterglow_provisioning.py` |
| legacy `gpu_quotas` 제거 | partial | source-reviewed, test-defined | 역사적 `001_baseline.sql` 테이블은 아직 물리 삭제하지 않았고 조건부 runbook만 있다 | `drover/migrations/001_baseline.sql`, `drover/migrations/README.md`, `docs/gpu-quota-table-retirement-runbook.md` |

위 표의 `test-defined`는 테스트가 계약을 정의한다는 뜻이다. 2026-09-24 로컬 `uv run pytest tests`는 638건 통과·3건 skip이었고 architecture guard 13건도 포함한다. skip된 live integration과 실제 OpenStack 배포·외부 서비스 호출은 검증하지 않았다.

## System context

```mermaid
graph LR
    User[Project client / Afterglow] -->|Keystone token, /v1| API[Drover FastAPI API]
    API -->|cluster, operation, job transactions| DB[(MariaDB)]
    API -->|cache, callback token, lock| Redis[(Redis auxiliary store)]
    Worker[Drover Worker] -->|lease and status| DB
    Worker -->|Nova, Neutron, Cinder, Octavia, Keystone, Barbican, Manila| OS[OpenStack services]
    VM[K3s server VM] -->|one-time cloud-init callback| API
    Worker -->|selected Stampede intent/admission boundary| AG[Afterglow internal API]
```

텍스트 흐름은 다음과 같다. 호출자는 Keystone 토큰으로 `/v1`에 요청한다. API는 프로젝트 소유권과 정책을 확인하고 MariaDB transaction 안에서 cluster, operation, job을 기록한다. Worker가 job lease를 획득한 뒤 OpenStack 자원을 만들고, 서버 VM이 callback을 보내면 agent/HA job을 이어서 실행한다. Afterglow는 호출자 UI/BFF와 일부 GPU admission/provisioning intent 경계일 뿐 Drover의 DB나 Worker를 대체하지 않는다.

## Code map

| 경로·심볼 | 책임 | 의존 방향 |
|---|---|---|
| `drover/main.py:app`, `readiness_checks` | FastAPI lifecycle, `/v1` router mount, liveness/readiness | API → DB/Redis/Keystone |
| `drover/api/clusters.py:create_k3s_cluster_async` | policy snapshot, cluster row, durable create job, SSE event replay | API → `services.store`, `services.jobs`, `services.operations` |
| `drover/api/callback.py:k3s_callback` | CIDR 검사, one-time token 소비, callback 상태 기록, HA/agent job enqueue | VM → API → DB/Redis |
| `drover/services/jobs.py` | MariaDB queue enqueue/claim, 15분 lease, heartbeat, retry/complete | Worker → provisioner/autoscale/deletion/reconciliation |
| `drover/services/operations.py` | operation 상태·순서 이벤트·idempotency hash·callback timeout | API/Worker → ORM |
| `drover/services/provisioner.py:create_cluster_job`, `bootstrap_ha_servers`, `provision_agents` | Nova/Cinder/Neutron/Octavia 자원과 K3s userdata 생성 | Worker → OpenStack adapters/cloud-init |
| `drover/services/autoscale.py`, `drover/services/stampede.py` | nodegroup 조정, pod 관찰 기반 Stampede scale-up/down | Worker → jobs, K8s API, optional Afterglow |
| `drover/services/reconciliation.py:reconcile_cluster` | recorded inventory와 실제 OpenStack ID/state 비교, drift 기록 | Worker → inventory/Keystone/OpenStack |
| `drover/services/afterglow.py` | 제한된 provisioning intent와 GPU admission HTTP boundary | Stampede → Afterglow internal endpoint |
| `drover/models/orm.py` | `K3sCluster`, `DroverJob`, `DroverOperation`, event, inventory의 DB schema | SQLAlchemy → MariaDB |
| `drover/migrations/manifest.txt`, `drover/migrations/*.sql` | immutable baseline와 migration ledger | `drover-migrate` → MariaDB |
| `drover/worker.py:_main_async` | reconcile/jobs/health/Stampede loop를 한 Worker process에서 실행 | Worker → services |
| `sdk/drover_sdk/service.py:DroverService` | openstacksdk에 `drover` catalog service와 `container-infra` alias 등록 | SDK → `Proxy` |
| `sdk/drover_sdk/proxy.py:Proxy` | `/v1` REST/SSE 호출, shell ticket 발급 및 operation polling helper | SDK client → Drover API |

## Runtime flows

### Cluster create → callback → ACTIVE

1. `POST /v1/clusters/async`가 request body hash와 선택적 `(project_id, Idempotency-Key)`를 확인한다. 정책·템플릿을 resolve한 뒤 cluster row를 먼저 기록하고, `_jobs.enqueue_job` 내부 MariaDB transaction에서 `DroverOperation(status=QUEUED)`와 `DroverJob(kind=create)`를 함께 기록한다.
2. SSE는 DB의 `DroverOperationEvent`를 순서대로 replay하며 operation ID를 모든 progress event에 싣는다. 연결이 끊겨도 job은 취소되지 않는다.
3. Worker가 같은 cluster에 활성 lease가 없는 경우 job을 claim하고 OpenStack 자원을 만든다. 일반 cluster create는 `drover/services/provisioner.py:create_cluster_job`에서 Security Group, HA이면 Octavia LB/FIP, Cinder boot volume, cloud-init callback token, Nova server와 inventory를 만든다.
4. 서버 VM이 `/v1/callback`으로 성공 결과를 보내면 Redis에서 callback token을 GETDEL 의미로 소비하고 kubeconfig를 암호화해 MariaDB에 저장한다. 단일 master는 `provision_agents`, HA는 `bootstrap_ha_servers`와 joiner callback을 거쳐 agent job을 enqueue한다.
5. agent job이 완료되면 cluster가 `ACTIVE`가 되고 create operation이 `SUCCEEDED`가 된다. callback 실패·누락·30분 timeout은 operation/cluster를 실패 처리한다.

선택한 `key_name`은 API admission에서 요청자 connection으로 공개키를 조회·검증한 뒤 기존 cluster/job `ssh_public_key`에 snapshot으로 저장한다. Worker는 tenant manager 계정으로 Nova를 호출하므로 caller의 keypair 이름을 전달하지 않는다. primary·HA server 및 agent의 Ubuntu cloud-init/FCOS Ignition이 같은 공개키를 설치한다. API/worker가 모두 새 버전이어야 이 계약을 보장하며, named-key job/cluster에 snapshot이 없는 구 데이터는 fail-closed 처리한다. 새 schema나 manager 키페어 리소스는 추가하지 않는다.

### Reconnection, idempotency, scale/delete

- create에 같은 idempotency key와 같은 canonical request hash를 재전송하면 기존 operation/cluster를 재사용하고, hash가 다르면 `409`다. 이 계약은 `POST /v1/clusters/async`에 한정된다.
- SSE 재연결은 `GET /v1/operations/{operation_id}`와 `GET /v1/operations/{operation_id}/events?since_sequence=N`으로 DB event sequence를 replay한다. SDK `Proxy`는 operation 상태를 조회할 수 있지만 서버 작업을 HTTP 연결 수명에 묶지 않는다.
- `PATCH /v1/clusters/{cluster_id}/scale`, nodegroup 조정 및 일반 delete는 durable job으로 enqueue된다. 현재 이 경로에는 create와 같은 외부 idempotency key 재사용/operation ID 응답 계약이 없다. 호출자는 자동 재시도 대신 correlation ID와 cluster 상태를 확인해야 한다.

### Nodegroups, Stampede, reconciliation

`drover/services/autoscale.py`는 desired nodegroup count를 durable `nodegroup_reconcile` job으로 맞추고, `stampede.py`는 K3s pod pending/resource pressure를 읽어 `min_size`·`max_size`, selector/taint, cooldown을 적용한다. GPU flavor가 필요한 경우 현재 GPU quota/admission authority인 Afterglow의 내부 admission을 조회하고, 특정 Stampede provisioning만 durable Afterglow intent를 claim/submit한다. Worker의 reconcile loop는 active cluster를 프로젝트별 concurrency로 enqueue하고 `reconcile_cluster`는 recorded resource ID를 다시 조회해 missing/mismatch/orphan을 DB에 기록한다.

### Certificate rotation

`POST /v1/clusters/{cluster_id}/rotate-certs`(`drover/api/certificates.py`)는 `master_count>=3`인 `ACTIVE`/`ERROR` cluster에서만 Redis rotation lock을 얻고 `drover/services/cert_rotation.py:rotate_certificates` SSE를 연다. control-plane 노드마다 `kube-system` Job(`nsenter -t 1 ... systemctl restart k3s`)을 만들고, Job 성공(최대 120초)을 확인한 뒤 `wait_node_ready`(기본 `drover_cert_rotation_node_timeout_sec=300`)가 Ready=True를 처음 관측하면 바로 다음 노드로 넘어간다. 대기 중에는 10초(`_NODE_READY_KEEPALIVE_SECONDS`)마다 SSE keepalive를 보낸다.

- 노드 사이에 고정 settle 대기는 없다. 안전 간격은 `systemctl restart k3s`가 k3s READY까지 블록한다는 가정에 기댄다(서버 설치 경로가 get.k3s.io 기본 unit `Type=notify`를 쓴다는 전제이며 이 저장소에서 검증하지 않았다). Node Ready condition은 node-monitor-grace-period 동안 stale True일 수 있으므로, control-plane restart 사이 최소 간격이 필요하면 이름 있는 settle 상수나 Job 완료 이후 heartbeat 확인을 추가한다.
- SSE 소비자가 끊기면 generator가 Ready polling task를 취소하고 endpoint가 rotation lock을 해제한다.
- admin `GET /v1/admin/clusters/{cluster_id}/certificate-expiry`와 `POST /v1/admin/clusters/{cluster_id}/rotate-certs`(`drover/api/admin.py`)에는 알려진 결함이 있다. 전자는 async `probe_tls_server_cert`를 await하지 않는다. 후자는 `rotate_certificates(cluster_id, project_id, initiated_by)` 자리에 dict·`None`·cluster ID를 넘기고 `K3sProgressMessage`를 `step, pct, msg` tuple로 unpack하며 rotation lock을 잡지 않아 항상 FAILED로 끝난다. 두 경로에는 테스트가 없고 아직 수정하지 않았다.

## Data and contracts

### Authoritative storage와 cache split

- **MariaDB가 정본**: `K3sCluster`, `K3sNodegroup`, `K3sAgentVM`, `DroverJob`, `DroverOperation`, `DroverOperationEvent`, `ManagedOpenStackResource`, policy/runtime setting과 암호화된 kubeconfig를 저장한다. queue claim/lease와 operation event도 SQL transaction/row lock으로 보호한다.
- **Redis는 보조 저장소**: callback/HA callback token과 HA join counter의 TTL 일회성 값, health 결과, 짧은 API/cache 응답, shell ticket, rotation lock, Stampede event list를 저장한다. Redis는 cluster/job/operation의 durable authority가 아니다. 일부 legacy DB-unavailable fallback이 있어도 정상 배포의 durable contract는 MariaDB다.
- **OpenStack은 외부 자원 authority**: Nova/Neutron/Cinder/Octavia/Keystone 등 실제 자원 상태는 `ManagedOpenStackResource` ID와 metadata로 추적하며 reconciliation이 DB inventory와 대조한다. Drover DB가 OpenStack 자원을 대체하지 않는다.

### 주요 상태와 불변식

- operation 상태: `QUEUED → RUNNING → WAITING_CALLBACK → SUCCEEDED|FAILED|CANCELLED`; event `sequence`는 operation별 증가한다.
- job 상태: `queued|running|completed|failed`; `_LEASE_SECONDS=900`, heartbeat와 attempt fence로 stale worker를 재실행하며 `_MAX_ATTEMPTS=3`이다.
- cluster 상태에는 `CREATING`, `PROVISIONING`, `ACTIVE`, `SCALING`, `DELETING`, `ERROR`, `DELETED`가 사용된다. project ID와 cluster ID 소유권을 모든 테넌트 조회에서 확인한다.
- callback token은 callback endpoint에서 source CIDR를 검사한 후 단 한 번 소비한다. kubeconfig는 API 응답에 평문으로 저장하지 않고 encryption key로 암호화한다.

### API, SDK, catalog 계약

API namespace는 `/v1`이며 health/discovery도 `/v1` 아래에 있다. Keystone service **name**과 **type**은 모두 `drover`이고, `drover-sdk`가 SDK에서 `drover`를 canonical service로 노출하면서 `container-infra`를 alias로 지원한다. 즉 `container-infra`는 Keystone catalog type이 아니라 SDK 호환 alias다. `drover_sdk.register(conn)` 후 `conn.drover`가 `/v1` endpoint를 사용한다.

`drover/auth.py:validate_token`은 `X-Project-Id`가 없는 SDK 호출에서 제출된 토큰을 Keystone `/v3/auth/tokens`로 검증하여 원래 project/token/roles를 보존한다. Keystone URL은 root와 `/v3` 형식을 모두 지원한다. 무범위 token 재인증은 사용자의 default project로 바뀌거나 unscoped token을 발급하므로 검증 용도로 사용하지 않는다. 명시적인 project header가 있을 때만 기존 Keystone-authorized rescope를 수행하며, 프로젝트 없는 토큰·검증 실패·기존 admin/owner 정책은 fail-closed로 유지한다. 배포 topology와 schema는 변경하지 않는다.

`drover/migrations/manifest.txt`와 migration SQL의 checksum ledger가 schema 선행 조건이다. `001_baseline.sql`은 historical `gpu_quotas`를 포함하고 있으며 source table 은퇴는 Afterglow import·sole-authority rollout·query 부재 감사 뒤에만 가능하다. 현재 코드가 그 table을 물리적으로 삭제했다고 주장하지 않는다.

## Deployment and operations

배포 단위는 다음과 같이 분리된다.

- `drover-api` (`drover.main:run`): FastAPI/Uvicorn, 기본 port `8011`, `/v1` REST/SSE/WebSocket와 liveness/readiness.
- `drover-worker` (`drover.worker:main`): jobs lease executor, callback recovery, reconciliation, health, Stampede loop. API process와 독립적으로 실행해야 한다.
- `drover-migrate` (`drover.scripts.migrate:main`): `manifest.txt` checksum ledger를 적용·검사하며 API/Worker보다 schema를 먼저 준비한다.
- `drover-sdk`: `sdk/`의 독립 Python package이며 API/Worker process에 import되어야 하는 내부 module이 아니다.
- `deploy/kolla/`: API, Worker, migrate container와 Keystone catalog registration/config를 Kolla-Ansible 자산으로 제공한다.
- 루트 `drover` wheel은 `deploy/kolla/ansible/roles/drover`를 `share/kolla-ansible/ansible/roles/drover` shared data로 설치한다. 기본 wheel은 Kolla-Ansible이나 서비스 runtime dependency를 설치하지 않으며 API/Worker/migration 실행에는 `drover[service]`가 필요하다.
- Kolla role의 `drover_image_tag`은 root wheel 버전과 별개로 마지막으로 공개된 runtime image tag를 가리킨다. 새 image를 실제 publish하기 전에는 package patch release가 이 기본값을 변경하지 않는다.

`GET /v1/health`와 `/v1/health/live`는 process liveness만 의미한다. `/v1/health/ready`는 MariaDB, Redis ping, migration ledger, Keystone service credentials를 모두 확인하고 하나라도 unavailable이면 `503`을 반환한다. 로그는 각 process의 표준 logging과 correlation ID에 남고, operation event 및 reconciliation drift는 MariaDB API 조회로 확인한다. health 결과·Stampede event는 Redis cache이므로 장애 시 최신 값이 없을 수 있다.

운영 선행 조건은 MariaDB schema migration, Redis 접근, Keystone service credentials, callback base URL/CIDR, kubeconfig encryption key와 OpenStack 네트워크/이미지/flavor 정책이다. 실제 OpenStack 자원과 K3s VM callback이 없으면 API readiness만으로 cluster provisioning 성공을 의미하지 않는다.

## Security boundaries

| 경계 | 실제 계약 |
|---|---|
| 사용자/프로젝트 → API | `X-Auth-Token` Keystone token과 optional project context를 검증하고 `oslo.policy`와 project ownership으로 접근을 제한한다. `X-Openstack-Request-Id`는 correlation/audit용이다. |
| Drover → OpenStack | Worker는 tenant project의 별도 manager connection으로 Nova, Neutron, Cinder, Octavia 등을 호출한다. Service/admin scope는 manager identity bootstrap에 사용한다. 자원 metadata/tag와 DB inventory로 소유 경계를 추적한다. |
| VM → callback | cloud-init이 callback URL과 one-time token을 사용한다. `drover_callback_allowed_cidrs`가 설정되면 source IP를 먼저 제한하고 token은 Redis에서 소비한다. |
| Drover → Afterglow | Stampede의 제한된 GPU admission/provisioning intent만 전용 내부 URL과 설정된 token으로 호출한다. Afterglow의 사용자 API token이나 내부 secret을 문서에 기록하지 않는다. |
| cluster plugin → OpenStack | OCCM/CSI/Ingress/KMS가 필요한 경우 cluster별 최소권한 application credential을 userdata에 전달하며 삭제 시 회수 경로를 사용한다. |
| 저장 데이터 | kubeconfig는 암호화해 MariaDB에 저장한다. password/token/encryption key 값은 환경변수 또는 secret file에서 읽고 문서·로그·README에 기록하지 않는다. |

Callback endpoint가 인증 불필요한 VM 경계라는 사실은 token+CIDR 검증을 생략한다는 뜻이 아니다. Redis callback token과 shell ticket은 짧은 TTL/일회성이며 MariaDB durable records에 의존하지 않는 보조 자격이다.

## Development and verification

소스에서 확인한 계층별 명령은 다음과 같다. 아래 명령은 외부 OpenStack/Keystone 환경 또는 dev dependencies가 필요할 수 있다.

- service runtime package: `uv sync --extra service --frozen`
- development dependencies and service runtime: `uv sync --all-extras --frozen`
- root wheel with the Kolla role: `uv build --wheel`
- durable create contract: `uv run pytest tests/test_durable_create.py -q`
- callback contract: `uv run pytest tests/test_k3s_callback.py -q`
- operations/jobs contract: `uv run pytest tests/test_operations_jobs.py tests/test_jobs.py -q`
- reconciliation and Stampede contracts: `uv run pytest tests/test_reconciliation.py tests/test_k3s_stampede.py -q`
- Afterglow boundary: `uv run pytest tests/test_afterglow_admission.py tests/test_afterglow_provisioning.py -q`
- API/CI structure: `uv run pytest tests/test_openapi_contract.py tests/test_ci_workflows.py -q`
- migration: `uv run drover-migrate --apply`
- architecture snapshot: `python3 scripts/check_architecture.py`

2026-09-24 로컬 `uv run pytest tests`는 638건 통과·3건 skip(약 11-18초)이었으며 `tests/test_architecture_guard.py` 13건도 포함한다. Disposable MariaDB/Redis, 유효한 Keystone credentials, live OpenStack이 필요한 skip·migration/readiness 검증은 통과로 승격하지 않는다.

GitHub Actions 형태는 `tests/test_ci_workflows.py`가 고정하며 성능 규정과 기준선은 [`AGENTS.md`](AGENTS.md)의 CI 절에 있다.

- main/dev 대상 PR(fork·dependabot 포함)은 `CI`(`.github/workflows/ci.yml`)를 한 번 실행한다. `ci.yml`에는 push trigger가 없고 `workflow_call`과 `workflow_dispatch`가 있다.
- dev/main push와 `v*` tag의 suite는 `Docker Build & Push`(`.github/workflows/docker-build.yml`)에서만 실행한다(tag는 별도로 `release.yml` wheel release도 실행한다). 그 `test` job이 `ci.yml`을 reusable workflow로 실행하고 `build-and-push`가 `needs: test`로 전체 결과를 기다린 뒤 GHCR에 발행한다. 이 workflow에는 `pull_request` trigger가 없다.
- `docker-build-and-scan`은 `setup-buildx-action` 없이 default docker driver로 `drover-api`/`drover-worker` target을 daemon에 직접 빌드(`load: true`)하고 Trivy 두 단계로 스캔한다. 발행용 `build-and-push`는 Buildx를 그대로 쓴다. docker driver 경로는 로컬에서 실행하지 않았으므로 CI 실행으로만 확인된다. PR은 발행용 Buildx 빌드와 metadata/labels 단계를 실행하지 않으므로 그 경로에만 있는 실패는 merge 후 dev/main push에서 드러나며, `needs: test` 뒤 발행 단계라 fail-closed다. 이 post-merge 검출은 [`AGENTS.md`](AGENTS.md) CI 절에서 수용한 검증 공백이다.
- `db-migration-and-readiness`의 MariaDB/Redis service health-check는 2초 interval에 각각 30회/15회 retry(60초/30초 window)다.

## Change guide

| 변경 유형 | 먼저 읽을 곳 | 함께 갱신할 계약/검증 |
|---|---|---|
| API route, auth, response/event | `drover/main.py`, 해당 `drover/api/*.py`, `docs/drover-api-v1-reference.md` | project ownership, `/v1` path, `tests/test_openapi_contract.py`/해당 API test, 이 문서 Code map·Data and contracts |
| create/scale/delete/nodegroup job | `drover/services/jobs.py`, `operations.py`, `provisioner.py`, `autoscale.py` | transaction/lease/status/idempotency 설명, durable/operation tests, Runtime flows |
| OpenStack resource or plugin | adapter in `drover/services/`, `provisioner.py`, `drover/models/orm.py` | inventory/reconciliation, migration SQL, security/deployment sections와 관련 tests |
| callback/cloud-init/token | `drover/api/callback.py`, `drover/services/store.py`, `redis_store.py`, cloud-init templates | CIDR/TTL/one-time contract, `tests/test_k3s_callback.py`, Security boundaries |
| Stampede/GPU/Afterglow boundary | `drover/services/stampede.py`, `afterglow.py`, `docs/afterglow-service-integration.md` | current intent/admission scope, `tests/test_k3s_stampede.py`, `tests/test_afterglow_*`, feature coverage |
| schema, migration, durable model | `drover/models/orm.py`, `drover/migrations/`, `drover/scripts/migrate.py` | migration ledger/readiness, `tests/test_durable_create.py`, Deployment and operations |
| SDK/catalog | `sdk/drover_sdk/service.py`, `sdk/drover_sdk/proxy.py`, `sdk/pyproject.toml` | `/v1` and alias wording, `sdk/tests/test_proxy.py`, docs catalog sections |
| CI workflow/test runtime | `.github/workflows/*.yml`, [`AGENTS.md`](AGENTS.md) CI 절 | `tests/test_ci_workflows.py`, 20회 이상 전후 실측 기록, Development and verification |
| bugfix/refactor with no topology change | affected source and tests | explain why topology/data contract is unchanged in the Maintenance review summary and restamp |

## Maintenance

Architecture maintenance는 문서 작업이 아니라 source snapshot을 확인하는 절차다.

1. 작업 전에 이 `ARCHITECTURE.md`와 영향을 받는 상세 문서를 읽는다.
2. code/config/schema/dependency/deploy/test 변경이면 같은 변경에서 관련 source-linked section과 상세 문서를 갱신한다. 구조 영향이 없는 변경도 그 이유를 review summary에 기록한다.
3. 실제 source와 테스트 정의를 검토한 뒤 review marker를 `python3 scripts/check_architecture.py --stamp --summary "..."`로 갱신한다. staged 범위만 검토할 때는 `--stamp --staged --summary "..."`를 사용한다.
4. 완료/commit 전 `python3 scripts/check_architecture.py` 또는 staged 제출 범위의 `python3 scripts/check_architecture.py --staged`를 실행한다. source가 문서보다 우선하며 stale이면 먼저 문서를 고친다.

<!-- architecture-review:start -->
```json
{
  "schema_version": 1,
  "source_sha256": "1acfc2bfbdd9e7e1af1bd9818e66a78b1875daf31a46fe9637af0af835364387",
  "reviewed_at": "2026-09-23T21:34:03Z",
  "summary": "CI review round 1: drover/services/cert_rotation.py wraps the node-Ready keepalive loop in try/finally and cancels the wait_node_ready task when the SSE consumer disconnects (runtime change: no orphan K8s polling), and documents the unverified Type=notify settle assumption that replaced the old implicit 10s gap; tests/test_k3s_cert_rotation.py adds the disconnect-cancel regression; tests/test_k3s_certs.py patches socket.create_connection so the TLS probe failure test no longer dials 192.0.2.1:6443; tests/test_ci_workflows.py pins the full ci.yml and docker-build.yml trigger sets; docker-build.yml comments the fail-safe push expression. AGENTS.md CI section adds per-event baselines (CI PR 106/153s n=10, CI push 98/144s n=30, Docker push test span 99/123s n=32), the post-change measurement method, the accepted post-merge Buildx gap, and relabels projections; ARCHITECTURE.md adds the Certificate rotation runtime flow with the known admin rotate-certs/certificate-expiry defects (unfixed), the Buildx gap, and 638 passed/3 skipped. No API, schema or deploy contract change."
}
```
<!-- architecture-review:end -->

## Glossary

- **BFF**: Afterglow가 사용자 UI와 Drover 사이에서 인증·프로젝트 context를 중계하는 Backend-for-Frontend 경계.
- **Durable job**: MariaDB에 queue row로 기록되어 Worker 재시작 뒤에도 lease/retry 가능한 작업.
- **Operation**: API 요청의 내구성 상태와 순서 event를 묶는 `DroverOperation` 기록.
- **Lease/attempt fence**: Worker가 job을 독점 실행하고 stale worker의 늦은 완료를 무시하게 하는 claim/attempt 계약.
- **Callback token**: cloud-init VM이 callback에 제시하는 Redis TTL 일회성 토큰.
- **Inventory**: `ManagedOpenStackResource`가 보유한 OpenStack service/resource ID와 state metadata.
- **Stampede**: K3s pending pod와 nodegroup 정책을 바탕으로 자동 scale-out/in하는 Drover autoscaler.
- **Admission**: 현재 GPU quota authority인 Afterglow가 특정 GPU node provisioning을 허용·차단하는 내부 결정.
- **Catalog alias**: Keystone에 `name: drover`, `type: drover`로 등록된 서비스를 `drover-sdk`가 `container-infra` alias로도 노출하는 호환 명칭; 별도 Drover queue가 아니다.
- **Reconciliation**: DB에 기록된 자원과 OpenStack 실제 상태를 비교해 missing, mismatch, orphan drift를 기록하는 주기 작업.
