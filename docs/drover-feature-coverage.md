# Drover 레거시 기능 커버리지 및 오픈스택 통합 사양서

본 문서는 레거시 오퍼레이션 및 이전 직접 오픈스택 API 연동 방식 대비 **Drover 서비스의 기능 커버리지, 교체 매핑, 오픈스택 서비스 통합 구조, 보안 아키텍처 및 의도적 제약사항**을 상세히 기술합니다.

**검증 경계:** 최신 완료 발행·canonical controller3대 배포는 `v0.4.13`; full service1413/3skip, 실제 local API/Worker amd64/arm64 실행을 확인했습니다. 새 disposable cluster에서 `stampede_enabled` 생략 시 cluster `true`·default 그룹 `false`, GPU flavor nodegroup 생략 시 `true`를 확인했고 `accelerator: RTX3060LHR` nodeSelector와 `nvidia.com/gpu: 1` Pod로 GPU0→1 증설·자동 `accelerator`/`afterglow.io/gpu-type` label·allocatable1·Running nvidia-smi·Operation SUCCEEDED를 확인한 뒤 cluster/VM/volume 정리와 임시 flavor access 제거를 확인했습니다([live acceptance](release-0.4.0.md#live-default-on-and-accelerator-acceptance)). taint 없는 GPU worker에 K3s traefik(emptyDir)이 배치되어 그 실행의 자연 축소는 `protected_pod`로 차단됐습니다. 0.4.12까지의 native fixture에서는 Cinder 재부착·Manila RWX·Service LB·HTTP/TLS Ingress·bearer-only RBAC·KMS Encrypt/Decrypt/암호화 저장, CPU0→2→0·baseline UID 보존, 실제 RTX3060 GPU0→1/Ready/allocatable1/Running nvidia-smi/SUCCEEDED와 원래 durable events500→200/null 보존/cursor replay를 검증했습니다. GPU 자연축소는 VM404까지 진행했지만 boot volume이 Cinder `error_deleting`으로 남아 Operation FAILED이며, owner 결정에 따라 force/reset 없이 volume·group tracking·ERROR fixture를 보존했습니다([exact receipt](release-0.4.0.md#native-gpu-natural-scale-in-and-final-cleanup-boundary)). 전체 guest credential rotation/재인가 matrix를 재실행했다는 주장은 하지 않습니다.

**Current integration boundary:** 0.4.15 is published and deployed to DMS Lab (2026-10-14): source `314c7a6a`, pin commit `977a1cad`, exact-SHA CI and tag `v0.4.15` Release/Docker workflows succeeded. The [production receipt](release-0.4.0.md#tagged-0415-publication-and-dms-lab-production-rollout--2026-10-14) records whole-cohort stop, cold restore 16 tables / 75,225 rows, no migration or generated-config change, six pinned replicas with 0 restarts, healthy APIs and readiness 200 on three listeners plus public. The 0.4.13 paragraph and ownership results below remain historical snapshots; the latest guest GPU/CSI workload acceptance remains [0.4.14](release-0.4.0.md#live-0414-gpu-csi-and-natural-scale-in-acceptance), not a new 0.4.15 guest run. All-old-worker drain and no mixed SQL consumers remain required.


## Native worker replica ownership

**Evidence: test-passed locally, not live-verified (parent receipt, 2026-10-14).** The parent executed the ownership extension on a frozen public clone synced byte-for-byte from the preserved original working tree before latest-dev integration, including concurrent WIP (262 git-visible files; manifest SHA-256 `49e0e62eb2d991a7940d244d8523acd6a9668aeedde19cef76a06d32f0c8928c`). Logs, JUnit XML and execution JSON are under `/Users/pieroot/.local/share/afterglow-proofs/afterglow-process-ownership-n7xrnua4/`. These are this worker model's observed local results, not inherited historical release evidence:

- Focused ownership/job lane, the 12 files listed in the regression recipe below: **374 passed**.
- Full service suite `pytest -p no:cacheprovider tests --ignore=tests/integration`: **1323 passed, 1 skipped**.
- Three-replica native `scripts/worker_ownership_smoke.py`: **exit 0**, initial owner counts **[11, 11, 11]**; `sticky_assignment`, `independent_control_heartbeat`, `hard_crash_takeover`, `read_only_retry`, `redis_outage_fail_closed`, `revocation_true_thread_drain`, `sql_heartbeat_during_sdk` all **true**; `sql_successful_jobs` **9**, duplicate healthy target polls **0**, remaining native assignments **4**. This exercised three real replica processes, native `WorkerRuntime`, real SQL claims/heartbeats/non-replay guards and a synthetic loopback HTTP provider. Owned fixtures were Docker Redis 7 on `127.0.0.1:56497` and tmpfs Docker MariaDB 11.4 on `127.0.0.1:56498`; Redis outage used `docker pause/unpause` instead of SIGSTOP/SIGCONT. The empty owned database `drover_ownership_smoke_n7xrnua4e` used ORM schema, not migration-ledger qualification.
- Prior smoke attempts on earlier sources: attempt 1 failed from harness provider listen-backlog resets (fixed with provider `request_queue_size=256`); attempt 2 failed when InnoDB 1213 deadlock killed the harness consumer loop (fixed by retrying like production). Attempts 3 and 4 passed; the final run on lint-fixed source passed.
- Defects found and fixed during verification: Lua empty-map encoding in `worker_ownership.py`; control-thread Sentinel `RedisBackend`; lease/heartbeat retention through drain; `operation_id` propagation; reconciliation SDK calls through `run_sync`; attempt checkpoint before dispatch; transaction-bound status writes; atomic callback-timeout rollback; lease identity comparison.
- `ruff check .`: only **2 pre-existing scoped-access findings** remain, outside this extension and already recorded in task 4.4 (`drover/api/configmaps.py` SIM102, `drover/services/cloud_shell.py` UP017). All **15 ownership-introduced findings** were fixed: import order, unused import, `collections.abc`, builtin `TimeoutError`, PEP 695 `run_sync[T]`, combined operation-status condition and `ExitStack`-owned smoke log handles. CI does not run `ruff format`; HEAD has 60 unformatted files, and no formatting was applied.
- Architecture stamp/check is recorded in the parent receipt from an isolated clone; the canonical checkout's shared review marker/digest/UTC block was left untouched.

- `drover/worker.py:WorkerRuntime` keeps Redis control on an independent OS thread/event loop. Its dedicated client is constructed through an independent native `RedisBackend` instance, preserving configured Sentinel master selection or direct Redis URL without sharing a domain-loop connection pool. SQL jobs and domain services remain on the original DB event loop. A blocked cluster SDK call cannot prevent Redis server-time membership renewal or ownership scheduling.
- Only the discovery leader reads the shared SQL cluster registry. Its credential-free target ID publication feeds one global reconciliation quota scheduler and separately assigned cluster health/Stampede targets. Recovery and delegation sweep roles are distributed among live eligible processes, rather than executed on every replica.
- `drover/services/worker_ownership.py` keeps healthy current owners stable, assigns unowned work across live members, validates unique lease tokens, rejects stale release, and takes over crashed owners after membership expiry. Redis outage and local expiry fail closed. Redis membership is an execution admission fence, not a replacement for tenant authority.
- Periodic work checkpoints before later state changes and scheduling. Target services reload current eligibility and retain owner/capability checks. POSIX stop immediately revokes local admission, cancels and drains every owned coroutine and shielded SDK thread before releasing tokens and closing resources. Already-sent network requests cannot be undone by revocation.
- Durable jobs are **still competing SQL consumers on every replica**, not Redis leader work. SQL row/attempt/cluster fences and heartbeat remain authoritative. A possible physical mutation is durably marked before it is sent; accepted/unknown mutation outcomes, cancellation and stale reclaim do not blindly replay cloud actions. Inventory/operator inspection is required for ambiguous work. Errors before mutation and safe observation retain normal bounded retry; caller trusts, source/owner and operation authority validation have no fallback identity.
- Cancellation settlement keeps a SQL job `running` until its real dispatch/SDK threads have drained, so safe pre-send requeue cannot overlap a late thread. Claimed operation metadata is passed explicitly to every job dispatch kind. Reconciliation recorded-resource and orphan observations use shielded SDK offloads, keeping the domain SQL heartbeat runnable and draining reads on cancellation.
- Dispatch entry revalidates the exact current SQL attempt before invoking a native domain handler. Cluster status writes—including zero-agent activation with no SDK call—lock and refresh that attempt, cluster source and operation state in the **same SQL write transaction**, keeping the claim fence held through commit. A paused former attempt cannot overwrite a takeover's failed-operation/cluster status with `ACTIVE`.
- SQL heartbeats keep the cancelled attempt's cluster exclusion live throughout actual SDK drain and stop only afterward. Callback-timeout `FAILED`/cluster `ERROR`, sequenced event and original-delegation rollback admission commit atomically in one transaction; recovery revocation rolls them all back rather than leaving an unqueued failed operation.

Locally passed native coverage includes server-time Lua exclusivity/three-member distribution/sticky ownership, leader publication, expiry takeover, token-only release, Redis outage/revocation, blocked-domain heartbeat/scheduling, true drain, target eligibility/publication fences, and SQL accepted/unknown non-replay. Consumer tests use native Drover services and synthetic boundaries, not production network credentials.

### Local smoke reproduction recipe and observed limits

Prerequisites: installed `redis-server`, POSIX signals, and an existing **disposable loopback MariaDB** service with an empty database and a user able to create tables in that database. The database name must match `drover_ownership_smoke_[A-Za-z0-9_]+`; the hook refuses non-loopback URLs or a nonempty database. Do not use production/deployed tenant data. Create an empty database in the local MariaDB console, for example `CREATE DATABASE drover_ownership_smoke_local;`, using an existing local test account. Put its actual `mysql+aiomysql://` URL into `DROVER_OWNERSHIP_SMOKE_DATABASE_URL` through the shell's hidden prompt, not argv or committed files:

```sh
cd /Users/pieroot/code/drover
read -r -s DROVER_OWNERSHIP_SMOKE_DATABASE_URL
export DROVER_OWNERSHIP_SMOKE_DATABASE_URL
uv run --frozen --extra service --extra dev python scripts/worker_ownership_smoke.py
unset DROVER_OWNERSHIP_SMOKE_DATABASE_URL
```

The standalone hook starts its own loopback Redis and synthetic HTTP provider, then three subprocesses using native `WorkerRuntime`, real SQL claims/heartbeats, native authority binding and physical-send guards. It asserts divided/sticky roles and targets, no healthy-owner duplicate polls, heartbeat under a blocked domain SDK call, cancellation/true thread drain, SIGKILL takeover, Redis outage fencing/rejoin, safe read retry, and single provider acceptance after lost response or accepted-then-killed work. Structured JSON is printed only after assertions complete; the parent's successful Docker-fixture run and its outage-control substitution are recorded above. The standalone hook stops only processes it created and preserves SQL rows for inspection. Rerun with a new empty suffixed database. ORM metadata setup is not migration-ledger verification; synthetic domain dispatch does not test real Keystone policies, provider SDK catalogs, guest rollout or live cloud behavior.

Regression reproduction recipe (the observed full service lane excludes integration tests; results are recorded above, not pending execution):

```sh
uv run --frozen --extra service --extra dev pytest -q tests/test_worker_ownership.py tests/test_worker_runtime.py tests/test_worker_targets.py tests/test_worker_job_drain_boundaries.py tests/test_staged_credential_drain.py tests/test_job_execution_safety.py tests/test_jobs.py tests/test_operations_jobs.py tests/test_execution_authority.py tests/test_reconciliation.py tests/test_k3s_health.py tests/test_k3s_stampede.py
uv run --frozen --extra service --extra dev pytest -p no:cacheprovider tests --ignore=tests/integration
uv run --frozen --extra service --extra dev ruff check .
openspec validate isolate-service-execution-credentials --strict
```
Architecture stamping/checking was handled separately by the parent in an isolated clone and is recorded in the parent receipt; do not restamp or edit the shared review marker/digest/UTC block for this evidence update.

**Not exercised/qualified:** production Redis/Sentinel failover or async-replication lease/journal loss, promoted-node TIME skew, mixed-version rolling upgrade, live provider/cloud (including Keystone/OpenStack/K3s/Nova/Octavia/Glance/KVM), real guest rollout or deployment. Only disposable owned fixtures and synthetic loopback/in-process providers were used: no private configuration or credentials, paid inference, image publication, commit, stage or push. These local passes are not production or live verification.


Stop/drain **all old SQL worker binaries before cutover**. The worker-owned ambiguity marker uses existing payload storage, which old binaries ignore; mixed old/new SQL consumers are not qualified. Stable owners do not rebalance on every late join: the initial membership cohort settles before assignment, and later additions receive unowned/new work without stealing healthy targets. Same service Redis/DB is required across every eligible replica; no controller host list is used.



---

## 1. 레거시 기능 그룹별 교체 및 SDK 매핑 서열 (Feature Replacement Mapping)

Drover는 Magnum REST wire API의 드롭인 대체가 아니라, Afterglow가 직접 URL로 호출하던 K3s 관리 기능을 Keystone catalog 기반 **Drover Native v1 API** 및 Python SDK (`drover-sdk`)로 전환하는 서비스입니다. Catalog service name/type은 모두 `drover`이며, SDK에서 `container-infra`는 alias입니다. 직접 URL에서 catalog로 옮기는 단계와 완료 여부는 [Afterglow 통합 rollout 계획](afterglow-service-integration.md) 및 현재 source를 별도로 확인해야 합니다.

| 기능 그룹 (Functional Group) | 레거시 / 이전 방식 | Drover 네이티브 엔드포인트 교체 사양 | SDK (`drover-sdk`) 매핑 메서드 |
| :--- | :--- | :--- | :--- |
| **클러스터 라이프사이클** | Afterglow의 직접 Drover URL 호출 | `POST /v1/clusters/async`<br>`GET /v1/clusters`<br>`DELETE /v1/clusters/{cluster_id}` | `conn.drover.create_cluster()`<br>`conn.drover.clusters()`<br>`conn.drover.delete_cluster()` |
| **노드 스케일링** | 직접 REST 스케일 요청 | `PATCH /v1/clusters/{cluster_id}/scale`<br>`PATCH /v1/clusters/{cluster_id}/nodegroups/{nodegroup_id}` | `conn.drover.scale_cluster()`<br>`conn.drover.update_nodegroup()` |
| **클러스터 템플릿** | 직접 REST 템플릿 관리 | `GET /v1/cluster-templates`<br>`POST /v1/cluster-templates` | `conn.drover.cluster_templates()`<br>`conn.drover.create_cluster_template()` |
| **K8s 리소스 직접 제어** | 별도 kubectl 또는 직접 REST 호출 | `GET/POST/PUT/DELETE /v1/clusters/{cluster_id}/...` | `conn.drover.configmaps()`<br>`conn.drover.secrets()`<br>`conn.drover.pods()` |
| **인증서 및 CA 관리** | 직접 REST 인증서 호출 | `GET /v1/clusters/{cluster_id}/ca-certificate`<br>`GET /v1/clusters/{cluster_id}/certificate-expiry`<br>`POST /v1/clusters/{cluster_id}/rotate-certs` | `conn.drover.ca_certificate()`<br>`conn.drover.certificate_expiry()`<br>`conn.drover.rotate_certs()` |
| **대화형 클라우드 셸** | 노드 직접 SSH 접속 | `POST /v1/clusters/{cluster_id}/shell-ticket`<br>`WebSocket /v1/clusters/{cluster_id}/shell` | `conn.drover.create_shell_ticket()`<br>(웹브라우저/터미널 전용 WebSocket) |
| **오퍼레이션 트레이싱** | 비동기 상태 유실 위험 | `GET /v1/operations/{operation_id}`<br>`GET /v1/operations/{operation_id}/events` | `conn.drover.get_operation()`<br>`conn.drover.operation_events()` |
| **클러스터 재인가 / owner 회수** | tenant manager authority 교체 | `GET/POST /v1/clusters/{id}/authorization`<br>`POST /v1/clusters/{id}/authorization/retire` | `cluster_authorization()`<br>`reauthorize_cluster()`<br>`retire_cluster_credentials()` |

---

## 2. 오픈스택 서비스별 통합 사양 (OpenStack Service Integrations)

Drover는 클러스터 생성 및 관리를 위해 OpenStack 핵심 서비스들과 직접 통합되어 동적으로 자원을 프로비저닝하고 태그 기반 추적을 수행합니다.

```mermaid
graph TD
    DroverWorker[Drover Worker Engine] -->|Nova SDK| Nova[Nova Compute]
    DroverWorker -->|Neutron SDK| Neutron[Neutron Network]
    DroverWorker -->|Cinder SDK| Cinder[Cinder Block Storage]
    DroverWorker -->|Octavia SDK| Octavia[Octavia Load Balancer]
    DroverWorker -->|Keystone SDK| Keystone[Keystone Identity]
    DroverWorker -->|Barbican SDK| Barbican[Barbican KMS]
    DroverWorker -->|Manila SDK| Manila[Manila Shared Filesystem]
```

### 2.1 Nova (Compute)
- K3s Master (Server) 및 Worker (Agent) 가상머신 프로비저닝.
- 노드 Flavor 검증 및 자원 정책 연동.
- VM 인터페이스 동적 연결/해제 (`POST/DELETE /v1/clusters/{cluster_id}/nodes/{vm_id}/interfaces`).
- 생성된 Nova 서버에는 지원되는 OpenStack 태그/메타데이터로 `drover.managed=true`, `drover.cluster_id=<cluster_id>`, `drover.operation_id=<operation_id>`가 기록되며, 지원하지 않는 자원은 관리 인벤토리의 ID로 추적합니다.

### 2.2 Neutron (Networking)
- 클러스터 전용 Security Group 및 보안 규칙 동적 생성/삭제.
- K3s API Server용 Floating IP (FIP) 및 Port 바인딩.
- `allowed_cidrs` 지정을 통한 K3s API 접근 IP 제어.
- 클러스터 생성의 `network_id`는 외부 Neutron 네트워크에 한정됩니다. 명시 값은 `k3s.default_network`와 같은 검증을 거치고, 생략 시 저장된 필수 정책을 조회·재검증합니다. 내부/공유 전용 네트워크나 누락·만료된 기본 정책은 DB 기록 전에 거부하며 Nova의 자동 네트워크 할당으로 넘어가지 않습니다. 유효한 ID는 cluster/job `network_id` 및 `resource_policy_snapshot["k3s.default_network"]`에 함께 기록되어 초기 서버, HA joiner, agent 및 nodegroup scale에 전달됩니다. [API admission](../drover/api/clusters.py), [카탈로그/검증](../drover/services/resource_policies.py), [policy store](../drover/services/resource_policy_store.py), [직접 VM 경로](../drover/services/provisioner.py), [nodegroup 경로](../drover/services/autoscale.py).
- provider NIC의 `node-ip`·`flannel-iface`·server `advertise-address`는 생성 네트워크 metadata의 MAC을 기준으로 고정되며, 저장된 pin을 재사용하므로 내부 NIC 추가 후 재시작해도 바뀌지 않습니다. 추가 NIC는 Ubuntu에서 DHCP route/DNS 및 IPv6 RA를, FCOS에서 자동 route/DNS 및 IPv6를 차단합니다. Pod 응답용 priority 29999 `to 10.42.0.0/16 table main` 규칙은 K3s `ExecStartPre`가 보장하고 실패 시 시작하지 않으며 watcher가 일시적 손실을 복구합니다. Ubuntu는 해당 규칙만 `protocol kernel`로 지정해 networkd foreign-rule 정리에서 제외합니다(guest에서 watcher 중지 후 provider reconfigure 시 unmarked 삭제·kernel-marked 잔존을 확인); 다른 외부 규칙 수거 정책은 유지합니다. FCOS는 pin된 provider NIC NetworkManager 연결의 영속 `ipv4.routing-rules`(table 254)에 규칙을 등록·reapply합니다. 새 userdata를 통한 전체 cluster 재부팅, FCOS 실제 guest 재구성 및 Pod→API 연결은 아직 확인하지 않았습니다. [pin 및 FCOS 생성](../drover/services/cloudinit.py), [Ubuntu server](../drover/templates/k3s_server.yaml.j2), [Ubuntu agent](../drover/templates/k3s_agent.yaml.j2). 이 userdata 변경은 신규 노드에 적용하며 기존 노드를 자동 재작성하지 않습니다.
- NIC hotplug handler는 모든 udev rule 처리 후 `RUN`에서 최종 `$name`을 사용해 `systemctl --no-block`으로 실행을 요청합니다. rename 전 커널 이름(`eth0`)에 설정을 쓰지 않으며, `ens8` 등 실제 보조 NIC에 route/DNS 차단이 적용됩니다. 정적인 udev rule 검사만으로 완료하지 않고 실제 hotplug·재부팅에서 확인합니다.

### 2.3 Cinder (Block Storage)
- K3s Server VM 부트 볼륨 (`boot_volume_size_gb`, 기본 30GB) 생성 및 가상머신 연결.
- **Cinder CSI Plugin**: 클러스터 내 Kubernetes 볼륨 동적 프로비저닝 지원 (`drover_cinder_csi_enabled: true`).
- **실제 guest 검증:** [exact0.4.10 fixture receipt](release-0.4.0.md#native-all-plugin-acceptance-on-the-0410-fixture)는 dynamic CSI volume의 worker 간 detach/remount 후64-byte marker/hash 연속성을 확인합니다. PVC/PV/attachment 및 native backend까지 정리했습니다.

### 2.4 Octavia (Load Balancing)
- **K3s HA API Load Balancer**: Multi-master (`master_count=3`) 설정 시 K3s Control Plane API (Port 6443) HA 로드밸런서, Listener, Pool 및 Member 생성.
- **OCCM (OpenStack Cloud Controller Manager)**: Kubernetes Ingress / Service Type LoadBalancer 수용 및 Octavia 로드밸런서 자동 동기화 (`drover_occm_enabled: true`).
- OCCM이 활성화되면 server 설치 인자에 `--disable=servicelb`를 넣어 K3s 내장 ServiceLB와 같은 Service status를 경쟁적으로 갱신하지 않습니다. agent 설치 인자에도 `--kubelet-arg=cloud-provider=external`을 넣어 OCCM이 provider ID와 노드 주소를 초기화합니다.
- 생성 provider 네트워크는 OCCM의 `internal-network-name`입니다. 같은 이름이 `k3s.occm_public_network`에도 지정돼 있으면 `public-network-name`을 생략합니다. OCCM의 public 분류는 기존 InternalIP를 삭제하므로 두 역할을 겹치게 렌더링하지 않습니다. 서로 다른 public network 정책과 floating-network 선택은 보존합니다.
- `[drover] occm_internal_lb` / Kolla `drover_occm_internal_lb`(기본 false)은 native `[LoadBalancer] internal-lb`를 설정합니다. provider-only site에서 true를 사용하면 Service 기본 VIP에 floating IP/router 연결을 요구하지 않습니다. 명시적인 native Service annotation은 controller의 기존 precedence를 따릅니다. [OCCM renderer](../drover/services/plugins/occm.py), [native v1.34.1 옵션](https://github.com/kubernetes/cloud-provider-openstack/blob/v1.34.1/pkg/openstack/openstack.go).
- OCCM의 `--cluster-name`은 불변 cluster ID입니다. 클러스터 삭제는 VM 삭제 뒤 `kube_service_<cluster_id>_` 이름과 OCCM 설명(`... from cluster <cluster_id>`)이 모두 일치하는 Service LB를 cascade 삭제하고, OCCM이 만든 설명의 VIP floating IP는 OCCM도 삭제했을 경우에만 함께 삭제합니다. OCCM은 `loadbalancer.openstack.org/keep-floatingip: "true"` Service의 FIP를 남기므로, 삭제는 노드·VM을 건드리기 전에 모든 namespace의 Service annotation을 읽고 LB를 쓰는 Service(생성자 또는 `load-balancer-id` 공유자) 중 하나라도 keep을 요청하면 FIP를 남깁니다. Kubernetes를 읽지 못했거나 LB의 Service가 없으면 의사를 알 수 없으므로 FIP를 남기고 LB 삭제로 연결만 해제합니다. 사용자 지정 FIP, 다른 cluster·사용자 LB와 octavia-ingress LB는 대상이 아닙니다. pending LB는 Octavia가 ACTIVE/ERROR로 정리한 뒤 삭제합니다.
- HA joiner(server 2·3)는 OCCM/CSI의 `cloud.conf`를 재렌더링하지 않고 server 1의 `kube-system/cloud-config` Secret을 공유합니다. Barbican KMS host 파일은 저장된 active guest credential로 각 joiner에 별도 렌더링합니다. control/guest secret은 이제 DB에 암호화 저장됩니다.
- **실제 LB/TLS 검증:** 같은 native fixture에서 ONLINE Service LB를 worker-pinned HTTP로 호출하고, native HTTP 및 TLS-first Ingress의 정상 Host/path200·음성503, verified HTTPS/정확한 peer certificate와 Barbican PKCS12 cert/key equivalence를 확인했습니다. 기존 HTTP listener→TLS443 변경을 지원한다고 과장하지 않습니다([receipt](release-0.4.0.md#native-all-plugin-acceptance-on-the-0410-fixture)).

### 2.5 Keystone (Identity & Access)
- 사용자 요청 시 호출자의 `X-Auth-Token`을 검증하고 프로젝트 스코프를 확인합니다. 프로젝트 헤더가 없으면 제출된 토큰 범위를 보존합니다.
- 서비스 자격으로 catalog의 `identity` internal endpoint를 해석하여 토큰 introspection, 명시적 rescope 및 관리자 역할 조회를 보냅니다. internal endpoint가 없거나 연결할 수 없으면 external/public endpoint로 fallback하지 않고 fail closed 합니다.
- VM 내부 플러그인은 별도 경계입니다. `provisioner._guest_plugin_settings`가 admitted operation session의 region별 `identity` public endpoint를 guest-only Settings copy에 적용합니다. backend internal 설정과 resource snapshot은 보존하며 플러그인 없는 생성은 catalog 조회를 하지 않습니다. 필요한 endpoint 누락/잘못된 URL은 자원 생성 전에 실패합니다.
- Keystone static webhook은 catalog prefix를 유지한 단일 terminal `/v3`와 TokenReview `v1beta1`을 사용합니다. apiserver→loopback webhook의 certificate pinning은 유지하며 Kubernetes authorization은 RBAC입니다. `os_cacert`는 guest file로 복사합니다. native v1.34.1은 insecure HTTPS flag가 없으므로 HTTPS + `os_insecure=true`는 명시적으로 실패합니다. [Keystone plugin](../drover/services/plugins/keystone_auth.py).
- 서비스 카탈로그 자동 등록 (`deploy/kolla/ansible/roles/drover/tasks/preconditions_keystone.yml`의 `name: drover`, `type: drover`).
- durable mutation은 requester-owned trust(`impersonation=True`)로만 실행합니다. 현재 held role IDs에서 required 모두와 보유한 optional만 위임하며 `admin`/`manager`는 위임하지 않습니다. caller token/password는 persist하지 않습니다. connection마다 enabled principal/project, 현재 operation capability/held roles와 token user/project/trustee/expiry를 검증합니다. token에는 admitted role IDs가 모두 있어야 하고 implied-role 확장은 허용하되 `admin`/`manager` token roles는 거부합니다. terminal authorization은 retry/fallback 없이 실패하고 directory/Keystone 통신 장애는 attempt-fenced retry입니다.
- callback 후속 HA/agent와 같은 create operation rollback은 create delegation을 재사용합니다. idle이면 local `released`로 전환하고 job 종료 후와 300초 sweep에서 released/revoked trust를 자신의 impersonating trust-scoped password token으로 DELETE합니다(project selector 없음). 성공/404는 `deleted`, Unauthorized/Forbidden은 released/revoked와 `trust inert until expiry`, 통신 장애는 다음 sweep 재시도입니다. expiry 뒤 Keystone GET 404를 확인하면 `expired`입니다. finite TTL은 DELETE 불가/장애 시 fallback bound이지 정상 완료 trust를 expiry까지 남기는 기본 경로가 아닙니다. 미커밋 admission은 caller token으로 정리합니다.
- user-owned restricted(`unrestricted=False`) control과 별도 guest app credential은 caller token으로 발급하고 암호화 저장합니다. control은 Drover 전용, guest는 OCCM/Cinder CSI/Manila/Octavia Ingress/Barbican KMS 설정용이며 service password/control credential을 guest에 넣지 않습니다. 생성 필요 여부는 `cluster_authority.GUEST_PLUGIN_NAMES` 및 활성 plugin 설정을 따릅니다.
- [Keystone master trusts API](https://github.com/openstack/keystone/blob/master/keystone/api/trusts.py)의 `_check_delegated_token`은 ordinary trust-scoped token을 막지 않습니다. `identity:delete_trust`의 admin/trustor policy와 impersonation을 사용하는 trustee 정리는 app-credential/OAuth/EC2 trust 관리 차단과 별개입니다. [Master users API](https://github.com/openstack/keystone/blob/master/keystone/api/users.py)의 delegated-token app-credential 관리 차단 때문에 owner의 non-delegated token으로 원격 회수하고 다른-owner secret은 지운 뒤 backlog로 보고합니다. 실제0.4.10/11 caller admission·guest 발급·owner credential404 및 bearer-only valid403→scoped200→revoke403/invalid401은 [receipt](release-0.4.0.md) 범위입니다. 전체 Keystone policy/회전/재인가 matrix가 검증됐다는 의미는 아닙니다.

### 2.6 Barbican & Manila (선택적 커스텀 연동)
- **Barbican KMS Plugin**: K3s Secret 암호화를 위한 KMS 바인딩 지원.
- **Manila CSI Plugin**: K3s Pod 공유 파일시스템(NFS/CephFS) 볼륨 프로비저닝 연동 (`drover_manila_csi_enabled`).
- Manila StorageClass share type은 `[drover] manila_csi_share_type` / Kolla `drover_manila_csi_share_type`으로 설정합니다(기본 `default`, 실제 cloud에 존재하는 type 필요). controller SA에만 namespaced `manila-cloud-secret` 이름 제한 `get`을 허용하고 provisioner·node-stage·node-publish Secret refs를 제공합니다. Manila/NFS controller socket은 pod-local `emptyDir`; node hostPath, forwarding socket/mount와 Bidirectional propagation은 유지합니다. [Manila renderer](../drover/services/plugins/manila_csi.py), [manifests](../drover/templates/k3s_plugins/manila_csi/manifests.yaml.j2), [NFS manifests](../drover/templates/k3s_plugins/manila_csi/nfs_csi.yaml.j2).
- Barbican KMS CA는 guest `/etc/kubernetes/barbican-ca.crt`로 복사하고 `tls-insecure`를 명시합니다. KMS v2가 첫 provider이므로 새 Secret write는 KMS를 요구하며, 두 번째 identity provider는 기존 plaintext read만 지원합니다. plaintext write fallback은 없습니다. [KMS plugin](../drover/services/plugins/barbican_kms.py), [encryption configuration](../drover/templates/k3s_plugins/barbican_kms/encryption_config.yaml.j2).
- `barbican.ensure_project_kek(conn)`는 admitted operation connection으로 project 공유 `afterglow-k8s-kek`를 조회하거나 order를 발급하며 tenant manager를 만들지 않습니다. guest KMS는 guest credential을 사용합니다. 실제 DMSLab legacy policy에 맞춘 `creator`는 caller가 이미 보유한 optional delegation일 뿐 새 role grant가 아닙니다. 실제 gRPC Encrypt/Decrypt와 Secret201/200 roundtrip, exact live read-only Kine record의 KMS-v2 prefix/평문·base64 부재 및 동시 cross-node Manila RWX 양방향 쓰기를 확인했습니다([exact guest receipt](release-0.4.0.md#native-all-plugin-acceptance-on-the-0410-fixture)). `member`만으로 다른 cloud policy가 충분하다고 일반화하거나 admin/manager를 위임하지 않습니다.
- KEK 및 Barbican raw REST는 connection의 SDK `key_manager` versioned endpoint를 사용해 configured interface·region·catalog prefix를 보존합니다. Public `order_ref`의 ID도 같은 endpoint에서 조회합니다. Native SDK는 implicit public 조회에서 `ConnectTimeout`, configured internal 조회에서 `200`을 반환했고 corrected-source smoke는 AES-256 KEK 발급·ACTIVE·재사용을 확인했습니다. [source](../drover/services/barbican.py), [real SDK loopback regression](../tests/test_native_barbican_endpoint.py), [0.4.10 경계](release-0.4.0.md#0410-patch--configured-barbican-catalog-scope).

---

## 3. 배포 아키텍처 및 스키마 준비성 (Deployment & Schema Readiness)

Drover는 **Kolla-Ansible** 컨테이너 배포 환경을 표준으로 지원합니다.

```
drover wheel
└── share/kolla-ansible/ansible/roles/drover/
    ├── defaults/main.yml     # Kolla 기본 포트, 이미지, 시크릿 경로 설정
    ├── tasks/
    │   ├── bootstrap_service.yml # MariaDB 데이터베이스 및 계정 생성과 migration 실행
    │   ├── config.yml            # drover.conf/policy.yaml 렌더링
    │   ├── preconditions_keystone.yml # Keystone service catalog 등록
    │   └── deploy.yml            # API/Worker container lifecycle
    └── templates/
        ├── drover.conf.j2        # Drover 메인 구성 파일 템플릿
        ├── drover-api.json.j2    # Kolla config_files 템플릿
        ├── drover-worker.json.j2 # Worker 컨테이너 템플릿
        └── drover-migrate.json.j2# Pre-start Migration 컨테이너 템플릿
```

소스 role은 root wheel의 `deploy/kolla/ansible/roles/drover` shared data로 설치하며 서비스에는 `drover[service]`가 필요합니다. 발행·DMS Lab 배포한 package/lock/registry tag는0.4.15/v0.4.15이고 SDK0.2.21·ledger001–004는 유지합니다. source-build는 tag가 아닌 full SHA `314c7a6ab743e7ee766ade41e11ddcb0040e3797`을 검증하며 pin commit은 `977a1cad411cc0eafb5a92090bdced272de0611a`입니다. exact-SHA CI와 tag Release/Docker가 성공했고 installed role20개 파일은 tag와 byte-identical입니다. [0.4.15 발행·배포 receipt](release-0.4.0.md#tagged-0415-publication-and-dms-lab-production-rollout--2026-10-14)를 역사적 [0.4.13 receipt](release-0.4.0.md#tagged-0413-publication-and-canonical-rollout), [0.4.14 receipt](release-0.4.0.md#live-0414-gpu-csi-and-natural-scale-in-acceptance)와 구분하며 published amd64와 local amd64/arm64도 혼동하지 않습니다.

### Schema Readiness 및 Pre-start Migration
- API/Worker보다 `drover-migrate`를 먼저 실행하여 manifest checksum ledger의 001–004를 적용합니다. `004_execution_authority.sql`은 delegations, control/guest credential generations, job delegation FK와 `reauthorize` operation kind를 추가합니다. 기존 manager rows/password는 authority로 전환하지 않습니다. [004 upgrade runbook](../drover/migrations/README.md#execution-authority-upgrade-004)을 따릅니다.
- API 서버는 요청 수신 시 DB 마이그레이션이 완전히 적용되지 않았거나 Redis/Keystone 커넥션이 정상화되지 않은 경우 `/v1/health/ready`에서 HTTP 503을 반환하여 트래픽 입입을 방지합니다.
- 클러스터 목록은 SQL이 설정된 동안 SQL 장애를 빈 목록이나 Redis fallback으로 숨기지 않습니다. Connection breaker가 열리면 이미 채운 목록 cache를 포함해 `GET /v1/clusters`의 기본/`no_cache`/`refresh_cache` 요청은503 `Cluster inventory unavailable`입니다. Cooldown 뒤 SQL이 복구되어야 목록이 돌아오며 schema/권한 오류는 connection cooldown으로 처리하지 않습니다. DB 미설정 legacy store만 Redis 목록을 유지합니다. 이 수정은 schema·설정 변경이 없고 현재 source의 로컬 검증이며 운영 배포 성공을 뜻하지 않습니다.

---

## 4. 보안, 인프라 동기화 및 오토스케일링 아키텍처

### 4.1 보안 아키텍처 (Security Architecture)
* **비밀번호 분리 및 마스킹**: 비밀번호, 암호화 키 등 민감한 데이터는 환경변수나 소스코드에 하드코딩되지 않으며, `/etc/drover/secrets/*` 파일 경로를 통해 읽어옵니다. 관리자 인벤토리 API(`GET /v1/admin/managed-resources`)는 시크릿 패턴을 자동으로 정규식 검사하여 마스킹 처리합니다.
* **cloud-init 보안**: guest 플러그인에는 caller-owned restricted guest Application Credential만 넣고 service password/control credential은 넣지 않습니다. control/guest secret은 DB에서 전용 `cluster_app_credential` AES-GCM domain으로 암호화 저장하며 세대 교체 시 retiring secret을 지웁니다.
* **Callback CIDR 제한**: K3s Server cloud-init이 호출하는 `/v1/callback` 엔드포인트는 `drover_callback_allowed_cidrs` 허용목록에 등록된 IP 범위에서만 접근할 수 있도록 소스 IP 레벨에서 차단 검증합니다.

### 4.2 인프라 동기화 (Reconciliation Loop)
- **Worker Periodic Scan**: `drover-worker` 엔진은 설정된 주기(`drover_reconcile_interval`)마다 오픈스택 실제 자원(`ManagedOpenStackResource`) 상태와 DB의 원하는 클러스터 상태를 교차 검증합니다.
- **Orphan & Drift Detection**: OpenStack 자원이 임의 삭제되었거나 갱신된 경우 `drift_status` 및 `last_reconciled_at` 필드를 업데이트하고 클러스터를 경고/ERROR 상태로 전환합니다.
- **Application Credential 소유자**: active generation credential ID와 control connection의 `current_user_id`로 user-scoped SDK GET을 사용합니다. historic manager-owned inventory는 건너뛰고 retiring backlog로 다룹니다. 404만 missing이고 인증·연결 장애는 missing으로 숨기지 않습니다. owner의 get capability와 token role IDs(implied-role 확장 허용, admin/manager 금지)를 매 connection에 확인합니다. periodic scan은 active control cluster만 대상으로 하고 legacy explicit reconcile은 `reauthorization_required`입니다. credential missing은 non-required drift이고 reconcile job의 authority failure도 그 자체로 cluster를 ERROR로 만들지 않아 ACTIVE-only 재인가를 차단하지 않습니다.

### 4.3 Stampede 오토스케일링 (Autoscaling)
- **실행 권한**: planner와 Stampede job은 active control credential owner의 현재 `drover:clusters:scale`을 재검증합니다. revoked owner/role은 `authority_revoked`, active control 없는 legacy cluster는 `reauthorization_required`로 차단합니다. Stampede enable도 active control이 없으면 409입니다. health Nova lookup은 `drover:clusters:get` capability를 사용하며 실패 시 private-IP 경로를 유지하고 service identity로 우회하지 않습니다.
- **Pod Request 기반 스케일링**: Kubernetes API의 Pod requests(CPU millicores, RAM bytes, NVIDIA GPU slots, extended resources)를 노드의 allocatable 용량과 직접 대조하여 부족분을 산정합니다. PVC 바인딩 지연, 고정 노드 지정, unsupported pod affinity/topology spread/host port 등 비용량적 원인으로 pending된 Pod는 워커 증설을 유발하지 않습니다.
- zero-size 그룹을 명시적 `nodeSelector` 또는 required nodeAffinity로 선택하면, native scheduler의 affinity/selector mismatch도 후보 수요로 처리합니다. 해당 그룹 label/taint 및 CPU/RAM/GPU fit 검사 후에만 예약하며 selector 없는 메시지, PVC·pinned·unsupported constraint는 증설하지 않습니다. [planner](../drover/services/stampede.py), [regressions](../tests/test_k3s_stampede.py).
- **GPU 워커 부트스트랩 및 가용성 검증**: 명시적 GPU flavor를 사용하는 노드그룹은 agent userdata에 `--default-runtime=nvidia` 및 `afterglow.io/gpu=true` 라벨을 주입하고, Ubuntu에서는 NVIDIA container toolkit을 설치하며 FCOS에서는 드라이버/런타임이 사전 탑재된 이미지를 사용합니다. Drover가 클러스터에 `afterglow-nvidia-device-plugin` DaemonSet을 배포하며, K3s 노드가 `Ready` 상태가 되는 것뿐만 아니라 `nvidia.com/gpu` allocatable이 실제 관측될 때까지 대기한 후 내구성 작업을 성공 처리합니다.
- **기본 활성화와 GPU 타입 선택(0.4.13)**: 새 클러스터와 명시적 flavor를 가진 새 nodegroup은 `stampede_enabled`를 생략해도 Stampede가 켜지며 `false`로 끌 수 있습니다. 사용자가 요청한 초기 `default-agent` worker는 자동 축소 대상이 아닙니다. GPU flavor worker(nodegroup·Stampede·초기 agent)는 kubelet 등록 시 `accelerator=<GPU 타입>`, `afterglow.io/gpu-type=<GPU 타입>`, `afterglow.io/gpu=true`를 자동으로 가집니다. 타입은 PCI alias를 Afterglow GPU quota와 같은 규칙(영문 대문자·숫자)으로 정규화한 값이며 예: `RTX-3060-LHR` → `RTX3060LHR`입니다. planner와 bootstrap이 같은 label을 사용하므로 `nodeSelector`/지원되는 required nodeAffinity로 타입을 선택하고 `nvidia.com/gpu`를 요청한 Pending Pod가 zero-size GPU 그룹을 증설해 그 노드에 배치됩니다. 여러 타입·빈 값·CPU flavor는 타입 label을 만들지 않고 다른 타입으로 fallback하지 않습니다. `afterglow.io/gpu*`는 사용자 label로 위조할 수 없고 `accelerator`는 nodegroup의 명시 값을 유지합니다. 기존 Node backfill은 하지 않습니다([사용 예제와 경계](drover-api-v1-reference.md#gpu-타입-기반-스케줄링), [label authority](../drover/services/gpu.py), [planner regressions](../tests/test_k3s_stampede.py), [bootstrap regressions](../tests/test_provisioner_agents.py)).
- **GPU metadata·CSI(0.4.14 source)**: 초기 agent·nodegroup·planner의 빈 목록 specs는 Nova `os-extra_specs`로 명시 조회하고 실패는 CPU로 fallback하지 않습니다. Cinder/Manila/NFS CSI node DaemonSet은 GPU `NoSchedule` taint를 좁게 허용하며 GPU Pod에도 matching toleration이 필요합니다. 자동 taint·기존 guest backfill·PVC 증설 guard·600초 idle/300초 cooldown은 바꾸지 않습니다. API 전용 GPU 그룹/Pod·storage 순서는 [사용 계약](drover-api-v1-reference.md#gpu-타입-기반-스케줄링), actual publication·배포·live 결과는 [0.4.14 receipt](release-0.4.0.md#0414-patch--fail-closed-flavor-metadata-and-gpu-csi-tolerations)를 따릅니다.
- **GPU 실패 분류**: Ready 미달은 GPU flavor에서도 `node_not_ready`이며, Ready 이후 실제 요청한 디바이스 수가 부족한 경우만 `gpu_not_allocatable`입니다. 부분 VM 생성은 `provision_failed`가 우선합니다(`stampede.py:_provision_and_track`).
- **원자적 예약 및 Fencing**: 수동 변경과 오토스케일러의 증설/축소 요청은 MariaDB 트랜잭션 내에서 row lock(`with_for_update`)과 함께 durable job(`stampede_provision`, `nodegroup_reconcile`)을 큐잉합니다. `in_flight_count`로 중복 증설을 차단하고 작업 종료 시 DB의 실제 VM 인벤토리를 기준으로 `node_count`를 동기화합니다.
- **오류 nodegroup 복구**: 실패 VM/부트 볼륨 drift로 cluster가 `ERROR`여도 기존 delete 권한·requester delegation으로 non-default nodegroup DELETE를 durable worker에 admit합니다. 생성/수정은 `ACTIVE`만 허용하고 다른 lifecycle·active mutation·default/in-flight·소유권·drain guard는 유지합니다. API는 상태를 강제로 정상화하거나 tracking을 먼저 지우지 않으며 정상 정리 후 기존 reconciliation이 회복을 판정합니다([source](../drover/services/nodegroup.py), [API contract](drover-api-v1-reference.md#41-노드그룹-api-v1clusterscluster_idnodegroups), [0.4.11 원인/경계](release-0.4.0.md#0411-patch--error-nodegroup-cleanup-admission)).
- **안정화 윈도우 및 안전한 Drain/축소**: 노드그룹의 사용률이 임계값(`drover_stampede_scale_down_threshold`, 기본 0.5) 미만으로 300–600초(`drover_stampede_scale_down_window`) 동안 지속되고 `min_size`를 초과할 때만 축소 후보를 선정합니다. 후보 노드의 모든 Pod가 남은 활성 스케줄 가능 노드로 안전하게 재배치(relocate) 가능한지(컨트롤러 유무, PVC, 로컬 스토리지, PDB 준수) 검증한 후 1개씩 축소합니다. 삭제 직전 live pending Pod, Ready, min-size headroom을 Nova ID 조회로 재검증하며 실제 404만 absent로 봅니다(403/400 등 관측 오류는 정리를 허가하지 않음). cordon은 Node annotation `drover.io/removing-vm-id`를 함께 기록합니다. drain 실패나 DELETE 직전 최종 Nova 조회·소유권 재검증 실패 시에는 이번 시도가 새로 건 cordon만 해제하고 리소스를 보존합니다. 이전 시도에서 남은 같은 VM annotation cordon은 DELETE 이후일 수 있어 해제하지 않습니다. 최종 재검증 뒤에는 DELETE 거부나 정리 실패가 있어도 cordon을 유지하고, 재시도는 같은 VM annotation의 자기 cordon만 재개합니다. Nova VM 소멸이 확인된 후에만 boot volume, Kubernetes Node, tracking row를 순차 정리합니다.
- **미등록 worker 수동 삭제**: 전체 Node 조회가 성공한 경우에만 없는 이름을 cordoned Node로 POST 예약하고 정상 drain·최종 Nova 재검증을 거칩니다. POST 201 외 응답과 통신 실패는 삭제를 중단하며 예약은 rollback으로 uncordon하지 않습니다. 자동 축소의 Ready/min-size guard는 그대로입니다(`autoscale.py:delete_nodegroup_vms`, `kube.py:cordon_node`).
- **실제 CPU 축소:** current native fixture의 CPU0→2→0, 두 Running demand Pods, durable scale-out 및 두 scale-in SUCCEEDED, VM/boot/Node404와 baseline Pod UID/노드/Running 유지가 [receipt](release-0.4.0.md#native-cpu-zero-to-two-to-zero-and-baseline-preservation)에 기록돼 있습니다. 600초 연속 idle·300초 cooldown·global pending/drain/min-size guard 및 desired count는 수동으로 바꾸지 않았습니다.
- **실제 GPU 축소 경계:** GPU 0→1 실행 뒤 deployment만 제거하자 600초 idle 후 scale-down이 큐잉되고 Nova VM/Node가 404가 됐습니다. Nova delete-on-termination이 요청한 boot volume 삭제는 backend host의 같은 시간대 `rbd.Timeout`과 함께 `error_deleting`으로 남았습니다. Drover는 `available`이 아닌 volume을 삭제하지 않고 `boot_volume_delete_unverified`로 실패하며 tracking row를 보존했습니다. 배포된 Cinder 일반 DELETE도 이 상태를 거부하므로 관리자 reset-state/force-delete가 필요하지만, owner 결정에 따라 수행하지 않았습니다. 빈 CPU 그룹·GPU namespace·임시 flavor access는 정확한 ID로 정리했습니다([receipt](release-0.4.0.md#native-gpu-natural-scale-in-and-final-cleanup-boundary)).
- **0.4.14 새 GPU fixture의 실제 성공**: 명시적 GPU NoSchedule 그룹0→1, normalized selector/resources/runtime/toleration Pod의 RTX3060 실행, GPU 노드의 CSI3개 Ready와 native Cinder RWO/Manila NFS RWX I/O를 확인했다. 해당 workload/PVC만 정리한 뒤600초 idle/300초 cooldown·min0/max1을 그대로 유지한 자연1→0 Operation이 SUCCEEDED였고 VM/Node/boot/data volume/share는404였다. 이전 `error_deleting` fixture/group tracking은 손대지 않았다([새 receipt](release-0.4.0.md#live-0414-gpu-csi-and-natural-scale-in-acceptance)).

---

## 5. 의도적인 설계 제약사항 (Intentional Non-Goals)

Drover 서비스의 아키텍처 단순화와 성능 최적화를 위해 아래 기능은 **의도적으로 지원하지 않는 범위(Non-Goals)**로 규정되었습니다:

1. **OpenStack Magnum Wire API 호환성 미지원**
   - Magnum의 기존 `/v1/clusters` REST JSON 포맷을 드롭인 대체(Drop-in replacement)하지 않습니다.
   - 모든 통합 클라이언트는 Drover Native `/v1` API 및 `drover-sdk` 라이브러리를 사용해야 합니다.

2. **OpenStack Placement API 직접 할당 연동 미지원**
   - Placement 서비스의 Resource Class 직접 커스텀 allocation 할당을 사용하지 않습니다.
   - Placement 직접 allocation을 사용하지 않고 Nova Flavor 스케줄링을 따릅니다. `drover_afterglow_admission_url` 설정 시 Stampede planner는 Afterglow admission을 검사하며 `autoscale.provision_nodegroup_vms`는 **각 새 native GPU worker create 전** 다시 admission을 검사합니다(기존 VM 복구는 새 create가 아님). denial/unavailability/transport 실패는 fail closed 입니다. Afterglow는 live Nova usage를 세므로 이 판정은 quota reservation이 아닙니다. URL 미설정 환경은 Nova flavor/native project quota로 직접 생성합니다. Afterglow provisioning intents와 그 설정은 제거됐습니다.

---

## 6. 코드 상 발견된 구현 주의사항 (Implementation Caveats)

1. **내구성 오퍼레이션 (`DroverOperation`) 기록**
   - 클러스터 생성/스케일/삭제 등 라이프사이클 변경 시, 비동기 작업 시작 직후 DB 내 `DroverOperation` 행과 `DroverJob`이 동일 트랜잭션으로 생성됩니다.
2. **SSE 연결 중단과 백그라운드 작업 분리**
   - create 등 내구성 Job 기반 SSE가 끊겨도 Worker 작업은 계속됩니다. 현재 tenant `POST /v1/clusters/{id}/delete-async`는 caller connection으로 직접 실행하며 caller-owned credential도 이 connection으로 회수합니다. durable trust/job 또는 disconnect 이후 실행 보장은 없고 creator identity fallback도 없습니다. durable tenant DELETE/admin delete와 구분합니다. SDK는 operation polling helper를 제공하며 모든 SSE를 자동으로 polling 복구한다고 보장하지 않습니다.
3. **일회성 콜백 토큰 (`GETDEL`)**
   - cloud-init이 수신하는 Redis 콜백 토큰은 30분 유효기간을 가지며, 1회 조회 시 즉시 삭제(`GETDEL`)되므로 재사용이 불가능합니다.


### Resource authority 재인가·삭제·upgrade

- `POST /v1/clusters/{id}/authorization`은 현재 `drover:clusters:reauthorize` operator의 caller token으로 credentials를 발급하고, `ACTIVE`이며 다른 mutation이 없는 cluster에 staged generation + durable job을 기록합니다. 202 응답의 operation ID를 poll해야 합니다. 신규 생성 generation 1은 active로 저장하지만 재인가는 rollout 성공 전에 active로 바꾸지 않습니다.
- `reauthorize`는 migration 004의 DB kind뿐 아니라 `services/operations.py:VALID_OP_KINDS`에서도 허용해야 합니다. 0.4.7은 누락으로 인한 credential 발급 후 enqueue `ValueError`/HTTP 500을 수정하며, 발급 실패·rollback 시 caller-owned credential cleanup과 staged/active 전환은 그대로 유지합니다.
- worker는 reauthorize capability 및 staged credential token을 확인합니다. 기존 guest가 있거나 legacy guest를 대체할 때 `kube-system/cloud-config`, `manila-cloud-secret` Secret과 `octavia-ingress-controller-config` ConfigMap의 credential keys만 교체합니다. 참조하는 Deployment/DaemonSet/StatefulSet을 restart하고 observed generation/updated/available(또는 ready)/revision으로 rollout 완료를 기다립니다.
- KMS required 또는 legacy detect 모드는 각 control-plane host에 privileged hostPID Job을 실행합니다. temporary Secret에서 env로 받은 credential을 `nsenter`로 `/etc/kubernetes/cloud.conf`(있으면)와 `/etc/kubernetes/barbican-cloud.conf`에 rewrite하고 `barbican-kms.service`를 restart해 active/socket을 확인합니다. detect 모드만 없는 KMS 파일을 허용합니다. 모든 rollout 뒤 Secret-write probe 성공이 activation의 선행 조건입니다.
- 성공한 generation은 atomic active, 이전 active/다른 staged는 retiring과 secret erasure입니다. 부분 실패는 staged `last_error`와 기존 active를 유지하며 이미 수정된 guest 객체를 자동 복구한다는 보장은 없습니다. `GET .../authorization`은 secret 없이 states/backlog를 반환하고 `POST .../authorization/retire`는 caller 소유 retiring credential만 owner token으로 삭제합니다. legacy owner 없는 credential은 operator 회수 대상입니다.
- cluster 삭제는 현재 delete actor의 trust로 실행해 creator가 없어도 가능합니다. tenant admission은 caller 소유 credential을 caller token으로 동기 삭제 시도하고, 완료 시 다른 owner/legacy의 secret을 지워 `owner_revocation_required`로 보고합니다. 모든 원격 credential이 함께 폐기됐다는 보장은 없습니다.
- upgrade 순서: 004 적용 → 구 worker로 pre-upgrade mutation/callback drain → 새 API/Worker → legacy ACTIVE cluster 재인가/rollout 성공 → `afterglow-cluster-mgr-*` 사용자와 옛 credential을 out of band 제거. 자세한 절차는 [migration README](../drover/migrations/README.md#execution-authority-upgrade-004)를 참조합니다.

`[drover]` 설정은 `operation_trust_ttl_seconds`(14400, 900–86400), `operation_trust_min_remaining_seconds`(300, 60–3600 및 TTL보다 작음), `delegated_required_roles`(["member"], nonempty), `delegated_optional_roles`(["load-balancer_member"], 보유할 때만), `guest_rollout_timeout_seconds`(600, 60–3600)입니다. Settings/Kolla 이름은 `drover_` prefix입니다. `drover_afterglow_provisioning_url`, `drover_afterglow_provisioning_token`, `drover_afterglow_provisioning_token_file` 및 `DROVER_AFTERGLOW_PROVISIONING_TOKEN_FILE`은 제거됐습니다. Kolla도 같은 provisioning 변수를 제거했고 `afterglow_k3s_provisioning_token` 파일을 `state: absent`로 정리합니다. admission 설정/파일은 유지됩니다.
---

## 상호 문서 참조
* [Drover 기술 문서 인덱스](README.md)
* [Afterglow 서비스 통합 가이드](afterglow-service-integration.md)
* [Drover Native v1 API 기술 Reference](drover-api-v1-reference.md)
