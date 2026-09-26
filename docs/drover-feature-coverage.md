# Drover 레거시 기능 커버리지 및 오픈스택 통합 사양서

본 문서는 레거시 오퍼레이션 및 이전 직접 오픈스택 API 연동 방식 대비 **Drover 서비스의 기능 커버리지, 교체 매핑, 오픈스택 서비스 통합 구조, 보안 아키텍처 및 의도적 제약사항**을 상세히 기술합니다.

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

### 2.4 Octavia (Load Balancing)
- **K3s HA API Load Balancer**: Multi-master (`master_count=3`) 설정 시 K3s Control Plane API (Port 6443) HA 로드밸런서, Listener, Pool 및 Member 생성.
- **OCCM (OpenStack Cloud Controller Manager)**: Kubernetes Ingress / Service Type LoadBalancer 수용 및 Octavia 로드밸런서 자동 동기화 (`drover_occm_enabled: true`).
- OCCM이 활성화되면 server 설치 인자에 `--disable=servicelb`를 넣어 K3s 내장 ServiceLB와 같은 Service status를 경쟁적으로 갱신하지 않습니다. agent 설치 인자에도 `--kubelet-arg=cloud-provider=external`을 넣어 OCCM이 provider ID와 노드 주소를 초기화합니다.
- 생성 provider 네트워크는 OCCM의 `internal-network-name`입니다. 같은 이름이 `k3s.occm_public_network`에도 지정돼 있으면 `public-network-name`을 생략합니다. OCCM의 public 분류는 기존 InternalIP를 삭제하므로 두 역할을 겹치게 렌더링하지 않습니다. 서로 다른 public network 정책과 floating-network 선택은 보존합니다.

### 2.5 Keystone (Identity & Access)
- 사용자 요청 시 호출자의 `X-Auth-Token`을 검증하고 프로젝트 스코프를 확인합니다. 프로젝트 헤더가 없으면 제출된 토큰 범위를 보존합니다.
- 서비스 자격으로 catalog의 `identity` internal endpoint를 해석하여 토큰 introspection, 명시적 rescope 및 관리자 역할 조회를 보냅니다. internal endpoint가 없거나 연결할 수 없으면 external/public endpoint로 fallback하지 않고 fail closed 합니다.
- VM 내부 플러그인 렌더링은 별도 경계입니다. `provisioner._guest_plugin_settings`가 인증된 manager session에서 region별 `identity` public endpoint를 조회하고 guest-only Settings copy에 적용합니다. backend의 internal 설정과 resource snapshot은 보존하며, 플러그인 없는 생성은 catalog 조회를 하지 않습니다. 필요한 public endpoint가 없거나 URL이 잘못되면 자원 생성 전에 실패합니다.
- 서비스 카탈로그 자동 등록 (`deploy/kolla/ansible/roles/drover/tasks/preconditions_keystone.yml`의 `name: drover`, `type: drover`).

### 2.6 Barbican & Manila (선택적 커스텀 연동)
- **Barbican KMS Plugin**: K3s Secret 암호화를 위한 KMS 바인딩 지원.
- **Manila CSI Plugin**: K3s Pod 공유 파일시스템(NFS/CephFS) 볼륨 프로비저닝 연동 (`drover_manila_csi_enabled`).

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

소스 role은 `deploy/kolla/ansible/roles/drover`에 있으며 root `drover` wheel의 shared data로 설치됩니다. wheel 기본 설치는 Kolla-Ansible 및 API/Worker runtime dependencies를 포함하지 않으며 서비스 process에는 `drover[service]` extra가 필요합니다. `drover_image_tag` 기본값은 `v0.2.24`입니다(`defaults/main.yml`). 실제 GHCR 이미지 발행과 digest 확인 없이 소스 기본값만으로 배포 완료를 판단하지 않습니다. `drover_source_version`은 별도 source-build pin이고 SDK 버전(`0.2.21`)도 독립적입니다. 이전 릴리스 경계는 [0.2.23 릴리스 노트](release-0.2.23.md)에 보존합니다.

### Schema Readiness 및 Pre-start Migration
- API 및 Worker 프로세스 시작 전, `drover-migrate` 컨테이너가 먼저 실행되어 `drover/migrations/manifest.txt` 및 `001_baseline.sql` 래저 체크섬을 검증하고 DB 마이그레이션을 안전하게 수행합니다.
- API 서버는 요청 수신 시 DB 마이그레이션이 완전히 적용되지 않았거나 Redis/Keystone 커넥션이 정상화되지 않은 경우 `/v1/health/ready`에서 HTTP 503을 반환하여 트래픽 입입을 방지합니다.

---

## 4. 보안, 인프라 동기화 및 오토스케일링 아키텍처

### 4.1 보안 아키텍처 (Security Architecture)
* **비밀번호 분리 및 마스킹**: 비밀번호, 암호화 키 등 민감한 데이터는 환경변수나 소스코드에 하드코딩되지 않으며, `/etc/drover/secrets/*` 파일 경로를 통해 읽어옵니다. 관리자 인벤토리 API(`GET /v1/admin/managed-resources`)는 시크릿 패턴을 자동으로 정규식 검사하여 마스킹 처리합니다.
* **cloud-init 보안**: cloud-init 설정 파일 권한은 파일시스템 모드 `0600`으로 제한되며, 클러스터 플러그인에 OpenStack 서비스 비밀번호가 미노출되도록 클러스터 전용 Application Credential을 주입합니다.
* **Callback CIDR 제한**: K3s Server cloud-init이 호출하는 `/v1/callback` 엔드포인트는 `drover_callback_allowed_cidrs` 허용목록에 등록된 IP 범위에서만 접근할 수 있도록 소스 IP 레벨에서 차단 검증합니다.

### 4.2 인프라 동기화 (Reconciliation Loop)
- **Worker Periodic Scan**: `drover-worker` 엔진은 설정된 주기(`drover_reconcile_interval`)마다 오픈스택 실제 자원(`ManagedOpenStackResource`) 상태와 DB의 원하는 클러스터 상태를 교차 검증합니다.
- **Orphan & Drift Detection**: OpenStack 자원이 임의 삭제되었거나 갱신된 경우 `drift_status` 및 `last_reconciled_at` 필드를 업데이트하고 클러스터를 경고/ERROR 상태로 전환합니다.
- **Application Credential 소유자**: worker가 사용하는 project manager의 `current_user_id`와 기록된 credential ID를 함께 SDK에 전달합니다. Keystone 404는 missing drift이고 인증·연결 장애는 missing으로 숨기지 않습니다. 이 변경은 DB schema나 외부 API를 바꾸지 않습니다.

### 4.3 Stampede 오토스케일링 (Autoscaling)
- **메트릭 기반 스케일링**: Stampede 엔진이 K3s 에이전트 노드그룹의 부하를 감지하여 자동으로 `Scale Out` 또는 `Scale In`을 트리거합니다.
- **경계 조건 및 Cooldown**: 노드그룹 생성 시 설정한 `min_size` 및 `max_size` 경계를 엄격히 준수하며, 급격한 핑퐁 스케일링을 방지하기 위한 Cooldown 쿨다운 기간을 적용합니다.

---

## 5. 의도적인 설계 제약사항 (Intentional Non-Goals)

Drover 서비스의 아키텍처 단순화와 성능 최적화를 위해 아래 기능은 **의도적으로 지원하지 않는 범위(Non-Goals)**로 규정되었습니다:

1. **OpenStack Magnum Wire API 호환성 미지원**
   - Magnum의 기존 `/v1/clusters` REST JSON 포맷을 드롭인 대체(Drop-in replacement)하지 않습니다.
   - 모든 통합 클라이언트는 Drover Native `/v1` API 및 `drover-sdk` 라이브러리를 사용해야 합니다.

2. **OpenStack Placement API 직접 할당 연동 미지원**
   - Placement 서비스의 Resource Class 직접 커스텀 allocation 할당을 사용하지 않습니다.
   - 노드 배치는 Nova Flavor 스케줄링을 따르며, GPU quota 판단 권한은 Afterglow (`app.services.gpu_quota`)에 있습니다. Drover는 일반 cluster create에서 quota authority가 되지 않고, 현재 `drover/services/stampede.py`의 GPU nodegroup scale 경로에서만 `drover/services/afterglow.py:check_gpu_admission`을 통해 admission 결과를 받아 provisioning intent를 제한적으로 진행합니다.

---

## 6. 코드 상 발견된 구현 주의사항 (Implementation Caveats)

1. **내구성 오퍼레이션 (`DroverOperation`) 기록**
   - 클러스터 생성/스케일/삭제 등 라이프사이클 변경 시, 비동기 작업 시작 직후 DB 내 `DroverOperation` 행과 `DroverJob`이 동일 트랜잭션으로 생성됩니다.
2. **SSE 연결 중단과 백그라운드 작업 분리**
   - 클라이언트 측에서 SSE HTTP 요청을 중단(Disconnect)하더라도 백그라운드 Worker의 자원 프로비저닝 작업은 취소되지 않으며 계속 진행됩니다. SDK Proxy는 이를 감지하여 자동 폴링 방식으로 오퍼레이션 완료를 추적합니다.
3. **일회성 콜백 토큰 (`GETDEL`)**
   - cloud-init이 수신하는 Redis 콜백 토큰은 30분 유효기간을 가지며, 1회 조회 시 즉시 삭제(`GETDEL`)되므로 재사용이 불가능합니다.

---

## 상호 문서 참조
* [Drover 기술 문서 인덱스](README.md)
* [Afterglow 서비스 통합 가이드](afterglow-service-integration.md)
* [Drover Native v1 API 기술 Reference](drover-api-v1-reference.md)
