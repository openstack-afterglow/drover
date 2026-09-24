# Drover OpenStack 기술 문서 체계 (Documentation Index)

Drover는 OpenStack 환경에서 K3s Kubernetes 클러스터의 라이프사이클 관리, 노드그룹 오토스케일링, 인프라 동기화(Reconciliation) 및 내구성 오퍼레이션을 제공하는 오픈스택 네이티브(OpenStack Native) 컨테이너 인프라 서비스입니다.

본 문서 집합은 Afterglow 서비스 연동 개발자, 클라우드 운영자 및 플랫폼 엔지위어를 위한 종합 기술 가이드와 Reference 사양을 제공합니다.

---

## 문서 목록 및 상호 참조

0. **[현재 아키텍처 정본](../ARCHITECTURE.md)**
   - 현재 소스 기준의 책임 경계, MariaDB 정본/Redis 보조 저장소 분리, runtime flow, 운영 한계
   - 코드·설정·schema·배포·테스트 변경 때 함께 갱신해야 하는 maintenance 절차

1. **[Afterglow 서비스 통합 및 엔드포인트 디스커버리 마이그레이션 가이드](afterglow-service-integration.md)**
   - Afterglow 서비스의 Keystone 카탈로그 기반 엔드포인트 자동 탐색(Discovery) 전환 계획과 운영 체크리스트
   - 배포 및 카탈로그 검증 체크리스트 (`openstack catalog show drover`, `openstack endpoint list --service drover`)
   - 계획으로 명시된 3단계 안전 전환 전략 (Shadow Discovery ➔ Service Proxy ➔ Direct Override Removal)
   - Python Afterglow 통합 코드 샘플 (`openstacksdk` Connection + `drover_sdk.register`)
   - 멱동성(`Idempotency-Key`), SSE 스트리밍 비동기 처리, 오퍼레이션 폴링 및 장애 대응 매트릭스

2. **[Drover Native v1 API 기술 Reference](drover-api-v1-reference.md)**
   - Drover 네이티브 REST, SSE, WebSocket API 전체 엔드포인트 카탈로그 (기계 읽기용 `/openapi.json` 안내 포함)
   - 헬스 체크, 테넌트 클러스터, 노드그룹, 템플릿, K8s 리소스 프록시, 인증서/웹셸, 오퍼레이션, 관리자 API, GPU quota 이관 안내
   - 요청/응답 Key 스키마, HTTP 헤더(`X-Auth-Token`, `X-Project-Id`, `X-Openstack-Request-Id`, `Idempotency-Key`), SSE 이벤트 형식 및 에러 처리 모델

3. **[Drover 레거시 기능 커버리지 및 오픈스택 통합 사양서](drover-feature-coverage.md)**
   - 기능 그룹별 교체 및 마이그레이션 매핑 서열
   - OpenStack 연동 사양 (Nova, Neutron, Cinder, Octavia, Keystone, Barbican, Manila)
   - 보안 아키텍처 (비밀번호 미노출, 최소권한 Application Credentials, Callback CIDR 제한)
   - 동기화(Reconciliation), Stampede 오토스케일링 및 의도적 설계 제약사항 (Magnum Wire 호환성, Placement allocation 미지원)

4. **[Drover `gpu_quotas` 테이블 은퇴 및 데이터 마이그레이션 운영 지침 (Runbook)](gpu-quota-table-retirement-runbook.md)**
   - Active GPU quota 소유권의 Afterglow 단독 이관에 따른 Drover legacy `gpu_quotas` 테이블의 소스 은퇴(Source Table Retirement) 절차
   - 마이그레이션 데이터 감사(Audit), Afterglow 권한 롤아웃 검증 및 물리적 `DROP TABLE` 전제 조건 체크리스트

5. **[Drover 0.2.23 릴리스 노트](release-0.2.23.md)**
   - v0.2.21 이후 패키지 통합, 인증서 회전 및 CI 개선, FastAPI/Starlette 보안 갱신
   - root wheel·Kolla 이미지 태그와 미실행 빌드·배포 검증 경계
---

## Drover 핵심 아키텍처 요약

```mermaid
graph TD
    Client[Afterglow / OpenStack SDK] -->|Keystone Token Auth & Catalog Discovery| API[Drover API FastAPI /v1]
    API -->|durable cluster, operation, job records| DB[(MariaDB / MySQL)]
    API -->|auxiliary cache, callback token, lock| Redis[(Redis)]
    Worker[Drover Worker Engine] -->|lease and status| DB
    Worker -->|health/cache/token helpers| Redis
    Worker -->|openstacksdk| OpenStack[Nova / Neutron / Cinder / Octavia / Keystone]
    K3sServer[K3s Server VM] -->|cloud-init Callback /v1/callback| API
```

* **Keystone Catalog Service**: service name/type `drover`; `container-infra`는 `drover-sdk`의 SDK alias일 뿐이다.
* **인증 및 권한**: 프로젝트 범위 Keystone 토큰 (`X-Auth-Token`) 및 `oslo.policy` 기반 RBAC (`drover:clusters:*`, `drover:operations:*`, `drover:admin` 등). 토큰 검증·관리자 역할 조회는 catalog의 `identity` internal endpoint만 사용하며 external/public fallback 없이 fail closed 합니다.
* **SDK 패키지**: `drover-sdk` Python 라이브러리를 통해 `conn.drover` 바인딩 및 카탈로그 자동 인지 사용

---

## 관련 링크 및 공식 참조
* [OpenStack SDK 공식 문서](https://docs.openstack.org/openstacksdk/latest/)
* [OpenStackClient CLI 공식 문서](https://docs.openstack.org/python-openstackclient/latest/)
