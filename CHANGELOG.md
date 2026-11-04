# Changelog

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
