# Drover `gpu_quotas` 테이블 은퇴 및 데이터 마이그레이션 운영 지침 (Runbook)

## 1. 개요 및 배경

Drover 서비스의 GPU 쿼터 소유권(Authority)은 **Afterglow** 서비스(`app.services.gpu_quota`)로 완전히 이관되었습니다.
이에 따라 Drover 내의 active GPU quota API (`/v1/gpu-quotas`, `/v1/admin/gpu-quotas`), `GpuQuota` ORM 모델, `drover-sdk` 쿼터 메서드 및 Stampede 오토스케일링의 GPU 쿼터 승인 게이트는 모두 제거되었습니다.

그러나 기존 Drover 데이터베이스의 `gpu_quotas` 테이블은 데이터 보존 및 감사(Audit) 목적으로 자동 삭제(DROP TABLE)되지 않으며, `001_baseline.sql` 마이그레이션 파일도 불변(Immutable) 상태로 유지됩니다.

---

## 2. Source Table Retirement 전제 조건 (Prerequisites)

Drover `gpu_quotas` 테이블의 향후 물리적 삭제(Retirement: `DROP TABLE gpu_quotas;`)는 **반드시** 다음 전제 조건이 모두 충족되고 검증된 이후에만 수행할 수 있습니다.

### 필수 체크리스트

1. **[Afterglow 데이터 마이그레이션 완료 및 감사 수용]**
   - Drover `gpu_quotas` 테이블의 모든 기존 테넌트/기본 쿼터 데이터를 `../afterglow/backend/scripts/import_gpu_quotas.py`로 Afterglow DB에 이관해야 합니다. Drover의 `drover/scripts/cutover.py`는 반대 방향(Afterglow → Drover) 도구이므로 이 절차에 사용하면 안 됩니다.
   - 쓰기를 중지한 유지보수 창에서 아래 순서로 감사·적용·재감사를 수행하고, 마지막 dry-run이 변경 없는 정확한 일치를 보고하는지 확인해야 합니다.
   - 프로젝트 ID, canonical GPU alias, limit 값 및 생성/수정 시각이 누락 없이 일치해야 합니다.

   ```bash
   cd ../afterglow/backend
   uv run python scripts/import_gpu_quotas.py \
     --source-db-url "$DROVER_DB_URL" \
     --target-db-url "$AFTERGLOW_DB_URL" \
     --dry-run
   uv run python scripts/import_gpu_quotas.py \
     --source-db-url "$DROVER_DB_URL" \
     --target-db-url "$AFTERGLOW_DB_URL"
   uv run python scripts/import_gpu_quotas.py \
     --source-db-url "$DROVER_DB_URL" \
     --target-db-url "$AFTERGLOW_DB_URL" \
     --dry-run
   ```

2. **[Afterglow Sole Quota Authority 롤아웃 완료]**
   - 모든 프로덕션/스테이징 환경에서 Afterglow가 단일 authoritative GPU quota 엔진으로 가동 중이며, VM 생성 preflight (`POST /api/v1/instances/async`) 및 K3s/Stampede 프로비저닝이 Afterglow GPU 쿼터를 정상적으로 검증하고 있는지 확인해야 합니다.

3. **[의존성 및 레거시 쿼리 부재 검증]**
   - Drover 백엔드, SDK, 외부 스크립트 모니터링 중 Drover DB의 `gpu_quotas` 테이블을 직접 참조하거나 조회하는 레거시 구성요소가 더 이상 존재하지 않음을 확인해야 합니다.

---

## 3. 물리적 삭제 절차 (Execution Steps)

위 3가지 전제 조건이 모두 수동 검증된 후, 운영자는 순방향(Forward) 마이그레이션 SQL 또는 DDL 명령을 통해 테이블을 은퇴 처리할 수 있습니다.

```sql
-- 전제 조건 검증 완료 후 실행
DROP TABLE IF EXISTS `gpu_quotas`;
```

> **주의**: 본 은퇴 절차는 절대 자동 실행되어서는 안 되며, Afterglow 마이그레이션 감증을 마친 클라우드 관리자의 명시적 승인 하에 실행되어야 합니다.
