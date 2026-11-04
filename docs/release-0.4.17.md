# Drover 0.4.17 — native quota admission and worker revocation fence

## 한국어

- 버전: **0.4.17** (로컬 릴리스 후보).
- 릴리스 일자: **미발행**; 준비일 **2026-11-04 Asia/Seoul (UTC+09:00)**.
- 배포 일자: **미배포**. 패키지·이미지 발행과 운영 배포는 별도 승인 및 검증 대상입니다.
- 변경/수정: 클러스터 생성 전 실제 프로젝트의 Neutron 보안 그룹·규칙 limit/used/reserved와 provider 기본 규칙·빈 프로젝트의 default 그룹 비용을 검사하고 worker 쓰기 전 재검사합니다. 부족409/불명503은 작업·자격증명 발급 전에 거부합니다. 구조화된 quota 정보를 유지하되 권위 있는 사전 거부 marker는 키 없는 요청에만 부여하여 같은 멱등성 키의 동시 승인 작업을 숨기지 않습니다. worker quota 오류를 원래 정보 그대로 terminal 처리하고 재시도하지 않습니다. 진행 중 heartbeat 갱신이 revoke를 덮어써 이전 worker 소유권을 복구하던 경쟁을 기존 lock/generation으로 차단합니다.
- 폐기/제거: **없음**. 기존 프로젝트 quota 확대, IAM 권한 확대, 암호화 키 변경, 자원 자동 삭제·불명 쓰기 재실행은 없습니다.

## English

- Version: **0.4.17** (local release candidate).
- Release date: **Not published**; prepared **2026-11-04 Asia/Seoul (UTC+09:00)**.
- Deployment date: **Not deployed**. Artifact publication and operational rollout require separate authorization and evidence.
- Changes/fixes: checks actual project Neutron security-group/rule limit, used and reserved counts, provider-generated rules and an empty project's implicit default group before credentials/queueing, then rechecks before worker writes. Known shortages return409 and unknown budgets503. Structured quota details remain available, while authoritative pre-admission refusal markers are unkeyed-only so overlapping keyed admissions cannot hide an accepted operation. Worker quota failures settle terminally with original details and no replay. A local generation under the existing coordinator lock prevents in-flight heartbeat renewal from restoring revoked worker ownership.
- Deprecated/removed: **None**. No automatic existing-project quota increase, IAM expansion, encryption-key change, resource deletion or replay of uncertain writes.

## Scope and compatibility

Prepared from fresh remote dev/published0.4.16 commit `bb153e5fbe30f0d602d4dfe86edbdff1e1fe5a51`, not the shared stale0.4.3 checkout. The isolated port changes five runtime files and six regression/fixture files; preserves published GPU metadata/initial-agent preflight, standard project authority, Sentinel client lifecycle and SDK drain ordering; and excludes unrelated requester-executor/credential-cleanup work. No runtime dependency changes or schema/migration additions are introduced. Existing001–005 checksums and fail-closed unsupported executor/cleanup-data checks remain unchanged.

The error-wire extension is intentional: only unkeyed quota preflight returns `detail.code=drover_pre_admission_refused`; keyed responses preserve reason/message/verified quota values without an absence assertion. Generic errors, worker errors and later provider races do not receive that marker. See the [API contract](drover-api-v1-reference.md#security-group-quota-admission-candidate-unpublished).

Quota/template reads retain caller or verified worker authority. Native policy denying these reads now causes an unavailable-budget refusal; no service-admin read fallback is added. The budget is an observation, not a native reservation, and Neutron remains authoritative for concurrent writes. Existing partial or unknown physical mutations require explicit inventory/owner review, not automatic retries.

## Publication and immutable source pins

Canonical tag/image version is numeric **0.4.17**. Existing workflow filters accept numeric X.Y.Z and historical v-prefixed tags; no trigger or publication gate is weakened. Root package/import/lock and Kolla registry defaults agree. API/worker publication waits for the reusable full suite. The wheel Release workflow retains its documented independent publication gap: place the numeric tag only after the **final exact commit SHA** passes the full remote suite. No push/tag/publication is authorized by this preparation.

A commit cannot embed its own SHA. The implementation/version commit is followed by a release-metadata commit that pins Kolla source mode to the immutable implementation commit. The numeric release tag must identify the final metadata commit, and image/wheel builds must use that exact final source. Do not move existing tags. The final metadata records the concrete implementation SHA.

## Qualification and operational boundary

Earlier pre-version isolated Python3.12.13 qualification: **1807 passed, 3 skipped**, full Ruff and architecture check passed; actual installed-SDK synthetic loopback smoke verified zero preflight writes and exactly one later409 POST without replay. That result is not the final versioned-source gate. Final versioned-source commands, package/image receipts and hashes are retained under `/Users/pieroot/.local/share/afterglow-proofs/drover-release-qualification-20261104/` and handed to the integration owner before publication.

No production cloud, database, IAM or service mutation was performed. Synthetic local provider/runtime tests do not prove Galera locking, Sentinel failover, actual policy permission to read quotas/templates, live cloud authentication or guest readiness. Operational rollout must preserve effective database/Redis/key/config identity, back up safely, stop mutation admission/autosizing, settle callbacks and queued/running work, and drain **every old SQL worker** before replacing the matched API/worker cohort. Preserve existing005 compatibility checks and never coerce unsupported requester/cleanup data or replay unknown outcomes. See [Kolla boundaries](../deploy/kolla/STAGING.md#0417-quota-and-revocation-fence-candidate).
