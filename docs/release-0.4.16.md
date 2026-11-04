# Drover 0.4.16 — standard project authority

## 한국어
- 버전: **0.4.16** (후보)
- 릴리스/배포 일자: **미발행 / 미배포**; 준비일 **2026-11-04 Asia/Seoul (UTC+09:00)**.
- 변경/수정: native project admin/manager/member의 소유 자원 관리와 reader의 비밀 없는 조회를 허용합니다. 전용 Drover 역할은 필요하지 않습니다. 실제 effective custom/group/domain implication, human/system 경계, 프로젝트·키·원래 reduced credential ceiling을 보존합니다. native capability endpoint와 Kolla 공통 read-only 정책 mount, fail-closed YAML reload를 제공합니다.
- 폐기/제거: 전용 Drover grade/leaf 필수조건 및 그것만으로 얻는 기본 권한; `[drover] policy_file`/`DROVER_POLICY_FILE`(표준 `[oslo_policy]`/`OSLO_POLICY_*`로 이전). 기존 IAM 역할·할당·암호화 키·자격증명 행 삭제/확대는 **없음**.

## English
- Version: **0.4.16** (candidate).
- Release/deployment date: **Not published / not deployed**; prepared **2026-11-04 Asia/Seoul (UTC+09:00)**.
- Changes/fixes: native project admin/manager/member manage owned resources; reader reads nonsecret metadata without dedicated Drover roles. Actual effective custom/group/domain implication, human/system boundaries, project/key ownership and original reduced-credential ceilings remain enforced. Adds native capability discovery, shared Kolla read-only policy mounts and fail-closed policy reload.
- Deprecated/removed: dedicated service-grade/leaf prerequisites and standalone default entitlement; `[drover] policy_file`/`DROVER_POLICY_FILE` (migrate to standard `[oslo_policy]`/`OSLO_POLICY_*`). IAM assignment deletion, encryption-key changes or credential expansion: **None**.

## Scope and compatibility

Candidate is based on remote `dev` ea0bc07, not the stale primary0.4.3 tree (92modified/41untracked paths at discovery). It does not import unpublished administrator executor, cleanup worker, SQL issuer, network or periodic-worker changes.

Production has already applied immutable005 (`c2353fa25dec5c210b826de5a2c8004b0580371db416f7ddfc3dd6fe61d1bd6a`), adding nullable requester columns and the cleanup journal. This release packages that exact file and checksum, preserving001–004 byte-for-byte. Canonical migration/API startup/readiness/worker startup now reject unknown/checksum-drift/pending ledgers and unsupported nonNULL executor-requester rows or any cleanup references. No executor interpretation or credential recovery is fabricated. Existing caller-mode NULL rows remain unchanged. Parent's read-only observation found2active credential rows,1deleted delegation, requester counts0 and cleanup0; this is not an executed migration or deployment by this slice.

Upgrade with matching API/worker and the canonical `drover-migrate --apply`, secure backup and existing encryption key. Older001–004 sites apply additive005; already005 sites require no new DDL. Stop mutation admission/autosizing and drain all existing worker/callback jobs before cohort replacement, preserving the0.4.15 no-mixed-consumer boundary. Existing unknown outcome jobs must not replay. Unsupported executor/cleanup data requires its actual matching implementation, not row deletion, ignored ledger or coercion to NULL. Native provider delegation still selects required held member plus optional held load-balancer_member; manager uses actual native manager→member implication (independently observed by parent), not a fabricated grant.

## Native action matrix

| Authority | Nonsecret inventory/capabilities | Owned management/content/explicit kubeconfig | Global templates/settings |
|---|---|---|---|
| Native member/manager/project-admin | yes | yes, subject to provider roles/state/key ownership | no |
| Native reader | yes | no | no |
| Custom/group/domain assignment implying core IDs | same as actual effective core role | same as actual effective core role | no implicit system authority |
| Legacy service role alone / name-only alias | empty capability response; inventory denied | no | no |
| Existing legacy implication to core IDs | same as actual effective core role | same as actual effective core role | no |
| Reduced credential / unknown/token-only auth method | issued-role-ID-limited metadata | no, even operator `@` | no |
| Verified current human system admin | yes | existing published execution prerequisites still apply | yes |

ConfigMap/Secret bodies, logs, shell and kubeconfig issuance are not reader metadata. Foreign resource remains404/403 as the existing route contract requires. No new native key/private/credential ownership bypass is introduced. Callback one-use token/CIDR authentication remains independent.

## Publication workflow

Canonical tag/image identity is **0.4.16**; workflow filters now accept numeric X.Y.Z and historical v-prefixed tags. Root package/import/lock and Kolla registry image defaults must agree. API/worker image publication waits for the reusable full suite; GitHub wheel Release remains a separate existing workflow without a tag-suite dependency, so parent must explicitly verify both exact-SHA workflow receipts before reporting release completion. No CI speedup is claimed. Build targets remain `docker/Dockerfile` drover-api and drover-worker. Source-build mode requires a full immutable source SHA: parent must pin the scoped implementation commit in a subsequent release-metadata commit before publishing; the existing0.4.15 pin is not acceptance for this candidate. Do not move an existing tag.

## Local qualification

Qualification commands and exact observed results are recorded by the final evidence receipt. Native smoke uses actual Uvicorn and installed Keystone clients over owned loopback HTTP, with synthetic domain storage/provider operations and lifespan off. It does not prove live provider/guest readiness, MariaDB/Galera locking, deployment policy or operational acceptance. No cloud/IAM grant writes, commits, pushes, tags or publication were performed by this slice.
