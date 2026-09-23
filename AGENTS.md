# Drover 작업 규칙

## Architecture maintenance

- 작업 전에 루트 [`ARCHITECTURE.md`](ARCHITECTURE.md)와 영향을 받는 `docs/` 상세 문서를 읽는다.
- code/config/schema/dependency/deploy/test 변경은 같은 변경에서 영향을 받는 architecture 본문과 source-linked 상세 문서를 갱신한다. 구조 영향이 없는 bugfix/refactor도 최신 review summary에 그 이유를 남긴다.
- source가 문서보다 우선한다. 계획·roadmap·이전 문구를 현재 구현의 증거로 승격하지 않는다.
- 실제 source를 검토한 뒤 `python3 scripts/check_architecture.py --stamp --summary "변경 경로와 영향"`를 실행한다. 부분 staging을 검토할 때는 `--stamp --staged --summary "..."`를 사용한다.
- 완료 또는 commit 전 `python3 scripts/check_architecture.py`를 통과시킨다. 이 저장소는 `python3 scripts/check_architecture.py --staged` pre-commit hook도 사용한다.
- credentials, raw token, password, encryption key를 문서·로그·예제에 기록하지 않는다.

## 검증 진술

`uv run pytest ...` 또는 live OpenStack 호출을 실제 실행하지 않았다면 문서에 `test-passed`/`live-verified`라고 쓰지 않는다. 테스트 파일 존재는 `test-defined`로만 기록한다. 일반 검증 진입점은 `uv run pytest tests`, `uv run ruff check .`, `uv run drover-migrate --apply`이며 외부 DB/Redis/Keystone 전제조건을 명시한다.

## CI 파이프라인 성능 규정 (critical-path first)

근거: 2026-09 Drover GitHub Actions 실측(아래 기준선)과 afterglow CI 개편 경험. `.github/workflows/`, CI에서 실행되는 테스트, `docker/Dockerfile`을 바꾸는 변경은 아래 규칙을 따른다. Drover는 OpenSpec을 쓰지 않으므로 전후 수치는 커밋 본문(또는 PR)과 이 절의 기준선에 남긴다.

### 기준선과 현재 형태

- 개선 전 기준선(2026-08-30 → 2026-09-19): `CI`(`ci.yml`) 크리티컬 패스(실행 생성 → 마지막 잡 종료) 중앙값 100초, p90 151초(n=40, runs 33339073898..35421775251). `Docker Build & Push`(`docker-build.yml`, reusable `test` + `build-and-push`) 중앙값 160초, p90 231초(n=40, runs 33358781597..35421775411).
- 잡 중앙값: `docker-build-and-scan` 94초(p90 148초, 느린 실행에서 apt-get 두 단계가 50-110초), `service` 90초(pytest 78초), `db-migration-and-readiness` 39초(Initialize containers 23초), `sdk` 10초, `package-wheel` 9초. 잡 대기열 중앙값 2초. 마지막으로 끝나는 잡은 `docker-build-and-scan` 25/40, `service` 15/40.
- 2026-09 개선: push와 PR의 중복 suite 실행 제거, cert rotation 테스트의 고정 10초 sleep 제거(로컬 pytest 78초 → 18초), 스캔 잡의 docker driver 빌드, 서비스 health-check 2초 interval. 개선 후 CI 수치는 20회 이상 실행이 쌓인 뒤 이 절에 추가한다. 투영치는 CI 중앙값 약 100초 → 약 82초이고, p90은 Debian apt 미러 지연 때문에 약 150초로 유지될 수 있다(회귀 아님).
- 현재 형태: main/dev 대상 PR(fork·dependabot 포함)은 `CI`를 한 번 실행한다. dev/main push와 `v*` tag는 `Docker Build & Push`의 `test`(`ci.yml` workflow_call)를 한 번 실행하고, `build-and-push`가 `needs: test`로 그 전체 결과를 기다린다. `CI`는 `workflow_dispatch`로 발행 없이 수동 실행할 수 있다.

### 규칙

1. **측정 먼저, 추정 금지.** CI를 바꾸기 전과 후에 최근 20회 이상 실행의 잡·스텝 시간을 `gh run list --workflow ci.yml`, `gh run list --workflow docker-build.yml`, `gh api repos/openstack-afterglow/drover/actions/runs/<id>/jobs`로 수집한다. 크리티컬 패스(실행 생성부터 마지막 필수 잡 종료까지)의 중앙값과 p90을 커밋 본문 또는 PR에 남긴다. 절감 효과는 합산되지 않으므로 가장 긴 잡(현재 `docker-build-and-scan`)부터 줄이고, 실제 CI 전후 수치로만 효과를 주장한다. 로그에서 추정한 값은 투영치로 표시한다.
2. **목표 지표를 먼저 정한다.** Drover는 public 저장소이고 GitHub-hosted runner만 쓰므로 wall-clock(대기 시간)이 목표다. private으로 바꾸거나 유료 runner를 도입하면 runner-minutes(비용)도 함께 본다.
3. **게이트 잡을 다른 잡 앞에 두지 않는다.** `python3 scripts/check_architecture.py` 같은 fail-fast 검사는 `service` 잡의 첫 스텝으로 두고, 다른 잡의 `needs:`로 걸지 않는다. 이미지 발행은 테스트 워크플로우 전체 결과(`build-and-push`의 `needs: test`)로 게이팅한다.
4. **잡당 고정비를 측정한다.** checkout, `setup-uv`/`uv sync`, Buildx 준비, 서비스 컨테이너 준비 시간을 잰다. 캐시 복원이 재설치보다 느리면 캐시를 쓰지 않는다. 서비스 컨테이너 health-check는 짧은 interval(2초)과 충분한 retries(interval×retries ≥ 30초) 또는 start-period로 설정한다. 스캔 전용 이미지 빌드는 default docker driver로 daemon에 직접 빌드해 BuildKit container boot와 `load: true` tarball export/import를 피한다. docker driver는 GHA build cache를 export할 수 없으므로, layer cache를 도입하려면 이 선택을 실측으로 비교한 뒤 바꾼다.
5. **샤딩은 고정비가 작을 때만 한다.** 테스트 러너가 작업을 나누는 단위(pytest는 파일/test item)로 균형을 맞춘다. 샤드 명령은 러너를 직접 호출한다. 래퍼 스크립트 뒤에 샤드 인자를 붙이면 전달되지 않아 전체 스위트가 조용히 돌 수 있으므로, 샤드별 실행 수를 CI에서 검증한다. 현재 `service` pytest는 약 20초라 샤딩 대상이 아니다.
6. **격리 해제는 opt-in으로만 한다.** 워커 간 상태 공유나 isolation 해제 같은 최적화는 전역에 적용하지 않는다. 먼저 순서를 섞어(shuffle) 2회 이상 실행해 상태 누수를 확인하고, 안전한 파일만 명시적으로 opt-in한다. monkeypatch·전역 상태를 바꾼 테스트는 반드시 복원한다.
7. **테스트는 hermetic해야 병렬화할 수 있다.** 단위 테스트는 실제 외부 서비스(Keystone, OpenStack API, K3s API server, 임의 IP의 TLS endpoint 등)에 접속하지 않는다. 운영 코드의 고정 대기(`asyncio.sleep` keepalive 등)는 모듈 상수로 빼고 완료 즉시 진행하게 해서 테스트가 실제 시간을 기다리지 않게 한다. 로컬 설정 파일 유무에 따라 결과나 시간이 달라지면 결함이다. pytest-xdist 등 병렬 실행을 도입하면 워커 수를 CI vCPU에 맞춰 명시한다(`-n auto` 금지).
8. **변경 감지의 diff 기준을 정확히 한다.** 현재 Drover CI에는 paths filter나 변경 감지가 없다. 도입하면 push는 `github.event.before..github.sha`로 비교하고, zero SHA·forced push·fetch 실패 시에는 전체를 대상으로 한다. PR은 base..head로 비교한다. `HEAD^1..HEAD`처럼 마지막 커밋만 보는 비교는 금지한다. 발행 산출물(이미지 등)은 실제 발행된 revision을 기준으로 판단한다.
9. **중복 실행은 입력 동일성으로만 제거한다.** 같은 이벤트·같은 SHA에서 같은 `ci.yml`이 두 번 도는 경우(예: 이전의 push 시 `CI`와 `Docker Build & Push / test`)는 트리거 구성으로 제거한다. PR 테스트를 건너뛰는 최적화는 같은 저장소의 브랜치에서 온 PR이고 merge 트리가 이미 테스트된 head 트리와 같을 때만 허용한다. fork PR과 dependabot PR은 항상 테스트한다. 브랜치 이름만으로 판단하지 않는다(fork의 동명 브랜치 우회).
10. **보안: public 저장소의 `pull_request` 코드를 self-hosted runner에서 실행하지 않는다.** 현재 모든 잡은 GitHub-hosted `ubuntu-latest`다. 워크플로우 YAML의 `if:`는 PR이 수정할 수 있으므로, self-hosted runner를 도입하면 runner group의 저장소 제한과 fork PR 승인 설정으로도 보장한다.
11. **CI 형태는 계약 테스트로 고정한다.** 트리거 구성(PR은 `CI`, push/tag는 `Docker Build & Push / test`), `build-and-push`의 `needs: test`, 스캔 잡의 docker driver와 `load: true`, 서비스 health-check interval/retry window, Trivy pin 같은 불변식은 `tests/test_ci_workflows.py`에서 검증한다. 워크플로우를 바꾸면 이 테스트를 같은 변경에서 갱신한다.
12. **지속 개선.** CI를 바꾸는 변경에는 전후 실측을 첨부한다. 크리티컬 패스 중앙값이 가장 최근에 기록된 기준선보다 20% 이상 나빠지거나(현재 기준 CI 120초 초과), 테스트 수가 크게 늘거나, 새 테스트 계층을 추가하면 1번 절차로 다시 측정하고 가장 긴 잡부터 개선한다. 보류된 후보와 보류 이유: drover-worker 이미지의 중복 Trivy 스캔(중앙값 18초, 두 target 스캔 계약), Dockerfile apt layer cache(p90 tail의 원인, docker driver와 상충하므로 실험 브랜치에서 측정), pytest-xdist(`service`가 크리티컬 패스가 아니게 됨), main merge·tag 재테스트 생략(발행 게이팅 위험).
