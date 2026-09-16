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
