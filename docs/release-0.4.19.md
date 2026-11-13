# Drover 0.4.19 — durable tenant cluster deletion

## 한국어

- 버전: **0.4.19** (로컬 릴리스 후보).
- 릴리스 일자: **미발행**; 준비일 **2026-11-13 Asia/Seoul (UTC+09:00)**.
- 배포 일자: **미배포**. 패키지 발행과 운영 배포는 별도 단계입니다.
- 변경/수정: Afterglow UI와 SDK가 이미 사용하는 `POST /v1/clusters/{id}/delete-async`가 이제 requester trust 기반 durable delete job을 enqueue하거나 진행 중 delete operation을 재사용합니다. 요청은 operation ID와 sequence가 포함된 저장 이벤트를 SSE로 재생합니다. 클라이언트 연결이 끊겨도 Worker 정리는 계속되고, 같은 endpoint 재호출은 진행 중 operation에 다시 연결됩니다. Job enqueue와 cluster `DELETING` 상태 기록은 같은 DB transaction에서 수행합니다.
- 폐기/제거: **없음**. API 경로와 `K3sProgressMessage` schema, SDK method, dependency, DB schema/migration은 변경하지 않습니다.

## English

- Version: **0.4.19** (local release candidate).
- Release date: **Not published**; prepared **2026-11-13 Asia/Seoul (UTC+09:00)**.
- Deployment date: **Not deployed**. Artifact publication and production rollout are separate.
- Changes/fixes: the existing Afterglow UI/SDK endpoint `POST /v1/clusters/{id}/delete-async` now enqueues a requester-trusted durable delete job or reuses the active delete operation. It streams persisted events with operation ID and sequence. Worker cleanup survives client disconnects, and retrying the same route reconnects to the active operation. Job enqueue and cluster `DELETING` state are persisted in one DB transaction.
- Deprecated/removed: **None**. The API path, `K3sProgressMessage` schema, SDK method, dependencies and database schema/migrations remain unchanged.

## Scope and compatibility

`DELETE /v1/clusters/{id}` and the SSE `POST .../delete-async` use the same durable job admission. The SSE route is now a progress adapter; it never calls `delete_cluster_progress` on the request connection. It polls/replays sequence-numbered operation events. Progress messages retain their existing fields and deletion-step names. On a repeated request, the handler attaches to the active delete operation instead of enqueueing another job. Already soft-deleted clusters return a single `completed` SSE message.

The worker remains the sole owner of cloud cleanup. Existing safe-delete and fail-closed resource ownership checks are unchanged. Caller-owned app-credential retirement remains a caller-token attempt; remaining credentials are erased from Drover storage and reported for owner revocation as before. No credential authority or creator fallback was added.

## Qualification status

Local Python 3.12.13 qualification passed: `uv lock --check`; `uv run pytest tests/test_k3s_clusters.py -q` (**58 passed**); full `uv run --frozen --all-extras pytest tests` (**1807 passed, 3 skipped**); root Ruff; SDK pytest (**114 passed**) and Ruff; `uv build --wheel`; architecture check/stamp. Both Docker targets built with `--no-cache --platform linux/amd64`; each imported version `0.4.19`. Source-build mode pins implementation `762fac3dd896e49e22e86c94a1b55ae77f293e47`; exact-SHA CI, tag publication and deployment remain pending. The wheel Release workflow does not independently wait for the reusable CI suite, so tag only after final exact-SHA CI passes.

## Deployment and recovery boundary

The stuck `test-k3s` row has status `DELETING`, `deleted_at=NULL`, and no existing durable delete job. A retry through the fixed SSE endpoint should enqueue/reuse the durable delete operation and finish remaining cleanup, but this is a cloud-mutating operation. Inspect the current project-scoped actor, job absence, Nova/Neutron/Cinder state and remaining credential ownership before issuing it. Verify the operation reaches `SUCCEEDED`, the cluster row receives `deleted_at`, the API list omits it, and resources/owner-revocation backlog match observed cloud state. Do not issue operator SQL edits or force-delete unknown resources to change the UI label.