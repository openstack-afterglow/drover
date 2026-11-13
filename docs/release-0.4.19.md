# Drover 0.4.19 — durable tenant cluster deletion

## 한국어

- 버전: **0.4.19** (published tag; metadata SHA `97fb46730a6659d09ee2ff983ac25c810f27a835`).
- 릴리스 일자: **2026-11-13**; tag와 wheel을 발행했습니다.
- 배포 일자: **2026-11-13**; DMS Lab 세 컨트롤러에 API/Worker를 배포했습니다.
- 변경/수정: Afterglow UI와 SDK가 이미 사용하는 `POST /v1/clusters/{id}/delete-async`가 이제 requester trust 기반 durable delete job을 enqueue하거나 진행 중 delete operation을 재사용합니다. 요청은 operation ID와 sequence가 포함된 저장 이벤트를 SSE로 재생합니다. 클라이언트 연결이 끊겨도 Worker 정리는 계속되고, 같은 endpoint 재호출은 진행 중 operation에 다시 연결됩니다. Job enqueue와 cluster `DELETING` 상태 기록은 같은 DB transaction에서 수행합니다.
- 폐기/제거: **없음**. API 경로와 `K3sProgressMessage` schema, SDK method, dependency, DB schema/migration은 변경하지 않습니다.

## English

- Version: **0.4.19** (published tag; metadata SHA `97fb46730a6659d09ee2ff983ac25c810f27a835`).
- Release date: **2026-11-13**; tag and wheel published.
- Deployment date: **2026-11-13**; API/Worker deployed to all three DMS Lab controllers.
- Changes/fixes: the existing Afterglow UI/SDK endpoint `POST /v1/clusters/{id}/delete-async` now enqueues a requester-trusted durable delete job or reuses the active delete operation. It streams persisted events with operation ID and sequence. Worker cleanup survives client disconnects, and retrying the same route reconnects to the active operation. Job enqueue and cluster `DELETING` state are persisted in one DB transaction.
- Deprecated/removed: **None**. The API path, `K3sProgressMessage` schema, SDK method, dependencies and database schema/migrations remain unchanged.

## Scope and compatibility

`DELETE /v1/clusters/{id}` and the SSE `POST .../delete-async` use the same durable job admission. The SSE route is now a progress adapter; it never calls `delete_cluster_progress` on the request connection. It polls/replays sequence-numbered operation events. Progress messages retain their existing fields and deletion-step names. On a repeated request, the handler attaches to the active delete operation instead of enqueueing another job. Already soft-deleted clusters return a single `completed` SSE message.

The worker remains the sole owner of cloud cleanup. Existing safe-delete and fail-closed resource ownership checks are unchanged. Caller-owned app-credential retirement remains a caller-token attempt; remaining credentials are erased from Drover storage and reported for owner revocation as before. No credential authority or creator fallback was added.

## Qualification status

Local Python 3.12.13 qualification passed: `uv lock --check`; `tests/test_k3s_clusters.py` (**58 passed**); service suite (**1807 passed, 3 skipped**); root Ruff; SDK suite (**114 passed**) and Ruff; `uv build --wheel`; architecture check/stamp. Both API/Worker targets built without cache for `linux/amd64`. Exact-SHA CI and tag workflows passed on metadata SHA `97fb46730a6659d09ee2ff983ac25c810f27a835` (runs `43389342037`, `43390025608`, `43390025833`). Tag `0.4.19` and wheel published. Kolla rollout and readiness evidence are recorded in `deploy/kolla/STAGING.md`.

## Deployment and recovery boundary

After rollout, DB still shows `test-k3s` as `DELETING`, `deleted_at=NULL`, owner `pieroot` (`0665ef750b0a4533af760d038ecc6098`), project `a4da5d82465b44d5afda1dc0388b687d`, and no active delete job or operation. I did not retry deletion: no valid project-scoped `pieroot` token was available (`admin-openrc-pieroot` has no password), the authenticated admin identity has no role assignment in this project, and the browser relay rejected access because it is out of date. Do not substitute admin credentials or force-delete resources. Restart the browser relay under the current OMP version or provide an authenticated `pieroot` browser session, then submit `POST /v1/clusters/{id}/delete-async` and verify operation `SUCCEEDED`, `deleted_at`, API-list omission and remaining resource state.