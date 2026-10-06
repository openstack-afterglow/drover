# Drover 0.4.0 release notes

Changes since `v0.3.1`. The deployed release base is retained, including the pinned Redis 5 asynchronous shutdown fix. Root package/runtime/lock metadata and the Kolla role image tag move together to `0.4.0` / `v0.4.0`; `drover-sdk` remains `0.2.21`. No database schema or migration is added.

## Autoscaling contract

- `drover/services/kube.py` accounts for exact CPU, memory, NVIDIA GPU, extended-resource and Pod-slot requests, including init containers, restartable init sidecars, overhead and Pod-level CPU/memory. Assigned nonterminal Pods consume capacity; missing observations are not spare capacity.
- `drover/services/stampede.py` assigns capacity-shortage Pending Pods to explicitly configured agent nodegroups by flavor, labels/selectors, affinity and taints. Bin packing applies all supported dimensions and configured allocatable headroom. PVC binding, pinned nodes and unsupported scheduling constraints do not trigger speculative workers.
- `drover/services/jobs.py` and `nodegroup.py` reserve counts and enqueue durable jobs in one MariaDB row-lock transaction. Active mutation jobs fence competing manual/automatic changes. Terminal settlement keeps partial-failure tracking and clears only the matching latest reservation.
- `drover/services/gpu.py` supplies NVIDIA runtime flags and the device-plugin DaemonSet. A Stampede GPU job succeeds only after K3s Ready and the requested number of real `nvidia.com/gpu` allocatable slots are observed. All GPU images require a compatible kernel driver already installed; Ubuntu installs the container toolkit, while FCOS requires the toolkit/runtime preinstalled too.
- Afterglow admission/provisioning is used only when configured and remains fail-closed. Intent failures never fall back to duplicate native VM creation. Without those URLs, normal OpenStack quota enforcement applies. Stable operation/node keys reuse Nova servers and boot volumes on retry.

## Scale-down safety

A node must remain continuously underutilized for `drover_stampede_scale_down_window` (300–600 seconds) before a one-worker reservation. Observation failures or gaps reset stabilization. Controller, selector/taint, storage and resource constraints must allow relocation; the actual Eviction API enforces PDBs and drain must finish before Nova deletion.

Immediately before deletion, Nova ID lookups (`compute.get_server`) establish live sizing headroom; only a genuine 404 means absent, so 403/400 and other observation failures never authorize cleanup (the SDK's `find_server` falls back to a name search after a denied GET and is not used here). Absent/deleting tracking rows cannot satisfy `min_size`, including mixed batches. Ownership is checked before cordon and again immediately before Nova deletion.

Cordon writes the Node annotation `drover.io/removing-vm-id` in the same merge patch. Drain failures and a failed final Nova lookup/ownership recheck attempt uncordon (clearing the annotation) and preserve resources, but only for a cordon newly acquired by that attempt. A matching annotated cordon observed before the attempt may predate an earlier DELETE, so it is inherited and never rolled back. Once the final recheck passes, the node stays cordoned even if Nova rejects the DELETE or the Nova wait, Cinder, Node or tracking cleanup fails. A retry resumes only its own annotated cordon and re-runs the other live guards; any other cordon still blocks as `node_not_ready`. Retries for Nova-confirmed absent/deleting servers resume disappearance/volume/Node/tracking cleanup without re-running live relocation or Ready guards. Post-delete count reconciliation excludes a 404 sibling from the live count but keeps its tracking row for its own cleanup. Normal Nova deletion is used, never force-delete. A job that exhausts retries leaves the annotated cordon in place; retry the operation or inspect the node before uncordoning.

The same ID-only lookup now backs `nova.wait_server_deleted` and `nova.delete_server_safe`. Whole-cluster deletion therefore no longer treats a denied server lookup as already deleted: `deletion.py` logs the failure and keeps that server's inventory record active instead of marking it deleted.

## Afterglow integration

See [API reference](drover-api-v1-reference.md) and [UI/BFF contract](afterglow-service-integration.md#54-stampede-오토스케일링-상태-및-이벤트-연동-사양-autoscaling-ui-integration).

`GET /v1/clusters/{id}/stampede` and `/stampede/status` expose DB-backed desired/tracked counts, active operation IDs and the last capacity/Pending/decision snapshot. GET does not refresh Kubernetes. `observed_at` indicates freshness; `ready_count` is K3s Ready, not GPU-ready, and is null before observation. GPU allocatable and operation outcomes must be checked separately. Redis `/stampede/events` is best-effort; DB operation/events are the durable record. `quota_state.allowed` is admission output, not a reservation of Nova quota or hardware.

## Verification and rollout boundary

Local evidence on the integrated `v0.3.1` base with the final deletion corrections: `uv run --frozen pytest tests` 843 passed / 3 skipped, `uv --directory sdk run --frozen pytest` 111 passed, `uv run --frozen ruff check .` passed. The denied-lookup, rejected-DELETE resume, inherited-cordon retention and absent-sibling regressions each failed against the previous logic in a throwaway mutation run; the final-recheck rollback regression was not mutation-checked. API and worker targets built locally from the final source for `linux/amd64` and `linux/arm64`; inside each image `platform.machine()` reported `x86_64`/`aarch64` and `drover.__version__` reported `0.4.0`. These are not live scaling evidence.

Before rollout, the DMSLab Kolla inventory resolved the `drover` group to `dms-controller1..3`, and the existing API on `dms-controller1` was healthy. Existing clusters and workloads are not test fixtures; their names do not establish ownership or safety. GPU flavor or image names do not prove driver or hardware readiness.

The user authorized a Drover-only Kolla rollout and isolated CPU/GPU verification, and separately approved a `v0.4.0` version-tag publication that also moves `latest`. The image publisher sets no `platforms`, so registry manifests must be checked after publication. The wheel workflow still does not wait for the tag suite, and Trivy findings remain non-blocking (`exit-code: '0'`).

## 0.4.1 patch

Live DMSLab verification of `v0.4.0` found that a GPU nodegroup could not be created. Nodegroup `flavor_id` validation reused the global `k3s.default_agent_flavor` tenant policy, which only accepts public flavors. Every DMSLab GPU flavor is private and is shared per project (`afterglow:access_mode=gpu_quota`), so `POST /v1/clusters/{id}/nodegroups` returned `422 Invalid flavor_id` for a flavor the project could boot.

`resource_policies.validate_nodegroup_resource` now validates a nodegroup's own flavor with a project-scoped Nova `get_flavor` lookup. Nova hides private flavors from non-admin tokens of other projects and enforces access at boot. Nodegroup create/update and `POST /stampede/enable` use it; `image_id` keeps the `k3s.server_image` policy. The global default-agent policy and cluster-create `agent_flavor_id` validation are unchanged. Root package/runtime/lock and the Kolla role image tag move to `0.4.1` / `v0.4.1`; no schema or migration change.

Local evidence: `uv run --frozen pytest tests` 845 passed / 3 skipped; `uv run --frozen ruff check .` passed. `tests/test_k3s_nodegroups.py::test_nodegroup_flavor_uses_project_visibility_not_public_default_policy` accepts a shared private flavor and rejects an invisible one. The accept case failed when the previous public-only policy was restored in a throwaway mutation run.

0.4.1 was published only as `dev`/`sha-e702cb6` images (no version tag; `latest` stayed on `v0.4.0`) and rolled out to DMSLab by digest. With it the GPU nodegroup was created and a CPU worker scaled up and joined in about four minutes.

## 0.4.2 patch

On 0.4.1 the Stampede GPU worker VM (RTX 3060 flavor, Ubuntu 24.04 NVIDIA image) booted but never joined K3s. Read through the QEMU guest agent, `k3s-agent-join.service` failed on every restart at the bootstrap check `nvidia-container-runtime --version`. That command prints its version and then exits 1 with `no runtime binary found from candidate list: [runc crun]`: K3s bundles its own runc outside `PATH` and provides none before it starts. Every fresh Ubuntu or FCOS GPU worker was affected, independent of driver health. In the same guest `nvidia-smi -L` listed the GPU, and `nvidia-container-cli info` succeeded (driver 535.309.01) with no runc/crun on `PATH`.

`drover/services/gpu.py:bootstrap_script` keeps the driver (`nvidia-smi -L`) and runtime-presence checks but validates the container stack with `timeout 30 nvidia-container-cli info`, which needs no low-level runtime. `tests/test_k3s_stampede.py::test_gpu_bootstrap_passes_before_k3s_provides_a_low_level_runtime` executes the rendered script against stubs with real 1.20.1 semantics and no runc on `PATH`. It passes when the driver stack works and fails when `nvidia-container-cli` fails. The driver-ok cases failed against the previous check in a throwaway mutation run. Suite: 849 passed / 3 skipped; Ruff passed. Root package/runtime/lock and the Kolla role image tag move to `0.4.2` / `v0.4.2`; no schema or migration change.

Deployment boundary observed on DMSLab: the Afterglow admission endpoint authenticates as its service user `afterglow_admin` scoped to the tenant project. That user has a role only on `afterglow-service`, so `/api/v1/internal/k3s/gpu-admission` returned 503 (Keystone 401) for every tenant project. Because Drover consults admission for every Stampede flavor when the URL is configured, CPU and GPU scale-up both stayed `gpu_admission_unavailable` (fail-closed) until `afterglow_admin` received `member` on the disposable test project. This is an Afterglow credential/onboarding gap and is not changed by Drover.
