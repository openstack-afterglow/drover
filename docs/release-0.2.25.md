# Drover 0.2.25 release notes

Changes since `v0.2.24`. This release has not been exercised against a live OCCM deployment yet: the DMSLab Kolla rollout is paused while a separate Waygate remediation runs on the deploy host. Record live evidence here after deployment.

## Fixed

- **Cluster deletion keeps the OCCM floating IPs that Services asked to retain.** v0.2.24 deleted every OCCM-created VIP floating IP along with the cluster's Service LBs. OCCM itself keeps that IP when the Service being deleted is annotated `loadbalancer.openstack.org/keep-floatingip: "true"` (`ensureLoadBalancerDeleted` in both v1.28.0, Drover's default OCCM image, and v1.34.1, the Kolla role's image), and only the Kubernetes API records that intent. Deletion now reads every Service's annotations with the cluster's admin kubeconfig before touching nodes or VMs, whenever the cluster has OCCM Service LBs. After the VMs are gone, an OCCM-described floating IP is deleted only if no Service using its LB (the creator the LB is named after, or a sharer naming it in `loadbalancer.openstack.org/load-balancer-id`) asked to keep it. When Kubernetes cannot be read, or an LB has no captured Service, the intent is unknown and the IP is kept; deleting the LB only detaches it. Kept IPs are logged as `OCCM Service LB <lb>: keeping floating IP <fip> of <namespace>/<service>`, an unreadable cluster as `k3s delete: OCCM Service intent unreadable, keeping OCCM floating IPs`.

## Verification (2026-09-27)

- `tests/test_managed_resources_lifecycle.py::test_occm_service_lb_cleanup_keeps_retained_floating_ips_within_this_cluster` covers keep on the creator, keep on a sharer, an LB without a captured Service, a user-supplied IP, other clusters' LBs and unreadable Kubernetes. Both cases fail on the v0.2.24 cleanup, which deleted all four OCCM-described IPs.
- `tests/test_managed_resources_lifecycle.py::test_deletion_order_and_ownership_constraints` asserts that Service intent is read before any VM is deleted and reaches the cleanup, and that an unreachable Kubernetes API still completes the deletion and keeps every OCCM IP.
- `tests/test_k3s_kube.py::test_list_service_annotations_keys_every_namespace_by_service` covers the all-namespace read.
- `uv run pytest tests`: 679 passed, 3 skipped. `uv --directory sdk run pytest`: 111 passed. `uv run ruff check .` passed.
- Both image targets were built locally for `linux/amd64` and `linux/arm64`. In each of the four images, the fixed cleanup driven by a fake OpenStack connection kept the retained IP and deleted only the other one. `drover-api` served `/v1/health/live` with 200, and `/v1/health/ready` returned 503 with no MariaDB, Redis or Keystone.

Not verified: a live cluster deletion with an annotated Service (pending deployment), and the `load-balancer-id` sharer path against a real shared LB.

## Release metadata

`pyproject.toml`, `drover/__init__.py`, the root package in `uv.lock`, and the Kolla role's `drover_image_tag` declare `0.2.25`. `drover-sdk` stays `0.2.21`. The tag workflow publishes the images for the runner's default platform (`linux/amd64`).
