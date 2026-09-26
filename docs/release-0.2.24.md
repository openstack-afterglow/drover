# Drover 0.2.24 release notes

Changes since `v0.2.23`. Live evidence below was collected on 2026-09-26 against the DMSLab Kolla deployment with the `dev` images `drover-api@sha256:60507b882ed1c043d8c1ed9e6feaa4a96a4599ccd8f1bcfde497779c1234a59c` and `drover-worker@sha256:ad813cb107ed7fe90caf1a2bbd0381e73e2d239b6cecbcaa4c980bee1ac2459d` (revision `c17f5e6`). It is not evidence for the tagged images; verify those separately after publication.

## Fixed

- **Pod reply routing survives network reconfiguration and reboot.** Ubuntu installs the priority 29999 `to 10.42.0.0/16 table main` rule with `protocol kernel`, so systemd-networkd no longer removes it as a foreign rule; an existing unmarked rule is migrated exactly. Fedora CoreOS stores the same rule on the pinned provider NIC's NetworkManager profile. K3s still refuses to start without the rule and the watcher still repairs transient loss.
- **Guest OpenStack plugins authenticate through a reachable endpoint.** Plugin configuration uses the authenticated catalog's region `identity` endpoint with interface `public`, not the backend's internal `auth_url`. Missing or invalid endpoints fail before OpenStack resources are created; plugin-free clusters do not query the catalog.
- **OCCM owns node initialization and LoadBalancer Services.** Servers start with `--disable=servicelb`, agents with `--kubelet-arg=cloud-provider=external`, and the provider network is no longer rendered as both `internal-network-name` and `public-network-name` (that overlap removed each node's `InternalIP`).
- **OCCM Service LoadBalancers are deleted with the cluster.** OCCM receives the immutable cluster ID as `--cluster-name`. After every VM is deleted, Drover deletes only LBs whose `kube_service_<cluster_id>_` name and OCCM description both match, waits for pending Octavia operations, and deletes only OCCM-described VIP floating IPs. Clusters created before this release keep display-name LB names and are not matched.
- **HA clusters no longer abort when OCCM is enabled.** Joining servers used to re-render `cloud.conf`, which requires the per-cluster application credential secret that Drover does not retain, so HA bootstrap raised before creating servers 2 and 3. Joiners now rely on the `cloud-config` Secret that server 1 creates, which is where OCCM, Cinder CSI and Barbican KMS read it.
- **Application credential reconciliation** calls `identity.get_application_credential(user, application_credential)` with the project manager's user ID.

## Live verification (2026-09-26)

A fresh two-node Ubuntu cluster on the external provider network, created through the authenticated Afterglow API with no manual guest or Kubernetes changes:

- OCCM initialized both nodes (`providerID` set, provider IP as `InternalIP`, uninitialized taint removed); the rendered cloud config used the public Keystone endpoint.
- Before and after attaching an internal NIC to each node: Pod-to-API TLS, cross-node Pod HTTP, and HTTP through an OCCM-created Octavia LoadBalancer floating IP returned the expected marker; the secondary NIC kept no default route, no DNS and `accept_ra=0`.
- `netplan apply` with the watcher stopped: 480 and 469 samples per node, rule missing 0 times.
- Reboot: rule present in every one of 682 and 571 samples taken while K3s was active; Pod, API and LoadBalancer checks passed once system Pods were Ready. The first post-reboot check ran before CoreDNS was Ready and failed name resolution; it passed after readiness.
- Cluster deletion removed the VMs, volumes, security group, application credential and the remaining OCCM Traefik LoadBalancer (`k3s delete: OCCM Service LB ... fully deleted`). The project's other differences from the pre-test baseline belonged to a concurrent Packer image build.

Not verified live: Fedora CoreOS (this deployment has no `k3s.fcos_image` policy); only NetworkManager 1.42's rule serialization was checked against the script's comparison string. HA bootstrap with OCCM is covered by a regression test that fails on the previous code, not by a live HA cluster. octavia-ingress-controller LBs are not part of the deletion cleanup.

## Release metadata

`pyproject.toml`, `drover/__init__.py`, the root package in `uv.lock`, and the Kolla role's `drover_image_tag` declare `0.2.24`. `drover-sdk` stays `0.2.21`. The tag workflow publishes the images for the runner's default platform (`linux/amd64`); both targets were additionally built and executed locally for `linux/amd64` and `linux/arm64`.
