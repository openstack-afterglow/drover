# Staging verification boundary

The repository validates unit behavior locally. The `Staging Gate` workflow verifies a separately deployed live Drover service; it does not install or reconfigure Kolla from a GitHub-hosted runner. Live checks are intentionally gated behind `DROVER_INTEGRATION_CLOUD=1` and a GitHub `staging` environment:

- The triggering CI revision is checked out exactly, while concurrency prevents overlapping cluster lifecycles.
- Packaged Kolla assets are validated locally; the live `drover` Keystone catalog entry and public `/v1/health/live` endpoint prove the external deployment separately.
- A dedicated project, network, subnet, image, flavor, volume availability zone, and external network must be configured for disposable tests.
- Drover resource policies must be pinned to those staging resources, including the HA load-balancer subnet, and `k3s.version` must be configured.
- A one-master lifecycle exercises create, scale, inventory, delete, and fail-closed cleanup.
- A three-master lifecycle verifies Nova, Cinder, Neutron, and Octavia inventory before delete and cleanup.

The workflow requires HTTPS `OS_AUTH_URL` and `DROVER_API_URL`, `OS_USERNAME`, `OS_PASSWORD`, `OS_PROJECT_NAME` or `OS_PROJECT_ID`, `DROVER_INTEGRATION_NETWORK_ID`, `DROVER_INTEGRATION_SUBNET_ID`, `DROVER_INTEGRATION_IMAGE_ID`, `DROVER_INTEGRATION_FLAVOR_ID`, `DROVER_INTEGRATION_EXTERNAL_NET_ID`, and `DROVER_INTEGRATION_VOLUME_AZ`. Credentials must be scoped to the isolated staging project and carry the Drover admin policy required by inventory assertions.

Local readiness smoke proves database, Redis, and migration-ledger readiness. It deliberately reports `503` while Keystone is unavailable; a `200` readiness response requires reachable configured Keystone service credentials.
