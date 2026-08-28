# Staging verification boundary

The repository validates unit behavior locally. The following checks require a Kolla/OpenStack staging environment and are intentionally gated behind `DROVER_INTEGRATION_CLOUD=1`:

- Kolla role deployment and `container-infra` Keystone catalog discovery at a Drover `/v1` endpoint.
- A disposable project/network/image/flavor with a one-master and a three-master cluster reaching `SUCCEEDED`.
- Tagged Nova, Cinder, Neutron, and Octavia inventory verification; bounded nodegroup scaling; recorded-resource cleanup after delete.
- Callback CIDR behavior through the deployed proxy and worker restart recovery after partial resource creation/callback receipt.

The staging workflow requires `OS_AUTH_URL`, `OS_USERNAME`, `OS_PASSWORD`, `OS_PROJECT_NAME` or `OS_PROJECT_ID`, `DROVER_INTEGRATION_NETWORK_ID`, `DROVER_INTEGRATION_IMAGE_ID`, `DROVER_INTEGRATION_FLAVOR_ID`, and `SERVICE_DROVER_INTERNAL_URL` or `DROVER_API_URL` in addition to `DROVER_INTEGRATION_CLOUD=1`.

Local readiness smoke proves database, Redis, and migration-ledger readiness. It deliberately reports `503` while Keystone is unavailable; a `200` readiness response requires reachable configured Keystone service credentials.
