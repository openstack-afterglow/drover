"""Drover service descriptor for openstacksdk."""

from openstack import service_description

from drover_sdk.proxy import Proxy


class DroverService(service_description.ServiceDescription):
    """Expose catalog type ``container-infra`` as ``conn.drover``."""

    def __init__(self):
        super().__init__(
            "container-infra",
            supported_versions={"1": Proxy},
            aliases=["drover"],
        )
