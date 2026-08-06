"""Drover service descriptor for openstacksdk."""

from openstack import service_description

from drover_sdk.proxy import Proxy


class DroverService(service_description.ServiceDescription):
    def __init__(self):
        super().__init__("drover", supported_versions={"1": Proxy})
