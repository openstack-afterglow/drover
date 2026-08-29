"""OpenStack data types used by OpenStack API wrappers."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class VolumeInfo(BaseModel):
    id: str
    name: str
    status: str
    size_gb: int
    volume_type: str | None = None
    bootable: bool = False
    attachments: list[dict] = Field(default_factory=list)
    created_at: str | None = None


class FlavorInfo(BaseModel):
    id: str
    name: str
    vcpus: int
    ram: int = 0
    disk: int = 0
    ram_mb: int = 0
    disk_gb: int = 0
    is_public: bool = True
    extra_specs: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _sync_ram_disk(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        values = dict(data)
        if "ram" in values and "ram_mb" not in values:
            values["ram_mb"] = values["ram"]
        elif "ram_mb" in values and "ram" not in values:
            values["ram"] = values["ram_mb"]
        if "disk" in values and "disk_gb" not in values:
            values["disk_gb"] = values["disk"]
        elif "disk_gb" in values and "disk" not in values:
            values["disk"] = values["disk_gb"]
        return values


class IpAddress(BaseModel):
    addr: str
    version: int = 4
    type: str = "fixed"
    mac_addr: str | None = None


class InstanceInfo(BaseModel):
    id: str
    name: str
    status: str
    flavor_id: str
    image_id: str | None = None
    addresses: dict[str, list[IpAddress]] = Field(default_factory=dict)
    key_name: str | None = None
    created_at: str | None = None
    task_state: str | None = None
    power_state: int | None = None


class ImageDetail(BaseModel):
    id: str
    name: str
    status: str
    size_bytes: int | None = None
    min_disk_gb: int = 0
    min_ram_mb: int = 0
    visibility: str = "private"
    created_at: str | None = None


class ImageInfo(BaseModel):
    id: str
    name: str
    status: str
    visibility: str = "private"


class FloatingIpInfo(BaseModel):
    id: str
    floating_ip_address: str
    fixed_ip_address: str | None = None
    status: str = ""
    port_id: str | None = None
    floating_network_id: str
    project_id: str | None = None
    instance_id: str | None = None
    instance_name: str | None = None


class NetworkInfo(BaseModel):
    id: str
    name: str
    status: str
    subnets: list[str] = Field(default_factory=list)
    is_external: bool = False
    is_shared: bool = False


class SubnetDetail(BaseModel):
    id: str
    name: str
    cidr: str
    gateway_ip: str | None = None
    dhcp_enabled: bool = True


class RouterInfo(BaseModel):
    id: str
    name: str
    status: str = ""
    project_id: str | None = None
    external_gateway_network_id: str | None = None
    connected_subnet_ids: list[str] = Field(default_factory=list)


class RouterInterface(BaseModel):
    id: str
    subnet_id: str
    subnet_name: str
    network_id: str
    ip_address: str


class RouterDetail(BaseModel):
    id: str
    name: str
    status: str
    project_id: str | None = None
    external_gateway_network_id: str | None = None
    external_gateway_network_name: str | None = None
    interfaces: list[RouterInterface] = Field(default_factory=list)


class NetworkDetail(BaseModel):
    id: str
    name: str
    status: str
    subnets: list[str] = Field(default_factory=list)
    is_external: bool = False
    is_shared: bool = False
    subnet_details: list[SubnetDetail] = Field(default_factory=list)
    routers: list[RouterInfo] = Field(default_factory=list)


class TopologyInstance(BaseModel):
    id: str
    name: str
    status: str
    project_id: str | None = None
    network_names: list[str] = Field(default_factory=list)
    ip_addresses: list[dict] = Field(default_factory=list)


class TopologyRouter(BaseModel):
    id: str
    name: str
    status: str
    external_gateway_network_id: str | None = None
    external_gateway_ips: list[str] = Field(default_factory=list)
    interface_ips: list[dict] = Field(default_factory=list)
    is_distributed: bool = False
    is_ha: bool = False
    connected_subnet_ids: list[str] = Field(default_factory=list)
    dvr_subnet_ids: list[str] = Field(default_factory=list)
    project_id: str | None = None


class TopologyNetwork(BaseModel):
    id: str
    name: str
    status: str
    is_external: bool = False
    is_shared: bool = False
    project_id: str | None = None
    subnet_details: list[SubnetDetail] = Field(default_factory=list)


class TopologyData(BaseModel):
    networks: list[TopologyNetwork] = Field(default_factory=list)
    routers: list[TopologyRouter] = Field(default_factory=list)
    instances: list[TopologyInstance] = Field(default_factory=list)
    floating_ips: list[FloatingIpInfo] = Field(default_factory=list)
