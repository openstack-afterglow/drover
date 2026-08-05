"""Pydantic schemas for Drover API requests and responses."""

from __future__ import annotations

import ipaddress
import re
import uuid
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator

_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$")
_NODE_TOKEN_RE = re.compile(r"^[A-Za-z0-9:_+/=.\-]{8,512}$")
_K8S_LABEL_NAME_RE = re.compile(r"^([a-zA-Z0-9][a-zA-Z0-9\-.]{0,252}/)?[a-zA-Z0-9][a-zA-Z0-9\-._]{0,62}$")
_K8S_LABEL_VALUE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-._]{0,62}$|^$")
_TAINT_EFFECTS = {"NoSchedule", "PreferNoSchedule", "NoExecute"}
_VALID_OS_TYPES = {"ubuntu", "fcos"}
_VALID_NG_ROLES = {"server", "agent"}


class K3sNodeHealth(BaseModel):
    name: str
    role: str
    ready: bool
    conditions: list[str] = Field(default_factory=list)
    kubelet_version: str | None = None


class K3sClusterHealth(BaseModel):
    cluster_id: str
    cluster_name: str
    status: str
    api_server_reachable: bool
    healthz_ok: bool
    nodes: list[K3sNodeHealth] = Field(default_factory=list)
    checked_at: str
    error: str | None = None
    reachability: str


class K3sProgressStep(str, Enum):
    SECURITY_GROUP = "security_group"
    SERVER_VOLUME = "server_volume"
    SERVER_CREATING = "server_creating"
    WAITING_CALLBACK = "waiting_callback"
    COMPLETED = "completed"
    FAILED = "failed"
    SERVER_HA_BOOTSTRAP = "server_ha_bootstrap"
    SERVER_HA_JOIN = "server_ha_join"
    ROTATE_DISCOVER = "rotate_discover"
    ROTATE_SERVER = "rotate_server"
    ROTATE_AGENT = "rotate_agent"
    ROTATE_VERIFY = "rotate_verify"
    DELETE_INIT = "delete_init"
    DELETE_LB_CLEANUP = "delete_lb_cleanup"
    DELETE_APP_CREDENTIAL = "delete_app_credential"
    DELETE_K8S_NODES = "delete_k8s_nodes"
    DELETE_AGENT_VMS = "delete_agent_vms"
    DELETE_SERVER_VM = "delete_server_vm"
    DELETE_SECURITY_GROUP = "delete_security_group"
    DELETE_RECORD = "delete_record"


class K3sProgressMessage(BaseModel):
    step: K3sProgressStep
    progress: int
    message: str
    cluster_id: str | None = None
    error: str | None = None
    elapsed_seconds: float | None = None


class CreateK3sClusterRequest(BaseModel):
    name: str = ""
    agent_count: int = Field(default=1, ge=0, le=10)
    agent_flavor_id: str | None = None
    network_id: str | None = None
    key_name: str | None = None
    os_type: str = "ubuntu"
    allowed_cidrs: list[str] | None = Field(default=None, max_length=20)
    template_id: str | None = None
    master_count: int = Field(default=1)
    stampede_enabled: bool = False

    @field_validator("master_count")
    @classmethod
    def validate_master_count(cls, v: int) -> int:
        if v not in (1, 3):
            raise ValueError("master_count는 1 또는 3이어야 합니다")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v.strip():
            return f"k3s-{uuid.uuid4().hex[:8]}"
        if not _NAME_RE.match(v):
            raise ValueError("이름은 영문/숫자로 시작하고, 영문·숫자·하이픈·언더스코어만 허용됩니다 (최대 63자)")
        return v

    @field_validator("os_type")
    @classmethod
    def validate_os_type(cls, v: str) -> str:
        if v not in _VALID_OS_TYPES:
            raise ValueError(f"os_type은 {sorted(_VALID_OS_TYPES)} 중 하나여야 합니다")
        return v

    @field_validator("allowed_cidrs")
    @classmethod
    def validate_allowed_cidrs(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        validated: list[str] = []
        for raw in v:
            if not isinstance(raw, str):
                raise ValueError("allowed_cidrs 의 각 항목은 문자열이어야 합니다")
            try:
                net = ipaddress.ip_network(raw, strict=False)
            except (ValueError, TypeError) as e:
                raise ValueError(f"allowed_cidrs '{raw}' 가 유효한 CIDR 이 아닙니다: {e}") from e
            validated.append(str(net))
        return validated


class K3sClusterInfo(BaseModel):
    id: str
    project_id: str
    name: str
    status: str
    status_reason: str | None = None
    server_vm_id: str | None = None
    agent_vm_ids: list[str] = Field(default_factory=list)
    agent_count: int = 0
    api_address: str | None = None
    server_ip: str | None = None
    network_id: str | None = None
    key_name: str | None = None
    k3s_version: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    deleted_at: str | None = None
    deleted_by_user_id: str | None = None
    deleted_reason: str | None = None
    occm_enabled: bool = False
    plugins_enabled: dict[str, bool] = Field(default_factory=dict)
    health_status: str | None = None
    api_lb_id: str | None = None
    api_fip_id: str | None = None
    api_fip_address: str | None = None
    os_type: str | None = None
    master_count: int = 1
    stampede_enabled: bool = False


class K3sClusterInfoDeleted(K3sClusterInfo):
    deleted_at: str | None = None
    deleted_by_user_id: str | None = None
    deleted_reason: str | None = None


class ScaleK3sClusterRequest(BaseModel):
    agent_count: int = Field(ge=0, le=10)


class K3sCallbackRequest(BaseModel):
    token: str = Field(min_length=8, max_length=128)
    success: bool
    kubeconfig: str | None = Field(default=None, max_length=65536)
    node_token: str | None = Field(default=None, max_length=512)
    server_ip: str | None = Field(default=None, max_length=64)
    error: str | None = Field(default=None, max_length=2048)
    occm_status: str | None = Field(default=None, max_length=64)
    plugin_status: dict[str, str | dict] | None = None
    secret_cloud_config_status: str | None = Field(default=None, max_length=64)

    @field_validator("node_token")
    @classmethod
    def validate_node_token(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not _NODE_TOKEN_RE.match(v):
            raise ValueError("node_token 형식이 올바르지 않습니다 (영숫자 + :_+/=.- 만 허용)")
        return v

    @field_validator("server_ip")
    @classmethod
    def validate_server_ip(cls, v: str | None) -> str | None:
        if v is None or not v:
            return v
        try:
            ipaddress.ip_address(v)
        except ValueError as e:
            raise ValueError(f"server_ip '{v}' 가 유효한 IP 주소가 아닙니다") from e
        return v


class K3sAttachInterfaceRequest(BaseModel):
    net_id: str


class K3sInterfaceInfo(BaseModel):
    port_id: str
    net_id: str
    fixed_ips: list[dict]
    vm_id: str
    node_role: str
    is_primary: bool


class ConfigMapInfo(BaseModel):
    name: str
    namespace: str
    data: dict[str, str] = Field(default_factory=dict)
    binary_data: dict[str, str] | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    created_at: str = ""


class ConfigMapCreateRequest(BaseModel):
    name: str
    data: dict[str, str] = Field(default_factory=dict)
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None


class ConfigMapWriteRequest(BaseModel):
    data: dict[str, str] = Field(default_factory=dict)
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None
    binary_data: dict[str, str] | None = None


class SecretInfo(BaseModel):
    name: str
    namespace: str
    type: str = "Opaque"
    data: dict[str, str] = Field(default_factory=dict)
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    created_at: str = ""


class SecretCreateRequest(BaseModel):
    name: str
    type: str = "Opaque"
    data: dict[str, str] = Field(default_factory=dict)
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None


class SecretWriteRequest(BaseModel):
    type: str = "Opaque"
    data: dict[str, str] = Field(default_factory=dict)
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None


class K3sClusterTemplateInfo(BaseModel):
    id: str
    name: str
    description: str | None = None
    k3s_version: str | None = None
    default_node_count: int = 1
    default_agent_flavor_id: str | None = None
    default_image_id: str | None = None
    plugins_enabled: dict[str, bool] = Field(default_factory=dict)
    os_type: str = "ubuntu"
    public_visible: bool = True
    created_by: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class CreateK3sClusterTemplateRequest(BaseModel):
    name: str
    description: str | None = None
    k3s_version: str | None = None
    default_node_count: int = Field(default=1, ge=0, le=20)
    default_agent_flavor_id: str | None = None
    default_image_id: str | None = None
    plugins_enabled: dict[str, bool] | None = None
    os_type: str = "ubuntu"
    public_visible: bool = True

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError("이름은 영문/숫자로 시작하고, 영문·숫자·하이픈·언더스코어만 허용됩니다 (최대 63자)")
        return v

    @field_validator("os_type")
    @classmethod
    def validate_os_type(cls, v: str) -> str:
        if v not in _VALID_OS_TYPES:
            raise ValueError(f"os_type은 {sorted(_VALID_OS_TYPES)} 중 하나여야 합니다")
        return v


class UpdateK3sClusterTemplateRequest(BaseModel):
    description: str | None = None
    k3s_version: str | None = None
    default_node_count: int | None = Field(default=None, ge=0, le=20)
    default_agent_flavor_id: str | None = None
    default_image_id: str | None = None
    plugins_enabled: dict[str, bool] | None = None
    os_type: str | None = None
    public_visible: bool | None = None

    @field_validator("os_type")
    @classmethod
    def validate_os_type(cls, v: str | None) -> str | None:
        if v is not None and v not in _VALID_OS_TYPES:
            raise ValueError(f"os_type은 {sorted(_VALID_OS_TYPES)} 중 하나여야 합니다")
        return v


class K3sNodegroupVMInfo(BaseModel):
    vm_id: str
    name: str | None = None
    status: str = "CREATING"


class K3sNodegroupInfo(BaseModel):
    id: str
    cluster_id: str
    name: str
    role: str = "agent"
    node_count: int = 0
    flavor_id: str | None = None
    image_id: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    taints: list[dict] = Field(default_factory=list)
    is_default: bool = False
    stampede_enabled: bool = False
    min_size: int = 0
    max_size: int = 5
    stampede_state: dict = Field(default_factory=dict)
    vms: list[K3sNodegroupVMInfo] = Field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None


class CreateK3sNodegroupRequest(BaseModel):
    name: str
    role: str = "agent"
    node_count: int = Field(default=0, ge=0, le=20)
    flavor_id: str | None = None
    image_id: str | None = None
    labels: dict[str, str] | None = None
    taints: list[dict] | None = None
    stampede_enabled: bool = False
    min_size: int = Field(default=0, ge=0)
    max_size: int = Field(default=5, ge=0)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError("이름은 영문/숫자로 시작하고, 영문·숫자·하이픈·언더스코어만 허용됩니다 (최대 63자)")
        return v

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        if v not in _VALID_NG_ROLES:
            raise ValueError(f"role은 {sorted(_VALID_NG_ROLES)} 중 하나여야 합니다")
        return v

    @field_validator("labels", mode="before")
    @classmethod
    def validate_labels(cls, v):
        if v is None:
            return v
        for k, val in v.items():
            if not _K8S_LABEL_NAME_RE.match(str(k)):
                raise ValueError(f"유효하지 않은 라벨 키: {k!r}")
            if not _K8S_LABEL_VALUE_RE.match(str(val)):
                raise ValueError(f"유효하지 않은 라벨 값: {val!r}")
        return v

    @field_validator("taints", mode="before")
    @classmethod
    def validate_taints(cls, v):
        if v is None:
            return v
        for taint in v:
            if isinstance(taint, dict):
                key = str(taint.get("key", ""))
                value = str(taint.get("value", ""))
                effect = str(taint.get("effect", ""))
                if key and not _K8S_LABEL_NAME_RE.match(key):
                    raise ValueError(f"유효하지 않은 taint 키: {key!r}")
                if value and not _K8S_LABEL_VALUE_RE.match(value):
                    raise ValueError(f"유효하지 않은 taint 값: {value!r}")
                if effect and effect not in _TAINT_EFFECTS:
                    raise ValueError(f"유효하지 않은 taint effect: {effect!r} (허용: {sorted(_TAINT_EFFECTS)})")
            elif isinstance(taint, str):
                if not re.match(r"^[a-zA-Z0-9\-._/]+(=[a-zA-Z0-9\-._]*)?:[a-zA-Z]+$", taint):
                    raise ValueError(f"유효하지 않은 taint 형식: {taint!r}")
        return v

    @model_validator(mode="after")
    def validate_scalable_agent_group(self) -> CreateK3sNodegroupRequest:
        if self.role == "server":
            raise ValueError("커스텀 server 노드그룹은 아직 지원되지 않습니다")
        if self.stampede_enabled and not self.flavor_id:
            raise ValueError("Stampede 노드그룹은 flavor_id가 필요합니다")
        if self.min_size > self.max_size:
            raise ValueError("min_size는 max_size보다 클 수 없습니다")
        return self


class UpdateK3sNodegroupRequest(BaseModel):
    node_count: int | None = Field(default=None, ge=0, le=20)
    flavor_id: str | None = None
    image_id: str | None = None
    labels: dict[str, str] | None = None
    taints: list[dict] | None = None
    stampede_enabled: bool | None = None
    min_size: int | None = Field(default=None, ge=0)
    max_size: int | None = Field(default=None, ge=0)

    @field_validator("labels", mode="before")
    @classmethod
    def validate_labels(cls, v):
        if v is None:
            return v
        for k, val in v.items():
            if not _K8S_LABEL_NAME_RE.match(str(k)):
                raise ValueError(f"유효하지 않은 라벨 키: {k!r}")
            if not _K8S_LABEL_VALUE_RE.match(str(val)):
                raise ValueError(f"유효하지 않은 라벨 값: {val!r}")
        return v

    @field_validator("taints", mode="before")
    @classmethod
    def validate_taints(cls, v):
        if v is None:
            return v
        for taint in v:
            if isinstance(taint, dict):
                key = str(taint.get("key", ""))
                value = str(taint.get("value", ""))
                effect = str(taint.get("effect", ""))
                if key and not _K8S_LABEL_NAME_RE.match(key):
                    raise ValueError(f"유효하지 않은 taint 키: {key!r}")
                if value and not _K8S_LABEL_VALUE_RE.match(value):
                    raise ValueError(f"유효하지 않은 taint 값: {value!r}")
                if effect and effect not in _TAINT_EFFECTS:
                    raise ValueError(f"유효하지 않은 taint effect: {effect!r} (허용: {sorted(_TAINT_EFFECTS)})")
            elif isinstance(taint, str):
                if not re.match(r"^[a-zA-Z0-9\-._/]+(=[a-zA-Z0-9\-._]*)?:[a-zA-Z]+$", taint):
                    raise ValueError(f"유효하지 않은 taint 형식: {taint!r}")
        return v

    @model_validator(mode="after")
    def validate_stampede_sizes(self) -> UpdateK3sNodegroupRequest:
        if self.stampede_enabled is True and self.flavor_id == "":
            raise ValueError("Stampede 노드그룹은 flavor_id가 필요합니다")
        if self.min_size is not None and self.max_size is not None and self.min_size > self.max_size:
            raise ValueError("min_size는 max_size보다 클 수 없습니다")
        return self


class CertificateInfo(BaseModel):
    not_after: str
    not_before: str
    subject: str
    issuer: str
    days_remaining: int


class CertificateExpiryResponse(BaseModel):
    ca: CertificateInfo | None = None
    client: CertificateInfo | None = None
    server_via_tls: list[CertificateInfo] = Field(default_factory=list)


class ContainerStatus(BaseModel):
    name: str
    image: str
    ready: bool = False
    restart_count: int = 0
    state: str = ""


class PodInfo(BaseModel):
    name: str
    namespace: str
    phase: str = ""
    ready: str = ""
    restarts: int = 0
    node: str | None = None
    pod_ip: str | None = None
    containers: list[ContainerStatus] = Field(default_factory=list)
    labels: dict[str, str] = Field(default_factory=dict)
    created_at: str = ""


class ServicePort(BaseModel):
    name: str | None = None
    port: int
    target_port: int | str | None = None
    node_port: int | None = None
    protocol: str = "TCP"


class ServiceInfo(BaseModel):
    name: str
    namespace: str
    type: str = "ClusterIP"
    cluster_ip: str | None = None
    external_ips: list[str] = Field(default_factory=list)
    ports: list[ServicePort] = Field(default_factory=list)
    selector: dict[str, str] = Field(default_factory=dict)
    created_at: str = ""


class DeploymentInfo(BaseModel):
    name: str
    namespace: str
    replicas: int = 0
    available: int = 0
    ready: int = 0
    updated: int = 0
    strategy: str = ""
    selector: dict[str, str] = Field(default_factory=dict)
    images: list[str] = Field(default_factory=list)
    created_at: str = ""


class ReplicaSetInfo(BaseModel):
    name: str
    namespace: str
    replicas: int = 0
    ready: int = 0
    available: int = 0
    owner_kind: str | None = None
    owner_name: str | None = None
    selector: dict[str, str] = Field(default_factory=dict)
    images: list[str] = Field(default_factory=list)
    created_at: str = ""


class ScaleDeploymentRequest(BaseModel):
    replicas: int = Field(ge=0, le=100)


class PodLogResponse(BaseModel):
    name: str
    namespace: str
    container: str | None = None
    log: str
