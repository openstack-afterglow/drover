from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from drover_sdk import register
from drover_sdk.proxy import Proxy
from drover_sdk.service import DroverService


def _response(status_code=200, *, payload=None, text=""):
    content = text.encode() if text else (b"json" if payload is not None else b"")
    return SimpleNamespace(
        status_code=status_code,
        content=content,
        text=text,
        json=lambda: payload,
    )


# Every non-text, non-streaming route: (method_name, args, kwargs, http_method, path, body, params).
# `body` is the expected JSON body kwarg (None when the call carries none); `params` is the
# expected query-string kwarg (None when the call carries no filters).
JSON_METHOD_TABLE = [
    ("clusters", (), {}, "GET", "/v1/clusters", None, None),
    ("clusters", (), {"include_deleted": True}, "GET", "/v1/clusters", None, {"include_deleted": True}),
    ("get_cluster", ("cluster-1",), {}, "GET", "/v1/clusters/cluster-1", None, None),
    (
        "scale_cluster",
        ("cluster-1",),
        {"agent_count": 3},
        "PATCH",
        "/v1/clusters/cluster-1/scale",
        {"agent_count": 3},
        None,
    ),
    ("delete_cluster", ("cluster-1",), {}, "DELETE", "/v1/clusters/cluster-1", None, None),
    (
        "node_interfaces",
        ("cluster-1", "vm-1"),
        {},
        "GET",
        "/v1/clusters/cluster-1/nodes/vm-1/interfaces",
        None,
        None,
    ),
    (
        "attach_node_interface",
        ("cluster-1", "vm-1"),
        {"net_id": "net-1"},
        "POST",
        "/v1/clusters/cluster-1/nodes/vm-1/interfaces",
        {"net_id": "net-1"},
        None,
    ),
    (
        "detach_node_interface",
        ("cluster-1", "vm-1", "port-1"),
        {},
        "DELETE",
        "/v1/clusters/cluster-1/nodes/vm-1/interfaces/port-1",
        None,
        None,
    ),
    ("enable_stampede", ("cluster-1",), {}, "POST", "/v1/clusters/cluster-1/stampede/enable", None, None),
    ("disable_stampede", ("cluster-1",), {}, "POST", "/v1/clusters/cluster-1/stampede/disable", None, None),
    ("stampede_status", ("cluster-1",), {}, "GET", "/v1/clusters/cluster-1/stampede", None, None),
    (
        "stampede_events",
        ("cluster-1",),
        {"limit": 10},
        "GET",
        "/v1/clusters/cluster-1/stampede/events",
        None,
        {"limit": 10},
    ),
    ("clusters_health", (), {}, "GET", "/v1/clusters/health", None, None),
    ("cluster_health", ("cluster-1",), {}, "GET", "/v1/clusters/cluster-1/health", None, None),
    ("check_cluster_health", ("cluster-1",), {}, "POST", "/v1/clusters/cluster-1/health/check", None, None),
    (
        "certificate_expiry",
        ("cluster-1",),
        {},
        "GET",
        "/v1/clusters/cluster-1/certificate-expiry",
        None,
        None,
    ),
    ("create_shell_ticket", ("cluster-1",), {}, "POST", "/v1/clusters/cluster-1/shell-ticket", None, None),
    ("namespaces", ("cluster-1",), {}, "GET", "/v1/clusters/cluster-1/namespaces", None, None),
    (
        "configmaps",
        ("cluster-1",),
        {"namespace": "kube-system"},
        "GET",
        "/v1/clusters/cluster-1/configmaps",
        None,
        {"namespace": "kube-system"},
    ),
    (
        "get_configmap",
        ("cluster-1", "default", "cm-1"),
        {},
        "GET",
        "/v1/clusters/cluster-1/namespaces/default/configmaps/cm-1",
        None,
        None,
    ),
    (
        "create_configmap",
        ("cluster-1", "default"),
        {"data": {"a": "b"}},
        "POST",
        "/v1/clusters/cluster-1/namespaces/default/configmaps",
        {"data": {"a": "b"}},
        None,
    ),
    (
        "update_configmap",
        ("cluster-1", "default", "cm-1"),
        {"data": {"a": "c"}},
        "PUT",
        "/v1/clusters/cluster-1/namespaces/default/configmaps/cm-1",
        {"data": {"a": "c"}},
        None,
    ),
    (
        "delete_configmap",
        ("cluster-1", "default", "cm-1"),
        {},
        "DELETE",
        "/v1/clusters/cluster-1/namespaces/default/configmaps/cm-1",
        None,
        None,
    ),
    (
        "secrets",
        ("cluster-1",),
        {"namespace": "kube-system"},
        "GET",
        "/v1/clusters/cluster-1/secrets",
        None,
        {"namespace": "kube-system"},
    ),
    (
        "get_secret",
        ("cluster-1", "default", "sec-1"),
        {},
        "GET",
        "/v1/clusters/cluster-1/namespaces/default/secrets/sec-1",
        None,
        None,
    ),
    (
        "create_secret",
        ("cluster-1", "default"),
        {"data": {"a": "b"}},
        "POST",
        "/v1/clusters/cluster-1/namespaces/default/secrets",
        {"data": {"a": "b"}},
        None,
    ),
    (
        "update_secret",
        ("cluster-1", "default", "sec-1"),
        {"data": {"a": "c"}},
        "PUT",
        "/v1/clusters/cluster-1/namespaces/default/secrets/sec-1",
        {"data": {"a": "c"}},
        None,
    ),
    (
        "delete_secret",
        ("cluster-1", "default", "sec-1"),
        {},
        "DELETE",
        "/v1/clusters/cluster-1/namespaces/default/secrets/sec-1",
        None,
        None,
    ),
    (
        "pods",
        ("cluster-1", "default"),
        {},
        "GET",
        "/v1/clusters/cluster-1/namespaces/default/pods",
        None,
        None,
    ),
    (
        "delete_pod",
        ("cluster-1", "default", "pod-1"),
        {},
        "DELETE",
        "/v1/clusters/cluster-1/namespaces/default/pods/pod-1",
        None,
        None,
    ),
    (
        "pod_log",
        ("cluster-1", "default", "pod-1"),
        {"tail_lines": 100, "container": "main"},
        "GET",
        "/v1/clusters/cluster-1/namespaces/default/pods/pod-1/log",
        None,
        {"tail_lines": 100, "container": "main"},
    ),
    (
        "services",
        ("cluster-1", "default"),
        {},
        "GET",
        "/v1/clusters/cluster-1/namespaces/default/services",
        None,
        None,
    ),
    (
        "delete_service",
        ("cluster-1", "default", "svc-1"),
        {},
        "DELETE",
        "/v1/clusters/cluster-1/namespaces/default/services/svc-1",
        None,
        None,
    ),
    (
        "deployments",
        ("cluster-1", "default"),
        {},
        "GET",
        "/v1/clusters/cluster-1/namespaces/default/deployments",
        None,
        None,
    ),
    (
        "replicasets",
        ("cluster-1", "default"),
        {},
        "GET",
        "/v1/clusters/cluster-1/namespaces/default/replicasets",
        None,
        None,
    ),
    (
        "restart_deployment",
        ("cluster-1", "default", "dep-1"),
        {},
        "POST",
        "/v1/clusters/cluster-1/namespaces/default/deployments/dep-1/restart",
        None,
        None,
    ),
    (
        "scale_deployment",
        ("cluster-1", "default", "dep-1"),
        {"replicas": 3},
        "PATCH",
        "/v1/clusters/cluster-1/namespaces/default/deployments/dep-1/scale",
        {"replicas": 3},
        None,
    ),
    ("nodegroups", ("cluster-1",), {}, "GET", "/v1/clusters/cluster-1/nodegroups", None, None),
    (
        "get_nodegroup",
        ("cluster-1", "ng-1"),
        {},
        "GET",
        "/v1/clusters/cluster-1/nodegroups/ng-1",
        None,
        None,
    ),
    (
        "create_nodegroup",
        ("cluster-1",),
        {"name": "gpu"},
        "POST",
        "/v1/clusters/cluster-1/nodegroups",
        {"name": "gpu"},
        None,
    ),
    (
        "update_nodegroup",
        ("cluster-1", "ng-1"),
        {"node_count": 5},
        "PATCH",
        "/v1/clusters/cluster-1/nodegroups/ng-1",
        {"node_count": 5},
        None,
    ),
    (
        "delete_nodegroup",
        ("cluster-1", "ng-1"),
        {},
        "DELETE",
        "/v1/clusters/cluster-1/nodegroups/ng-1",
        None,
        None,
    ),
    ("cluster_templates", (), {}, "GET", "/v1/cluster-templates", None, None),
    ("get_cluster_template", ("tmpl-1",), {}, "GET", "/v1/cluster-templates/tmpl-1", None, None),
    (
        "create_cluster_template",
        (),
        {"name": "gpu-template"},
        "POST",
        "/v1/cluster-templates",
        {"name": "gpu-template"},
        None,
    ),
    (
        "update_cluster_template",
        ("tmpl-1",),
        {"name": "renamed"},
        "PATCH",
        "/v1/cluster-templates/tmpl-1",
        {"name": "renamed"},
        None,
    ),
    ("delete_cluster_template", ("tmpl-1",), {}, "DELETE", "/v1/cluster-templates/tmpl-1", None, None),
    ("cluster_stats", (), {}, "GET", "/v1/stats/clusters", None, None),
    ("admin_clusters", (), {}, "GET", "/v1/admin/clusters", None, None),
    (
        "admin_clusters",
        (),
        {"status": "CREATE_IN_PROGRESS,UPDATE_IN_PROGRESS,ERROR"},
        "GET",
        "/v1/admin/clusters",
        None,
        {"status": "CREATE_IN_PROGRESS,UPDATE_IN_PROGRESS,ERROR"},
    ),
    ("admin_cluster", ("cluster-1",), {}, "GET", "/v1/admin/clusters/cluster-1", None, None),
    (
        "admin_scale_cluster",
        ("cluster-1",),
        {"agent_count": 4},
        "PATCH",
        "/v1/admin/clusters/cluster-1/scale",
        {"agent_count": 4},
        None,
    ),
    ("admin_delete_cluster", ("cluster-1",), {}, "DELETE", "/v1/admin/clusters/cluster-1", None, None),
    (
        "admin_certificate_expiry",
        ("cluster-1",),
        {},
        "GET",
        "/v1/admin/clusters/cluster-1/certificate-expiry",
        None,
        None,
    ),
    ("admin_cluster_templates", (), {}, "GET", "/v1/admin/cluster-templates", None, None),
    ("resource_policies", (), {}, "GET", "/v1/admin/resource-policies", None, None),
    (
        "resource_policy_catalog",
        ("k3s.server_image",),
        {},
        "GET",
        "/v1/admin/resource-policies/catalog/k3s.server_image",
        None,
        None,
    ),
    (
        "update_resource_policy",
        ("k3s.server_image",),
        {"resource_id": "image-1"},
        "PUT",
        "/v1/admin/resource-policies/k3s.server_image",
        {"resource_id": "image-1"},
        None,
    ),
    ("runtime_settings", (), {}, "GET", "/v1/admin/runtime-settings", None, None),
    (
        "update_runtime_setting",
        ("k3s.version",),
        {"value": "v1.31.5+k3s1"},
        "PUT",
        "/v1/admin/runtime-settings/k3s.version",
        {"value": "v1.31.5+k3s1"},
        None,
    ),
    ("effective_gpu_quotas", (), {}, "GET", "/v1/gpu-quotas/effective", None, None),
    ("gpu_quota_status", (), {}, "GET", "/v1/gpu-quotas/status", None, None),
    (
        "check_gpu_quota",
        (),
        {"extra_specs": {"pci_passthrough:alias": "RTX3090:1"}},
        "POST",
        "/v1/gpu-quotas/check",
        {"extra_specs": {"pci_passthrough:alias": "RTX3090:1"}},
        None,
    ),
    ("default_gpu_quotas", (), {}, "GET", "/v1/admin/gpu-quotas/defaults", None, None),
    (
        "set_default_gpu_quota",
        ("RTX3090", 4),
        {},
        "PUT",
        "/v1/admin/gpu-quotas/defaults",
        {"gpu_type": "RTX3090", "limit": 4},
        None,
    ),
    ("delete_default_gpu_quota", ("RTX3090",), {}, "DELETE", "/v1/admin/gpu-quotas/defaults/RTX3090", None, None),
    ("project_gpu_quotas", ("proj-1",), {}, "GET", "/v1/admin/gpu-quotas/proj-1", None, None),
    (
        "set_project_gpu_quota",
        ("proj-1", "RTX3090", 2),
        {},
        "PUT",
        "/v1/admin/gpu-quotas/proj-1",
        {"gpu_type": "RTX3090", "limit": 2},
        None,
    ),
    (
        "delete_project_gpu_quota",
        ("proj-1", "RTX3090"),
        {},
        "DELETE",
        "/v1/admin/gpu-quotas/proj-1/RTX3090",
        None,
        None,
    ),
    (
        "callback",
        (),
        {"token": "tok-1", "success": True},
        "POST",
        "/v1/callback",
        {"token": "tok-1", "success": True},
        None,
    ),
]

# GET routes that return the raw (non-JSON) response body.
TEXT_METHOD_TABLE = [
    ("kubeconfig", ("cluster-1",), "/v1/clusters/cluster-1/kubeconfig"),
    ("ca_certificate", ("cluster-1",), "/v1/clusters/cluster-1/ca-certificate"),
    ("admin_kubeconfig", ("cluster-1",), "/v1/admin/clusters/cluster-1/kubeconfig"),
    ("admin_ca_certificate", ("cluster-1",), "/v1/admin/clusters/cluster-1/ca-certificate"),
]

# SSE routes that must issue the request with stream=True and hand back a line iterator.
STREAM_METHOD_TABLE = [
    ("create_cluster", (), "POST", "/v1/clusters/async"),
    ("delete_cluster_async", ("cluster-1",), "POST", "/v1/clusters/cluster-1/delete-async"),
    ("rotate_certs", ("cluster-1",), "POST", "/v1/clusters/cluster-1/rotate-certs"),
    ("admin_delete_cluster_async", ("cluster-1",), "POST", "/v1/admin/clusters/cluster-1/delete-async"),
    ("admin_rotate_certs", ("cluster-1",), "POST", "/v1/admin/clusters/cluster-1/rotate-certs"),
]

_PUBLIC_PROXY_METHODS = {
    name for name in vars(Proxy) if name != "request" and not name.startswith("_") and callable(getattr(Proxy, name))
}


def test_route_tables_cover_every_public_proxy_method():
    """Regression guard: every public Proxy method must appear in exactly one route table."""
    covered = {row[0] for row in JSON_METHOD_TABLE}
    covered |= {row[0] for row in TEXT_METHOD_TABLE}
    covered |= {row[0] for row in STREAM_METHOD_TABLE}
    assert covered == _PUBLIC_PROXY_METHODS


@pytest.mark.parametrize("method_name,args,kwargs,http_method,path,body,params", JSON_METHOD_TABLE)
def test_json_methods_issue_expected_request(method_name, args, kwargs, http_method, path, body, params):
    proxy = Proxy(session=MagicMock(), service_type="drover")
    status = 204 if http_method == "DELETE" else 200
    response = _response(status, payload={"ok": True}) if status != 204 else _response(204)
    proxy.request = MagicMock(return_value=response)

    result = getattr(proxy, method_name)(*args, **kwargs)

    expected_kwargs = {"raise_exc": True}
    if body is not None:
        expected_kwargs["json"] = body
    if params is not None:
        expected_kwargs["params"] = params
    proxy.request.assert_called_once_with(path, http_method, **expected_kwargs)
    assert result == ({"ok": True} if status != 204 else None)


@pytest.mark.parametrize("method_name,args,path", TEXT_METHOD_TABLE)
def test_text_methods_return_raw_body(method_name, args, path):
    proxy = Proxy(session=MagicMock(), service_type="drover")
    proxy.request = MagicMock(return_value=_response(200, text="apiVersion: v1\nkind: Config\n"))

    result = getattr(proxy, method_name)(*args)

    proxy.request.assert_called_once_with(path, "GET", raise_exc=True)
    assert result.startswith("apiVersion")


@pytest.mark.parametrize("method_name,args,http_method,path", STREAM_METHOD_TABLE)
def test_stream_methods_request_without_buffering(method_name, args, http_method, path):
    proxy = Proxy(session=MagicMock(), service_type="drover")
    lines = ['data: {"step": 1}', 'data: {"step": 2}']
    response = SimpleNamespace(iter_lines=MagicMock(return_value=iter(lines)))
    proxy.request = MagicMock(return_value=response)

    result = getattr(proxy, method_name)(*args)

    expected_kwargs = {"raise_exc": True, "stream": True}
    if method_name == "create_cluster":
        # create_cluster(**attrs) always forwards a (possibly empty) JSON body.
        expected_kwargs["json"] = {}
    proxy.request.assert_called_once_with(path, http_method, **expected_kwargs)
    response.iter_lines.assert_called_once_with(decode_unicode=True)
    assert list(result) == lines


def test_gpu_quota_identifiers_are_escaped_in_paths():
    proxy = Proxy(session=MagicMock(), service_type="drover")
    proxy.request = MagicMock(return_value=_response(200, payload={"ok": True}))

    proxy.delete_default_gpu_quota("RTX/3090")
    proxy.request.assert_called_with("/v1/admin/gpu-quotas/defaults/RTX%2F3090", "DELETE", raise_exc=True)

    proxy.project_gpu_quotas("proj/1")
    proxy.request.assert_called_with("/v1/admin/gpu-quotas/proj%2F1", "GET", raise_exc=True)

    proxy.set_project_gpu_quota("proj/1", "RTX/3090", 2)
    proxy.request.assert_called_with(
        "/v1/admin/gpu-quotas/proj%2F1", "PUT", raise_exc=True, json={"gpu_type": "RTX/3090", "limit": 2}
    )

    proxy.delete_project_gpu_quota("proj/1", "RTX/3090")
    proxy.request.assert_called_with("/v1/admin/gpu-quotas/proj%2F1/RTX%2F3090", "DELETE", raise_exc=True)

    proxy = Proxy(session=MagicMock(), service_type="drover")
    proxy.request = MagicMock(return_value=_response(200, payload={"ok": True}))

    proxy.get_cluster("cluster/../../other")

    proxy.request.assert_called_once_with(
        "/v1/clusters/cluster%2F..%2F..%2Fother",
        "GET",
        raise_exc=True,
    )


def test_gpu_quota_request_uses_catalog_relative_path(monkeypatch):
    request = MagicMock(return_value=_response(200, payload={"ok": True}))
    monkeypatch.setattr("openstack.proxy.Proxy.request", request)
    catalog_proxy = Proxy(session=MagicMock(), service_type="drover")

    result = catalog_proxy.check_gpu_quota({"pci_passthrough:alias": "GTX-1080ti:1"})

    request.assert_called_once_with(
        "/gpu-quotas/check",
        "POST",
        raise_exc=True,
        json={"extra_specs": {"pci_passthrough:alias": "GTX-1080ti:1"}},
    )
    assert result == {"ok": True}


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("/v1", "/"),
        ("/v1/", "/"),
        ("/v1/clusters", "/clusters"),
        ("/v1?detail=true", "/?detail=true"),
        ("/v1#versions", "/#versions"),
        ("/v10/clusters", "/v10/clusters"),
        ("https://drover.example/v1/clusters", "https://drover.example/v1/clusters"),
    ],
)
def test_request_normalizes_only_catalog_version_prefix(monkeypatch, url, expected):
    request = MagicMock(return_value=_response(200, payload={"ok": True}))
    monkeypatch.setattr("openstack.proxy.Proxy.request", request)
    catalog_proxy = Proxy(session=MagicMock(), service_type="drover")

    catalog_proxy.request(url, "GET", "request failed", True)

    request.assert_called_once_with(expected, "GET", "request failed", True)


def test_service_description_constructs_with_required_service_type():
    description = DroverService()
    assert description.service_type == "drover"
    assert description.supported_versions == {"1": Proxy}


def test_register_enables_non_official_service_before_attaching_proxy(monkeypatch):
    """Register enables Drover and applies its optional trusted endpoint override."""

    class Config:
        def __init__(self):
            self.enabled = set()

        def enable_service(self, service_type):
            self.enabled.add(service_type)

        def has_service(self, service_type):
            return service_type in self.enabled

    class Connection:
        def __init__(self):
            self.config = Config()
            self.drover = None

        def add_service(self, service):
            assert self.config.has_service(service.service_type)
            self.drover = Proxy(session=MagicMock(), service_type=service.service_type)

    monkeypatch.delenv("SERVICE_DROVER_INTERNAL_URL", raising=False)
    catalog_connection = Connection()
    catalog_proxy = register(catalog_connection)

    assert isinstance(catalog_proxy, Proxy)
    assert catalog_connection.config.has_service("drover")
    assert catalog_proxy.endpoint_override is None

    monkeypatch.setenv("SERVICE_DROVER_INTERNAL_URL", "http://drover-api:8011/")
    override_proxy = register(Connection())

    assert override_proxy.endpoint_override == "http://drover-api:8011"
