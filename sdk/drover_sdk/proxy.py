"""OpenStack SDK proxy for Drover v1."""

from __future__ import annotations

from urllib.parse import quote

from openstack import proxy


def _segment(value: object) -> str:
    return quote(str(value), safe="")


def _query(**kwargs: object) -> dict | None:
    """Drop unset kwargs and return a query-param dict, or ``None`` if empty.

    Callers pass query filters as kwargs (e.g. ``include_deleted=True``,
    ``status="ERROR"``) — only explicitly-provided (non-``None``) values are
    forwarded, so server-side defaults are preserved when a filter is omitted.
    """
    filtered = {k: v for k, v in kwargs.items() if v is not None}
    return filtered or None


class Proxy(proxy.Proxy):
    def _json_request(self, method: str, path: str, *, body: dict | None = None, params: dict | None = None):
        kwargs: dict = {}
        if body is not None:
            kwargs["json"] = body
        if params:
            kwargs["params"] = params
        response = self.request(path, method, raise_exc=True, **kwargs)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    def _text_request(self, method: str, path: str, *, params: dict | None = None) -> str:
        kwargs = {"params": params} if params else {}
        response = self.request(path, method, raise_exc=True, **kwargs)
        return response.text

    def _stream_request(self, method: str, path: str, *, body: dict | None = None, params: dict | None = None):
        """Issue a streaming (SSE) request and return a line iterator.

        ``stream=True`` is forwarded through the SDK session so the response
        body is not buffered — each yielded item is one decoded line of the
        upstream ``text/event-stream``.
        """
        kwargs: dict = {"stream": True}
        if body is not None:
            kwargs["json"] = body
        if params:
            kwargs["params"] = params
        response = self.request(path, method, raise_exc=True, **kwargs)
        return response.iter_lines(decode_unicode=True)

    # -- Tenant clusters -----------------------------------------------

    def clusters(self, **query):
        return self._json_request("GET", "/v1/clusters", params=_query(**query))

    def get_cluster(self, cluster_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}")

    def kubeconfig(self, cluster_id):
        return self._text_request("GET", f"/v1/clusters/{_segment(cluster_id)}/kubeconfig")

    def create_cluster(self, **attrs):
        return self._stream_request("POST", "/v1/clusters/async", body=attrs)

    def scale_cluster(self, cluster_id, **attrs):
        return self._json_request("PATCH", f"/v1/clusters/{_segment(cluster_id)}/scale", body=attrs)

    def delete_cluster(self, cluster_id):
        return self._json_request("DELETE", f"/v1/clusters/{_segment(cluster_id)}")

    def delete_cluster_async(self, cluster_id):
        return self._stream_request("POST", f"/v1/clusters/{_segment(cluster_id)}/delete-async")

    def node_interfaces(self, cluster_id, vm_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/nodes/{_segment(vm_id)}/interfaces")

    def attach_node_interface(self, cluster_id, vm_id, **attrs):
        return self._json_request(
            "POST",
            f"/v1/clusters/{_segment(cluster_id)}/nodes/{_segment(vm_id)}/interfaces",
            body=attrs,
        )

    def detach_node_interface(self, cluster_id, vm_id, port_id):
        return self._json_request(
            "DELETE",
            f"/v1/clusters/{_segment(cluster_id)}/nodes/{_segment(vm_id)}/interfaces/{_segment(port_id)}",
        )

    def enable_stampede(self, cluster_id):
        return self._json_request("POST", f"/v1/clusters/{_segment(cluster_id)}/stampede/enable")

    def disable_stampede(self, cluster_id):
        return self._json_request("POST", f"/v1/clusters/{_segment(cluster_id)}/stampede/disable")

    def stampede_status(self, cluster_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/stampede")

    def stampede_events(self, cluster_id, **query):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/stampede/events", params=_query(**query))

    def clusters_health(self):
        return self._json_request("GET", "/v1/clusters/health")

    def cluster_health(self, cluster_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/health")

    def check_cluster_health(self, cluster_id):
        return self._json_request("POST", f"/v1/clusters/{_segment(cluster_id)}/health/check")

    def ca_certificate(self, cluster_id):
        return self._text_request("GET", f"/v1/clusters/{_segment(cluster_id)}/ca-certificate")

    def certificate_expiry(self, cluster_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/certificate-expiry")

    def rotate_certs(self, cluster_id):
        return self._stream_request("POST", f"/v1/clusters/{_segment(cluster_id)}/rotate-certs")

    def create_shell_ticket(self, cluster_id):
        return self._json_request("POST", f"/v1/clusters/{_segment(cluster_id)}/shell-ticket")

    # -- Kubernetes resources --------------------------------------------

    def namespaces(self, cluster_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/namespaces")

    def configmaps(self, cluster_id, **query):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/configmaps", params=_query(**query))

    def get_configmap(self, cluster_id, namespace, name):
        return self._json_request(
            "GET",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/configmaps/{_segment(name)}",
        )

    def create_configmap(self, cluster_id, namespace, **attrs):
        return self._json_request(
            "POST",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/configmaps",
            body=attrs,
        )

    def update_configmap(self, cluster_id, namespace, name, **attrs):
        return self._json_request(
            "PUT",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/configmaps/{_segment(name)}",
            body=attrs,
        )

    def delete_configmap(self, cluster_id, namespace, name):
        return self._json_request(
            "DELETE",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/configmaps/{_segment(name)}",
        )

    def secrets(self, cluster_id, **query):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/secrets", params=_query(**query))

    def get_secret(self, cluster_id, namespace, name):
        return self._json_request(
            "GET",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/secrets/{_segment(name)}",
        )

    def create_secret(self, cluster_id, namespace, **attrs):
        return self._json_request(
            "POST",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/secrets",
            body=attrs,
        )

    def update_secret(self, cluster_id, namespace, name, **attrs):
        return self._json_request(
            "PUT",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/secrets/{_segment(name)}",
            body=attrs,
        )

    def delete_secret(self, cluster_id, namespace, name):
        return self._json_request(
            "DELETE",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/secrets/{_segment(name)}",
        )

    def pods(self, cluster_id, namespace):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/pods")

    def delete_pod(self, cluster_id, namespace, name):
        return self._json_request(
            "DELETE",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/pods/{_segment(name)}",
        )

    def pod_log(self, cluster_id, namespace, name, **query):
        return self._json_request(
            "GET",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/pods/{_segment(name)}/log",
            params=_query(**query),
        )

    def services(self, cluster_id, namespace):
        return self._json_request(
            "GET", f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/services"
        )

    def delete_service(self, cluster_id, namespace, name):
        return self._json_request(
            "DELETE",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/services/{_segment(name)}",
        )

    def deployments(self, cluster_id, namespace):
        return self._json_request(
            "GET", f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/deployments"
        )

    def replicasets(self, cluster_id, namespace):
        return self._json_request(
            "GET", f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/replicasets"
        )

    def restart_deployment(self, cluster_id, namespace, name):
        return self._json_request(
            "POST",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}"
            f"/deployments/{_segment(name)}/restart",
        )

    def scale_deployment(self, cluster_id, namespace, name, **attrs):
        return self._json_request(
            "PATCH",
            f"/v1/clusters/{_segment(cluster_id)}/namespaces/{_segment(namespace)}/deployments/{_segment(name)}/scale",
            body=attrs,
        )

    # -- Nodegroups -------------------------------------------------------

    def nodegroups(self, cluster_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/nodegroups")

    def get_nodegroup(self, cluster_id, nodegroup_id):
        return self._json_request("GET", f"/v1/clusters/{_segment(cluster_id)}/nodegroups/{_segment(nodegroup_id)}")

    def create_nodegroup(self, cluster_id, **attrs):
        return self._json_request("POST", f"/v1/clusters/{_segment(cluster_id)}/nodegroups", body=attrs)

    def update_nodegroup(self, cluster_id, nodegroup_id, **attrs):
        return self._json_request(
            "PATCH",
            f"/v1/clusters/{_segment(cluster_id)}/nodegroups/{_segment(nodegroup_id)}",
            body=attrs,
        )

    def delete_nodegroup(self, cluster_id, nodegroup_id):
        return self._json_request("DELETE", f"/v1/clusters/{_segment(cluster_id)}/nodegroups/{_segment(nodegroup_id)}")

    # -- Cluster templates --------------------------------------------------

    def cluster_templates(self):
        return self._json_request("GET", "/v1/cluster-templates")

    def get_cluster_template(self, template_id):
        return self._json_request("GET", f"/v1/cluster-templates/{_segment(template_id)}")

    def create_cluster_template(self, **attrs):
        return self._json_request("POST", "/v1/cluster-templates", body=attrs)

    def update_cluster_template(self, template_id, **attrs):
        return self._json_request("PATCH", f"/v1/cluster-templates/{_segment(template_id)}", body=attrs)

    def delete_cluster_template(self, template_id):
        return self._json_request("DELETE", f"/v1/cluster-templates/{_segment(template_id)}")

    # -- Stats --------------------------------------------------------------

    def cluster_stats(self):
        return self._json_request("GET", "/v1/stats/clusters")

    # -- Admin ----------------------------------------------------------------

    def admin_clusters(self, **query):
        return self._json_request("GET", "/v1/admin/clusters", params=_query(**query))

    def admin_cluster(self, cluster_id):
        return self._json_request("GET", f"/v1/admin/clusters/{_segment(cluster_id)}")

    def admin_kubeconfig(self, cluster_id):
        return self._text_request("GET", f"/v1/admin/clusters/{_segment(cluster_id)}/kubeconfig")

    def admin_scale_cluster(self, cluster_id, **attrs):
        return self._json_request("PATCH", f"/v1/admin/clusters/{_segment(cluster_id)}/scale", body=attrs)

    def admin_delete_cluster(self, cluster_id):
        return self._json_request("DELETE", f"/v1/admin/clusters/{_segment(cluster_id)}")

    def admin_delete_cluster_async(self, cluster_id):
        return self._stream_request("POST", f"/v1/admin/clusters/{_segment(cluster_id)}/delete-async")

    def admin_ca_certificate(self, cluster_id):
        return self._text_request("GET", f"/v1/admin/clusters/{_segment(cluster_id)}/ca-certificate")

    def admin_certificate_expiry(self, cluster_id):
        return self._json_request("GET", f"/v1/admin/clusters/{_segment(cluster_id)}/certificate-expiry")

    def admin_rotate_certs(self, cluster_id):
        return self._stream_request("POST", f"/v1/admin/clusters/{_segment(cluster_id)}/rotate-certs")

    def admin_cluster_templates(self):
        return self._json_request("GET", "/v1/admin/cluster-templates")

    def resource_policies(self):
        return self._json_request("GET", "/v1/admin/resource-policies")

    def resource_policy_catalog(self, policy_key):
        return self._json_request(
            "GET",
            f"/v1/admin/resource-policies/catalog/{_segment(policy_key)}",
        )

    def update_resource_policy(self, policy_key, **attrs):
        return self._json_request(
            "PUT",
            f"/v1/admin/resource-policies/{_segment(policy_key)}",
            body=attrs,
        )

    def runtime_settings(self):
        return self._json_request("GET", "/v1/admin/runtime-settings")

    def update_runtime_setting(self, setting_key, **attrs):
        return self._json_request(
            "PUT",
            f"/v1/admin/runtime-settings/{_segment(setting_key)}",
            body=attrs,
        )

    # -- GPU Quotas -----------------------------------------------------------

    def effective_gpu_quotas(self):
        return self._json_request("GET", "/v1/gpu-quotas/effective")

    def gpu_quota_status(self):
        return self._json_request("GET", "/v1/gpu-quotas/status")

    def check_gpu_quota(self, extra_specs: dict):
        return self._json_request("POST", "/v1/gpu-quotas/check", body={"extra_specs": extra_specs})

    def default_gpu_quotas(self):
        return self._json_request("GET", "/v1/admin/gpu-quotas/defaults")

    def set_default_gpu_quota(self, gpu_type: str, limit: int):
        return self._json_request("PUT", "/v1/admin/gpu-quotas/defaults", body={"gpu_type": gpu_type, "limit": limit})

    def delete_default_gpu_quota(self, gpu_type: str):
        return self._json_request("DELETE", f"/v1/admin/gpu-quotas/defaults/{_segment(gpu_type)}")

    def project_gpu_quotas(self, project_id: str):
        return self._json_request("GET", f"/v1/admin/gpu-quotas/{_segment(project_id)}")

    def set_project_gpu_quota(self, project_id: str, gpu_type: str, limit: int):
        return self._json_request(
            "PUT", f"/v1/admin/gpu-quotas/{_segment(project_id)}", body={"gpu_type": gpu_type, "limit": limit}
        )

    def delete_project_gpu_quota(self, project_id: str, gpu_type: str):
        return self._json_request("DELETE", f"/v1/admin/gpu-quotas/{_segment(project_id)}/{_segment(gpu_type)}")

    # -- Baked guest callback ------------------------------------------------

    def callback(self, **attrs):
        """Guest-VM cloud-init callback — unauthenticated on the server side.

        Exposed for completeness/testing; production traffic reaches this
        route directly from baked VM cloud-init scripts, not through the SDK.
        """
        return self._json_request("POST", "/v1/callback", body=attrs)
