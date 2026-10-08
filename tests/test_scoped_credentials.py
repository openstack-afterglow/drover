"""Synthetic smoke-native HTTP coverage, not live Kubernetes verification.

The route uses real Keystone parsing, _kube_client, httpx SSA/RBAC/TokenRequest
requests, and a loopback Kubernetes boundary. Only storage, Keystone's remote
response and certificate loading are synthetic; issuance itself is never mocked.
"""

from __future__ import annotations

import base64
import json
import ssl
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import urlsplit

import httpx
import pytest
import yaml
from fastapi import HTTPException
from requests import Response

from drover.main import app
from drover.services import credentials, kube, store
from drover.services.errors import K3sApiError

USER_ROLES = ["member", "drover-access_user"]
EDITOR_ROLES = ["member", "drover-workloads_editor"]
ADMIN_ROLES = ["member", "drover-access_admin"]


def _encode(value):
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")


class SmokeKubernetes:
    """Small external HTTP boundary enforcing the exact submitted RBAC rules."""

    def __init__(self):
        self.objects = {}
        self.requests = []
        self.tokens = {}
        self.failure = None
        self.bad_identity = False
        self.bad_uid = False
        self.invalid_expiry = False
        self.extend_expiry = False
        self.admission_unready = 0  # dry-run probes admitted before the binding enforces
        self.server = None

    def handle(self, method, path, headers, content):
        dry_run = urlsplit(path).query == "dryRun=All"
        path = urlsplit(path).path
        self.requests.append((method, path))
        if self.failure and self.failure in path:
            return 503, {"message": "synthetic provider outage"}
        bearer = headers.get("Authorization", "").removeprefix("Bearer ")
        if bearer:
            account = self.tokens.get(bearer)
            if not account:
                return 401, {}
            parts = path.strip("/").split("/")
            if parts[0] == "api":
                group, parts = "", parts[2:]
            else:
                group, parts = parts[1], parts[3:]
            namespace = None
            if len(parts) > 2 and parts[0] == "namespaces":
                namespace, parts = parts[1], parts[2:]
            resource = parts[0]
            if len(parts) > 2:
                resource += "/" + parts[2]
            verb = {"GET": "get" if len(parts) > 1 else "list", "POST": "create", "DELETE": "delete", "PATCH": "patch"}[method]
            for obj in self.objects.values():
                if obj["kind"] not in ("RoleBinding", "ClusterRoleBinding"):
                    continue
                if not any(s["name"] == account and s["namespace"] == credentials.ISSUER_NAMESPACE for s in obj["subjects"]):
                    continue
                if obj["kind"] == "RoleBinding" and obj["metadata"]["namespace"] != namespace:
                    continue
                ref = obj["roleRef"]
                roles = [r for r in self.objects.values() if r["kind"] == ref["kind"] and r["metadata"]["name"] == ref["name"]]
                if any(group in rule["apiGroups"] and resource in rule["resources"] and verb in rule["verbs"] for role in roles for rule in role["rules"]):
                    return 200, {"items": []}
            return 403, {"reason": "Forbidden"}
        if method == "PATCH":
            body = yaml.safe_load(content)
            parts = path.split("/")
            if "/namespaces/" in path and body["kind"] != "Namespace":
                body["metadata"]["namespace"] = parts[parts.index("namespaces") + 1]
            body["metadata"]["uid"] = "uid-" + body["metadata"]["name"]
            if body["kind"] == "ValidatingAdmissionPolicy":
                body["metadata"]["generation"] = 1
            self.objects[path] = body
            return 201, body
        if method == "GET" and path in self.objects:
            return 200, self.objects[path]
        if method == "POST" and path.endswith("/pods") and dry_run:
            if self.admission_unready:
                self.admission_unready -= 1
                return 201, {"kind": "Pod"}
            return 422, {"message": "ValidatingAdmissionPolicy 'drover-restricted-workload' denied request: Restricted workload credential isolation"}
        if method == "POST" and path.endswith("/token"):
            body = json.loads(content)
            account = path.split("/")[-2]
            expiry = int(datetime.now(UTC).timestamp()) + body["spec"]["expirationSeconds"]
            if self.extend_expiry:
                expiry += 3600
            claims = {"sub": f"system:serviceaccount:{credentials.ISSUER_NAMESPACE}:{account}", "exp": expiry,
                      "kubernetes.io": {"namespace": credentials.ISSUER_NAMESPACE,
                      "serviceaccount": {"name": account, "uid": "uid-" + account}}}
            if self.bad_identity:
                claims["sub"] = "system:serviceaccount:kube-system:admin"
            if self.bad_uid:
                claims["kubernetes.io"]["serviceaccount"]["uid"] = "another-principal-uid"
            token = _encode({"alg": "synthetic"}) + "." + _encode(claims) + "." + str(len(self.tokens))
            self.tokens[token] = account
            expiration = "invalid" if self.invalid_expiry else datetime.fromtimestamp(expiry, UTC).isoformat()
            return 201, {"status": {"token": token, "expirationTimestamp": expiration}}
        return 404, {}


@pytest.fixture
async def native_boundary(monkeypatch):
    boundary = SmokeKubernetes()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def dispatch(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            status, result = boundary.handle(self.command, self.path, self.headers, body)
            payload = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_PATCH = do_POST = do_DELETE = dispatch

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    boundary.server = f"http://127.0.0.1:{server.server_port}"
    principal = {"project_id": "project-one", "user_id": "user-one", "roles": USER_ROLES,
                 "expires_at": (datetime.now(UTC) + timedelta(seconds=800)).isoformat(), "is_system_admin": False}
    stored = yaml.safe_dump({"apiVersion": "v1", "clusters": [{"name": "admin", "cluster": {
        "server": boundary.server, "certificate-authority-data": "synthetic-ca"}}],
        "users": [{"name": "admin", "user": {"client-certificate-data": "Y2VydA==", "client-key-data": "a2V5"}}]})
    cluster = {"id": "cluster-one", "project_id": "project-one", "name": "native-smoke"}

    async def get_cluster(project_id, cluster_id):
        return cluster if project_id == cluster["project_id"] and cluster_id == cluster["id"] else None

    monkeypatch.setattr(store, "get_cluster", AsyncMock(side_effect=get_cluster))
    monkeypatch.setattr(store, "get_kubeconfig", AsyncMock(return_value=stored))
    monkeypatch.setattr(store, "get_kubeconfig_admin", AsyncMock(side_effect=AssertionError("Unscoped admin lookup")))
    def synthetic_server_context(kubeconfig_yaml, cert_pem, key_pem, server_url):
        assert server_url == boundary.server
        assert kubeconfig_yaml == stored
        assert cert_pem == b"cert" and key_pem == b"key"
        return False

    monkeypatch.setattr(kube, "_verified_server_context", synthetic_server_context)
    monkeypatch.setattr(credentials, "ADMISSION_PROBE_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("drover.api.clusters.rec", AsyncMock())
    monkeypatch.setattr("drover.auth._resolve_internal_keystone_endpoint", lambda: "http://synthetic-keystone/v3")
    monkeypatch.setattr("drover.auth._is_system_admin", lambda user_id: principal["is_system_admin"])
    def current_roles(user_id, project_id):
        assert user_id == principal["user_id"] and project_id == principal["project_id"]
        return list(principal["roles"])

    monkeypatch.setattr("drover.auth._current_project_roles", current_roles)

    def keystone_request(session, url, method, **kwargs):
        assert method == "GET"
        assert url == "http://synthetic-keystone/v3/auth/tokens"
        assert kwargs["headers"]["X-Subject-Token"] == "synthetic-keystone-token"
        token = {"methods": ["token"], "expires_at": principal["expires_at"],
                 "issued_at": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
                 "user": {"id": principal["user_id"], "name": "smoke", "domain": {"id": "default"}},
                 "project": {"id": principal["project_id"], "name": "smoke", "domain": {"id": "default"}},
                 "roles": [{"id": role, "name": role} for role in principal["roles"]]}
        response = Response()
        response.status_code = 200
        response._content = json.dumps({"token": token}).encode()
        response.headers["X-Subject-Token"] = "synthetic-keystone-token"
        return response

    monkeypatch.setattr("drover.auth.ks_session.Session.request", keystone_request)
    app.dependency_overrides.clear()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://drover-smoke",
                                headers={"X-Auth-Token": "synthetic-keystone-token"}) as route:
        try:
            yield SimpleNamespace(k8s=boundary, principal=principal, route=route, stored=stored, cluster=cluster)
        finally:
            app.dependency_overrides.clear()
            server.shutdown()
            server.server_close()
            thread.join()


async def _download(smoke, grade="user"):
    return await smoke.route.get("/v1/clusters/cluster-one/kubeconfig", params={"grade": grade})


def _token(response):
    config = yaml.safe_load(response.text)
    assert set(config["users"][0]["user"]) == {"token"}
    return config["users"][0]["user"]["token"]


@pytest.mark.asyncio
async def test_smoke_native_user_download_issues_real_http_and_readonly_rbac(native_boundary):
    smoke = native_boundary
    first, second = await _download(smoke), await _download(smoke)
    assert first.status_code == second.status_code == 200
    assert _token(first) != _token(second)
    assert first.headers["cache-control"] == "no-store"
    expiry = datetime.fromisoformat(first.headers["x-credential-expires-at"])
    assert datetime.now(UTC) < expiry <= datetime.fromisoformat(smoke.principal["expires_at"])
    assert expiry <= datetime.now(UTC) + timedelta(seconds=credentials.TOKEN_TTL_SECONDS)
    assert sum(method == "POST" and path.endswith("/token") for method, path in smoke.k8s.requests) == 2
    async with httpx.AsyncClient(base_url=smoke.k8s.server, headers={"Authorization": "Bearer " + _token(first)}) as k8s:
        assert (await k8s.get("/api/v1/namespaces/default/pods")).status_code == 200
        assert (await k8s.get("/api/v1/namespaces/default/pods/example/log")).status_code == 200
        for method, path in [("POST", "/api/v1/namespaces/default/pods"), ("DELETE", "/api/v1/namespaces/default/pods/example"),
                             ("GET", "/api/v1/namespaces/default/secrets"), ("POST", "/api/v1/namespaces/default/pods/example/exec"),
                             ("GET", "/apis/rbac.authorization.k8s.io/v1/clusterroles")]:
            assert (await k8s.request(method, path)).status_code == 403
    store.get_kubeconfig_admin.assert_not_called()
    assert all(call.kwargs.get("project_id") == "project-one" for call in store.get_kubeconfig.call_args_list)


@pytest.mark.asyncio
async def test_head_authorizes_selected_grade_without_issuing_a_credential(native_boundary):
    smoke = native_boundary
    response = await smoke.route.head("/v1/clusters/cluster-one/kubeconfig")
    assert response.status_code == 200
    assert response.content == b""
    assert response.headers["cache-control"] == "no-store"
    assert smoke.k8s.requests == []
    smoke.principal["roles"] = ["member", "drover-inventory_reader"]
    denied = await smoke.route.head("/v1/clusters/cluster-one/kubeconfig")
    assert denied.status_code == 403
    assert smoke.k8s.requests == []


@pytest.mark.asyncio
async def test_editor_private_namespace_no_global_or_escalation_rights(native_boundary):
    smoke = native_boundary
    smoke.principal["roles"] = EDITOR_ROLES
    response = await _download(smoke, "editor")
    assert response.status_code == 200
    namespace = credentials.workload_namespace("project-one", "user-one")
    assert response.headers["x-workload-namespace"] == namespace
    objects = list(smoke.k8s.objects.values())
    assert not any(obj["kind"] in ("ClusterRole", "ClusterRoleBinding") for obj in objects)
    ns = next(obj for obj in objects if obj["kind"] == "Namespace" and obj["metadata"]["name"] == namespace)
    assert ns["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    policy = next(obj for obj in objects if obj["kind"] == "ValidatingAdmissionPolicy")
    assert policy["spec"]["failurePolicy"] == "Fail"
    expressions = " ".join(v["expression"] for v in policy["spec"]["validations"])
    for constraint in ("serviceAccountName == 'default'", "automountServiceAccountToken == false", "secretKeyRef", "secretRef", "imagePullSecrets", "volumes.all"):
        assert constraint in expressions
    assert next(obj for obj in objects if obj["kind"] == "ValidatingAdmissionPolicyBinding")["spec"]["validationActions"] == ["Deny"]
    async with httpx.AsyncClient(base_url=smoke.k8s.server, headers={"Authorization": "Bearer " + _token(response)}) as k8s:
        assert (await k8s.post(f"/api/v1/namespaces/{namespace}/pods")).status_code == 200
        assert (await k8s.post(f"/api/v1/namespaces/{namespace}/pods/example/exec")).status_code == 200
        for path in ("/api/v1/nodes", "/api/v1/namespaces/other/pods", f"/api/v1/namespaces/{namespace}/secrets",
                     f"/api/v1/namespaces/{namespace}/serviceaccounts", f"/apis/rbac.authorization.k8s.io/v1/namespaces/{namespace}/roles"):
            assert (await k8s.get(path)).status_code == 403
        assert (await k8s.post(f"/api/v1/namespaces/{namespace}/serviceaccounts/default/token")).status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("roles,verified,status,admin", [
    (["reader", "drover-inventory_reader"], False, 403, False), (["member", "drover-clusters_editor"], False, 403, False),
    (["member", "drover-clusters_admin"], False, 403, False), (["admin", "drover-access_admin"], False, 403, False),
    (USER_ROLES, False, 200, False), (ADMIN_ROLES, False, 200, True),
    (["member", "drover_admin"], False, 403, False), ([], True, 200, True),
])
async def test_leaf_credential_privileges(native_boundary, roles, verified, status, admin):
    smoke = native_boundary
    smoke.principal.update(roles=roles, is_system_admin=verified)
    response = await _download(smoke, "admin" if admin else "user")
    assert response.status_code == status
    if status == 200:
        if admin:
            assert response.text == smoke.stored
            assert not smoke.k8s.requests
        else:
            _token(response)
    else:
        assert not smoke.k8s.requests
        store.get_kubeconfig.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [USER_ROLES, EDITOR_ROLES, ADMIN_ROLES])
async def test_head_never_issues_reads_or_audits(native_boundary, roles):
    smoke = native_boundary
    smoke.principal["roles"] = roles
    grade = "editor" if roles == EDITOR_ROLES else "admin" if roles == ADMIN_ROLES else "user"
    response = await smoke.route.head("/v1/clusters/cluster-one/kubeconfig", params={"grade": grade})
    assert response.status_code == 200
    assert not response.content
    assert not smoke.k8s.requests
    store.get_kubeconfig.assert_not_called()
    from drover.api.clusters import rec
    rec.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [USER_ROLES, EDITOR_ROLES, ADMIN_ROLES])
async def test_wrong_project_fails_before_issuance(native_boundary, roles):
    smoke = native_boundary
    smoke.principal.update(project_id="wrong-project", roles=roles)
    grade = "editor" if roles == EDITOR_ROLES else "admin" if roles == ADMIN_ROLES else "user"
    assert (await _download(smoke, grade)).status_code == 404
    assert (await smoke.route.head("/v1/clusters/cluster-one/kubeconfig", params={"grade": grade})).status_code == 404
    assert not smoke.k8s.requests
    store.get_kubeconfig.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["serviceaccounts", "clusterroles", "clusterrolebindings", "/token"])
async def test_provider_errors_never_return_admin_config(native_boundary, failure):
    smoke = native_boundary
    smoke.k8s.failure = failure
    response = await _download(smoke)
    assert response.status_code == 502
    assert "client-certificate-data" not in response.text


@pytest.mark.asyncio
async def test_editor_missing_admission_provider_fails_closed(native_boundary):
    smoke = native_boundary
    smoke.principal["roles"] = EDITOR_ROLES
    smoke.k8s.failure = "admissionregistration"
    assert (await _download(smoke, "editor")).status_code == 502
    assert not smoke.k8s.tokens


@pytest.mark.asyncio
async def test_editor_unready_admission_fails_closed(native_boundary):
    smoke = native_boundary
    smoke.principal["roles"] = EDITOR_ROLES
    smoke.k8s.admission_unready = credentials.ADMISSION_PROBE_ATTEMPTS
    assert (await _download(smoke, "editor")).status_code == 502
    assert not smoke.k8s.tokens
    probes = [path for method, path in smoke.k8s.requests if method == "POST" and path.endswith("/pods")]
    assert len(probes) == credentials.ADMISSION_PROBE_ATTEMPTS


@pytest.mark.asyncio
async def test_editor_waits_for_new_admission_binding_to_enforce(native_boundary):
    # Real apiservers admit dry-run pods briefly after a policy/binding is first applied.
    smoke = native_boundary
    smoke.principal["roles"] = EDITOR_ROLES
    smoke.k8s.admission_unready = 2
    assert (await _download(smoke, "editor")).status_code == 200
    assert len(smoke.k8s.tokens) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute", ["bad_identity", "bad_uid", "extend_expiry", "invalid_expiry"])
async def test_provider_identity_and_expiration_must_match(native_boundary, attribute):
    setattr(native_boundary.k8s, attribute, True)
    assert (await _download(native_boundary)).status_code == 502


@pytest.mark.asyncio
async def test_separate_principals_get_separate_accounts(native_boundary):
    smoke = native_boundary
    first = await _download(smoke)
    smoke.principal["user_id"] = "user-two"
    second = await _download(smoke)
    first_config, second_config = yaml.safe_load(first.text), yaml.safe_load(second.text)
    assert first_config["users"][0]["name"] != second_config["users"][0]["name"]
    assert first.headers["x-workload-namespace"] != second.headers["x-workload-namespace"]
    assert credentials.workload_namespace("project-two", "user-one") != credentials.workload_namespace("project-one", "user-one")


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("user_id", ""), ("project_id", ""), ("expires_at", "invalid"),
                                         ("expires_at", "2099-01-01T00:00:00"),
                                         ("expires_at", "2000-01-01T00:00:00Z")])
async def test_invalid_principal_expiration_fails_without_provider(native_boundary, field, value):
    principal = dict(native_boundary.principal)
    principal[field] = value
    with pytest.raises(HTTPException) as error:
        await credentials.issue_kubeconfig("cluster-one", principal, grade="user")
    assert error.value.status_code == 401
    assert not native_boundary.k8s.requests


@pytest.mark.asyncio
async def test_short_keystone_lifetime_and_invalid_grade_fail_closed(native_boundary):
    principal = dict(native_boundary.principal)
    principal["expires_at"] = (datetime.now(UTC) + timedelta(seconds=599)).isoformat()
    with pytest.raises(HTTPException) as error:
        await credentials.issue_kubeconfig("cluster-one", principal, grade="user")
    assert error.value.status_code == 401
    with pytest.raises(HTTPException) as error:
        await credentials.issue_kubeconfig("cluster-one", native_boundary.principal, grade="admin")
    assert error.value.status_code == 403
    assert not native_boundary.k8s.requests


@pytest.mark.asyncio
async def test_transport_failure_is_sanitized_and_no_admin_fallback(native_boundary, monkeypatch):
    async def fail(*args, **kwargs):
        raise httpx.ConnectError("provider-private-detail")
    monkeypatch.setattr(httpx.AsyncClient, "patch", fail)
    with pytest.raises(K3sApiError) as error:
        await credentials.issue_kubeconfig("cluster-one", native_boundary.principal, grade="user")
    assert error.value.status_code == 502
    assert "provider-private-detail" not in error.value.detail
    store.get_kubeconfig_admin.assert_not_called()


def test_issuance_tls_requires_stored_ca_and_hostname(monkeypatch):
    context = MagicMock()
    make_context = MagicMock(return_value=context)
    monkeypatch.setattr(kube, "_make_ssl_context", make_context)
    config = yaml.safe_dump({"clusters": [{"cluster": {
        "certificate-authority-data": base64.b64encode(b"stored-private-ca").decode(),
    }}]})
    result = kube._verified_server_context(config, b"cert", b"key", "https://k8s.example:6443")
    assert result is context
    make_context.assert_called_once_with(b"cert", b"key")
    context.load_verify_locations.assert_called_once_with(cadata="stored-private-ca")
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


@pytest.mark.parametrize("endpoint", ["http://k8s.example:6443", "https://", "https://user:pass@k8s.example"])
def test_issuance_tls_rejects_unauthenticated_endpoints(endpoint):
    with pytest.raises(K3sApiError) as error:
        kube._verified_server_context("{}", b"cert", b"key", endpoint)
    assert error.value.status_code == 502


@pytest.mark.asyncio
async def test_invalid_stored_ca_prevents_token_issuance(native_boundary, monkeypatch):
    def invalid_ca(*args):
        raise ssl.SSLError("synthetic invalid CA")
    monkeypatch.setattr(kube, "_verified_server_context", invalid_ca)
    assert (await _download(native_boundary)).status_code == 502
    assert not native_boundary.k8s.requests


@pytest.mark.asyncio
async def test_default_download_is_readonly_even_for_access_admin(native_boundary):
    native_boundary.principal["roles"] = ADMIN_ROLES
    response = await native_boundary.route.get("/v1/clusters/cluster-one/kubeconfig")
    assert response.status_code == 200
    _token(response)
    assert "client-certificate-data" not in response.text
    assert any(path.endswith("/token") for _, path in native_boundary.k8s.requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("grade", ["editor", "admin"])
async def test_user_cannot_select_elevated_grade(native_boundary, grade):
    for method in ("GET", "HEAD"):
        response = await native_boundary.route.request(method, "/v1/clusters/cluster-one/kubeconfig", params={"grade": grade})
        assert response.status_code == 403
    assert not native_boundary.k8s.requests


@pytest.mark.asyncio
async def test_invalid_grade_is_rejected_without_issuance(native_boundary):
    response = await _download(native_boundary, "owner")
    assert response.status_code == 422
    assert not native_boundary.k8s.requests


@pytest.mark.asyncio
async def test_editor_namespace_preparation_has_no_discarded_token(native_boundary):
    native_boundary.principal["roles"] = EDITOR_ROLES
    namespace = await credentials.ensure_workload_namespace("cluster-one", native_boundary.principal)
    assert namespace == credentials.workload_namespace("project-one", "user-one")
    assert not native_boundary.k8s.tokens
    assert not any(path.endswith("/token") for _, path in native_boundary.k8s.requests)
    assert any(obj["kind"] == "ValidatingAdmissionPolicyBinding" for obj in native_boundary.k8s.objects.values())
