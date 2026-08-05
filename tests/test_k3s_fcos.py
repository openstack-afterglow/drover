"""FCOS (Fedora CoreOS) k3s 노드 지원 테스트."""

import base64
import gzip
import json

import pytest


def _decode_userdata(data: str) -> dict:
    """base64 인코딩된 userdata를 Ignition JSON dict로 디코딩."""
    return json.loads(base64.b64decode(data).decode())


def _decode_file_content(file_entry: dict) -> str:
    """Ignition file entry에서 원본 텍스트 추출 (gzip+base64)."""
    source = file_entry["contents"]["source"]
    b64_data = source.split(",", 1)[1]
    raw = base64.b64decode(b64_data)
    if file_entry["contents"].get("compression") == "gzip":
        raw = gzip.decompress(raw)
    return raw.decode()


class TestFCOSServerUserdata:
    def test_fcos_server_returns_ignition_json(self):
        """FCOS 서버 userdata가 유효한 Ignition JSON을 반환해야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="test-cluster",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok-fcos",
            os_type="fcos",
        )
        assert result.config_drive is True
        ign = _decode_userdata(result.data)
        assert ign["ignition"]["version"] == "3.4.0"

    def test_fcos_server_ignition_has_required_files(self):
        """FCOS Ignition에 callback.sh와 install.sh가 포함되어야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="my-cluster",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok-fcos",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        paths = {f["path"] for f in ign["storage"]["files"]}
        assert "/opt/k3s/callback.sh" in paths
        assert "/opt/k3s/install.sh" in paths
        assert "/etc/systemd/system/k3s-install.service" in paths

    def test_fcos_server_uses_direct_drover_callback(self):
        """새 FCOS 노드는 Drover의 versioned callback endpoint로 보고한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="my-cluster",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok-fcos",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        files = {entry["path"]: entry for entry in ign["storage"]["files"]}

        callback_script = _decode_file_content(files["/opt/k3s/callback.sh"])
        install_script = _decode_file_content(files["/opt/k3s/install.sh"])
        assert 'CALLBACK_URL="http://api.example.com"' in callback_script
        assert "${CALLBACK_URL}/v1/callback" in callback_script
        assert "/api/k3s/callback" not in callback_script
        assert "http://api.example.com/v1/callback" in install_script
        assert "/api/k3s/callback" not in install_script

    def test_fcos_server_ignition_k3s_install_enabled(self):
        """FCOS Ignition에서 k3s-install.service가 enabled여야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="my-cluster",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok-fcos",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        units = {u["name"]: u for u in ign["systemd"]["units"]}
        assert "k3s-install.service" in units
        assert units["k3s-install.service"]["enabled"] is True

    def test_fcos_server_ignition_with_cloud_conf(self):
        """FCOS Ignition에 cloud.conf 파일이 포함되어야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok",
            cloud_conf="[Global]\nauth-url=https://keystone:5000/v3\n",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        assert "/etc/kubernetes/cloud.conf" in files
        # 권한 0600 = 384
        assert files["/etc/kubernetes/cloud.conf"]["mode"] == 0o600

    def test_fcos_server_ignition_with_plugins(self):
        """FCOS Ignition에 플러그인 매니페스트 파일이 포함되어야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok",
            plugin_manifests=[
                {"name": "occm", "content": "apiVersion: v1\nkind: List\n"},
                {"name": "cinder_csi", "content": "apiVersion: v1\nkind: List\n"},
            ],
            needs_external_cloud_provider=True,
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        paths = {f["path"] for f in ign["storage"]["files"]}
        assert "/opt/k3s/occm-manifests.yaml" in paths
        assert "/opt/k3s/cinder_csi-manifests.yaml" in paths

        # install.sh에 --disable-cloud-controller가 포함되어야 함
        files = {f["path"]: f for f in ign["storage"]["files"]}
        install_content = _decode_file_content(files["/opt/k3s/install.sh"])
        assert "--disable-cloud-controller" in install_content

    def test_fcos_server_ignition_tls_san(self):
        """FCOS Ignition install.sh에 extra_tls_sans가 포함되어야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok",
            extra_tls_sans=["203.0.113.10"],
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        install_content = _decode_file_content(files["/opt/k3s/install.sh"])
        assert "203.0.113.10" in install_content

    def test_fcos_server_callback_contains_token(self):
        """FCOS callback.sh에 올바른 토큰이 포함되어야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="supersecrettoken",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        cb_content = _decode_file_content(files["/opt/k3s/callback.sh"])
        assert "supersecrettoken" in cb_content
        assert "http://api.example.com" in cb_content

    def test_fcos_server_install_uses_server_node_name(self):
        """server_node_name이 install.sh의 --node-name에 반영되어야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok",
            os_type="fcos",
            server_node_name="test-x7k2m",
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        install_content = _decode_file_content(files["/opt/k3s/install.sh"])
        assert "--node-name=test-x7k2m" in install_content

    def test_fcos_server_install_default_node_name_fallback(self):
        """server_node_name 미지정 시 {cluster_name}-server가 기본값이어야 한다."""
        from drover.services.cloudinit import generate_server_userdata

        result = generate_server_userdata(
            primary_network_id="net-primary",
            cluster_name="mycluster",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        install_content = _decode_file_content(files["/opt/k3s/install.sh"])
        assert "--node-name=mycluster-server" in install_content


class TestFCOSAgentUserdata:
    def test_fcos_agent_returns_ignition_json(self):
        """FCOS 에이전트 userdata가 유효한 Ignition JSON을 반환해야 한다."""
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="10.0.0.1",
            node_token="node-token-abc",
            os_type="fcos",
        )
        assert result.config_drive is True
        ign = _decode_userdata(result.data)
        assert ign["ignition"]["version"] == "3.4.0"

    def test_fcos_agent_ignition_has_required_files(self):
        """FCOS 에이전트 Ignition에 agent-join.sh가 포함되어야 한다."""
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="10.0.0.1",
            node_token="node-token-abc",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        paths = {f["path"] for f in ign["storage"]["files"]}
        assert "/opt/k3s/agent-join.sh" in paths
        assert "/etc/systemd/system/k3s-agent-join.service" in paths

    def test_fcos_agent_ignition_agent_join_enabled(self):
        """FCOS 에이전트 Ignition에서 k3s-agent-join.service가 enabled여야 한다."""
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="10.0.0.1",
            node_token="node-token-abc",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        units = {u["name"]: u for u in ign["systemd"]["units"]}
        assert "k3s-agent-join.service" in units
        assert units["k3s-agent-join.service"]["enabled"] is True

    def test_fcos_agent_ignition_with_ssh_key(self):
        """FCOS 에이전트 Ignition에 SSH 공개키가 passwd.users에 포함되어야 한다."""
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="10.0.0.1",
            node_token="tok",
            ssh_public_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 test@host",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        assert "passwd" in ign
        users = {u["name"]: u for u in ign["passwd"]["users"]}
        assert "core" in users
        assert "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5 test@host" in users["core"]["sshAuthorizedKeys"]

    def test_fcos_agent_ignition_without_ssh_key(self):
        """SSH 키 없을 때 FCOS 에이전트 Ignition에 passwd 섹션이 없어야 한다."""
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="10.0.0.1",
            node_token="tok",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        assert "passwd" not in ign

    def test_fcos_agent_join_script_contains_server_ip(self):
        """FCOS 에이전트 join 스크립트에 서버 IP가 포함되어야 한다."""
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="192.168.1.100",
            node_token="tok",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        join_content = _decode_file_content(files["/opt/k3s/agent-join.sh"])
        assert "192.168.1.100" in join_content
        assert "INSTALL_K3S_SKIP_SELINUX_RPM=true" in join_content


class TestFCOSAgentNodeIp:
    """FCOS 에이전트 join 스크립트의 persisted pin 계약 검증."""

    def test_fcos_agent_join_contains_pin_contract(self):
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="10.0.0.1",
            node_token="tok",
            os_type="fcos",
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        join_content = _decode_file_content(files["/opt/k3s/agent-join.sh"])
        assert "/opt/k3s/pin-primary-network.sh" in join_content
        assert 'NODE_IP="${AFTERGLOW_K3S_NODE_IP}"' in join_content
        assert '_EXEC_ARGS="agent"' in join_content
        assert "--node-ip" not in join_content
        assert "ip route get 8.8.8.8" not in join_content

    def test_fcos_agent_join_extra_args_preserved_without_node_ip(self):
        from drover.services.cloudinit import generate_agent_userdata

        result = generate_agent_userdata(
            primary_network_id="net-primary",
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            server_ip="10.0.0.1",
            node_token="tok",
            os_type="fcos",
            extra_agent_args=["--kubelet-arg=cloud-provider=external"],
        )
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        join_content = _decode_file_content(files["/opt/k3s/agent-join.sh"])
        assert "agent --kubelet-arg=cloud-provider=external" in join_content
        assert "--node-ip" not in join_content


class TestFCOSOsTypeValidation:
    def test_invalid_os_type_raises_error(self):
        """잘못된 os_type은 ValidationError를 발생시켜야 한다."""
        from pydantic import ValidationError

        from drover.models.schemas import CreateK3sClusterRequest

        with pytest.raises(ValidationError):
            CreateK3sClusterRequest(name="test", os_type="windows")

    def test_valid_ubuntu_os_type(self):
        """ubuntu os_type은 유효해야 한다."""
        from drover.models.schemas import CreateK3sClusterRequest

        req = CreateK3sClusterRequest(name="test", os_type="ubuntu")
        assert req.os_type == "ubuntu"

    def test_valid_fcos_os_type(self):
        """fcos os_type은 유효해야 한다."""
        from drover.models.schemas import CreateK3sClusterRequest

        req = CreateK3sClusterRequest(name="test", os_type="fcos")
        assert req.os_type == "fcos"

    def test_default_os_type_is_ubuntu(self):
        """os_type 미지정 시 기본값이 ubuntu여야 한다."""
        from drover.models.schemas import CreateK3sClusterRequest

        req = CreateK3sClusterRequest(name="test")
        assert req.os_type == "ubuntu"


class TestFCOSCallbackScript:
    """A1: FCOS 콜백 스크립트가 Ubuntu 기준으로 정렬됐는지 검증."""

    def _get_callback_content(self, **kwargs) -> str:
        from drover.services.cloudinit import generate_server_userdata

        defaults = dict(
            cluster_name="test",
            k3s_version="v1.31.4+k3s1",
            callback_url="http://api.example.com",
            callback_token="tok",
            os_type="fcos",
        )
        defaults.update(kwargs)
        result = generate_server_userdata(primary_network_id="net-primary", **defaults)
        ign = _decode_userdata(result.data)
        files = {f["path"]: f for f in ign["storage"]["files"]}
        return _decode_file_content(files["/opt/k3s/callback.sh"])

    def test_fcos_callback_exports_path(self):
        """FCOS callback.sh 상단에 PATH export가 있어야 한다."""
        cb = self._get_callback_content()
        assert 'export PATH="/usr/local/bin:$PATH"' in cb

    def test_fcos_callback_has_livez_wait(self):
        """FCOS callback.sh에 /livez kube-apiserver 준비 대기 루프가 있어야 한다."""
        cb = self._get_callback_content()
        assert "/livez" in cb
        assert "APISERVER_READY" in cb

    def test_fcos_callback_detects_nrestarts(self):
        """FCOS callback.sh에 k3s NRestarts 재시작 루프 감지가 있어야 한다."""
        cb = self._get_callback_content()
        assert "NRestarts" in cb
        assert "RESTART_THRESHOLD" in cb

    def test_fcos_callback_plugin_apply_validate_false(self):
        """FCOS callback.sh의 플러그인 apply가 --validate=false와 stderr 캡처를 포함해야 한다."""
        cb = self._get_callback_content(
            plugin_manifests=[{"name": "occm", "content": "apiVersion: v1\nkind: List\n"}],
            needs_external_cloud_provider=True,
        )
        assert "--validate=false" in cb
        assert "_ERR_FILE=" in cb

    def test_fcos_callback_plugin_status_has_status_error_structure(self):
        """FCOS callback.sh의 plugin_status가 {status, error} 구조여야 한다."""
        cb = self._get_callback_content(
            plugin_manifests=[{"name": "occm", "content": "apiVersion: v1\nkind: List\n"}],
            needs_external_cloud_provider=True,
        )
        assert "status:" in cb
        assert "error:" in cb
        assert "_ERROR=" in cb

    def test_fcos_callback_has_secret_cloud_config_status(self):
        """cloud_conf 있을 때 FCOS callback.sh에 SECRET_CLOUD_CONFIG_STATUS가 있어야 한다."""
        cb = self._get_callback_content(
            cloud_conf="[Global]\nauth-url=https://keystone:5000/v3\n",
        )
        assert "SECRET_CLOUD_CONFIG_STATUS" in cb
        assert "secret_cloud_config_status" in cb

    def test_fcos_callback_server_ip_uses_persisted_pin(self):
        """FCOS callback.sh는 route discovery 없이 persisted IP를 사용해야 한다."""
        cb = self._get_callback_content()
        assert 'SERVER_IP="${AFTERGLOW_K3S_NODE_IP}"' in cb
        assert "ip route get 8.8.8.8" not in cb
        assert "hostname -I" not in cb
        assert "primary network pin missing" in cb
