"""Tests for Drover Kolla-Ansible deployment assets and wheel packaging."""

import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import jinja2
import yaml
from jinja2 import meta

import drover

REPO_ROOT = Path(__file__).resolve().parents[1]
KOLLA_DIR = REPO_ROOT / "deploy" / "kolla"
ROLE_DIR = KOLLA_DIR / "ansible" / "roles" / "drover"


def test_obsolete_assets_removed():
    """Verify obsolete templates and old non-standard role structure are removed."""
    obsolete_paths = [
        KOLLA_DIR / "defaults" / "main.yml",
        KOLLA_DIR / "templates" / "drover-api.json.j2",
        KOLLA_DIR / "templates" / "drover-worker.json.j2",
        KOLLA_DIR / "templates" / "drover-migrate.json.j2",
        KOLLA_DIR / "templates" / "policy.yaml.j2",
        KOLLA_DIR / "tasks" / "register.yml",
        KOLLA_DIR / "tasks" / "main.yml",
        KOLLA_DIR / "tasks" / "config.yml",
        KOLLA_DIR / "tasks" / "deploy.yml",
        KOLLA_DIR / "tasks" / "bootstrap.yml",
        KOLLA_DIR / "tasks" / "precheck.yml",
    ]
    for path in obsolete_paths:
        assert not path.exists(), f"Obsolete source path still exists: {path}"


def test_required_lifecycle_files_exist():
    """Verify all authoritative Ansible role lifecycle files exist in the role tree."""
    required_task_files = [
        "main.yml",
        "deploy.yml",
        "config.yml",
        "precheck.yml",
        "preconditions.yml",
        "preconditions_db.yml",
        "preconditions_keystone.yml",
        "bootstrap_service.yml",
        "start.yml",
        "pull.yml",
        "reconfigure.yml",
        "upgrade.yml",
        "destroy.yml",
        "loadbalancer.yml",
        "source_build.yml",
    ]
    tasks_dir = ROLE_DIR / "tasks"
    assert tasks_dir.is_dir(), f"Missing tasks directory: {tasks_dir}"

    for task_file in required_task_files:
        path = tasks_dir / task_file
        assert path.is_file(), f"Missing required lifecycle file: {path}"

    assert (ROLE_DIR / "defaults" / "main.yml").is_file()
    assert (ROLE_DIR / "templates" / "drover.conf.j2").is_file()


def test_yaml_files_are_valid():
    """Ensure all YAML files in the production role tree parse cleanly."""
    yaml_files = list(ROLE_DIR.glob("**/*.yml"))
    assert len(yaml_files) >= 10, f"Expected at least 10 YAML files in {ROLE_DIR}"
    for yml_file in yaml_files:
        content = yml_file.read_text(encoding="utf-8")
        if content.strip() not in {"", "---"}:
            assert yaml.safe_load(content) is not None, f"Failed to parse YAML file: {yml_file}"


def test_jinja2_templates_compile():
    """Ensure Jinja2 templates compile without syntax errors."""
    template_files = list((ROLE_DIR / "templates").glob("*.j2"))
    assert len(template_files) >= 1, f"Expected at least 1 Jinja2 template in {ROLE_DIR}"
    env = jinja2.Environment()
    for tpl_file in template_files:
        content = tpl_file.read_text(encoding="utf-8")
        parsed = env.parse(content)
        assert parsed is not None, f"Failed to parse Jinja template: {tpl_file}"


def test_defaults_variable_interface():
    """Verify variable defaults, image namespace/tags, source pin, and endpoints."""
    defaults_file = ROLE_DIR / "defaults" / "main.yml"
    assert defaults_file.is_file()
    defaults = yaml.safe_load(defaults_file.read_text(encoding="utf-8"))

    # Image namespace and tags
    assert defaults["drover_image_namespace"] == "ghcr.io/openstack-afterglow"
    assert defaults["drover_image_tag"] == f"v{drover.__version__}"
    assert defaults["drover_source_version"] == "66d33447d0a6f8b1b2ba34f88b360a6bf9c28399"

    # Required service definitions
    assert "drover_services" in defaults
    services = defaults["drover_services"]
    assert "drover-api" in services
    assert "drover-worker" in services

    # Secret directory and file defaults
    assert defaults["drover_secrets_dir"] == "{{ drover_config_dir }}/secrets"
    assert defaults["drover_container_secrets_dir"] == "/etc/drover/secrets"
    assert defaults["drover_keystone_password_file"] == "{{ drover_container_secrets_dir }}/os_password"
    assert defaults["drover_database_password_file"] == "{{ drover_container_secrets_dir }}/database_password"
    assert defaults["drover_redis_password_file"] == "{{ drover_container_secrets_dir }}/redis_password"
    assert defaults["drover_kubeconfig_encryption_key_file"] == "{{ drover_container_secrets_dir }}/kubeconfig_encryption_key"

    # Passwordless database and redis URLs
    assert ":" not in defaults["drover_database_url"].split("@")[0].split("//")[1]
    assert ":" not in defaults["drover_redis_url"].split("@")[0].split("//")[1] if "@" in defaults["drover_redis_url"] else True

    # Secret mounts in services
    secret_mount = "{{ drover_secrets_dir }}:{{ drover_container_secrets_dir }}:ro"
    assert secret_mount in services["drover-api"]["volumes"]
    assert secret_mount in services["drover-worker"]["volumes"]

    # Service environments contain no secrets
    api_env = defaults["drover_service_environments"]["drover-api"]
    worker_env = defaults["drover_service_environments"]["drover-worker"]
    assert "OS_PASSWORD" not in api_env
    assert "DROVER_KUBECONFIG_ENCRYPTION_KEY" not in api_env
    assert "OS_PASSWORD" not in worker_env
    assert "DROVER_KUBECONFIG_ENCRYPTION_KEY" not in worker_env

    # Health probe endpoints in API service
    api_healthcheck = services["drover-api"]["healthcheck"]["test"]
    assert any("/v1/health" in arg for arg in api_healthcheck)

    # Validate Keystone user and project defaults
    assert defaults["drover_keystone_user"] == "drover"
    assert defaults["drover_service_project_name"] == "drover-service"

    # Verify origin endpoint URLs do not contain obsolete /v1 suffix
    assert defaults["drover_public_endpoint_url"] == "{{ drover_external_url }}"
    assert not defaults["drover_public_endpoint_url"].endswith("/v1")
    assert not defaults["drover_internal_endpoint_url"].endswith("/v1")
    assert not defaults["drover_admin_endpoint_url"].endswith("/v1")

    # Ensure no concrete passwords are baked into defaults file
    raw_defaults = defaults_file.read_text(encoding="utf-8")
    assert "secret_password" not in raw_defaults
    assert "my_secret_key" not in raw_defaults

def test_version_lockstep():
    """Verify version lockstep between root Drover package, marker, default image tag, and wheel metadata."""
    root_version = drover.__version__
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert root_version == project["project"]["version"]

    # Marker distribution package check
    marker_file = KOLLA_DIR / "src" / "drover_kolla" / "__init__.py"
    assert marker_file.is_file(), "Marker package deploy/kolla/src/drover_kolla/__init__.py missing"

    # Defaults image tag check
    defaults_file = ROLE_DIR / "defaults" / "main.yml"
    defaults = yaml.safe_load(defaults_file.read_text(encoding="utf-8"))
    assert defaults["drover_image_tag"] == f"v{root_version}"

    # Wheel pyproject metadata check
    pyproject_file = KOLLA_DIR / "pyproject.toml"
    assert pyproject_file.is_file()
    kolla_project = tomllib.loads(pyproject_file.read_text(encoding="utf-8"))
    assert kolla_project["project"]["name"] == "drover-kolla"
    assert kolla_project["project"]["requires-python"] == ">=3.11"
    assert kolla_project["tool"]["hatch"]["version"]["path"] == "../../drover/__init__.py"


def test_action_dispatch_and_ordering():
    """Verify action validation, task inclusion, and migration-before-start ordering."""
    main_tasks_file = ROLE_DIR / "tasks" / "main.yml"
    assert main_tasks_file.is_file()
    main_tasks = yaml.safe_load(main_tasks_file.read_text(encoding="utf-8"))

    # Action assertion check
    assert_task = main_tasks[0]
    assert "ansible.builtin.assert" in assert_task
    allowed_actions = assert_task["ansible.builtin.assert"]["that"][0]
    for action in ['precheck', 'pull', 'deploy', 'reconfigure', 'upgrade', 'destroy', 'config']:
        assert action in allowed_actions

    # Check deploy.yml ordering: precheck -> config -> preconditions -> bootstrap_service -> start
    deploy_file = ROLE_DIR / "tasks" / "deploy.yml"
    deploy_tasks = yaml.safe_load(deploy_file.read_text(encoding="utf-8"))
    included = [t["ansible.builtin.include_tasks"] for t in deploy_tasks if "ansible.builtin.include_tasks" in t]

    assert "bootstrap_service.yml" in included
    assert "start.yml" in included
    bootstrap_idx = included.index("bootstrap_service.yml")
    start_idx = included.index("start.yml")
    assert bootstrap_idx < start_idx, "bootstrap_service.yml must precede start.yml"

    # Check upgrade.yml ordering
    upgrade_file = ROLE_DIR / "tasks" / "upgrade.yml"
    upgrade_tasks = yaml.safe_load(upgrade_file.read_text(encoding="utf-8"))
    upgrade_included = [t["ansible.builtin.include_tasks"] for t in upgrade_tasks if "ansible.builtin.include_tasks" in t]
    assert upgrade_included == ["pull.yml", "bootstrap_service.yml", "start.yml"]

    # Check destroy.yml task
    destroy_file = ROLE_DIR / "tasks" / "destroy.yml"
    destroy_tasks = yaml.safe_load(destroy_file.read_text(encoding="utf-8"))
    assert any(t.get("community.docker.docker_container", {}).get("state") == "absent" for t in destroy_tasks)


def test_keystone_registration_contract():
    """Verify Keystone registration task structure uses type 'drover' without obsolete 'container-infra'."""
    ks_file = ROLE_DIR / "tasks" / "preconditions_keystone.yml"
    assert ks_file.is_file()
    ks_tasks = yaml.safe_load(ks_file.read_text(encoding="utf-8"))

    reg_task = next(t for t in ks_tasks if t.get("ansible.builtin.import_role", {}).get("name") == "service-ks-register")
    vars_map = reg_task["vars"]

    assert vars_map["project_name"] == "drover"

    services = vars_map["service_ks_register_services"]
    assert len(services) == 1
    svc = services[0]
    assert svc["name"] == "drover"
    assert svc["type"] == "drover", f"Expected Keystone service type 'drover', got {svc['type']}"
    assert "container-infra" not in svc["type"]

    # Verify origin endpoints (no /v1 suffix in Keystone catalog)
    endpoint_urls = [ep["url"] for ep in svc["endpoints"]]
    for url in endpoint_urls:
        assert not url.endswith("/v1")

    users = vars_map["service_ks_register_users"]
    assert len(users) == 1
    assert users[0]["user"] == "{{ drover_keystone_user }}"
    assert users[0]["role"] == "admin"


def test_migration_and_bootstrap_contract():
    """Verify bootstrap_service task applies migrations before container launch."""
    bootstrap_file = ROLE_DIR / "tasks" / "bootstrap_service.yml"
    tasks = yaml.safe_load(bootstrap_file.read_text(encoding="utf-8"))

    migrate_task = next(t for t in tasks if "Apply Drover database migrations" in t.get("name", ""))
    container_spec = migrate_task["community.docker.docker_container"]

    assert container_spec["command"] == ["python", "-m", "drover.scripts.migrate", "--apply"]
    assert container_spec["state"] == "started"
    assert container_spec["detach"] is False
    assert container_spec["cleanup"] is True
    assert container_spec["network_mode"] == "host"
    assert container_spec["volumes"] == [
        "{{ drover_config_dir }}/drover.conf:/app/drover.conf:ro",
        "{{ drover_secrets_dir }}:{{ drover_container_secrets_dir }}:ro",
    ]

    env = container_spec["env"]
    assert "DATABASE_URL" in env
    assert "REDIS_URL" in env
    assert "DROVER_KUBECONFIG_ENCRYPTION_KEY" not in env
    assert "OS_PASSWORD" not in env

def test_config_resolution_and_rendering():
    """Verify config.yml project lookup, assertion, and drover.conf rendering."""
    config_file = ROLE_DIR / "tasks" / "config.yml"
    tasks = yaml.safe_load(config_file.read_text(encoding="utf-8"))

    assert any("openstack.cloud.project_info" in t.get("module_name", "") or "project_info" in str(t) for t in tasks)
    assert any("ansible.builtin.assert" in t for t in tasks)

    tpl_task = next(t for t in tasks if t.get("ansible.builtin.template", {}).get("src") == "drover.conf.j2")
    assert tpl_task["ansible.builtin.template"]["dest"] == "{{ drover_config_dir }}/drover.conf"


def test_drover_conf_template_render():
    """Verify the production drover.conf template renders its current variable interface."""
    tpl_file = ROLE_DIR / "templates" / "drover.conf.j2"
    assert tpl_file.is_file()
    source = tpl_file.read_text(encoding="utf-8")
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    env.filters["bool"] = bool
    parsed = env.parse(source)
    context = {name: "" for name in meta.find_undeclared_variables(parsed)}
    context.update(
        drover_keystone_auth_url="http://127.0.0.1:5000/v3",
        drover_keystone_user="drover",
        drover_keystone_password_file="/etc/drover/secrets/os_password",
        drover_database_password_file="/etc/drover/secrets/database_password",
        drover_redis_password_file="/etc/drover/secrets/redis_password",
        drover_kubeconfig_encryption_key_file="/etc/drover/secrets/kubeconfig_encryption_key",
        drover_service_project_name="drover-service",
        drover_service_project_id="test-proj-id-123",
        drover_keystone_project_domain_name="Default",
        drover_keystone_user_domain_name="Default",
        drover_keystone_region_name="RegionOne",
        drover_keystone_interface="internal",
        openstack_insecure=False,
        openstack_cacert="",
        drover_database_url="mysql+aiomysql://drover@127.0.0.1:3306/drover",
        drover_redis_url="redis://127.0.0.1:6379/7",
        drover_callback_base_url="http://127.0.0.1:8011",
        drover_boot_volume_size_gb=30,
        drover_occm_enabled=True,
        drover_cinder_csi_enabled=True,
        drover_manila_csi_enabled=False,
        drover_keystone_auth_enabled=False,
        drover_octavia_ingress_enabled=False,
        drover_barbican_kms_enabled=False,
        drover_stampede_enabled=False,
    )
    rendered = env.from_string(source).render(context)
    assert "[keystone]" in rendered
    assert 'service_project_id = "test-proj-id-123"' in rendered
    assert 'callback_base_url = "http://127.0.0.1:8011"' in rendered
    assert "boot_volume_size_gb = 30" in rendered
    assert "occm_enabled = true" in rendered
    assert 'password_file = "/etc/drover/secrets/os_password"' in rendered
    assert 'password_file = "/etc/drover/secrets/database_password"' in rendered
    assert 'password_file = "/etc/drover/secrets/redis_password"' in rendered
    assert 'kubeconfig_encryption_key_file = "/etc/drover/secrets/kubeconfig_encryption_key"' in rendered
    assert "password =" not in rendered
    assert "kubeconfig_encryption_key =" not in rendered

def test_container_start_and_pull_policy():
    """Verify pull policy logic in pull.yml and start.yml."""
    pull_file = ROLE_DIR / "tasks" / "pull.yml"
    pull_tasks = yaml.safe_load(pull_file.read_text(encoding="utf-8"))
    pull_task = pull_tasks[0]
    assert pull_task["community.docker.docker_image"]["source"] == "pull"
    assert pull_task["community.docker.docker_image"]["force_source"] is True

    start_file = ROLE_DIR / "tasks" / "start.yml"
    start_tasks = yaml.safe_load(start_file.read_text(encoding="utf-8"))
    start_task = next(t for t in start_tasks if "community.docker.docker_container" in t)
    pull_expr = start_task["community.docker.docker_container"]["pull"]
    assert "never" in pull_expr and "always" in pull_expr


def test_haproxy_loadbalancer_contract():
    """Verify HAProxy task imports loadbalancer-config and configures health checks."""
    lb_file = ROLE_DIR / "tasks" / "loadbalancer.yml"
    lb_tasks = yaml.safe_load(lb_file.read_text(encoding="utf-8"))
    lb_task = next(t for t in lb_tasks if t.get("ansible.builtin.import_role", {}).get("name") == "loadbalancer-config")
    assert lb_task["vars"]["project_name"] == "drover"

    defaults = yaml.safe_load((ROLE_DIR / "defaults" / "main.yml").read_text(encoding="utf-8"))
    api_haproxy = defaults["drover_services"]["drover-api"]["haproxy"]["drover-api"]
    backend_extra = api_haproxy["backend_http_extra"]
    assert any("option httpchk GET /v1/health" in opt for opt in backend_extra)


def test_drover_kolla_wheel_packaging_lifecycle(tmp_path):
    """Build deploy/kolla wheel, inspect RECORD/shared-data, install into prefix, verify role tree, uninstall."""
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()

    build_cmd = ["uv", "build", "--wheel", "--out-dir", str(dist_dir)]
    res = subprocess.run(build_cmd, cwd=KOLLA_DIR, capture_output=True, text=True)
    assert res.returncode == 0, f"uv build failed:\nstdout: {res.stdout}\nstderr: {res.stderr}"

    wheels = list(dist_dir.glob("*.whl"))
    assert len(wheels) == 1, f"Expected 1 wheel file, found {wheels}"
    wheel_path = wheels[0]
    version = drover.__version__
    assert f"drover_kolla-{version}" in wheel_path.name

    # 2. Inspect wheel archive shared-data and RECORD
    with zipfile.ZipFile(wheel_path, "r") as zf:
        namelist = zf.namelist()
        shared_data_prefix = f"drover_kolla-{version}.data/data/share/kolla-ansible/ansible/roles/drover/"
        role_files_in_zip = [name for name in namelist if name.startswith(shared_data_prefix)]
        assert len(role_files_in_zip) > 0, "No shared-data role files found in wheel archive!"

        # Destination path check: must be share/kolla-ansible/ansible/roles/drover/
        for member in role_files_in_zip:
            if not member.endswith("/"):
                info = zf.getinfo(member)
                assert info.file_size > 0, f"Shared data file in wheel is empty: {member}"

        # Verify key role files in wheel archive
        assert any(member.endswith("defaults/main.yml") for member in role_files_in_zip)
        assert any(member.endswith("tasks/main.yml") for member in role_files_in_zip)
        assert any(member.endswith("tasks/preconditions_keystone.yml") for member in role_files_in_zip)
        assert any(member.endswith("templates/drover.conf.j2") for member in role_files_in_zip)

        metadata_members = [name for name in namelist if name.endswith(".dist-info/METADATA")]
        assert len(metadata_members) == 1
        metadata_content = zf.read(metadata_members[0]).decode("utf-8")
        assert "Requires-Python: >=3.11" in metadata_content

        # Inspect RECORD file
        record_members = [name for name in namelist if name.endswith("RECORD")]
        assert len(record_members) == 1
        record_content = zf.read(record_members[0]).decode("utf-8")
        assert "share/kolla-ansible/ansible/roles/drover/tasks/main.yml" in record_content

    # 3. Install into an isolated venv without application dependencies.
    venv_dir = tmp_path / "venv"
    create_venv = subprocess.run(
        ["uv", "venv", "--python", sys.executable, str(venv_dir)],
        capture_output=True,
        text=True,
    )
    assert create_venv.returncode == 0, create_venv.stderr
    venv_python = venv_dir / "bin" / "python"
    install_cmd = ["uv", "pip", "install", "--python", str(venv_python), "--no-deps", str(wheel_path)]
    res_inst = subprocess.run(install_cmd, capture_output=True, text=True)
    assert res_inst.returncode == 0, f"uv pip install failed:\nstdout: {res_inst.stdout}\nstderr: {res_inst.stderr}"

    installed_role_dir = venv_dir / "share" / "kolla-ansible" / "ansible" / "roles" / "drover"
    assert installed_role_dir.is_dir(), f"Installed role directory missing: {installed_role_dir}"
    assert (installed_role_dir / "defaults" / "main.yml").is_file()
    assert (installed_role_dir / "tasks" / "main.yml").is_file()
    assert (installed_role_dir / "tasks" / "deploy.yml").is_file()
    assert (installed_role_dir / "tasks" / "preconditions_keystone.yml").is_file()
    assert (installed_role_dir / "templates" / "drover.conf.j2").is_file()

    inst_defaults = yaml.safe_load((installed_role_dir / "defaults" / "main.yml").read_text(encoding="utf-8"))
    assert inst_defaults["drover_image_tag"] == f"v{version}"

    # 4. Uninstall the wheel and confirm its owned role files are removed.
    uninstall_cmd = ["uv", "pip", "uninstall", "--python", str(venv_python), "drover-kolla"]
    res_uninst = subprocess.run(uninstall_cmd, capture_output=True, text=True)
    assert res_uninst.returncode == 0, f"uv pip uninstall failed:\nstdout: {res_uninst.stdout}\nstderr: {res_uninst.stderr}"
    remaining_files = list(installed_role_dir.glob("**/*")) if installed_role_dir.exists() else []
    assert not [path for path in remaining_files if path.is_file()]
