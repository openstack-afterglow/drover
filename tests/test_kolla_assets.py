"""Tests for Kolla-Ansible deployment assets under deploy/kolla/."""

import json
from pathlib import Path

import jinja2
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
KOLLA_DIR = REPO_ROOT / "deploy" / "kolla"


def test_yaml_files_are_valid():
    yaml_files = list(KOLLA_DIR.glob("**/*.yml"))
    assert len(yaml_files) >= 5, f"Expected at least 5 YAML files in {KOLLA_DIR}"
    for yml_file in yaml_files:
        content = yml_file.read_text(encoding="utf-8")
        parsed = yaml.safe_load(content)
        assert parsed is not None, f"Failed to parse YAML file: {yml_file}"


def test_jinja2_templates_compile():
    template_files = list(KOLLA_DIR.glob("templates/*.j2"))
    assert len(template_files) >= 4, f"Expected at least 4 Jinja2 templates in {KOLLA_DIR}"
    env = jinja2.Environment()
    for tpl_file in template_files:
        content = tpl_file.read_text(encoding="utf-8")
        # Ensure Jinja syntax compiles without syntax errors
        parsed = env.parse(content)
        assert parsed is not None, f"Failed to parse Jinja template: {tpl_file}"


def test_defaults_variable_interface():
    defaults_file = KOLLA_DIR / "defaults" / "main.yml"
    assert defaults_file.is_file()
    defaults = yaml.safe_load(defaults_file.read_text(encoding="utf-8"))

    # Required service definitions
    assert "drover_services" in defaults
    services = defaults["drover_services"]
    assert "drover-api" in services
    assert "drover-worker" in services
    assert "drover-migrate" in services

    # Validate health probe endpoints in API service healthcheck & readiness_check
    api_healthcheck = services["drover-api"]["healthcheck"]["test"]
    api_readiness = services["drover-api"]["readiness_check"]["test"]
    assert any("/v1/health/live" in arg for arg in api_healthcheck)
    assert any("/v1/health/ready" in arg for arg in api_readiness)

    # Validate secret mount directory
    assert "drover_secret_dir" in defaults
    assert defaults["drover_secret_dir"] == "/etc/drover/secrets"

    # Validate variable interfaces exist
    expected_vars = [
        "drover_database_url",
        "drover_redis_url",
        "drover_callback_base_url",
        "drover_kubeconfig_encryption_key",
        "drover_keystone_auth_url",
        "drover_keystone_password",
        "drover_keystone_service_name",
        "drover_keystone_service_type",
        "drover_keystone_admin_role",
        "drover_replace_magnum_catalog",
        "drover_public_endpoint",
        "drover_internal_endpoint",
        "drover_admin_endpoint",
        "drover_callback_allowed_cidrs",
        "drover_mariadb_address",
        "drover_mariadb_port",
    ]
    for v in expected_vars:
        assert v in defaults, f"Missing variable interface '{v}' in defaults/main.yml"

    # Specific Keystone catalog values
    assert defaults["drover_keystone_service_name"] == "drover"
    assert defaults["drover_keystone_service_type"] == "container-infra"
    assert defaults["drover_keystone_admin_role"] == "admin"
    assert defaults["drover_replace_magnum_catalog"] is False

    # Ensure public, internal, and admin endpoints end with /v1
    assert defaults["drover_public_endpoint"].endswith("/v1")
    assert defaults["drover_internal_endpoint"].endswith("/v1")
    assert defaults["drover_admin_endpoint"].endswith("/v1")

    # Ensure no concrete passwords are baked into defaults file
    raw_defaults = defaults_file.read_text(encoding="utf-8")
    assert "secret_password" not in raw_defaults
    assert "my_secret_key" not in raw_defaults


def test_migration_admission_ordering():
    main_tasks_file = KOLLA_DIR / "tasks" / "main.yml"
    assert main_tasks_file.is_file()
    tasks = yaml.safe_load(main_tasks_file.read_text(encoding="utf-8"))

    imported_files = []
    for task in tasks:
        if "ansible.builtin.import_tasks" in task:
            imported_files.append(task["ansible.builtin.import_tasks"])

    assert "precheck.yml" in imported_files
    assert "config.yml" in imported_files
    assert "register.yml" in imported_files
    assert "bootstrap.yml" in imported_files
    assert "deploy.yml" in imported_files

    # Enforce admission order: bootstrap.yml (migration gate) MUST come before deploy.yml
    bootstrap_idx = imported_files.index("bootstrap.yml")
    deploy_idx = imported_files.index("deploy.yml")
    assert bootstrap_idx < deploy_idx, "bootstrap.yml (migration gate) must precede deploy.yml"

    migration_container = json.loads((KOLLA_DIR / "templates" / "drover-migrate.json.j2").read_text(encoding="utf-8"))
    assert migration_container["command"] == "drover-migrate --apply"


def test_keystone_registration_tasks_structure():
    register_tasks_file = KOLLA_DIR / "tasks" / "register.yml"
    assert register_tasks_file.is_file()
    tasks = yaml.safe_load(register_tasks_file.read_text(encoding="utf-8"))

    # Convert task list into searchable structure
    task_names = [t.get("name", "") for t in tasks]

    # 1. User & Admin Role Grant
    assert any("Create Drover Keystone service user" in name for name in task_names)
    role_grant_task = next(t for t in tasks if "Grant admin role" in t.get("name", ""))
    assert role_grant_task["openstack.cloud.role_assignment"]["role"] == "{{ drover_keystone_admin_role }}"

    # 2. Collision Guard for container-infra
    query_task = next(t for t in tasks if "Query existing Keystone services" in t.get("name", ""))
    assert query_task["openstack.cloud.identity_service_info"]["type"] == "{{ drover_keystone_service_type }}"

    assert_task = next(t for t in tasks if "Verify Magnum catalog collision guard" in t.get("name", ""))
    assert_cond = assert_task["ansible.builtin.assert"]["that"][0]
    assert "selectattr('name', 'ne', drover_keystone_service_name)" in assert_cond

    # 3. Single service entry creation
    svc_task = next(t for t in tasks if "Create or update Drover Keystone service entry" in t.get("name", ""))
    assert svc_task["openstack.cloud.identity_service"]["name"] == "{{ drover_keystone_service_name }}"
    assert svc_task["openstack.cloud.identity_service"]["service_type"] == "{{ drover_keystone_service_type }}"

    # 4. Endpoint loop check
    endpoint_task = next(t for t in tasks if "Register Drover public" in t.get("name", ""))
    endpoints = [item["interface"] for item in endpoint_task["loop"]]
    assert set(endpoints) == {"public", "internal", "admin"}
    for item in endpoint_task["loop"]:
        assert "_endpoint" in item["url"]


def test_keystone_catalog_single_service_no_competing_endpoints():
    register_tasks_file = KOLLA_DIR / "tasks" / "register.yml"
    tasks = yaml.safe_load(register_tasks_file.read_text(encoding="utf-8"))

    # Assert no rescue blocks or secondary catalog service creation tasks exist
    for task in tasks:
        assert "rescue" not in task, "register.yml must not contain rescue blocks that create competing services"

    # Count service creation tasks - must be exactly 1
    svc_tasks = [t for t in tasks if "openstack.cloud.identity_service" in t]
    assert len(svc_tasks) == 1, f"Expected exactly 1 identity_service task, found {len(svc_tasks)}"
    assert svc_tasks[0]["openstack.cloud.identity_service"]["name"] == "{{ drover_keystone_service_name }}"

    # Count endpoint creation tasks - must be exactly 1
    ep_tasks = [t for t in tasks if "openstack.cloud.endpoint" in t]
    assert len(ep_tasks) == 1, f"Expected exactly 1 endpoint task, found {len(ep_tasks)}"
    assert ep_tasks[0]["openstack.cloud.endpoint"]["service"] == "{{ drover_keystone_service_name }}"

    # Collision guard assertion MUST run before service creation
    query_idx = next(i for i, t in enumerate(tasks) if "Query existing Keystone services" in t.get("name", ""))
    assert_idx = next(i for i, t in enumerate(tasks) if "Verify Magnum catalog collision guard" in t.get("name", ""))
    create_idx = next(i for i, t in enumerate(tasks) if "Create or update Drover Keystone service entry" in t.get("name", ""))

    assert query_idx < assert_idx < create_idx, "Collision guard query and assertion must precede service creation"
def test_kolla_drover_conf_renders_callback_allowed_cidrs():
    tpl_file = KOLLA_DIR / "templates" / "drover.conf.j2"
    assert tpl_file.is_file()
    env = jinja2.Environment()
    template = env.from_string(tpl_file.read_text(encoding="utf-8"))
    rendered = template.render(
        drover_keystone_auth_url="http://127.0.0.1:5000/v3",
        drover_keystone_username="drover",
        drover_keystone_password="secret_password",
        drover_keystone_project_name="service",
        drover_keystone_project_domain_name="Default",
        drover_keystone_user_domain_name="Default",
        drover_keystone_region="RegionOne",
        drover_keystone_interface="internal",
        drover_keystone_insecure=False,
        drover_openstack_cacert="",
        drover_service_project_id="",
        drover_admin_legacy_project_policy=False,
        drover_database_url="mysql+aiomysql://drover:pass@127.0.0.1:3306/drover",
        drover_redis_url="redis://127.0.0.1:6379/7",
        drover_callback_base_url="http://127.0.0.1:8011",
        drover_kubeconfig_encryption_key="a" * 64,
        drover_boot_volume_size_gb=30,
        drover_occm_enabled=True,
        drover_cinder_csi_enabled=True,
        drover_manila_csi_enabled=False,
        drover_k3s_health_interval=180,
        drover_callback_allowed_cidrs=["10.0.0.0/8", "172.16.0.0/12"],
    )
    assert "[drover]" in rendered
    assert "callback_allowed_cidrs = \"10.0.0.0/8,172.16.0.0/12\"" in rendered
def test_kolla_policy_yaml_renders_overrides():
    tpl_file = KOLLA_DIR / "templates" / "policy.yaml.j2"
    assert tpl_file.is_file()
    env = jinja2.Environment()
    template = env.from_string(tpl_file.read_text(encoding="utf-8"))

    # Test default render without overrides
    rendered_default = template.render()
    parsed_default = yaml.safe_load(rendered_default)
    assert parsed_default["drover:admin"] == "rule:context_is_admin"

    # Test dict overrides
    rendered_override = template.render(
        drover_policy_overrides={
            "drover:templates:manage": "role:template_manager or is_system_admin:True",
        }
    )
    parsed_override = yaml.safe_load(rendered_override)
    assert parsed_override["drover:templates:manage"] == "role:template_manager or is_system_admin:True"
