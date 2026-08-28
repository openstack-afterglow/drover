"""Tests for GitHub Actions CI/CD workflows and deployment gate contracts."""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"


def test_workflow_yaml_syntax():
    """Verify all workflow files parse cleanly as valid YAML."""
    yml_files = list(WORKFLOWS_DIR.glob("*.yml"))
    assert len(yml_files) >= 3, f"Expected at least 3 workflow files in {WORKFLOWS_DIR}"
    for yml_file in yml_files:
        content = yml_file.read_text(encoding="utf-8")
        parsed = yaml.safe_load(content)
        assert isinstance(parsed, dict), f"Failed to parse workflow file {yml_file}"
        assert "name" in parsed, f"Workflow {yml_file.name} missing 'name'"
        assert "jobs" in parsed, f"Workflow {yml_file.name} missing 'jobs'"


def test_ci_workflow_structure():
    """Verify CI workflow contains required jobs, services, targets, and artifact steps."""
    ci_file = WORKFLOWS_DIR / "ci.yml"
    assert ci_file.is_file()
    ci = yaml.safe_load(ci_file.read_text(encoding="utf-8"))

    jobs = ci.get("jobs", {})
    assert "service" in jobs
    assert "sdk" in jobs
    assert "docker-build-and-scan" in jobs
    assert "db-migration-and-readiness" in jobs
    assert "package-kolla-assets" in jobs

    # Validate docker-build-and-scan job
    scan_job = jobs["docker-build-and-scan"]
    steps = scan_job.get("steps", [])
    api_target_step = next((s for s in steps if s.get("with", {}).get("target") == "drover-api"), None)
    worker_target_step = next((s for s in steps if s.get("with", {}).get("target") == "drover-worker"), None)
    assert api_target_step is not None, "Missing drover-api docker build step"
    assert worker_target_step is not None, "Missing drover-worker docker build step"

    trivy_steps = [s for s in steps if "trivy-action" in s.get("uses", "")]
    assert len(trivy_steps) >= 2, "Expected Trivy scanning steps for both Docker targets"

    # Validate db-migration-and-readiness services & steps
    db_job = jobs["db-migration-and-readiness"]
    services = db_job.get("services", {})
    assert "mariadb" in services
    assert "redis" in services
    assert services["mariadb"]["image"].startswith("mariadb")
    assert services["redis"]["image"].startswith("redis")

    db_steps = db_job.get("steps", [])
    migrate_step = next((s for s in db_steps if "drover-migrate" in s.get("run", "")), None)
    assert migrate_step is not None, "Missing drover-migrate step in db-migration-and-readiness job"

    readiness_step = next((s for s in db_steps if "readiness_checks" in s.get("run", "")), None)
    assert readiness_step is not None, "Missing readiness check smoke step"

    # Validate package-kolla-assets
    kolla_job = jobs["package-kolla-assets"]
    kolla_steps = kolla_job.get("steps", [])
    artifact_step = next((s for s in kolla_steps if "upload-artifact" in s.get("uses", "")), None)
    assert artifact_step is not None, "Missing upload-artifact step in package-kolla-assets job"
    assert artifact_step["with"]["path"] == "deploy/kolla/"


def test_staging_workflow_structure():
    """Verify Staging workflow enforces contract assertions, Kolla deploy, Keystone catalog, and integration suite."""
    staging_file = WORKFLOWS_DIR / "staging.yml"
    assert staging_file.is_file()
    staging = yaml.safe_load(staging_file.read_text(encoding="utf-8"))

    jobs = staging.get("jobs", {})
    assert "staging-gate" in jobs

    gate_job = jobs["staging-gate"]
    steps = gate_job.get("steps", [])

    # Step 1: Assert staging gate contract/secrets
    assertion_step = next((s for s in steps if "Assert required staging gate secrets" in s.get("name", "")), None)
    assert assertion_step is not None, "Missing staging secrets assertion step"
    run_code = assertion_step.get("run", "")
    assert "DROVER_INTEGRATION_CLOUD" in run_code
    assert "OS_AUTH_URL" in run_code
    assert "OS_USERNAME" in run_code
    assert "OS_PASSWORD" in run_code

    # Step 2: Deploy Kolla role assets
    deploy_step = next((s for s in steps if "Deploy Kolla role" in s.get("name", "")), None)
    assert deploy_step is not None, "Missing Deploy Kolla role step"

    # Step 3: Assert Keystone catalog endpoint discovery for /v1
    catalog_step = next((s for s in steps if "Assert Keystone catalog" in s.get("name", "")), None)
    assert catalog_step is not None, "Missing Assert Keystone catalog step"
    cat_code = catalog_step.get("run", "")
    assert "container-infra" in cat_code
    assert "endswith('/v1')" in cat_code or 'endswith("/v1")' in cat_code

    # Step 4: Run integration suite
    integration_step = next((s for s in steps if "Execute live OpenStack integration" in s.get("name", "")), None)
    assert integration_step is not None, "Missing integration test suite step"
    assert "pytest tests/integration" in integration_step.get("run", "")


def test_pyproject_script_entrypoints():
    """Verify pyproject.toml defines required executable entrypoints."""
    pyproject_file = REPO_ROOT / "pyproject.toml"
    assert pyproject_file.is_file()
    import tomllib

    data = tomllib.loads(pyproject_file.read_text(encoding="utf-8"))
    scripts = data.get("project", {}).get("scripts", {})

    assert scripts.get("drover-api") == "drover.main:run"
    assert scripts.get("drover-worker") == "drover.worker:main"
    assert scripts.get("drover-migrate") == "drover.scripts.migrate:main"
