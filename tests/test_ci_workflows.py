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
    assert all(
        s["uses"] == "aquasecurity/trivy-action@57a97c7e7821a5776cebc9bb87c984fa69cba8f1"
        for s in trivy_steps
    ), "Trivy action must use the immutable known-safe 0.35.0 revision"

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
    assert migrate_step["run"] == "uv run drover-migrate --apply"

    readiness_step = next((s for s in db_steps if "readiness_checks" in s.get("run", "")), None)
    assert readiness_step is not None, "Missing readiness check smoke step"
    assert "init_db(os.environ['DATABASE_URL'])" in readiness_step["run"]

    # Validate package-kolla-assets
    kolla_job = jobs["package-kolla-assets"]
    kolla_steps = kolla_job.get("steps", [])
    artifact_step = next((s for s in kolla_steps if "upload-artifact" in s.get("uses", "")), None)
    assert artifact_step is not None, "Missing upload-artifact step in package-kolla-assets job"
    assert artifact_step["with"]["path"] == "deploy/kolla/"


def test_staging_workflow_structure():
    """Verify the manual live gate is isolated from automatic CI and fail-closed."""
    staging_file = WORKFLOWS_DIR / "staging.yml"
    assert staging_file.is_file()
    workflow_source = staging_file.read_text(encoding="utf-8")
    assert "workflow_dispatch:" in workflow_source
    assert "workflow_run:" not in workflow_source
    staging = yaml.safe_load(workflow_source)

    assert staging["concurrency"] == {
        "group": "drover-staging-gate",
        "cancel-in-progress": False,
    }
    gate_job = staging["jobs"]["staging-gate"]
    assert gate_job["environment"] == "staging"
    assert gate_job["timeout-minutes"] == 90
    steps = gate_job["steps"]

    checkout_step = next(step for step in steps if step.get("name") == "Checkout requested revision")
    assert checkout_step["with"]["ref"] == "${{ github.sha }}"

    assertion_step = next(
        step for step in steps if "Assert required staging gate secrets" in step.get("name", "")
    )
    assertion_env = assertion_step["env"]
    assert assertion_env["DROVER_INTEGRATION_CLOUD"] == "1"
    for required_name in (
        "OS_AUTH_URL",
        "OS_USERNAME",
        "OS_PASSWORD",
        "DROVER_INTEGRATION_NETWORK_ID",
        "DROVER_INTEGRATION_SUBNET_ID",
        "DROVER_INTEGRATION_IMAGE_ID",
        "DROVER_INTEGRATION_FLAVOR_ID",
        "DROVER_INTEGRATION_EXTERNAL_NET_ID",
        "DROVER_INTEGRATION_VOLUME_AZ",
        "DROVER_API_URL",
    ):
        assert required_name in assertion_env
    assert "HTTPS required" in assertion_step["run"]

    asset_step = next(step for step in steps if step.get("name") == "Validate packaged Kolla role assets")
    assert "live deployment is verified separately" in asset_step["run"]

    catalog_step = next(step for step in steps if step.get("name") == "Assert live Drover catalog and liveness")
    catalog_code = catalog_step["run"]
    assert "services(type='drover')" in catalog_code
    assert "/v1/health/live" in catalog_code
    assert "container-infra" not in catalog_code

    integration_step = next(
        step for step in steps if step.get("name") == "Execute live OpenStack integration test suite"
    )
    assert integration_step["run"] == "uv run pytest tests/integration -v"
    assert integration_step["env"]["DROVER_API_URL"] == "${{ vars.DROVER_API_URL }}"


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


def test_release_workflow_structure():
    """Verify GitHub Release workflow structure, tag trigger, permissions, and wheel verification."""
    release_file = WORKFLOWS_DIR / "release.yml"
    assert release_file.is_file(), "release.yml must exist in .github/workflows/"

    workflow_source = release_file.read_text(encoding="utf-8")
    release_data = yaml.safe_load(workflow_source)

    # Must trigger on v* tags only
    triggers = release_data.get("on", release_data.get(True, {}))
    assert "push" in triggers, "release workflow must trigger on push"
    push_tags = triggers["push"].get("tags", [])
    assert any("v*" in tag or "v" in tag for tag in push_tags), "release workflow must trigger on v* tags"
    assert "pull_request" not in triggers, "release workflow must not trigger on pull_request"

    # Must enforce least privilege permissions
    top_permissions = release_data.get("permissions", {})
    assert top_permissions.get("contents") == "read", "Top-level permissions must be read-only"

    jobs = release_data.get("jobs", {})
    assert "release" in jobs or "build-and-release" in jobs, "Missing release job"
    release_job = jobs.get("release") or jobs.get("build-and-release")

    job_permissions = release_job.get("permissions", {})
    assert job_permissions.get("contents") == "write", "Release job must have write permissions for GitHub Release"

    steps = release_job.get("steps", [])
    setup_uv_step = next(step for step in steps if step.get("uses") == "astral-sh/setup-uv@v6")
    assert setup_uv_step["with"]["python-version"] == "3.11"

    # Check lockstep step
    lockstep_step = next((s for s in steps if "lockstep" in s.get("name", "").lower() or "lockstep" in s.get("run", "").lower()), None)
    assert lockstep_step is not None, "Missing tag and version lockstep check step"

    # Check wheel build step
    build_step = next((s for s in steps if "deploy/kolla" in s.get("run", "")), None)
    assert build_step is not None, "Missing deploy/kolla wheel build step"

    # Check clean venv test & uninstall step
    venv_step = next((s for s in steps if "kolla-ansible" in s.get("run", "") and "venv" in s.get("run", "")), None)
    assert venv_step is not None, "Missing clean venv installation & verification step"
    venv_run = venv_step["run"]
    assert "share/kolla-ansible/ansible/roles/drover" in venv_run
    assert "pip uninstall" in venv_run
    assert "python3.11 -m venv" in venv_run

    # Check upload artifact step
    upload_step = next((s for s in steps if "upload-artifact" in s.get("uses", "")), None)
    assert upload_step is not None, "Missing upload-artifact step"
    assert "deploy/kolla/dist" in upload_step["with"]["path"] or "*.whl" in upload_step["with"]["path"]

    # Check softprops/action-gh-release step
    gh_release_step = next((s for s in steps if "action-gh-release" in s.get("uses", "")), None)
    assert gh_release_step is not None, "Missing action-gh-release step"
    assert gh_release_step["with"]["files"] == "deploy/kolla/dist/*.whl"

    # Must not publish to PyPI
    assert "pypa/gh-action-pypi-publish" not in workflow_source
