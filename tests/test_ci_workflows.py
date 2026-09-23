"""Tests for GitHub Actions CI/CD workflows and deployment gate contracts."""

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"


def _load_workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS_DIR / name).read_text(encoding="utf-8"))


def _triggers(workflow: dict) -> dict:
    # PyYAML parses the bare `on:` key as boolean True.
    return workflow.get("on", workflow.get(True, {}))


def _trigger_names(workflow: dict) -> set[str]:
    # `on:` may be a mapping, a list (`on: [push, pull_request]`) or a single string (`on: push`).
    triggers = _triggers(workflow)
    if isinstance(triggers, str):
        return {triggers}
    return set(triggers or ())


def _needs(job: dict) -> list[str]:
    # `needs: test` and `needs: [test]` are equivalent.
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


def _sets_continue_on_error(node: dict) -> bool:
    # Absent or the literal `false` keep failures failing; `true` or any `${{ }}` expression may not.
    return node.get("continue-on-error", False) is not False


def _pr_reachable_workflows() -> dict[str, dict]:
    """Workflows that run pull request code: PR-triggered files plus the local reusable workflows they call."""
    paths = [*WORKFLOWS_DIR.glob("*.yml"), *WORKFLOWS_DIR.glob("*.yaml")]
    workflows = {path.name: yaml.safe_load(path.read_text(encoding="utf-8")) for path in paths}
    pending = [name for name, wf in workflows.items() if _trigger_names(wf) & {"pull_request", "pull_request_target"}]
    reachable: dict[str, dict] = {}
    while pending:
        name = pending.pop()
        if name in reachable:
            continue
        reachable[name] = workflows[name]
        for job_name, job in workflows[name]["jobs"].items():
            uses = job.get("uses")
            if uses is None:
                continue
            assert uses.startswith("./.github/workflows/"), (
                f"{name} `{job_name}` runs PR code through a remote reusable workflow whose runners this test cannot check"
            )
            pending.append(uses.removeprefix("./.github/workflows/"))
    return reachable


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
    """Verify CI jobs, service dependencies, Docker targets, and root-wheel artifact packaging."""
    ci_file = WORKFLOWS_DIR / "ci.yml"
    assert ci_file.is_file()
    ci = yaml.safe_load(ci_file.read_text(encoding="utf-8"))

    jobs = ci.get("jobs", {})
    assert "service" in jobs
    assert "sdk" in jobs
    assert "docker-build-and-scan" in jobs
    assert "db-migration-and-readiness" in jobs
    assert "package-wheel" in jobs

    # Validate docker-build-and-scan job
    scan_job = jobs["docker-build-and-scan"]
    steps = scan_job.get("steps", [])
    api_target_step = next((s for s in steps if s.get("with", {}).get("target") == "drover-api"), None)
    worker_target_step = next((s for s in steps if s.get("with", {}).get("target") == "drover-worker"), None)
    assert api_target_step is not None, "Missing drover-api docker build step"
    assert worker_target_step is not None, "Missing drover-worker docker build step"
    assert api_target_step["with"]["file"] == "docker/Dockerfile"
    assert worker_target_step["with"]["file"] == "docker/Dockerfile"

    trivy_steps = [s for s in steps if "trivy-action" in s.get("uses", "")]
    assert len(trivy_steps) >= 2, "Expected Trivy scanning steps for both Docker targets"
    assert all(
        s["uses"] == "aquasecurity/trivy-action@57a97c7e7821a5776cebc9bb87c984fa69cba8f1" for s in trivy_steps
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

    # Validate root-wheel artifact packaging
    wheel_job = jobs["package-wheel"]
    wheel_steps = wheel_job.get("steps", [])
    artifact_step = next((s for s in wheel_steps if "upload-artifact" in s.get("uses", "")), None)
    assert artifact_step is not None, "Missing upload-artifact step in package-wheel"
    assert artifact_step["with"]["name"] == "drover-wheel"
    assert artifact_step["with"]["path"] == "dist/*.whl"


def test_suite_runs_once_per_event_and_gates_publication():
    """PR는 CI 한 번, push/tag는 Docker Build & Push의 reusable `test` 한 번만 실행한다."""
    ci_triggers = _triggers(_load_workflow("ci.yml"))
    # 값 없는 `workflow_call:`은 None으로 파싱되므로 truthiness가 아니라 key 존재로 확인한다.
    assert "workflow_call" in ci_triggers, "Docker Build & Push reuses ci.yml as its test gate"
    assert "push" not in ci_triggers, "push is tested once by docker-build.yml `test`; a CI push trigger duplicates it"
    # 전체 trigger 집합을 고정한다. public 저장소에서 pull_request_target·workflow_run 같은
    # 권한 있는 trigger나 schedule/push 재추가는 이 계약을 함께 바꾸는 명시적 결정이어야 한다.
    assert set(ci_triggers) == {"workflow_call", "pull_request", "workflow_dispatch"}, (
        f"unexpected ci.yml triggers: {sorted(ci_triggers)}"
    )
    assert ci_triggers["pull_request"] == {"branches": ["main", "dev"]}, (
        "every PR (fork and dependabot included) runs CI without path or branch-name skips"
    )

    docker = _load_workflow("docker-build.yml")
    docker_triggers = _triggers(docker)
    assert "pull_request" not in docker_triggers, "PRs are covered by CI; no duplicate suite or non-pushing image build"
    assert set(docker_triggers) == {"push", "workflow_dispatch"}, (
        f"unexpected docker-build.yml triggers: {sorted(docker_triggers)}"
    )
    assert docker_triggers["push"]["branches"] == ["main", "dev"]
    assert docker_triggers["push"]["tags"] == ["v*"]

    jobs = docker["jobs"]
    assert jobs["test"]["uses"] == "./.github/workflows/ci.yml"
    assert "test" in _needs(jobs["build-and-push"]), "image publication must wait for the whole test workflow"


def test_publication_gate_is_fail_closed():
    """A failed, skipped or cancelled suite never publishes images and never reports success to its caller."""
    docker_jobs = _load_workflow("docker-build.yml")["jobs"]
    for name in ("test", "build-and-push"):
        job = docker_jobs[name]
        # A job-level if replaces the implicit success() on needs. always(), !cancelled() and
        # !failure() && !cancelled() (the usual companion of change detection) all run
        # build-and-push after `test` failed or was skipped.
        assert "if" not in job, f"docker-build.yml `{name}` must not set a job-level if: {job.get('if')!r}"
        assert not _sets_continue_on_error(job), f"docker-build.yml `{name}` must not set continue-on-error"

    for job_name, job in _load_workflow("ci.yml")["jobs"].items():
        assert not _sets_continue_on_error(job), (
            f"ci.yml `{job_name}` continue-on-error reports a failing suite as success"
        )
        for step in job.get("steps", []):
            label = step.get("name") or step.get("run") or step.get("uses")
            assert not _sets_continue_on_error(step), (
                f"ci.yml `{job_name}` step {label!r} continue-on-error reports a failing check as success"
            )


def test_every_ci_job_and_step_runs_unconditionally_in_parallel():
    """Every ci.yml job and step runs for every PR and workflow_call, and no job waits on another."""
    for job_name, job in _load_workflow("ci.yml")["jobs"].items():
        # Skipped jobs and steps report success. A skip keyed on github.actor, head.repo, event_name
        # or a branch name is exactly the bypass rule 9 forbids; a skip is allowed only for a
        # same-repo PR whose merge tree equals an already tested head tree, and it must change
        # this contract explicitly.
        assert "if" not in job, f"ci.yml `{job_name}` must not set a job-level if: {job.get('if')!r}"
        for step in job.get("steps", []):
            label = step.get("name") or step.get("run") or step.get("uses")
            assert "if" not in step, f"ci.yml `{job_name}` step {label!r} must not set if: {step.get('if')!r}"
        # Rule 3: fail-fast checks run inside their job, not as a needs: gate in front of other jobs.
        assert not _needs(job), f"ci.yml `{job_name}` must not wait on {_needs(job)}; ci.yml jobs run in parallel"


def test_pr_code_runs_on_github_hosted_runners_with_read_only_token():
    """Rule 10: public-repo PR code runs only on GitHub-hosted ubuntu runners with a read-only token."""
    reachable = _pr_reachable_workflows()
    assert "ci.yml" in reachable
    for name, workflow in reachable.items():
        assert workflow.get("permissions") == {"contents": "read"}, (
            f"{name} runs PR code and must keep top-level permissions: contents: read"
        )
        for job_name, job in workflow["jobs"].items():
            assert "permissions" not in job, f"{name} `{job_name}` job-level permissions override the read-only pin"
            if "uses" in job:
                continue  # the called workflow is itself in `reachable` and checked there
            runs_on = job.get("runs-on")
            assert isinstance(runs_on, str) and runs_on.startswith("ubuntu-"), (
                f"{name} `{job_name}` runs PR code on {runs_on!r}; only GitHub-hosted ubuntu-* labels are allowed"
            )


def test_service_job_shape_and_every_built_image_is_scanned():
    """The architecture check leads `service`, the full suite runs unfiltered, and Trivy scans each built image."""
    jobs = _load_workflow("ci.yml")["jobs"]

    service_steps = jobs["service"]["steps"]
    assert service_steps[0].get("uses", "").startswith("actions/checkout@")
    assert service_steps[1].get("run") == "python3 scripts/check_architecture.py", (
        "rule 3: the fail-fast architecture check is the first step after checkout in `service`"
    )
    service_runs = [step["run"] for step in service_steps if "run" in step]
    assert [run for run in service_runs if "pytest" in run] == ["uv run pytest tests"], (
        "`service` runs the whole suite exactly once, with no path, -k or deselect filter"
    )
    assert "uv run ruff check ." in service_runs

    sdk_job = jobs["sdk"]
    assert sdk_job["defaults"]["run"]["working-directory"] == "sdk"
    assert [step["run"] for step in sdk_job["steps"] if "pytest" in step.get("run", "")] == ["uv run pytest"]

    scan_steps = jobs["docker-build-and-scan"]["steps"]
    built_tag_sets = [
        {tag.strip() for tag in re.split(r"[,\n]", step["with"]["tags"]) if tag.strip()}
        for step in scan_steps
        if "build-push-action" in step.get("uses", "")
    ]
    scanned = [step["with"]["image-ref"] for step in scan_steps if "trivy-action" in step.get("uses", "")]
    for tags in built_tag_sets:
        assert tags & set(scanned), f"built image {sorted(tags)} is never scanned by Trivy"
    all_built = set().union(*built_tag_sets)
    for ref in scanned:
        assert ref in all_built, f"Trivy scans {ref!r}, which this job does not build"


def test_scan_job_builds_into_daemon_with_docker_driver():
    """스캔용 이미지는 default docker driver로 daemon에 직접 빌드해 BuildKit boot와 tarball load를 피한다."""
    steps = _load_workflow("ci.yml")["jobs"]["docker-build-and-scan"]["steps"]
    for step in steps:
        if "setup-buildx-action" in step.get("uses", ""):
            assert step.get("with", {}).get("driver") == "docker", (
                "a docker-container builder re-adds the BuildKit boot and the load:true export/import"
            )

    build_steps = [s for s in steps if "build-push-action" in s.get("uses", "")]
    assert {s["with"]["target"] for s in build_steps} == {"drover-api", "drover-worker"}
    for step in build_steps:
        assert step["with"].get("load") is True, "Trivy scans the locally loaded image"
        assert "push" not in step["with"], "the scan job never publishes"
        assert "cache-to" not in step["with"], "the docker driver cannot export a build cache"


def test_db_service_health_checks_poll_fast_with_enough_retries():
    """서비스 컨테이너 health-check는 짧은 interval과 충분한 retry window를 쓴다."""
    services = _load_workflow("ci.yml")["jobs"]["db-migration-and-readiness"]["services"]
    for name in ("mariadb", "redis"):
        options = services[name]["options"]
        interval = re.search(r"--health-interval=(\d+)s\b", options)
        retries = re.search(r"--health-retries=(\d+)\b", options)
        assert interval and retries, f"{name} must set --health-interval and --health-retries"
        interval_s = int(interval.group(1))
        assert interval_s <= 2, f"{name} health interval {interval_s}s delays job start"
        assert interval_s * int(retries.group(1)) >= 30, f"{name} health window must stay at least 30s"


def test_docker_build_preserves_published_kolla_image_tag():
    """docker-build.yml must preserve the published tag the Kolla role consumes."""

    workflow_file = WORKFLOWS_DIR / "docker-build.yml"
    assert workflow_file.is_file()
    workflow = yaml.safe_load(workflow_file.read_text(encoding="utf-8"))
    triggers = workflow.get("on", workflow.get(True, {}))
    assert triggers.get("push", {}).get("tags") == ["v*"], "tag push must drive image publication"
    expected_tag = "v0.2.21"
    kolla_defaults = yaml.safe_load(
        (REPO_ROOT / "deploy" / "kolla" / "ansible" / "roles" / "drover" / "defaults" / "main.yml").read_text(encoding="utf-8")
    )
    assert kolla_defaults["drover_image_tag"] == expected_tag, "Kolla must keep the published image tag until images are released"
    for image_key in ("meta-api", "meta-worker"):
        step = next(s for s in workflow["jobs"]["build-and-push"]["steps"] if s.get("id") == image_key)
        tags = step["with"]["tags"]
        assert "type=ref,event=tag" in tags, (
            f"{image_key} must publish the raw git tag; metadata-action semver strips the v prefix "
            "that deploy/kolla drover_image_tag and precheck consume"
        )
        assert "type=semver" not in tags, f"{image_key} must not strip the v prefix"
        assert "type=raw,value=dev" in tags, f"{image_key} must keep the dev floating tag"


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

    assertion_step = next(step for step in steps if "Assert required staging gate secrets" in step.get("name", ""))
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

    asset_step = next(step for step in steps if step.get("name") == "Validate Kolla role source assets")
    assert "Kolla role source assets validated" in asset_step["run"]

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
    """Verify GitHub Release workflow packages the root wheel and its role data."""
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
    assert setup_uv_step["with"]["python-version"] == "3.12"

    # Check lockstep step
    lockstep_step = next(
        (s for s in steps if "lockstep" in s.get("name", "").lower() or "lockstep" in s.get("run", "").lower()), None
    )
    assert lockstep_step is not None, "Missing tag and version lockstep check step"

    build_step = next((s for s in steps if s.get("name") == "Build root wheel"), None)
    assert build_step is not None, "Missing root wheel build step"
    assert "uv build --wheel" in build_step["run"]

    # Check clean venv test & uninstall step
    venv_step = next((s for s in steps if "kolla-ansible" in s.get("run", "") and "venv" in s.get("run", "")), None)
    assert venv_step is not None, "Missing clean venv installation & verification step"
    venv_run = venv_step["run"]
    assert "share/kolla-ansible/ansible/roles/drover" in venv_run
    assert "uv pip uninstall --python /tmp/drover-venv/bin/python drover" in venv_run
    assert "uv venv --python 3.12" in venv_run

    # Check upload artifact step
    upload_step = next((s for s in steps if "upload-artifact" in s.get("uses", "")), None)
    assert upload_step is not None, "Missing upload-artifact step"
    assert "deploy/kolla/dist" in upload_step["with"]["path"] or "*.whl" in upload_step["with"]["path"]

    # Check softprops/action-gh-release step
    gh_release_step = next((s for s in steps if "action-gh-release" in s.get("uses", "")), None)
    assert gh_release_step is not None, "Missing action-gh-release step"
    assert gh_release_step["with"]["files"] == "dist/*.whl"

    # Must not publish to PyPI
    assert "pypa/gh-action-pypi-publish" not in workflow_source
