# Drover

Drover는 OpenStack 환경에서 K3s 클러스터와 노드그룹의 lifecycle, 내구성 operation/job, OpenStack inventory reconciliation을 제공하는 control plane 서비스입니다.

- 현재 구조와 변경 시 갱신 규칙: [ARCHITECTURE.md](ARCHITECTURE.md)
- API 및 운영 문서 색인: [docs/README.md](docs/README.md)
- 패키지·실행 진입점: [pyproject.toml](pyproject.toml)
- CI 계약: [.github/workflows/ci.yml](.github/workflows/ci.yml)

문서 또는 코드 변경 전 `ARCHITECTURE.md`를 읽고, 변경 후 source를 다시 검토한 다음 다음 명령으로 architecture snapshot을 확인합니다.

```bash
python3 scripts/check_architecture.py
```

## Package and Kolla role

The root `drover` wheel owns the Python package and the Kolla-Ansible role as wheel shared data. Installing the wheel places the role at `share/kolla-ansible/ansible/roles/drover` under the installation prefix; it does not install Kolla-Ansible or service runtime dependencies.

Use the `service` extra to run the API, worker, or migration entry points locally:

```bash
uv sync --extra service --extra dev --frozen
```

Build the distributable root wheel with `uv build --wheel`. Container builds use the repository-root context and `docker/Dockerfile`:

```bash
docker build --file docker/Dockerfile --target drover-api .
```
