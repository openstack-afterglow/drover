from __future__ import annotations

import asyncio
import logging
import secrets
from typing import TYPE_CHECKING

from keystoneauth1 import session as ks_session
from keystoneauth1.identity import v3

from drover.config import get_settings

if TYPE_CHECKING:
    import openstack

_logger = logging.getLogger(__name__)

# admin 프로젝트 ID / admin role ID 캐시 (서비스 admin 크리덴셜 조회 결과)
_admin_project_id_cache: str | None = None
_admin_role_id_cache: str | None = None


def list_all_project_ids() -> set[str]:
    """admin 자격으로 전체 프로젝트 ID 셋 반환. 캐싱 없음 — orphan 스캔처럼 호출 빈도 낮은 경로용."""
    ks = _get_admin_ks_client()
    return {p.id for p in ks.projects.list()}


def _get_admin_ks_client():
    """서비스 admin 크리덴셜로 Keystone v3 Client 생성.

    사용자 세션은 OpenStack policy 때문에 role_assignments 조회가 제한될 수 있으므로,
    시스템 admin 판별은 서비스 크리덴셜로 수행한다.
    """
    from keystoneclient.v3 import client as ks_client

    settings = get_settings()
    admin_auth = v3.Password(
        auth_url=settings.os_auth_url,
        username=settings.os_username,
        password=settings.os_password,
        project_name=settings.os_project_name,
        user_domain_name=settings.os_user_domain_name,
        project_domain_name=settings.os_project_domain_name,
    )
    admin_sess = ks_session.Session(auth=admin_auth, timeout=15, verify=settings.ssl_verify)
    return ks_client.Client(session=admin_sess)


def _resolve_admin_ids() -> tuple[str | None, str | None]:
    """admin 프로젝트 ID와 admin role ID 반환 (프로세스 수명 동안 1회 조회)."""
    global _admin_project_id_cache, _admin_role_id_cache
    if _admin_project_id_cache and _admin_role_id_cache:
        return _admin_project_id_cache, _admin_role_id_cache

    settings = get_settings()
    try:
        ks = _get_admin_ks_client()
        admin_projects = ks.projects.list(name=settings.os_project_name)
        admin_roles = ks.roles.list(name="admin")
        if admin_projects and admin_roles:
            _admin_project_id_cache = admin_projects[0].id
            _admin_role_id_cache = admin_roles[0].id
            return _admin_project_id_cache, _admin_role_id_cache
    except Exception:
        _logger.warning("admin 프로젝트/role ID 조회 실패", exc_info=True)
    return None, None


def _has_system_admin_role(user_id: str) -> bool:
    """Keystone system:all scope admin role 보유 여부 확인."""
    try:
        _, admin_role_id = _resolve_admin_ids()
        if not admin_role_id:
            return False
        ks = _get_admin_ks_client()
        assignments = ks.role_assignments.list(
            user=user_id,
            role=admin_role_id,
            system="all",
        )
        return len(assignments) > 0
    except Exception:
        _logger.warning("_has_system_admin_role 체크 실패", exc_info=True)
        return False


def _has_admin_project_role(user_id: str) -> bool:
    """기존 'admin project + admin role' 정책으로 admin 여부 확인 (호환 모드용)."""
    try:
        admin_project_id, admin_role_id = _resolve_admin_ids()
        if not admin_project_id or not admin_role_id:
            return False
        ks = _get_admin_ks_client()
        assignments = ks.role_assignments.list(
            user=user_id,
            project=admin_project_id,
            role=admin_role_id,
        )
        return len(assignments) > 0
    except Exception:
        _logger.warning("_has_admin_project_role 체크 실패", exc_info=True)
        return False


def _is_system_admin(user_id: str, *_args, **_kwargs) -> bool:
    """system admin 여부 판별 (dual-mode).

    항상: Keystone system:all scope role 검사.
    admin_legacy_project_policy=True 일 때만 OR: 기존 'admin project + admin role' 검사.
    추가 인자(session, settings)는 호환성을 위해 받되 사용하지 않는다.
    """
    if not user_id:
        return False
    if _has_system_admin_role(user_id):
        return True
    settings = get_settings()
    if settings.admin_legacy_project_policy:
        return _has_admin_project_role(user_id)
    return False


def invalidate_admin_caches() -> None:
    """admin_project_id / admin_role_id 프로세스 캐시 리셋.

    admin project 또는 role이 운영 중 재생성될 때 system-roles 엔드포인트에서 명시 호출.
    """
    global _admin_project_id_cache, _admin_role_id_cache
    _admin_project_id_cache = None
    _admin_role_id_cache = None


def authenticate(username: str, password: str, project_name: str, domain_name: str = "Default") -> dict:
    """
    Keystone 인증 후 토큰과 사용자 정보를 반환.

    project_name이 지정되면 해당 프로젝트로 scoped 인증.
    미지정 시 unscoped 인증 → 사용자의 프로젝트 목록 조회 → 첫 번째 프로젝트로 scoped 토큰 발급.
    """
    settings = get_settings()

    if project_name:
        # 명시적 프로젝트 지정 시 직접 scoped 인증
        return _authenticate_scoped(
            username,
            password,
            project_name,
            domain_name,
            settings,
        )

    # 1) unscoped 인증
    unscoped_auth = v3.Password(
        auth_url=settings.os_auth_url,
        username=username,
        password=password,
        user_domain_name=domain_name,
        unscoped=True,
    )
    unscoped_sess = ks_session.Session(auth=unscoped_auth, timeout=30, verify=settings.ssl_verify)
    unscoped_access = unscoped_auth.get_access(unscoped_sess)
    unscoped_token = unscoped_access.auth_token
    user_id = unscoped_access.user_id

    # 2) 사용자의 프로젝트 목록 조회
    from keystoneclient.v3 import client as ks_client

    ks = ks_client.Client(session=unscoped_sess)
    projects = ks.projects.list(user=user_id)
    enabled_projects = [p for p in projects if p.enabled]

    if not enabled_projects:
        raise Exception("접근 가능한 프로젝트가 없습니다")

    # 3) default_project_id가 있으면 우선, 없으면 첫 번째 프로젝트
    default_pid = getattr(unscoped_access, "project_id", None)
    target_project = None
    if default_pid:
        target_project = next((p for p in enabled_projects if p.id == default_pid), None)
    if not target_project:
        target_project = enabled_projects[0]

    # 4) 선택된 프로젝트로 scoped 토큰 발급
    scoped_auth = v3.Token(
        auth_url=settings.os_auth_url,
        token=unscoped_token,
        project_id=target_project.id,
    )
    scoped_sess = ks_session.Session(auth=scoped_auth, timeout=30, verify=settings.ssl_verify)
    scoped_access = scoped_auth.get_access(scoped_sess)

    return {
        "token": scoped_access.auth_token,
        "project_id": scoped_access.project_id or "",
        "project_name": scoped_access.project_name or "",
        "user_id": scoped_access.user_id or "",
        "username": scoped_access.username or username,
        "expires_at": scoped_access.expires.isoformat() if scoped_access.expires else "",
        "roles": list(scoped_access.role_names) if scoped_access.role_names else [],
        "is_system_admin": _is_system_admin(scoped_access.user_id, scoped_sess, settings),
    }


def _authenticate_scoped(
    username: str,
    password: str,
    project_name: str,
    domain_name: str,
    settings,
) -> dict:
    """지정된 프로젝트로 직접 scoped 인증."""
    auth_plugin = v3.Password(
        auth_url=settings.os_auth_url,
        username=username,
        password=password,
        project_name=project_name,
        user_domain_name=domain_name,
        project_domain_name=settings.os_project_domain_name,
    )
    sess = ks_session.Session(auth=auth_plugin, timeout=30, verify=settings.ssl_verify)
    access = auth_plugin.get_access(sess)

    return {
        "token": access.auth_token,
        "project_id": access.project_id or "",
        "project_name": access.project_name or "",
        "user_id": access.user_id or "",
        "username": access.username or username,
        "expires_at": access.expires.isoformat() if access.expires else "",
        "roles": list(access.role_names) if access.role_names else [],
        "is_system_admin": _is_system_admin(access.user_id, sess, settings),
    }


def validate_token(token: str, project_id: str = "") -> dict:
    """
    토큰 유효성 검증. 유효하면 토큰 정보 반환, 아니면 예외 발생.
    """
    settings = get_settings()

    kwargs = {"auth_url": settings.os_auth_url, "token": token}
    if project_id:
        kwargs["project_id"] = project_id

    auth_plugin = v3.Token(**kwargs)
    sess = ks_session.Session(auth=auth_plugin, timeout=30, verify=settings.ssl_verify)
    access = auth_plugin.get_access(sess)

    return {
        "token": access.auth_token,
        "project_id": access.project_id or "",
        "project_name": access.project_name or "",
        "user_id": access.user_id or "",
        "username": access.username or "",
        "expires_at": access.expires.isoformat() if access.expires else "",
        "roles": list(access.role_names) if access.role_names else [],
        "is_system_admin": _is_system_admin(access.user_id, sess, settings),
    }


def get_openstack_connection(token: str, project_id: str) -> openstack.connection.Connection:
    """
    검증된 토큰으로 openstacksdk Connection 객체 반환.
    load_envvars=False 로 OS_* 환경변수를 무시해 v3.Token에 불필요한 인자가
    전달되는 오류를 방지한다.
    """
    import openstack

    settings = get_settings()

    return openstack.connect(
        load_envvars=False,
        load_yaml_config=False,
        auth_url=settings.os_auth_url,
        auth_type="token",
        token=token,
        project_id=project_id or None,
        region_name=settings.os_region_name,
        interface=settings.os_interface,
        api_timeout=30,
        verify=settings.ssl_verify,
    )


def get_admin_connection_for_project(project_id: str) -> openstack.connection.Connection:
    """관리자 크리덴셜로 특정 프로젝트에 스코프된 OpenStack 연결 반환.

    콜백 등 사용자 토큰이 없는 상황에서 프로젝트 리소스를 조작할 때 사용.
    """
    import openstack

    settings = get_settings()
    return openstack.connect(
        load_envvars=False,
        load_yaml_config=False,
        auth_url=settings.os_auth_url,
        auth_type="password",
        username=settings.os_username,
        password=settings.os_password,
        project_id=project_id,
        user_domain_name=settings.os_user_domain_name,
        project_domain_name=settings.os_project_domain_name,
        region_name=settings.os_region_name,
        interface=settings.os_interface,
        api_timeout=30,
        verify=settings.ssl_verify,
    )


def get_admin_project_connection() -> openstack.connection.Connection:
    """afterglow admin 크리덴셜로 admin 프로젝트에 스코프된 연결.

    Barbican project-quotas 등 admin 프로젝트 스코프가 필요한 cross-project 관리 API에 사용.
    사용자의 현재 프로젝트 선택과 무관하게 항상 admin 프로젝트로 스코프됨.
    """
    import openstack

    settings = get_settings()
    return openstack.connect(
        load_envvars=False,
        load_yaml_config=False,
        auth_url=settings.os_auth_url,
        auth_type="password",
        username=settings.os_username,
        password=settings.os_password,
        project_name=settings.os_project_name,
        user_domain_name=settings.os_user_domain_name,
        project_domain_name=settings.os_project_domain_name,
        region_name=settings.os_region_name,
        interface=settings.os_interface,
        api_timeout=30,
        verify=settings.ssl_verify,
    )


def get_service_project_connection() -> openstack.connection.Connection:
    """Union Mount 전용 service 프로젝트로 스코프된 admin 자격 conn.

    os_service_project_id가 미설정이면 RuntimeError로 fail-fast.
    admin 프로젝트로 조용히 fallback하지 않는다.
    """
    settings = get_settings()
    if not settings.os_service_project_id:
        raise RuntimeError(
            "os_service_project_id 설정이 없습니다. "
            "afterglow.conf [openstack] service_project_id 또는 OS_SERVICE_PROJECT_ID 환경변수를 설정하세요."
        )
    return get_admin_connection_for_project(settings.os_service_project_id)


def revoke_token(token: str) -> None:
    """Keystone에 토큰 폐기 요청 (DELETE /v3/auth/tokens)."""
    settings = get_settings()
    auth_plugin = v3.Token(auth_url=settings.os_auth_url, token=token)
    sess = ks_session.Session(auth=auth_plugin, timeout=10, verify=settings.ssl_verify)
    sess.delete(
        f"{settings.os_auth_url}/auth/tokens",
        endpoint_filter=None,
        headers={"X-Subject-Token": token},
        authenticated=False,
    )


def get_user(conn: openstack.connection.Connection, user_id: str) -> dict:
    """user_id로 사용자 상세 정보 조회 (이름, 이메일 등)."""
    u = conn.identity.get_user(user_id)
    return {
        "id": u.id,
        "name": u.name or "",
        "email": getattr(u, "email", None) or "",
    }


def _ensure_cluster_manager_user_sync_with_admin_conn(project_id: str, admin_conn, settings) -> tuple[str, str, str]:
    """Keystone 신규 사용자 생성 + 역할 부여 (sync). 반환: (user_id, username, password).

    DB 저장은 호출자가 별도로 수행 (event loop 격리)."""
    username = f"afterglow-cluster-mgr-{project_id[:8]}"
    password = secrets.token_urlsafe(32)

    domain_id = None
    try:
        domains = list(admin_conn.identity.domains(name=settings.os_user_domain_name))
        if domains:
            domain_id = domains[0].id
    except Exception:
        _logger.warning("도메인 ID 조회 실패, 기본 도메인 사용")

    try:
        create_kwargs: dict = {"name": username, "password": password, "enabled": True}
        if domain_id:
            create_kwargs["domain_id"] = domain_id
        user = admin_conn.identity.create_user(**create_kwargs)
        user_id = user.id
    except Exception as e:
        _logger.warning("관리 사용자 생성 실패 (이미 존재 가능성): %s", e)
        existing = list(admin_conn.identity.users(name=username))
        if not existing:
            raise RuntimeError(f"관리 사용자 {username} 생성 및 조회 모두 실패") from e
        user_id = existing[0].id
        admin_conn.identity.update_user(user_id, password=password)

    for role_name in ("member", "load-balancer_member"):
        try:
            roles = list(admin_conn.identity.roles(name=role_name))
            if not roles:
                raise RuntimeError(
                    f"'{role_name}' 역할이 Keystone에 존재하지 않습니다. 운영자가 해당 role을 사전 생성해야 합니다."
                )
            admin_conn.identity.assign_project_role_to_user(project=project_id, user=user_id, role=roles[0].id)
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"역할 {role_name} 부여 실패: {e}") from e

    return user_id, username, password


async def ensure_cluster_manager_user(project_id: str) -> tuple[str, str]:
    """프로젝트의 관리 사용자 (user_id, plaintext_password) 반환.

    DB 캐시 우선, 없으면 admin conn 으로 신규 생성 후 DB 에 암호화 저장.
    동시 생성 경쟁 시 Keystone Conflict → list 재조회 멱등 처리.

    async 로 작성하여 호출자 event loop 의 SQLAlchemy connection pool 을 그대로 사용
    (asyncio.run 으로 새 loop 를 생성하면 pool affinity 충돌 발생).
    """
    from drover.crypto import decrypt_manager_password, encrypt_manager_password
    from drover.services.store import get_manager_credentials, save_manager_credentials

    cached = await get_manager_credentials(project_id)
    if cached:
        return cached["user_id"], decrypt_manager_password(cached["encrypted_password"])

    settings = get_settings()
    admin_conn = await asyncio.to_thread(get_admin_connection_for_project, project_id)
    try:
        user_id, username, password = await asyncio.to_thread(
            _ensure_cluster_manager_user_sync_with_admin_conn, project_id, admin_conn, settings
        )
        encrypted_pw = encrypt_manager_password(password)
        await save_manager_credentials(project_id, user_id, username, encrypted_pw)
        return user_id, password
    finally:
        await asyncio.to_thread(admin_conn.close)


def _connect_as_manager(project_id: str, password: str, settings):
    import openstack

    return openstack.connect(
        load_envvars=False,
        load_yaml_config=False,
        auth_url=settings.os_auth_url,
        auth_type="password",
        username=f"afterglow-cluster-mgr-{project_id[:8]}",
        password=password,
        project_id=project_id,
        user_domain_name=settings.os_user_domain_name,
        project_domain_name=settings.os_project_domain_name,
        region_name=settings.os_region_name,
        interface=settings.os_interface,
        api_timeout=30,
        verify=settings.ssl_verify,
    )


def _create_app_cred_sync(project_id: str, cluster_name: str, user_id: str, password: str) -> dict:
    settings = get_settings()
    mgr_conn = _connect_as_manager(project_id, password, settings)
    try:
        app_cred = mgr_conn.identity.create_application_credential(
            user=user_id,
            name=f"drover-appcred-{cluster_name}",
            description=f"Drover k3s cluster {cluster_name} application credential",
            roles=[{"name": "member"}],
        )
        return {"id": app_cred.id, "secret": app_cred.secret, "user_id": user_id}
    finally:
        mgr_conn.close()


async def create_app_credential_for_cluster(project_id: str, cluster_name: str) -> dict:
    """관리 사용자 자격으로 클러스터 전용 App Credential 발급.

    Returns:
        {"id": ..., "secret": ..., "user_id": ...}
    """
    user_id, password = await ensure_cluster_manager_user(project_id)
    return await asyncio.to_thread(_create_app_cred_sync, project_id, cluster_name, user_id, password)


def _delete_app_cred_sync(project_id: str, app_cred_id: str, user_id: str, password: str) -> None:
    settings = get_settings()
    mgr_conn = _connect_as_manager(project_id, password, settings)
    try:
        mgr_conn.identity.delete_application_credential(user_id, app_cred_id, ignore_missing=True)
    finally:
        mgr_conn.close()


async def delete_app_credential(project_id: str, app_cred_id: str) -> None:
    """관리 사용자 자격으로 App Credential 회수 (best-effort)."""
    try:
        user_id, password = await ensure_cluster_manager_user(project_id)
        await asyncio.to_thread(_delete_app_cred_sync, project_id, app_cred_id, user_id, password)
    except Exception:
        _logger.warning("App Credential %s 회수 실패 (best-effort)", app_cred_id, exc_info=True)


def list_user_groups(token: str, user_id: str) -> list[dict]:
    """사용자가 속한 keystone 그룹 목록.

    keystone policy 거부, API 오류, SDK 예외 등 어떤 실패도 PermissionError 로
    변환해 호출부가 graceful 빈 리스트로 응답할 수 있게 한다. 정확한 원인은
    서버 로그(traceback)에 남긴다.
    """
    settings = get_settings()
    auth_plugin = v3.Token(auth_url=settings.os_auth_url, token=token)
    sess = ks_session.Session(auth=auth_plugin, verify=settings.ssl_verify)
    from keystoneclient.v3 import client as ks_client

    ks = ks_client.Client(session=sess)
    try:
        groups = ks.groups.list(user=user_id)
    except Exception as e:
        _logger.exception("list_user_groups 실패 (user_id=%s)", user_id)
        raise PermissionError(str(e)) from e
    return [
        {
            "id": g.id,
            "name": g.name,
            "description": getattr(g, "description", None),
            "domain_id": g.domain_id,
        }
        for g in groups
    ]


def ensure_ec2_credentials(token: str, user_id: str, project_id: str) -> dict:
    """Keystone EC2 credential 자동 발급 + 인메모리 캐시.

    Returns: {"access": str, "secret": str}
    """
    import httpx

    settings = get_settings()
    base = settings.os_auth_url.rstrip("/")
    # /v3/... 형태이면 그대로, 없으면 /v3 붙이기
    if not base.endswith("/v3"):
        base = base

    headers = {"X-Auth-Token": token}
    url = f"{base}/users/{user_id}/credentials/OS-EC2"

    try:
        resp = httpx.get(url, headers=headers, verify=settings.ssl_verify, timeout=15)
        resp.raise_for_status()
        existing = resp.json().get("credentials", [])
    except Exception:
        _logger.warning("EC2 credentials 조회 실패, 신규 발급 시도", exc_info=True)
        existing = []

    cred = next((c for c in existing if c.get("tenant_id") == project_id), None)

    if not cred:
        try:
            post_resp = httpx.post(
                url,
                json={"tenant_id": project_id},
                headers=headers,
                verify=settings.ssl_verify,
                timeout=15,
            )
            post_resp.raise_for_status()
            cred = post_resp.json().get("credential", {})
        except Exception as e:
            _logger.exception("EC2 credential 발급 실패: user=%s project=%s", user_id, project_id)
            raise RuntimeError(f"EC2 credential 발급 실패: {e}") from e

    return {"access": cred["access"], "secret": cred["secret"]}


def list_projects(token: str) -> list[dict]:
    """
    사용자가 접근 가능한 프로젝트 목록 반환.
    Keystone v3 API를 사용하여 토큰의 사용자가 접근할 수 있는 모든 프로젝트를 조회.
    """
    settings = get_settings()

    # 토큰으로 세션 생성
    auth_plugin = v3.Token(
        auth_url=settings.os_auth_url,
        token=token,
    )
    sess = ks_session.Session(auth=auth_plugin, verify=settings.ssl_verify)

    # Keystone v3 API로 프로젝트 목록 조회
    from keystoneclient.v3 import client as ks_client

    ks = ks_client.Client(session=sess)

    # 현재 사용자가 접근 가능한 프로젝트 목록
    user_id = auth_plugin.get_access(sess).user_id
    projects = ks.projects.list(user=user_id)

    try:
        domain_map = {d.id: d.name for d in ks.domains.list()}
    except Exception:
        domain_map = {}

    return [
        {
            "id": p.id,
            "name": p.name,
            "description": p.description or "",
            "domain_id": p.domain_id,
            "domain_name": domain_map.get(p.domain_id),
            "enabled": p.enabled,
        }
        for p in projects
    ]
