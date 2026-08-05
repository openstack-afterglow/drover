"""캐시 키 빌더 — 문자열 조립만 담당 (Redis I/O 없음).

키 형식 규약:
- 프로젝트 스코프: afterglow:{service}:{project_id}:{resource}[:{sub}]
- 사용자 스코프:   afterglow:user:{user_id}:{resource}
- 어드민 스코프:   afterglow:admin:{resource}
- 버전 카운터:     afterglow:ver:{service}:{project_id}
- 뮤테이션 카운터: afterglow:mutcount:{service}:{project_id}
- 태그(SET):       tag:{service}:{project_id}

이 모듈은 Redis 호출을 하지 않는다. 키 형식이 모든 호출부에서 일관되도록
중앙집중화하는 것이 목적이다.
"""


def project_key(service: str, project_id: str, resource: str, *, sub: str | None = None) -> str:
    """프로젝트 스코프 키.

    예: project_key("nova", "abc123", "servers")          → afterglow:nova:abc123:servers
        project_key("nova", "abc123", "servers", sub="x") → afterglow:nova:abc123:servers:x
    """
    base = f"afterglow:{service}:{project_id}:{resource}"
    if sub:
        return f"{base}:{sub}"
    return base


def user_key(user_id: str, resource: str) -> str:
    """사용자 스코프 키. 예: afterglow:user:u1:gpu_quota"""
    return f"afterglow:user:{user_id}:{resource}"


def admin_key(resource: str) -> str:
    """어드민(시스템 전역) 스코프 키. 예: afterglow:admin:projects"""
    return f"afterglow:admin:{resource}"


def tag_key(service: str, project_id: str) -> str:
    """태그 SET 키. invalidate_tag() 의 대상으로 쓰인다."""
    return f"tag:{service}:{project_id}"


def mutcount_key(service: str, project_id: str) -> str:
    """뮤테이션 카운터 키 (dynamic TTL 산출용, Phase B 에서 활용)."""
    return f"afterglow:mutcount:{service}:{project_id}"


def version_key(service: str, project_id: str) -> str:
    """리소스 버전 카운터 키 (versioned key 무효화용)."""
    return f"afterglow:ver:{service}:{project_id}"
