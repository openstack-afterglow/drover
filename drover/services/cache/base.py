"""Cache 백엔드 추상화 (Abstract Base Class).

v1은 Redis/Valkey 단일 백엔드를 사용하지만, v2에서 Memcached 등 다른 백엔드를
무중단으로 도입할 수 있도록 인터페이스를 분리한다.

설계 원칙:
- 모든 메서드는 실패 시 silent fail (예외 흡수, 의미 있는 기본값 반환).
- SCAN 류는 인터페이스에 노출하지 않는다 (Memcached v2 호환).
  → 모든 무효화는 delete() 또는 invalidate_tag() 로 수행.
- write-through 금지: set() 은 read-through (캐시 미스 → fn → set) 경로 전용.
  mutation 응답 직후 set() 호출 금지 (Nova create_server 등 부분 BUILD 상태가
  영구 stale 의 원인이 됨).
"""

from abc import ABC, abstractmethod


class Cache(ABC):
    """캐시 백엔드 인터페이스.

    구현체는 모든 메서드에서 백엔드 장애를 silent 하게 흡수하고 의미 있는
    기본값을 돌려줘야 한다 (None, 0, False 등). 호출부는 캐시 실패 시
    fn 을 그대로 실행하는 read-through 패턴을 가정한다.
    """

    @abstractmethod
    async def get(self, key: str) -> bytes | None:
        """단일 키를 조회. 미스 또는 장애 시 None."""

    @abstractmethod
    async def set(self, key: str, value: bytes, ttl: int) -> None:
        """단일 키를 TTL 과 함께 저장.

        NOTE: read-through 경로 전용. mutation 응답 직후 호출 금지
        (write-through forbidden invariant — 부분 BUILD 상태가 영구 stale 의 원인).
        """

    @abstractmethod
    async def delete(self, *keys: str) -> int:
        """주어진 키들을 삭제. 실제 삭제된 개수를 반환."""

    @abstractmethod
    async def incr(self, key: str, *, ttl: int | None = None) -> int:
        """원자적으로 카운터를 증가시키고 새 값을 반환. ttl 지정 시 EXPIRE 추가."""

    @abstractmethod
    async def add_to_tag(self, tag: str, key: str) -> None:
        """tag 그룹에 key 를 등록. invalidate_tag(tag) 시 일괄 삭제 대상이 된다."""

    @abstractmethod
    async def invalidate_tag(self, tag: str) -> int:
        """tag 에 묶인 모든 key 와 tag 자체를 삭제. 삭제된 키 개수를 반환."""

    @abstractmethod
    async def ping(self) -> bool:
        """백엔드 헬스체크. 응답 가능하면 True."""

    @abstractmethod
    async def close(self) -> None:
        """리소스 정리 (커넥션 풀 종료 등)."""
