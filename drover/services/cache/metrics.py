"""캐시 메트릭 — 프로세스-로컬 카운터.

Redis 가 아닌 프로세스 내 dict 에 누적되는 정수 카운터. /healthz 같은
디버깅 엔드포인트에서 hit-ratio 등을 노출하는 용도. dict.__setitem__ /
+=  는 CPython 에서 GIL 보호 하에 원자적이므로 별도 Lock 불필요.

표준 카운터 이름:
- cache.hit
- cache.miss
- cache.write
- cache.invalidate
- cache.error
"""

from collections import defaultdict

_counters: dict[str, int] = defaultdict(int)


def increment(name: str) -> None:
    """카운터를 1 증가."""
    _counters[name] += 1


def get(name: str) -> int:
    """단일 카운터 값을 반환. 미존재 시 0."""
    return _counters[name]


def get_all() -> dict[str, int]:
    """모든 카운터의 스냅샷을 반환 (얕은 복사)."""
    return dict(_counters)


def reset() -> None:
    """모든 카운터를 0으로. 테스트 전용."""
    _counters.clear()
