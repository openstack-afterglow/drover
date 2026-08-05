"""캐시 stampede 완화 — v2 placeholder.

v1 은 jitter-only stampede mitigation 을 사용한다 (ttl.py 의 TTL 카테고리가
티어별로 분리되어 동시 만료 확률이 낮음). v2 에서 single-flight 락
(예: Redis SETNX + watchdog) 을 도입할 자리이다.
"""

# Single-flight lock placeholder for v2.
# v1 uses jitter-only stampede mitigation (see ttl.py).
