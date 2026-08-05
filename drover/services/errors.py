"""K3s 서비스 도메인 예외 — FastAPI 의존 없음."""


class K3sApiError(Exception):
    """K8s API 호출 실패. status_code는 HTTP 상태 코드."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
