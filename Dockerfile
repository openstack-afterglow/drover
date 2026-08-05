FROM python:3.12-slim AS drover-builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    git \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project

COPY drover/ ./drover/
RUN uv sync --frozen --no-dev

FROM python:3.12-slim AS drover-runtime

WORKDIR /app

COPY --from=drover-builder /app/.venv /app/.venv
COPY pyproject.toml uv.lock LICENSE ./
COPY drover/ ./drover/

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && python -m compileall -q drover \
    && adduser --disabled-password --gecos "" appuser \
    && adduser appuser root \
    && chown -R appuser:appuser /app

ENV PATH="/app/.venv/bin:$PATH"

USER appuser

FROM drover-runtime AS drover-api
CMD ["uvicorn", "drover.main:app", "--host", "0.0.0.0", "--port", "8011"]

FROM drover-runtime AS drover-worker
CMD ["python", "-m", "drover.worker"]
