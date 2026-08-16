"""Drover standalone FastAPI application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from drover.api import (
    admin,
    callback,
    certificates,
    clusters,
    configmaps,
    gpu_quotas,
    health,
    k3s_services,
    nodegroups,
    pods,
    resource_policies,
    secrets,
    shell,
    stats,
    templates,
    workloads,
)
from drover.cache import close_cache
from drover.config import get_settings
from drover.db import close_db, init_db
from drover.models.schemas import (
    HealthResponse,
    RootDiscoveryResponse,
    VersionDiscoveryResponse,
)
from drover.rate_limit import limiter
from drover.services.errors import K3sApiError

_logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()
    init_db(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        connect_timeout=settings.database_connect_timeout,
        pool_timeout=settings.database_pool_timeout,
    )
    try:
        yield
    finally:
        await close_cache()
        await close_db()


app = FastAPI(title="Drover", version="1.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.exception_handler(K3sApiError)
async def k3s_api_error_handler(request: Request, exc: K3sApiError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


app.add_middleware(SlowAPIMiddleware)

# Routers mounted under /v1 (health router included BEFORE clusters router so /v1/clusters/health matches before /{cluster_id})
app.include_router(health.router, prefix="/v1/clusters", tags=["health"])
app.include_router(clusters.router, prefix="/v1/clusters", tags=["clusters"])
app.include_router(callback.router, prefix="/v1", tags=["callback"])
app.include_router(configmaps.router, prefix="/v1/clusters", tags=["configmaps"])
app.include_router(secrets.router, prefix="/v1/clusters", tags=["secrets"])
app.include_router(pods.router, prefix="/v1/clusters", tags=["pods"])
app.include_router(k3s_services.router, prefix="/v1/clusters", tags=["services"])
app.include_router(workloads.router, prefix="/v1/clusters", tags=["workloads"])
app.include_router(nodegroups.router, prefix="/v1/clusters", tags=["nodegroups"])
app.include_router(certificates.router, prefix="/v1/clusters", tags=["certificates"])
app.include_router(shell.router, prefix="/v1/clusters", tags=["shell"])
app.include_router(templates.router, prefix="/v1/cluster-templates", tags=["templates"])
app.include_router(admin.router, prefix="/v1/admin", tags=["admin"])
app.include_router(resource_policies.router, prefix="/v1/admin", tags=["admin"])
app.include_router(stats.router, prefix="/v1/stats", tags=["stats"])
app.include_router(gpu_quotas.tenant_router, prefix="/v1/gpu-quotas", tags=["gpu-quotas"])
app.include_router(gpu_quotas.admin_router, prefix="/v1/admin/gpu-quotas", tags=["admin-gpu-quotas"])


def _version_document(request: Request) -> dict:
    href = f"{str(request.base_url).rstrip('/')}/v1/"
    return {
        "id": "v1.0",
        "status": "CURRENT",
        "min_version": "1.0",
        "version": "1.0",
        "links": [{"rel": "self", "href": href}],
    }


@app.get(
    "/",
    response_model=RootDiscoveryResponse,
    summary="Root version discovery",
    description="Get available API versions.",
)
async def root_discovery(request: Request):
    return {"versions": [_version_document(request)]}


@app.get(
    "/v1/",
    response_model=VersionDiscoveryResponse,
    summary="V1 version discovery",
    description="Get API v1 version document.",
)
async def version_discovery(request: Request):
    return {"version": _version_document(request)}


@app.get(
    "/v1/health",
    response_model=HealthResponse,
    summary="Service health check",
    description="Get service health status.",
)
async def health_check():
    return {"status": "ok"}


def run() -> None:
    uvicorn.run("drover.main:app", host="0.0.0.0", port=8011)
