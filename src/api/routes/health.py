"""
Health Check Routes - API endpoints for system health monitoring.
"""

import time
from fastapi import APIRouter, Depends, status
from datetime import datetime

from api.models import HealthCheckResponse, ServicesHealthResponse
from api.dependencies import get_database, get_cache, get_app_start_time
from database.kpme_db import KPMEDatabase
from cache.factory import BaseCacheClient


router = APIRouter(prefix="/api/v1/health", tags=["Health"])


@router.get(
    "",
    response_model=HealthCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Basic Health Check",
    description="Simple health check endpoint - returns 200 if service is running"
)
async def health_check(
    app_start_time: float = Depends(get_app_start_time)
):
    """
    Basic health check.

    Returns:
        - status: "healthy"
        - timestamp: Current UTC time
        - version: API version
        - uptime_seconds: How long the service has been running
    """
    uptime = time.time() - app_start_time

    return HealthCheckResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        version="1.0.0",
        uptime_seconds=uptime
    )


@router.get(
    "/ready",
    response_model=HealthCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Readiness Probe",
    description="Kubernetes readiness probe - checks if service is ready to accept traffic"
)
async def readiness_check(
    app_start_time: float = Depends(get_app_start_time)
):
    """
    Readiness probe for Kubernetes/container orchestration.

    Checks if the service is ready to handle requests.
    """
    uptime = time.time() - app_start_time

    return HealthCheckResponse(
        status="ready",
        timestamp=datetime.utcnow(),
        version="1.0.0",
        uptime_seconds=uptime
    )


@router.get(
    "/live",
    response_model=HealthCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Liveness Probe",
    description="Kubernetes liveness probe - checks if service is alive"
)
async def liveness_check(
    app_start_time: float = Depends(get_app_start_time)
):
    """
    Liveness probe for Kubernetes/container orchestration.

    Checks if the service is still running.
    """
    uptime = time.time() - app_start_time

    return HealthCheckResponse(
        status="alive",
        timestamp=datetime.utcnow(),
        version="1.0.0",
        uptime_seconds=uptime
    )


@router.get(
    "/services",
    response_model=ServicesHealthResponse,
    status_code=status.HTTP_200_OK,
    summary="External Services Health",
    description="Check health of external services (database, cache, etc.)"
)
async def services_health(
    db: KPMEDatabase = Depends(get_database),
    cache: BaseCacheClient = Depends(get_cache)
):
    """
    Check health of all external services.

    Checks:
    - Database: Connection, query performance, record counts
    - Cache: Connection, ping response time

    Returns status for each service and overall health status.
    """
    # Check database
    db_start = time.perf_counter()
    try:
        stats = db.get_stats()
        db_time_ms = int((time.perf_counter() - db_start) * 1000)

        database_health = {
            "status": "healthy",
            "total_establishments": stats.get("total_establishments", 0),
            "total_staff": stats.get("total_staff", 0),
            "response_time_ms": db_time_ms
        }
    except Exception as e:
        database_health = {
            "status": "unhealthy",
            "error": str(e),
            "response_time_ms": int((time.perf_counter() - db_start) * 1000)
        }

    # Check cache
    cache_start = time.perf_counter()
    try:
        is_available = await cache.ping()
        cache_time_ms = int((time.perf_counter() - cache_start) * 1000)

        cache_type = "redis" if hasattr(cache, 'redis') else "memory"

        cache_health = {
            "status": "healthy" if is_available else "unhealthy",
            "type": cache_type,
            "response_time_ms": cache_time_ms
        }
    except Exception as e:
        cache_health = {
            "status": "unhealthy",
            "type": "unknown",
            "error": str(e),
            "response_time_ms": int((time.perf_counter() - cache_start) * 1000)
        }

    # Determine overall status
    overall_status = "healthy"
    if database_health.get("status") != "healthy" or cache_health.get("status") != "healthy":
        overall_status = "degraded"

    return ServicesHealthResponse(
        database=database_health,
        cache=cache_health,
        overall_status=overall_status,
        timestamp=datetime.utcnow()
    )
