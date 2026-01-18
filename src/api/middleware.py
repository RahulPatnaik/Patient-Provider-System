"""
Custom Middleware - FastAPI middleware for request/response processing.
"""

import time
import logging
import uuid
from typing import Callable
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware


logger = logging.getLogger("api.middleware")


# ============================================================================
# Request Logging Middleware
# ============================================================================


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Log all incoming requests and outgoing responses.

    Logs:
    - Request ID
    - HTTP method and path
    - Client IP
    - Response status code
    - Processing time
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate request ID if not provided
        request_id = request.headers.get("X-Request-ID", f"req_{uuid.uuid4().hex[:12]}")

        # Start timer
        start_time = time.perf_counter()

        # Log incoming request
        client_ip = request.client.host if request.client else "unknown"
        logger.info(
            f"[{request_id}] {request.method} {request.url.path} - Client: {client_ip}"
        )

        # Process request
        try:
            response = await call_next(request)

            # Calculate processing time
            process_time_ms = int((time.perf_counter() - start_time) * 1000)

            # Add custom headers
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Process-Time-MS"] = str(process_time_ms)

            # Log response
            logger.info(
                f"[{request_id}] {request.method} {request.url.path} - "
                f"Status: {response.status_code} - Time: {process_time_ms}ms"
            )

            return response

        except Exception as e:
            # Calculate processing time even for errors
            process_time_ms = int((time.perf_counter() - start_time) * 1000)

            # Log error
            logger.error(
                f"[{request_id}] {request.method} {request.url.path} - "
                f"Error: {str(e)} - Time: {process_time_ms}ms",
                exc_info=True
            )

            # Return error response
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "detail": str(e),
                    "request_id": request_id
                },
                headers={
                    "X-Request-ID": request_id,
                    "X-Process-Time-MS": str(process_time_ms)
                }
            )


# ============================================================================
# Error Handling Middleware
# ============================================================================


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """
    Catch and format all unhandled exceptions.

    Converts exceptions to proper JSON error responses.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            response = await call_next(request)
            return response

        except ValueError as e:
            # Validation errors
            request_id = request.headers.get("X-Request-ID", "unknown")
            logger.warning(f"[{request_id}] Validation error: {str(e)}")

            return JSONResponse(
                status_code=400,
                content={
                    "error": "Validation error",
                    "detail": str(e),
                    "request_id": request_id
                }
            )

        except PermissionError as e:
            # Authorization errors
            request_id = request.headers.get("X-Request-ID", "unknown")
            logger.warning(f"[{request_id}] Permission error: {str(e)}")

            return JSONResponse(
                status_code=403,
                content={
                    "error": "Permission denied",
                    "detail": str(e),
                    "request_id": request_id
                }
            )

        except Exception as e:
            # All other unhandled exceptions
            request_id = request.headers.get("X-Request-ID", "unknown")
            logger.error(f"[{request_id}] Unhandled exception: {str(e)}", exc_info=True)

            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "detail": str(e),
                    "request_id": request_id
                }
            )


# ============================================================================
# Performance Monitoring Middleware (Placeholder)
# ============================================================================


class PerformanceMonitoringMiddleware(BaseHTTPMiddleware):
    """
    Monitor API performance and collect metrics.

    Note: This is a placeholder. In production, integrate with proper
    monitoring tools like Prometheus, Datadog, or New Relic.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.perf_counter()

        response = await call_next(request)

        # Calculate processing time
        process_time_ms = int((time.perf_counter() - start_time) * 1000)

        # TODO: Send metrics to monitoring service
        # For now, just add header
        if not response.headers.get("X-Process-Time-MS"):
            response.headers["X-Process-Time-MS"] = str(process_time_ms)

        return response


# ============================================================================
# CORS Configuration
# ============================================================================


def get_cors_middleware(app):
    """
    Configure CORS middleware for the FastAPI app.

    Args:
        app: FastAPI application instance

    Returns:
        Configured CORSMiddleware

    Note: In production, restrict allow_origins to specific domains.
    """
    return CORSMiddleware(
        app,
        allow_origins=["*"],  # TODO: Restrict in production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID", "X-Process-Time-MS"]
    )


# ============================================================================
# Helper Functions
# ============================================================================


def setup_middlewares(app):
    """
    Setup all middlewares for the FastAPI app.

    Order matters - middlewares are executed in reverse order of addition:
    1. CORS (outermost)
    2. Request Logging
    3. Error Handling
    4. Performance Monitoring (innermost)

    Args:
        app: FastAPI application instance
    """
    # Add middlewares in reverse order of execution
    app.add_middleware(PerformanceMonitoringMiddleware)
    app.add_middleware(ErrorHandlingMiddleware)
    app.add_middleware(RequestLoggingMiddleware)

    # CORS is added separately using get_cors_middleware()
