"""
FastAPI Application Entry Point - KPME Provider Validation System.

This is the main application file that initializes and configures the FastAPI app.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import provider, health, admin, ocr, patients, chatbot, stt
from api.middleware import setup_middlewares
from api.dependencies import get_database, get_cache, reset_dependencies


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("api.main")


# ============================================================================
# Lifespan Events
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifespan events.

    Startup:
    - Initialize database connection
    - Initialize cache connection
    - Load KPME data if needed

    Shutdown:
    - Close database connections
    - Close cache connections
    - Cleanup resources
    """
    # Startup
    logger.info("=" * 80)
    logger.info("KPME Provider Validation System - Starting up...")
    logger.info("=" * 80)

    try:
        # Initialize database
        logger.info("Initializing KPME database...")
        db = get_database()
        stats = db.get_stats()
        logger.info(f"Database loaded: {stats.get('total_establishments', 0)} establishments, "
                   f"{stats.get('total_staff', 0)} staff records")

        # Initialize cache
        logger.info("Initializing cache...")
        cache = get_cache()
        cache_available = await cache.ping()
        cache_type = "Redis" if hasattr(cache, 'redis') else "Memory"
        logger.info(f"Cache initialized: {cache_type} ({'healthy' if cache_available else 'unhealthy'})")

        logger.info("=" * 80)
        logger.info("System ready to accept requests!")
        logger.info("API Documentation: http://localhost:8000/docs")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Startup failed: {str(e)}", exc_info=True)
        raise

    yield

    # Shutdown
    logger.info("=" * 80)
    logger.info("KPME Provider Validation System - Shutting down...")
    logger.info("=" * 80)

    try:
        # Cleanup resources
        reset_dependencies()
        logger.info("Resources cleaned up successfully")

    except Exception as e:
        logger.error(f"Shutdown error: {str(e)}", exc_info=True)

    logger.info("System shutdown complete")


# ============================================================================
# FastAPI Application
# ============================================================================


app = FastAPI(
    title="KPME Provider Validation API",
    description="""
    **KPME Karnataka Healthcare Provider Validation System**

    This API provides comprehensive validation services for healthcare providers
    in Karnataka, India using the KPME (Karnataka Private Medical Establishments) database.

    ## Features

    - **Fast Certificate Validation**: Ultra-fast KPME certificate checks (deterministic, no AI)
    - **Complete Provider Validation**: Full validation workflow with AI-powered synthesis
    - **Batch Processing**: Validate multiple providers in parallel
    - **Health Monitoring**: Comprehensive health checks and service status
    - **Admin Tools**: System statistics and database search capabilities

    ## Architecture

    - **Deterministic-First**: 90% operations use direct database queries (no AI calls)
    - **AI Synthesis**: AI used only for complex multi-source decision making
    - **Cache-First**: Redis/Memory caching for ultra-fast responses
    - **Dual-Path**: Simple path (fast validator) vs Complex path (full validation)

    ## Authentication

    Currently using placeholder authentication. In production, implement proper API key validation.

    ## Rate Limiting

    Currently no rate limiting. In production, implement rate limiting per API key.

    ## Support

    For questions or issues, contact the development team.
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


# ============================================================================
# CORS Configuration
# ============================================================================


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-Process-Time-MS"]
)


# ============================================================================
# Middleware Setup
# ============================================================================


setup_middlewares(app)


# ============================================================================
# Router Registration
# ============================================================================


# Provider validation routes
app.include_router(provider.router)

# Health check routes
app.include_router(health.router)

# Admin routes
app.include_router(admin.router)

# OCR routes
app.include_router(ocr.router)

# Patient records routes
app.include_router(patients.router)

# Chatbot routes
app.include_router(chatbot.router)

# Speech-to-Text routes
app.include_router(stt.router)


# ============================================================================
# Root Endpoint
# ============================================================================


@app.get("/", tags=["Root"])
async def root():
    """
    Root endpoint - API information.

    Returns basic information about the API and links to documentation.
    """
    return {
        "name": "KPME Provider Validation API",
        "version": "1.0.0",
        "description": "Karnataka healthcare provider validation system",
        "docs": "/docs",
        "health": "/api/v1/health",
        "endpoints": {
            "validate_provider": "POST /api/v1/providers/validate",
            "fast_validate": "POST /api/v1/providers/validate/fast",
            "batch_validate": "POST /api/v1/providers/validate/batch",
            "health_check": "GET /api/v1/health",
            "system_stats": "GET /api/v1/admin/stats"
        }
    }


# ============================================================================
# Run Server (Development)
# ============================================================================


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting development server...")

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
