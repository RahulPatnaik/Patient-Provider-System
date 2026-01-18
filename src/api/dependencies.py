"""
FastAPI Dependencies - Shared dependencies for route handlers.
"""

import uuid
import time
from typing import Optional
from fastapi import Header, HTTPException, status
from datetime import datetime

from database.kpme_db import KPMEDatabase, get_kpme_db
from cache.factory import BaseCacheClient, get_cache_instance
from agents.supervisor import SupervisorAgent, get_supervisor
from agents.fast_validator import FastValidatorAgent


# ============================================================================
# Singleton Instances
# ============================================================================

_db_instance: Optional[KPMEDatabase] = None
_cache_instance: Optional[BaseCacheClient] = None
_supervisor_instance: Optional[SupervisorAgent] = None
_fast_validator_instance: Optional[FastValidatorAgent] = None
_app_start_time: float = time.time()


def get_app_start_time() -> float:
    """Get application start time."""
    return _app_start_time


# ============================================================================
# Database Dependency
# ============================================================================


def get_database() -> KPMEDatabase:
    """
    Get KPME database instance.

    Returns:
        KPMEDatabase: Database instance
    """
    global _db_instance
    if _db_instance is None:
        _db_instance = get_kpme_db()
    return _db_instance


# ============================================================================
# Cache Dependency
# ============================================================================


def get_cache() -> BaseCacheClient:
    """
    Get cache client instance.

    Returns:
        BaseCacheClient: Cache client (Redis or Memory)
    """
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = get_cache_instance()
    return _cache_instance


# ============================================================================
# Agent Dependencies
# ============================================================================


def get_supervisor_agent() -> SupervisorAgent:
    """
    Get Supervisor Agent instance.

    Returns:
        SupervisorAgent: Supervisor agent for complete validation
    """
    global _supervisor_instance
    if _supervisor_instance is None:
        _supervisor_instance = get_supervisor()
    return _supervisor_instance


def get_fast_validator_agent() -> FastValidatorAgent:
    """
    Get Fast Validator Agent instance.

    Returns:
        FastValidatorAgent: Fast validator for quick certificate checks
    """
    global _fast_validator_instance
    if _fast_validator_instance is None:
        _fast_validator_instance = FastValidatorAgent()
    return _fast_validator_instance


# ============================================================================
# Request ID Dependency
# ============================================================================


def get_request_id(x_request_id: Optional[str] = Header(None)) -> str:
    """
    Get or generate request ID for tracking.

    Args:
        x_request_id: Optional request ID from header

    Returns:
        str: Request ID (from header or generated)
    """
    if x_request_id:
        return x_request_id
    return f"req_{uuid.uuid4().hex[:12]}"


# ============================================================================
# Authentication Dependency (Placeholder)
# ============================================================================


def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """
    Verify API key for authentication.

    Note: This is a placeholder. In production, implement proper authentication.

    Args:
        x_api_key: API key from header

    Returns:
        str: Validated API key

    Raises:
        HTTPException: If API key is missing or invalid
    """
    # For now, allow all requests (no authentication)
    # In production, implement proper API key validation
    return x_api_key or "anonymous"


# ============================================================================
# Rate Limiting Dependency (Placeholder)
# ============================================================================


async def check_rate_limit(
    x_api_key: str = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
) -> bool:
    """
    Check rate limit for the request.

    Note: This is a placeholder. In production, implement proper rate limiting.

    Args:
        x_api_key: API key from header
        x_forwarded_for: Client IP from proxy header

    Returns:
        bool: True if request is within rate limit

    Raises:
        HTTPException: If rate limit exceeded
    """
    # For now, allow all requests (no rate limiting)
    # In production, implement rate limiting using Redis or similar
    return True


# ============================================================================
# Cleanup Functions
# ============================================================================


def reset_dependencies():
    """
    Reset all singleton dependencies.

    Useful for testing or reloading.
    """
    global _db_instance, _cache_instance, _supervisor_instance, _fast_validator_instance
    _db_instance = None
    _cache_instance = None
    _supervisor_instance = None
    _fast_validator_instance = None
