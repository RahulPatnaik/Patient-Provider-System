"""
Admin Routes - API endpoints for manual review and administrative tasks.
"""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from datetime import datetime

from api.models import (
    SystemStatsResponse,
    ManualReviewItemResponse,
    ManualReviewDecisionRequest,
    ValidateProviderResponse,
    ErrorResponse
)
from api.dependencies import get_database, verify_api_key
from database.kpme_db import KPMEDatabase


router = APIRouter(prefix="/api/v1/admin", tags=["Admin"])


# ============================================================================
# System Statistics
# ============================================================================


@router.get(
    "/stats",
    response_model=SystemStatsResponse,
    status_code=status.HTTP_200_OK,
    summary="Get System Statistics",
    description="Retrieve system-wide statistics including database counts and validation metrics"
)
async def get_system_stats(
    db: KPMEDatabase = Depends(get_database),
    api_key: str = Depends(verify_api_key)
):
    """
    Get comprehensive system statistics.

    Returns:
    - Database statistics (establishments, staff, districts)
    - Validation statistics (total, auto-approved, auto-rejected, manual review)
    - Performance metrics (average response time, cache hit rate)

    Note: Validation statistics are placeholder values. In production,
    implement proper metrics tracking using Redis or a dedicated metrics service.
    """
    # Get database stats
    db_stats = db.get_stats()

    # TODO: Implement proper validation metrics tracking
    # For now, return placeholder values
    validation_stats = {
        "total_validations": 0,
        "auto_approved": 0,
        "auto_rejected": 0,
        "manual_review": 0,
        "avg_response_time_ms": 0.0,
        "cache_hit_rate": 0.0
    }

    return SystemStatsResponse(
        total_establishments=db_stats.get("total_establishments", 0),
        total_staff=db_stats.get("total_staff", 0),
        total_districts=db_stats.get("total_districts", 0),
        total_validations=validation_stats["total_validations"],
        auto_approved=validation_stats["auto_approved"],
        auto_rejected=validation_stats["auto_rejected"],
        manual_review=validation_stats["manual_review"],
        avg_response_time_ms=validation_stats["avg_response_time_ms"],
        cache_hit_rate=validation_stats["cache_hit_rate"],
        timestamp=datetime.utcnow()
    )


# ============================================================================
# Manual Review Queue
# ============================================================================


@router.get(
    "/manual-review",
    response_model=List[ManualReviewItemResponse],
    status_code=status.HTTP_200_OK,
    summary="Get Manual Review Queue",
    description="Retrieve providers pending manual review",
    responses={
        200: {"description": "List of providers pending manual review"},
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    }
)
async def get_manual_review_queue(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of records to return"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get list of providers pending manual review.

    Note: This is a placeholder implementation. In production, implement
    a dedicated manual review queue using a database table or Redis queue.

    Returns:
    - List of validation requests that require manual review
    - Sorted by submission time (oldest first)
    - Pagination support via skip/limit parameters
    """
    # TODO: Implement manual review queue storage and retrieval
    # For now, return empty list
    return []


@router.post(
    "/manual-review/{validation_id}/approve",
    response_model=ValidateProviderResponse,
    status_code=status.HTTP_200_OK,
    summary="Approve Manual Review",
    description="Approve a provider that was flagged for manual review",
    responses={
        200: {"description": "Provider approved successfully"},
        404: {"model": ErrorResponse, "description": "Validation ID not found"},
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    }
)
async def approve_manual_review(
    validation_id: str,
    request: ManualReviewDecisionRequest,
    api_key: str = Depends(verify_api_key)
):
    """
    Approve a provider from manual review queue.

    Note: This is a placeholder implementation. In production:
    1. Validate reviewer permissions
    2. Retrieve validation request from queue
    3. Update provider status to approved
    4. Log reviewer decision
    5. Notify relevant parties
    6. Move to approved providers table
    """
    # TODO: Implement manual review approval logic
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Manual review approval not yet implemented"
    )


@router.post(
    "/manual-review/{validation_id}/reject",
    response_model=ValidateProviderResponse,
    status_code=status.HTTP_200_OK,
    summary="Reject Manual Review",
    description="Reject a provider that was flagged for manual review",
    responses={
        200: {"description": "Provider rejected successfully"},
        404: {"model": ErrorResponse, "description": "Validation ID not found"},
        401: {"model": ErrorResponse, "description": "Unauthorized"}
    }
)
async def reject_manual_review(
    validation_id: str,
    request: ManualReviewDecisionRequest,
    api_key: str = Depends(verify_api_key)
):
    """
    Reject a provider from manual review queue.

    Note: This is a placeholder implementation. In production:
    1. Validate reviewer permissions
    2. Retrieve validation request from queue
    3. Update provider status to rejected
    4. Log reviewer decision with notes
    5. Notify relevant parties
    6. Archive validation request
    """
    # TODO: Implement manual review rejection logic
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Manual review rejection not yet implemented"
    )


# ============================================================================
# Database Search/Query Endpoints
# ============================================================================


@router.get(
    "/search/establishments",
    status_code=status.HTTP_200_OK,
    summary="Search KPME Establishments",
    description="Search for establishments in KPME database"
)
async def search_establishments(
    query: str = Query(..., min_length=2, description="Search query"),
    district: str = Query(None, description="Filter by district"),
    category: str = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    db: KPMEDatabase = Depends(get_database),
    api_key: str = Depends(verify_api_key)
):
    """
    Search for healthcare establishments in KPME database.

    Supports:
    - Name search (partial match)
    - District filtering
    - Category filtering
    - Result limiting
    """
    try:
        results = db.search_establishment_by_name(query, limit=limit)

        # Apply additional filters if provided
        if district:
            results = [r for r in results if r.get("district") == district]

        if category:
            results = [r for r in results if r.get("category") == category]

        return {
            "query": query,
            "total_results": len(results),
            "results": results
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )


@router.get(
    "/search/staff",
    status_code=status.HTTP_200_OK,
    summary="Search KPME Staff",
    description="Search for staff in KPME database by registration number"
)
async def search_staff(
    registration_number: str = Query(..., description="Staff registration number"),
    db: KPMEDatabase = Depends(get_database),
    api_key: str = Depends(verify_api_key)
):
    """
    Search for healthcare staff by registration number.
    """
    try:
        results = db.search_staff_by_registration(registration_number)

        return {
            "registration_number": registration_number,
            "total_results": len(results),
            "results": results
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )
