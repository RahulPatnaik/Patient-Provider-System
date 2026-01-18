"""
Provider Routes - API endpoints for provider validation operations.
"""

import time
import asyncio
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from datetime import datetime

from api.models import (
    ValidateProviderRequest,
    ValidateProviderResponse,
    BatchValidateRequest,
    BatchValidateResponse,
    FastValidateRequest,
    FastValidateResponse,
    ValidationDecision,
    ValidationPath,
    ConfidenceLevel,
    KPMEDataResponse,
    DataQualityResponse,
    ComplianceResponse,
    ErrorResponse
)
from api.dependencies import (
    get_supervisor_agent,
    get_fast_validator_agent,
    get_request_id,
    verify_api_key,
    check_rate_limit
)
from agents.supervisor import SupervisorAgent
from agents.fast_validator import FastValidatorAgent


router = APIRouter(prefix="/api/v1/providers", tags=["Provider Validation"])


# ============================================================================
# Helper Functions
# ============================================================================


def classify_confidence(confidence: float) -> ConfidenceLevel:
    """Classify confidence score into level."""
    if confidence >= 0.9:
        return ConfidenceLevel.VERY_HIGH
    elif confidence >= 0.7:
        return ConfidenceLevel.HIGH
    elif confidence >= 0.5:
        return ConfidenceLevel.MEDIUM
    elif confidence >= 0.3:
        return ConfidenceLevel.LOW
    else:
        return ConfidenceLevel.VERY_LOW


# ============================================================================
# Validation Endpoints
# ============================================================================


@router.post(
    "/validate",
    response_model=ValidateProviderResponse,
    status_code=status.HTTP_200_OK,
    summary="Validate Healthcare Provider",
    description="Validate a healthcare provider using the full validation workflow (simple or complex path)",
    responses={
        200: {"description": "Validation completed successfully"},
        400: {"model": ErrorResponse, "description": "Invalid request data"},
        429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def validate_provider(
    request: ValidateProviderRequest,
    request_id: str = Depends(get_request_id),
    supervisor: SupervisorAgent = Depends(get_supervisor_agent),
    api_key: str = Depends(verify_api_key),
    rate_limit: bool = Depends(check_rate_limit)
):
    """
    Validate a healthcare provider through the supervisor agent.

    The supervisor will:
    1. Preprocess and clean the data
    2. Determine validation path (simple or complex)
    3. Execute appropriate validators
    4. Calculate confidence scores
    5. Make final decision (auto-approve, auto-reject, manual review)

    Returns complete validation results with confidence scoring.
    """
    start_time = time.perf_counter()

    try:
        # Prepare provider data
        provider_data = {
            "establishment_name": request.establishment_name,
            "certificate_number": request.certificate_number,
            "phone": request.phone,
            "email": request.email,
            "address": request.address,
            "district": request.district,
            "category": request.category,
            "system_of_medicine": request.system_of_medicine
        }

        # Remove None values
        provider_data = {k: v for k, v in provider_data.items() if v is not None}

        # Run validation through supervisor
        result = await supervisor.validate_provider(provider_data)

        # Calculate execution time
        execution_time_ms = int((time.perf_counter() - start_time) * 1000)

        # Build KPME data response
        kpme_data = None
        if result.provider_data.kpme_data:
            kpme_data = KPMEDataResponse(
                certificate_number=result.provider_data.kpme_data.get("certificate_number", ""),
                establishment_name=result.provider_data.kpme_data.get("establishment_name"),
                category=result.provider_data.kpme_data.get("category"),
                district=result.provider_data.kpme_data.get("district"),
                phone=result.provider_data.kpme_data.get("phone"),
                email=result.provider_data.kpme_data.get("email"),
                address=result.provider_data.kpme_data.get("address"),
                is_valid=result.provider_data.kpme_validated,
                is_expired=result.provider_data.kpme_data.get("is_expired", False),
                confidence=result.provider_data.kpme_data.get("confidence", 0.0)
            )

        # Build data quality response
        data_quality = None
        # EnrichedProviderModel doesn't have a data_quality dict, only confidence scores
        # Build data quality from available fields
        data_quality = DataQualityResponse(
            completeness_score=result.provider_data.data_quality_confidence,
            accuracy_score=result.provider_data.data_quality_confidence,
            overall_score=result.provider_data.data_quality_confidence,
            missing_fields=[],  # TODO: Track missing fields
            issues=[]  # TODO: Track quality issues
        )

        # Build compliance response
        compliance = None
        if result.provider_data.compliance_data:
            compliance = ComplianceResponse(
                is_compliant=result.provider_data.compliance_data.get("is_compliant", False),
                checks_passed=result.provider_data.compliance_data.get("checks_passed", 0),
                checks_failed=result.provider_data.compliance_data.get("checks_failed", 0),
                violations=result.provider_data.compliance_data.get("violations", []),
                confidence=result.provider_data.compliance_data.get("confidence", 0.0)
            )

        # Build response
        response = ValidateProviderResponse(
            request_id=request_id,
            timestamp=datetime.utcnow(),
            decision=ValidationDecision(result.decision.value),
            validation_path=ValidationPath(result.provider_data.validation_path) if result.provider_data.validation_path else ValidationPath.COMPLEX,
            final_confidence=result.final_confidence,
            confidence_level=classify_confidence(result.final_confidence),
            provider_data=result.provider_data.model_dump(),
            kpme_data=kpme_data,
            data_quality=data_quality,
            compliance=compliance,
            execution_time_ms=execution_time_ms,
            cache_hit=False,  # TODO: Implement cache hit detection
            reasoning=result.decision_reasoning if result.decision_reasoning else []
        )

        return response

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR in validate_provider: {error_details}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation failed: {str(e)}"
        )


@router.post(
    "/validate/fast",
    response_model=FastValidateResponse,
    status_code=status.HTTP_200_OK,
    summary="Fast Certificate Validation",
    description="Quick KPME certificate validation (deterministic, no AI, ultra-fast)",
    responses={
        200: {"description": "Validation completed successfully"},
        400: {"model": ErrorResponse, "description": "Invalid request data"},
        429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def fast_validate(
    request: FastValidateRequest,
    request_id: str = Depends(get_request_id),
    fast_validator: FastValidatorAgent = Depends(get_fast_validator_agent),
    api_key: str = Depends(verify_api_key),
    rate_limit: bool = Depends(check_rate_limit)
):
    """
    Fast KPME certificate validation.

    This endpoint uses the Fast Validator Agent which is:
    - Fully deterministic (no AI/LLM calls)
    - Ultra-fast (2,306 validations/second)
    - Cache-first architecture
    - Zero API costs

    Perfect for:
    - Real-time validation in forms
    - Bulk certificate checks
    - High-throughput scenarios
    """
    start_time = time.perf_counter()

    try:
        # Run fast validation
        result = await fast_validator.validate_fast(
            certificate_number=request.certificate_number,
            provider_name=request.provider_name,
            phone=request.phone
        )

        # Calculate execution time
        execution_time_ms = int((time.perf_counter() - start_time) * 1000)

        # Build response
        response = FastValidateResponse(
            request_id=request_id,
            timestamp=datetime.utcnow(),
            is_valid=result.is_valid,
            certificate_number=result.certificate_number,
            establishment_name=result.establishment_name,
            is_expired=result.is_expired,
            confidence=result.confidence,
            execution_time_ms=execution_time_ms,
            cache_hit=result.cache_hit,
            details=result.details
        )

        return response

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fast validation failed: {str(e)}"
        )


@router.post(
    "/validate/batch",
    response_model=BatchValidateResponse,
    status_code=status.HTTP_200_OK,
    summary="Batch Provider Validation",
    description="Validate multiple providers in a single request (parallel execution)",
    responses={
        200: {"description": "Batch validation completed"},
        400: {"model": ErrorResponse, "description": "Invalid request data"},
        429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def batch_validate(
    request: BatchValidateRequest,
    request_id: str = Depends(get_request_id),
    supervisor: SupervisorAgent = Depends(get_supervisor_agent),
    api_key: str = Depends(verify_api_key),
    rate_limit: bool = Depends(check_rate_limit)
):
    """
    Validate multiple providers in parallel.

    Executes validations concurrently (up to max_concurrent) for better performance.
    Returns individual results for each provider along with batch statistics.
    """
    start_time = time.perf_counter()

    # Limit batch size
    if len(request.providers) > 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Batch size exceeds maximum limit of 100 providers"
        )

    results = []
    errors = []

    # Semaphore to limit concurrent validations
    semaphore = asyncio.Semaphore(request.max_concurrent)

    async def validate_single(index: int, provider_request: ValidateProviderRequest):
        """Validate a single provider with semaphore control."""
        async with semaphore:
            try:
                # Prepare provider data
                provider_data = {
                    "establishment_name": provider_request.establishment_name,
                    "certificate_number": provider_request.certificate_number,
                    "phone": provider_request.phone,
                    "email": provider_request.email,
                    "address": provider_request.address,
                    "district": provider_request.district,
                    "category": provider_request.category,
                    "system_of_medicine": provider_request.system_of_medicine
                }

                # Remove None values
                provider_data = {k: v for k, v in provider_data.items() if v is not None}

                # Run validation
                validation_start = time.perf_counter()
                result = await supervisor.validate_provider(provider_data)
                validation_time_ms = int((time.perf_counter() - validation_start) * 1000)

                # Build KPME data response
                kpme_data = None
                if result.provider_data.kpme_data:
                    kpme_data = KPMEDataResponse(
                        certificate_number=result.provider_data.kpme_data.get("certificate_number", ""),
                        establishment_name=result.provider_data.kpme_data.get("establishment_name"),
                        category=result.provider_data.kpme_data.get("category"),
                        district=result.provider_data.kpme_data.get("district"),
                        phone=result.provider_data.kpme_data.get("phone"),
                        email=result.provider_data.kpme_data.get("email"),
                        address=result.provider_data.kpme_data.get("address"),
                        is_valid=result.provider_data.kpme_validated,
                        is_expired=result.provider_data.kpme_data.get("is_expired", False),
                        confidence=result.provider_data.kpme_data.get("confidence", 0.0)
                    )

                # Build data quality response
                data_quality = DataQualityResponse(
                    completeness_score=result.provider_data.data_quality_confidence,
                    accuracy_score=result.provider_data.data_quality_confidence,
                    overall_score=result.provider_data.data_quality_confidence,
                    missing_fields=[],
                    issues=[]
                )

                # Build compliance response
                compliance = None
                if result.provider_data.compliance_data:
                    compliance = ComplianceResponse(
                        is_compliant=result.provider_data.compliance_data.get("is_compliant", False),
                        checks_passed=result.provider_data.compliance_data.get("checks_passed", 0),
                        checks_failed=result.provider_data.compliance_data.get("checks_failed", 0),
                        violations=result.provider_data.compliance_data.get("violations", []),
                        confidence=result.provider_data.compliance_data.get("confidence", 0.0)
                    )

                # Build response
                response = ValidateProviderResponse(
                    request_id=f"{request_id}_item_{index}",
                    timestamp=datetime.utcnow(),
                    decision=ValidationDecision(result.decision.value),
                    validation_path=ValidationPath(result.provider_data.validation_path) if result.provider_data.validation_path else ValidationPath.COMPLEX,
                    final_confidence=result.final_confidence,
                    confidence_level=classify_confidence(result.final_confidence),
                    provider_data=result.provider_data.model_dump(),
                    kpme_data=kpme_data,
                    data_quality=data_quality,
                    compliance=compliance,
                    execution_time_ms=validation_time_ms,
                    cache_hit=False,
                    reasoning=result.decision_reasoning if result.decision_reasoning else []
                )

                results.append(response)

            except Exception as e:
                errors.append({
                    "index": index,
                    "establishment_name": provider_request.establishment_name,
                    "error": str(e)
                })

    # Run all validations in parallel
    tasks = [
        validate_single(i, provider_req)
        for i, provider_req in enumerate(request.providers)
    ]
    await asyncio.gather(*tasks)

    # Calculate execution time
    execution_time_ms = int((time.perf_counter() - start_time) * 1000)

    # Build batch response
    response = BatchValidateResponse(
        request_id=request_id,
        timestamp=datetime.utcnow(),
        total_providers=len(request.providers),
        completed=len(results),
        failed=len(errors),
        execution_time_ms=execution_time_ms,
        results=results,
        errors=errors
    )

    return response
