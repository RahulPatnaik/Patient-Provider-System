"""
API Request/Response Models for KPME Provider Validation System.

These models define the API contract for all endpoints.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum


# ============================================================================
# Enums
# ============================================================================


class ValidationDecision(str, Enum):
    """Validation decision types."""
    AUTO_APPROVED = "auto_approved"
    AUTO_REJECTED = "auto_rejected"
    MANUAL_REVIEW = "manual_review"


class ValidationPath(str, Enum):
    """Validation path types."""
    SIMPLE = "simple"
    COMPLEX = "complex"


class ConfidenceLevel(str, Enum):
    """Confidence level classification."""
    VERY_HIGH = "very_high"  # >= 90%
    HIGH = "high"            # >= 70%
    MEDIUM = "medium"        # >= 50%
    LOW = "low"              # >= 30%
    VERY_LOW = "very_low"    # < 30%


# ============================================================================
# Request Models
# ============================================================================


class ValidateProviderRequest(BaseModel):
    """Request model for provider validation."""

    # Required fields
    establishment_name: str = Field(..., description="Healthcare establishment name")

    # Optional fields
    certificate_number: Optional[str] = Field(None, description="KPME certificate number")
    phone: Optional[str] = Field(None, description="Contact phone number")
    email: Optional[str] = Field(None, description="Contact email")
    address: Optional[str] = Field(None, description="Physical address")
    district: Optional[str] = Field(None, description="District in Karnataka")
    category: Optional[str] = Field(None, description="Establishment category")
    system_of_medicine: Optional[str] = Field(None, description="System of medicine")

    # Validation options
    force_complex_path: bool = Field(False, description="Force complex path validation")
    use_cache: bool = Field(True, description="Use cached results if available")

    class Config:
        json_schema_extra = {
            "example": {
                "establishment_name": "AAROGYA HOSPITAL",
                "certificate_number": "BDR00172ALHL2",
                "phone": "9448452147",
                "district": "Bangalore Urban",
                "use_cache": True
            }
        }


class BatchValidateRequest(BaseModel):
    """Request model for batch validation."""

    providers: List[ValidateProviderRequest] = Field(..., description="List of providers to validate")
    max_concurrent: int = Field(5, ge=1, le=20, description="Maximum concurrent validations")

    class Config:
        json_schema_extra = {
            "example": {
                "providers": [
                    {
                        "establishment_name": "AAROGYA HOSPITAL",
                        "certificate_number": "BDR00172ALHL2"
                    },
                    {
                        "establishment_name": "TEST CLINIC",
                        "phone": "9876543210"
                    }
                ],
                "max_concurrent": 5
            }
        }


class FastValidateRequest(BaseModel):
    """Request model for fast validation (certificate-only)."""

    certificate_number: str = Field(..., description="KPME certificate number")
    provider_name: Optional[str] = Field(None, description="Provider name for verification")
    phone: Optional[str] = Field(None, description="Phone for verification")
    use_cache: bool = Field(True, description="Use cached results if available")

    class Config:
        json_schema_extra = {
            "example": {
                "certificate_number": "BDR00172ALHL2",
                "provider_name": "AAROGYA HOSPITAL",
                "use_cache": True
            }
        }


class ManualReviewDecisionRequest(BaseModel):
    """Request model for manual review decision."""

    decision: ValidationDecision = Field(..., description="Manual review decision")
    reviewer_notes: str = Field(..., description="Reviewer's notes/reasoning")
    reviewer_id: str = Field(..., description="ID of the reviewer")

    class Config:
        json_schema_extra = {
            "example": {
                "decision": "auto_approved",
                "reviewer_notes": "Verified with KPME portal manually",
                "reviewer_id": "admin@example.com"
            }
        }


# ============================================================================
# Response Models
# ============================================================================


class KPMEDataResponse(BaseModel):
    """KPME validation data in API response."""

    certificate_number: str
    establishment_name: Optional[str] = None
    category: Optional[str] = None
    district: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    is_valid: bool
    is_expired: bool
    confidence: float = Field(ge=0.0, le=1.0)


class DataQualityResponse(BaseModel):
    """Data quality assessment in API response."""

    completeness_score: float = Field(ge=0.0, le=1.0)
    accuracy_score: float = Field(ge=0.0, le=1.0)
    overall_score: float = Field(ge=0.0, le=1.0)
    missing_fields: List[str]
    issues: List[str]


class ComplianceResponse(BaseModel):
    """Compliance check in API response."""

    is_compliant: bool
    checks_passed: int
    checks_failed: int
    violations: List[str]
    confidence: float = Field(ge=0.0, le=1.0)


class ValidateProviderResponse(BaseModel):
    """Response model for provider validation."""

    # Request tracking
    request_id: str = Field(..., description="Unique request identifier")
    timestamp: datetime = Field(..., description="Validation timestamp")

    # Validation result
    decision: ValidationDecision = Field(..., description="Validation decision")
    validation_path: ValidationPath = Field(..., description="Path used for validation")

    # Confidence scoring
    final_confidence: float = Field(..., ge=0.0, le=1.0, description="Overall confidence score")
    confidence_level: ConfidenceLevel = Field(..., description="Confidence classification")

    # Provider data
    provider_data: Dict[str, Any] = Field(..., description="Enriched provider information")

    # Validation details
    kpme_data: Optional[KPMEDataResponse] = Field(None, description="KPME validation data")
    data_quality: Optional[DataQualityResponse] = Field(None, description="Data quality assessment")
    compliance: Optional[ComplianceResponse] = Field(None, description="Compliance check results")

    # Metadata
    execution_time_ms: int = Field(..., description="Total execution time in milliseconds")
    cache_hit: bool = Field(False, description="Whether result was served from cache")
    reasoning: List[str] = Field(default_factory=list, description="Decision reasoning")

    class Config:
        json_schema_extra = {
            "example": {
                "request_id": "req_abc123",
                "timestamp": "2026-01-04T19:30:00Z",
                "decision": "auto_approved",
                "validation_path": "simple",
                "final_confidence": 0.92,
                "confidence_level": "very_high",
                "provider_data": {
                    "establishment_name": "AAROGYA HOSPITAL",
                    "certificate_number": "BDR00172ALHL2",
                    "district": "Bangalore Urban"
                },
                "execution_time_ms": 450,
                "cache_hit": False,
                "reasoning": ["KPME certificate valid", "High data quality"]
            }
        }


class BatchValidateResponse(BaseModel):
    """Response model for batch validation."""

    request_id: str
    timestamp: datetime
    total_providers: int
    completed: int
    failed: int
    execution_time_ms: int
    results: List[ValidateProviderResponse]
    errors: List[Dict[str, Any]] = Field(default_factory=list)

    class Config:
        json_schema_extra = {
            "example": {
                "request_id": "batch_xyz789",
                "timestamp": "2026-01-04T19:30:00Z",
                "total_providers": 10,
                "completed": 9,
                "failed": 1,
                "execution_time_ms": 2500,
                "results": [],
                "errors": [{"index": 5, "error": "Invalid certificate format"}]
            }
        }


class FastValidateResponse(BaseModel):
    """Response model for fast validation."""

    request_id: str
    timestamp: datetime
    is_valid: bool
    certificate_number: str
    establishment_name: Optional[str] = None
    is_expired: bool
    confidence: float = Field(ge=0.0, le=1.0)
    execution_time_ms: int
    cache_hit: bool
    details: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        json_schema_extra = {
            "example": {
                "request_id": "fast_def456",
                "timestamp": "2026-01-04T19:30:00Z",
                "is_valid": True,
                "certificate_number": "BDR00172ALHL2",
                "establishment_name": "AAROGYA HOSPITAL",
                "is_expired": False,
                "confidence": 0.9,
                "execution_time_ms": 2,
                "cache_hit": True
            }
        }


class HealthCheckResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="Overall health status")
    timestamp: datetime
    version: str = Field(..., description="API version")
    uptime_seconds: float = Field(..., description="Server uptime in seconds")

    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2026-01-04T19:30:00Z",
                "version": "1.0.0",
                "uptime_seconds": 3600.5
            }
        }


class ServicesHealthResponse(BaseModel):
    """External services health check."""

    database: Dict[str, Any]
    cache: Dict[str, Any]
    overall_status: str
    timestamp: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "database": {
                    "status": "healthy",
                    "total_establishments": 1000,
                    "response_time_ms": 5
                },
                "cache": {
                    "status": "healthy",
                    "type": "redis",
                    "response_time_ms": 2
                },
                "overall_status": "healthy",
                "timestamp": "2026-01-04T19:30:00Z"
            }
        }


class SystemStatsResponse(BaseModel):
    """System statistics response."""

    # Database stats
    total_establishments: int
    total_staff: int
    total_districts: int

    # Validation stats
    total_validations: int
    auto_approved: int
    auto_rejected: int
    manual_review: int

    # Performance stats
    avg_response_time_ms: float
    cache_hit_rate: float = Field(ge=0.0, le=1.0)

    # Timestamp
    timestamp: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "total_establishments": 1000,
                "total_staff": 4483,
                "total_districts": 31,
                "total_validations": 1250,
                "auto_approved": 850,
                "auto_rejected": 200,
                "manual_review": 200,
                "avg_response_time_ms": 450.5,
                "cache_hit_rate": 0.65,
                "timestamp": "2026-01-04T19:30:00Z"
            }
        }


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    request_id: Optional[str] = Field(None, description="Request ID for tracking")
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Validation failed",
                "detail": "Certificate number is required",
                "request_id": "req_error123",
                "timestamp": "2026-01-04T19:30:00Z"
            }
        }


class ManualReviewItemResponse(BaseModel):
    """Manual review queue item."""

    validation_id: str
    provider_data: Dict[str, Any]
    final_confidence: float
    submitted_at: datetime
    reasoning: List[str]

    class Config:
        json_schema_extra = {
            "example": {
                "validation_id": "val_review123",
                "provider_data": {
                    "establishment_name": "TEST CLINIC",
                    "certificate_number": "TEST12345"
                },
                "final_confidence": 0.55,
                "submitted_at": "2026-01-04T19:00:00Z",
                "reasoning": ["Partial data match", "Requires manual verification"]
            }
        }
