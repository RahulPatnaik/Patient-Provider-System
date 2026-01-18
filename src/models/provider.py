"""
Provider data models for KPME Karnataka healthcare establishments.

KPME-focused models (EL Branch):
- ProviderInputModel: Input data for validation
- EnrichedProviderModel: After validation and enrichment
- ProviderOutputModel: Final output for storage
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, EmailStr, field_validator
from enum import Enum


# ============================================================================
# Enums
# ============================================================================

class ProviderType(str, Enum):
    """KPME establishment types."""
    HOSPITAL = "hospital"
    CLINIC = "clinic"
    DIAGNOSTIC_CENTER = "diagnostic_center"
    PHARMACY = "pharmacy"
    OTHER = "other"


class SystemOfMedicine(str, Enum):
    """Systems of medicine supported in KPME."""
    ALLOPATHY = "Allopathy"
    AYURVEDA = "Ayurveda"
    HOMEOPATHY = "Homeopathy"
    UNANI = "Unani"
    SIDDHA = "Siddha"
    OTHER = "Other"


class ValidationStatus(str, Enum):
    """Validation status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    VALIDATED = "validated"
    FAILED = "failed"


class Decision(str, Enum):
    """Final decision after validation."""
    AUTO_APPROVED = "auto_approved"
    MANUAL_REVIEW = "manual_review"
    AUTO_REJECTED = "auto_rejected"


# ============================================================================
# Sub-Models
# ============================================================================

class AddressModel(BaseModel):
    """Address information."""
    street: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    state: str = "Karnataka"
    pin_code: Optional[str] = None
    country: str = "India"


class LocationModel(BaseModel):
    """GPS coordinates."""
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class CertificateModel(BaseModel):
    """KPME certificate information."""
    certificate_number: str
    certificate_type: Optional[str] = None
    issue_date: Optional[str] = None
    validity_date: Optional[str] = None
    is_expired: bool = False


# ============================================================================
# Input Model
# ============================================================================

class ProviderInputModel(BaseModel):
    """
    Input model for KPME provider validation.

    This is what users/API submit for validation.
    """
    # Basic Information
    establishment_name: str = Field(..., min_length=2)
    provider_type: Optional[ProviderType] = None
    category: Optional[str] = None  # KPME category (Level 1, Level 2, etc.)

    # KPME Certificate
    certificate_number: Optional[str] = None
    certificate_validity: Optional[str] = None

    # Contact Information
    phone: Optional[str] = None
    email: Optional[EmailStr] = None
    website: Optional[str] = None

    # Address
    address: Optional[str] = None
    district: Optional[str] = None
    area: Optional[str] = None
    pin_code: Optional[str] = None

    # Location
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Medical Information
    system_of_medicine: Optional[SystemOfMedicine] = None
    num_beds: Optional[int] = None
    specialties: Optional[List[str]] = None

    # Additional Information
    registration_number: Optional[str] = None  # Additional registration
    owner_name: Optional[str] = None

    @field_validator('phone', mode='before')
    @classmethod
    def validate_phone(cls, v):
        """Validate phone number format."""
        if v is None:
            return v
        # Convert to string (handles both string and float/int)
        v_str = str(v)
        # Clean phone number
        cleaned = v_str.replace(" ", "").replace("-", "").replace("+", "").replace("(", "").replace(")", "")
        # Remove trailing .0 if present
        if cleaned.endswith(".0"):
            cleaned = cleaned[:-2]
        return cleaned if len(cleaned) >= 10 else None

    @field_validator('num_beds')
    @classmethod
    def validate_beds(cls, v):
        """Validate number of beds."""
        if v is None:
            return v
        return int(v) if v >= 0 else None


# ============================================================================
# Enriched Model
# ============================================================================

class EnrichedProviderModel(BaseModel):
    """
    Enriched provider model after validation.

    Contains original input + validation results + enrichment data.
    """
    # Original input
    provider_input: ProviderInputModel

    # Validation flags
    kpme_validated: bool = False
    certificate_valid: bool = False
    certificate_expired: bool = False
    data_quality_passed: bool = False
    compliance_passed: bool = False

    # Validation results
    kpme_data: Optional[Dict[str, Any]] = None
    compliance_data: Optional[Dict[str, Any]] = None
    web_scraper_data: Optional[Dict[str, Any]] = None
    enrichment_data: Optional[Dict[str, Any]] = None

    # Confidence scores
    kpme_confidence: float = 0.0
    data_quality_confidence: float = 0.0
    compliance_confidence: float = 0.0
    overall_confidence: float = 0.0

    # Metadata
    validation_timestamp: datetime = Field(default_factory=datetime.utcnow)
    validation_path: Optional[str] = None  # "simple" or "complex"
    data_sources: List[str] = Field(default_factory=list)


# ============================================================================
# Output Model
# ============================================================================

class ProviderOutputModel(BaseModel):
    """
    Final provider output model for database storage.

    Contains all validation data + final decision.
    """
    # Enriched provider data
    provider_data: EnrichedProviderModel

    # Validation status
    validation_status: ValidationStatus

    # Final decision
    decision: Optional[Decision] = None
    decision_reasoning: Optional[List[str]] = None

    # Confidence and scores
    final_confidence: float = 0.0

    # Issues and warnings
    issues: List[Dict[str, str]] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Metadata
    processed_by: str = "KPME Validation System"
    version: str = "1.0.0-EL"


# ============================================================================
# Response Models for API
# ============================================================================

class ValidationResponse(BaseModel):
    """API response for validation requests."""
    success: bool
    message: str
    provider_id: Optional[str] = None
    validation_status: Optional[ValidationStatus] = None
    decision: Optional[Decision] = None
    confidence: Optional[float] = None
    details: Optional[Dict[str, Any]] = None


class ProviderListResponse(BaseModel):
    """API response for listing providers."""
    total: int
    providers: List[ProviderOutputModel]
    page: int = 1
    page_size: int = 10
