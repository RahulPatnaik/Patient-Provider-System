"""
Abstract base class for license validation services (State/Council).
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

from cache.base import BaseCacheClient


class LicenseData(BaseModel):
    """Standardized license data model (region-agnostic)."""
    license_number: str
    region: str  # State code (US) or Council code (India)
    region_type: str  # "state" or "council"
    status: str  # "active", "expired", "suspended", "revoked"
    issue_date: Optional[str] = None
    expiration_date: Optional[str] = None
    provider_name: Optional[str] = None
    license_type: Optional[str] = None
    disciplinary_actions: List[Dict[str, Any]] = []
    additional_data: Dict[str, Any] = {}


class LicenseValidationResult(BaseModel):
    """Standardized license validation result (region-agnostic)."""
    is_valid: bool
    license_number: str
    region: str  # State/Council code
    region_type: str  # "state" or "council"
    exists: bool
    is_active: bool
    is_expired: bool
    has_disciplinary_actions: bool
    name_matches: Optional[bool] = None
    confidence: float  # 0.0 to 1.0
    error: Optional[str] = None


class BaseLicenseValidator(ABC):
    """
    Abstract base class for license validation services.

    Concrete implementations:
    - USStateLicenseClient (USA states)
    - IndiaStateMedicalClient (India medical councils)
    """

    def __init__(self, cache: BaseCacheClient):
        """
        Initialize license validator with cache.

        Args:
            cache: Cache client for storing license data
        """
        self.cache = cache

    @abstractmethod
    async def validate_license(
        self,
        license_number: str,
        region: str,
        provider_name: Optional[str] = None
    ) -> LicenseValidationResult:
        """
        Validate license for a specific region (state/council).

        Args:
            license_number: License number to validate
            region: Region code (e.g., "CA" for California, "MH" for Maharashtra)
            provider_name: Provider name for matching (optional)

        Returns:
            License validation result with confidence score
        """
        pass

    @abstractmethod
    async def lookup_license(
        self,
        license_number: str,
        region: str,
        provider_name: Optional[str] = None
    ) -> LicenseData:
        """
        Look up license details.

        Args:
            license_number: License number
            region: Region code
            provider_name: Provider name (optional)

        Returns:
            License data

        Raises:
            Exception: If license not found or API error
        """
        pass

    @abstractmethod
    async def validate_multiple(
        self,
        licenses: List[Dict[str, str]]
    ) -> List[LicenseValidationResult]:
        """
        Validate multiple licenses concurrently.

        Args:
            licenses: List of dicts with keys: license_number, region, provider_name (optional)

        Returns:
            List of validation results
        """
        pass

    @abstractmethod
    def _get_cache_key(self, license_number: str, region: str) -> str:
        """
        Generate cache key for license data.

        Args:
            license_number: License number
            region: Region code

        Returns:
            Cache key with appropriate prefix
        """
        pass