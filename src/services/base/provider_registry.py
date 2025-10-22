"""
Abstract base class for provider registry services (NPI, NMC, etc.).
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

from cache.base import BaseCacheClient


class ProviderData(BaseModel):
    """Standardized provider data model (region-agnostic)."""
    identifier: str  # NPI, NMR ID, etc.
    identifier_type: str  # "npi", "nmr", etc.
    provider_type: str  # "Individual", "Organization"
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    organization_name: Optional[str] = None
    specialty: Optional[str] = None
    address: Dict[str, str]
    phone: Optional[str] = None
    status: str  # "active", "inactive"
    last_updated: Optional[str] = None
    additional_data: Dict[str, Any] = {}


class ProviderValidationResult(BaseModel):
    """Standardized provider validation result (region-agnostic)."""
    is_valid: bool
    identifier: str
    identifier_type: str  # "npi", "nmr"
    exists: bool
    is_active: bool
    provider_type: Optional[str] = None
    confidence: float  # 0.0 to 1.0
    error: Optional[str] = None


class BaseProviderRegistry(ABC):
    """
    Abstract base class for provider registry services.

    Concrete implementations:
    - NPIRegistryClient (USA)
    - NMCRegistryClient (India)
    """

    def __init__(self, cache: BaseCacheClient):
        """
        Initialize provider registry with cache.

        Args:
            cache: Cache client for storing provider data
        """
        self.cache = cache

    @abstractmethod
    async def validate_provider(self, identifier: str) -> ProviderValidationResult:
        """
        Validate provider identifier (NPI, NMR ID, etc.).

        Args:
            identifier: Provider identifier to validate

        Returns:
            Provider validation result with confidence score
        """
        pass

    @abstractmethod
    async def lookup_provider(self, identifier: str) -> ProviderData:
        """
        Look up provider details by identifier.

        Args:
            identifier: Provider identifier

        Returns:
            Provider data

        Raises:
            Exception: If provider not found or API error
        """
        pass

    @abstractmethod
    async def batch_validate(self, identifiers: List[str]) -> List[ProviderValidationResult]:
        """
        Validate multiple providers concurrently.

        Args:
            identifiers: List of provider identifiers

        Returns:
            List of validation results
        """
        pass

    @abstractmethod
    def _get_cache_key(self, identifier: str) -> str:
        """
        Generate cache key for provider data.

        Args:
            identifier: Provider identifier

        Returns:
            Cache key with appropriate prefix
        """
        pass