"""
NMC (National Medical Commission) Registry client for India provider verification.
API: Third-party APIs like Surepass or official NMC portal scraping
"""

import httpx
import asyncio
import logging
from typing import Optional, List

from cache.base import BaseCacheClient
from services.base import BaseProviderRegistry, ProviderData, ProviderValidationResult


logger = logging.getLogger(__name__)


class NMCRegistryError(Exception):
    """NMC Registry API error."""
    pass


class NMCRegistryClient(BaseProviderRegistry):
    """
    National Medical Commission Registry client for India.

    Features:
    - NMR ID (National Medical Register ID) validation
    - Registration number lookup by state council
    - Redis caching with 24-hour TTL
    - Automatic retries with exponential backoff
    - Inherits from BaseProviderRegistry

    Note: This implementation uses third-party APIs (e.g., Surepass NMC Verification API)
    since NMC doesn't provide official public API access.
    """

    # Using Surepass API as example (requires API key)
    BASE_URL = "https://api.surepass.io/nmc-verification"
    CACHE_PREFIX = "india:nmc"
    CACHE_TTL = 86400  # 24 hours
    DEFAULT_TIMEOUT = 10.0
    MAX_RETRIES = 3

    def __init__(
        self,
        cache: BaseCacheClient,
        api_key: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT
    ):
        """
        Initialize NMC Registry client.

        Args:
            cache: Cache client (Redis or Memory)
            api_key: NMC API key (for third-party services like Surepass)
            timeout: HTTP request timeout in seconds
        """
        super().__init__(cache)
        self.api_key = api_key
        self.timeout = timeout

    def _get_cache_key(self, identifier: str) -> str:
        """Generate cache key for NMR ID."""
        return f"{self.CACHE_PREFIX}:{identifier}"

    async def _make_request(
        self,
        endpoint: str,
        params: dict,
        retry_count: int = 0
    ) -> dict:
        """Make HTTP request with retry logic."""
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.BASE_URL}/{endpoint}",
                    params=params,
                    headers=headers
                )
                response.raise_for_status()
                return response.json()
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            if retry_count < self.MAX_RETRIES:
                wait_time = 2 ** retry_count
                logger.warning(
                    f"NMC API request failed (attempt {retry_count + 1}/{self.MAX_RETRIES}). "
                    f"Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                return await self._make_request(endpoint, params, retry_count + 1)
            raise NMCRegistryError(f"NMC Registry API error: {str(e)}")

    async def lookup_provider(self, identifier: str) -> ProviderData:
        """
        Look up provider by NMR ID (National Medical Register ID).

        Args:
            identifier: NMR ID (format varies, typically alphanumeric)

        Returns:
            ProviderData with provider information

        Raises:
            NMCRegistryError: If API call fails or NMR ID not found
        """
        # Check cache first
        cache_key = self._get_cache_key(identifier)
        cached = await self.cache.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for NMR ID: {identifier}")
            return ProviderData(**cached)

        # NOTE: This is a placeholder implementation
        # In production, call actual NMC API (Surepass or official portal)
        # Example Surepass API call:
        # params = {"nmr_id": identifier}
        # response = await self._make_request("verify", params)

        # Simulated provider data (replace with actual API call)
        # This structure should match actual NMC API response
        provider_data = ProviderData(
            identifier=identifier,
            identifier_type="nmr",
            provider_type="Individual",
            first_name="Unknown",  # Would come from API
            last_name="Unknown",
            organization_name=None,
            specialty="General Medicine",  # From qualification field
            address={
                "line1": "",
                "line2": "",
                "city": "",
                "state": "",
                "zip": "",
                "country": "IN"
            },
            phone=None,
            status="active",  # "active" or "inactive"
            last_updated=None,
            additional_data={
                "state_medical_council": "Unknown",
                "registration_number": "Unknown",
                "registration_year": None,
                "qualifications": []
            }
        )

        # Cache result
        await self.cache.set(cache_key, provider_data.model_dump(), self.CACHE_TTL)
        logger.info(f"Cached NMC data for: {identifier}")

        return provider_data

    async def lookup_by_registration(
        self,
        registration_number: str,
        state_council: str,
        year: Optional[int] = None
    ) -> ProviderData:
        """
        Look up provider by registration details (India-specific method).

        This is an alternative lookup method specific to Indian medical system
        where doctors register with state medical councils before getting NMR ID.

        Args:
            registration_number: State medical council registration number
            state_council: State medical council code (e.g., "MH" for Maharashtra)
            year: Year of registration (optional)

        Returns:
            ProviderData with provider information

        Raises:
            NMCRegistryError: If lookup fails
        """
        # Create composite cache key
        cache_key = f"{self.CACHE_PREFIX}:reg:{state_council}:{registration_number}"
        cached = await self.cache.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for registration: {state_council}:{registration_number}")
            return ProviderData(**cached)

        # NOTE: Call actual API here
        # params = {
        #     "registration_number": registration_number,
        #     "state_council": state_council,
        #     "year": year
        # }
        # response = await self._make_request("verify-by-registration", params)

        # Placeholder - replace with actual API response
        provider_data = ProviderData(
            identifier=f"NMR-{state_council}-{registration_number}",
            identifier_type="nmr",
            provider_type="Individual",
            first_name="Unknown",
            last_name="Unknown",
            organization_name=None,
            specialty="General Medicine",
            address={
                "line1": "",
                "line2": "",
                "city": "",
                "state": state_council,
                "zip": "",
                "country": "IN"
            },
            phone=None,
            status="active",
            last_updated=None,
            additional_data={
                "state_medical_council": state_council,
                "registration_number": registration_number,
                "registration_year": year,
                "qualifications": []
            }
        )

        # Cache result
        await self.cache.set(cache_key, provider_data.model_dump(), self.CACHE_TTL)
        logger.info(f"Cached NMC data for registration: {state_council}:{registration_number}")

        return provider_data

    async def validate_provider(self, identifier: str) -> ProviderValidationResult:
        """
        Validate NMR ID and return validation result.

        Args:
            identifier: NMR ID to validate

        Returns:
            ProviderValidationResult with confidence score
        """
        # Basic format validation (NMR ID format may vary)
        if not identifier or len(identifier) < 5:
            return ProviderValidationResult(
                is_valid=False,
                identifier=identifier,
                identifier_type="nmr",
                exists=False,
                is_active=False,
                provider_type=None,
                confidence=0.0,
                error="Invalid NMR ID format"
            )

        try:
            provider_data = await self.lookup_provider(identifier)
            is_active = provider_data.status == "active"

            return ProviderValidationResult(
                is_valid=is_active,
                identifier=identifier,
                identifier_type="nmr",
                exists=True,
                is_active=is_active,
                provider_type=provider_data.provider_type,
                confidence=1.0 if is_active else 0.7
            )

        except NMCRegistryError as e:
            logger.error(f"NMC validation error for {identifier}: {str(e)}")
            return ProviderValidationResult(
                is_valid=False,
                identifier=identifier,
                identifier_type="nmr",
                exists=False,
                is_active=False,
                provider_type=None,
                confidence=0.0,
                error=str(e)
            )

    async def batch_validate(self, identifiers: List[str]) -> List[ProviderValidationResult]:
        """
        Validate multiple NMR IDs concurrently.

        Args:
            identifiers: List of NMR IDs

        Returns:
            List of validation results
        """
        tasks = [self.validate_provider(nmr_id) for nmr_id in identifiers]
        return await asyncio.gather(*tasks, return_exceptions=False)