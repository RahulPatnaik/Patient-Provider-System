"""
NPI Registry client for USA provider verification.
API Docs: https://npiregistry.cms.hhs.gov/api-page
"""

import httpx
import asyncio
import logging
from typing import Optional, List

from cache.base import BaseCacheClient
from services.base import BaseProviderRegistry, ProviderData, ProviderValidationResult


logger = logging.getLogger(__name__)


class NPIRegistryError(Exception):
    """NPI Registry API error."""
    pass


class NPIRegistryClient(BaseProviderRegistry):
    """
    NPI Registry client for USA.

    Features:
    - NPI lookup and validation
    - Redis caching with 24-hour TTL
    - Automatic retries with exponential backoff
    - Inherits from BaseProviderRegistry
    """

    BASE_URL = "https://npiregistry.cms.hhs.gov/api"
    CACHE_PREFIX = "usa:npi"
    CACHE_TTL = 86400  # 24 hours
    DEFAULT_TIMEOUT = 10.0
    MAX_RETRIES = 3

    def __init__(self, cache: BaseCacheClient, timeout: float = DEFAULT_TIMEOUT):
        """
        Initialize NPI Registry client.

        Args:
            cache: Cache client (Redis or Memory)
            timeout: HTTP request timeout in seconds
        """
        super().__init__(cache)
        self.timeout = timeout

    def _get_cache_key(self, identifier: str) -> str:
        """Generate cache key for NPI."""
        return f"{self.CACHE_PREFIX}:{identifier}"

    async def _make_request(
        self,
        endpoint: str,
        params: dict,
        retry_count: int = 0
    ) -> dict:
        """Make HTTP request with retry logic."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.BASE_URL}/{endpoint}", params=params)
                response.raise_for_status()
                return response.json()
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            if retry_count < self.MAX_RETRIES:
                wait_time = 2 ** retry_count
                logger.warning(
                    f"NPI API request failed (attempt {retry_count + 1}/{self.MAX_RETRIES}). "
                    f"Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                return await self._make_request(endpoint, params, retry_count + 1)
            raise NPIRegistryError(f"NPI Registry API error: {str(e)}")

    async def lookup_provider(self, identifier: str) -> ProviderData:
        """
        Look up provider by NPI number.

        Args:
            identifier: 10-digit NPI number

        Returns:
            ProviderData with provider information

        Raises:
            NPIRegistryError: If API call fails or NPI not found
        """
        # Check cache first
        cache_key = self._get_cache_key(identifier)
        cached = await self.cache.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for NPI: {identifier}")
            return ProviderData(**cached)

        # Make API request
        params = {"version": "2.1", "number": identifier}
        response = await self._make_request("", params)

        # Parse response
        if response.get("result_count", 0) == 0:
            raise NPIRegistryError(f"NPI {identifier} not found in registry")

        result = response["results"][0]
        basic = result.get("basic", {})
        addresses = result.get("addresses", [])
        taxonomies = result.get("taxonomies", [])

        # Get primary practice address
        practice_address = next(
            (addr for addr in addresses if addr.get("address_purpose") == "LOCATION"),
            addresses[0] if addresses else {}
        )

        # Extract specialty from primary taxonomy
        primary_taxonomy = next(
            (tax for tax in taxonomies if tax.get("primary")),
            taxonomies[0] if taxonomies else {}
        )

        # Create standardized provider data
        provider_data = ProviderData(
            identifier=result.get("number"),
            identifier_type="npi",
            provider_type="Individual" if result.get("enumeration_type") == "NPI-1" else "Organization",
            first_name=basic.get("first_name"),
            last_name=basic.get("last_name"),
            organization_name=basic.get("organization_name"),
            specialty=primary_taxonomy.get("desc"),
            address={
                "line1": practice_address.get("address_1", ""),
                "line2": practice_address.get("address_2", ""),
                "city": practice_address.get("city", ""),
                "state": practice_address.get("state", ""),
                "zip": practice_address.get("postal_code", ""),
                "country": practice_address.get("country_code", "US")
            },
            phone=practice_address.get("telephone_number"),
            status="active" if basic.get("status", "").upper() == "A" else "inactive",
            last_updated=basic.get("last_updated"),
            additional_data={
                "taxonomies": [
                    {
                        "code": tax.get("code"),
                        "description": tax.get("desc"),
                        "primary": tax.get("primary", False)
                    }
                    for tax in taxonomies
                ]
            }
        )

        # Cache result
        await self.cache.set(cache_key, provider_data.model_dump(), self.CACHE_TTL)
        logger.info(f"Cached NPI data for: {identifier}")

        return provider_data

    async def validate_provider(self, identifier: str) -> ProviderValidationResult:
        """
        Validate NPI and return validation result.

        Args:
            identifier: NPI number to validate

        Returns:
            ProviderValidationResult with confidence score
        """
        # Basic format validation
        if not identifier or len(identifier) != 10 or not identifier.isdigit():
            return ProviderValidationResult(
                is_valid=False,
                identifier=identifier,
                identifier_type="npi",
                exists=False,
                is_active=False,
                provider_type=None,
                confidence=0.0,
                error="Invalid NPI format"
            )

        try:
            provider_data = await self.lookup_provider(identifier)
            is_active = provider_data.status == "active"

            return ProviderValidationResult(
                is_valid=is_active,
                identifier=identifier,
                identifier_type="npi",
                exists=True,
                is_active=is_active,
                provider_type=provider_data.provider_type,
                confidence=1.0 if is_active else 0.7
            )

        except NPIRegistryError as e:
            logger.error(f"NPI validation error for {identifier}: {str(e)}")
            return ProviderValidationResult(
                is_valid=False,
                identifier=identifier,
                identifier_type="npi",
                exists=False,
                is_active=False,
                provider_type=None,
                confidence=0.0,
                error=str(e)
            )

    async def batch_validate(self, identifiers: List[str]) -> List[ProviderValidationResult]:
        """
        Validate multiple NPIs concurrently.

        Args:
            identifiers: List of NPI numbers

        Returns:
            List of validation results
        """
        tasks = [self.validate_provider(npi) for npi in identifiers]
        return await asyncio.gather(*tasks, return_exceptions=False)