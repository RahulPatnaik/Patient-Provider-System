"""
NPI Registry API client for provider verification.
API Docs: https://npiregistry.cms.hhs.gov/api-page
"""

import httpx
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timedelta


class NPIRegistryError(Exception):
    """NPI Registry API error."""
    pass


class NPIRegistryClient:
    """
    Async client for NPI Registry API.

    Features:
    - NPI lookup and validation
    - Automatic retries with exponential backoff
    - Response caching (24 hour TTL)
    - Error handling and timeout management
    """

    BASE_URL = "https://npiregistry.cms.hhs.gov/api"
    DEFAULT_TIMEOUT = 10.0
    MAX_RETRIES = 3

    def __init__(self, timeout: float = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self._cache: Dict[str, Dict[str, Any]] = {}

    async def _make_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.BASE_URL}/{endpoint}", params=params)
                response.raise_for_status()
                return response.json()
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            if retry_count < self.MAX_RETRIES:
                wait_time = 2 ** retry_count  # Exponential backoff
                await asyncio.sleep(wait_time)
                return await self._make_request(endpoint, params, retry_count + 1)
            raise NPIRegistryError(f"NPI Registry API error: {str(e)}")

    def _get_from_cache(self, npi: str) -> Optional[Dict[str, Any]]:
        """Get cached result if exists and not expired."""
        if npi in self._cache:
            cached = self._cache[npi]
            if datetime.now() < cached["expires_at"]:
                return cached["data"]
            else:
                del self._cache[npi]
        return None

    def _add_to_cache(self, npi: str, data: Dict[str, Any]):
        """Add result to cache with 24 hour TTL."""
        self._cache[npi] = {
            "data": data,
            "expires_at": datetime.now() + timedelta(hours=24)
        }

    async def lookup_npi(self, npi: str) -> Dict[str, Any]:
        """
        Look up provider by NPI number.

        Args:
            npi: 10-digit National Provider Identifier

        Returns:
            Dict with provider information:
            {
                "npi": str,
                "provider_type": str,
                "first_name": str,
                "last_name": str,
                "organization_name": str,
                "address": dict,
                "phone": str,
                "taxonomy": list,
                "status": str,
                "last_updated": str
            }

        Raises:
            NPIRegistryError: If API call fails or NPI not found
        """
        # Check cache first
        cached = self._get_from_cache(npi)
        if cached:
            return cached

        # Make API request
        params = {
            "version": "2.1",
            "number": npi
        }

        response = await self._make_request("", params)

        # Parse response
        if response.get("result_count", 0) == 0:
            raise NPIRegistryError(f"NPI {npi} not found in registry")

        result = response["results"][0]

        # Extract relevant data
        basic = result.get("basic", {})
        addresses = result.get("addresses", [])
        taxonomies = result.get("taxonomies", [])

        # Get primary practice address
        practice_address = next(
            (addr for addr in addresses if addr.get("address_purpose") == "LOCATION"),
            addresses[0] if addresses else {}
        )

        provider_data = {
            "npi": result.get("number"),
            "provider_type": "Individual" if result.get("enumeration_type") == "NPI-1" else "Organization",
            "first_name": basic.get("first_name", ""),
            "last_name": basic.get("last_name", ""),
            "organization_name": basic.get("organization_name", ""),
            "address": {
                "line1": practice_address.get("address_1", ""),
                "line2": practice_address.get("address_2", ""),
                "city": practice_address.get("city", ""),
                "state": practice_address.get("state", ""),
                "zip": practice_address.get("postal_code", ""),
                "country": practice_address.get("country_code", "US")
            },
            "phone": practice_address.get("telephone_number", ""),
            "taxonomy": [
                {
                    "code": tax.get("code"),
                    "description": tax.get("desc"),
                    "primary": tax.get("primary", False)
                }
                for tax in taxonomies
            ],
            "status": basic.get("status", ""),
            "last_updated": basic.get("last_updated", "")
        }

        # Cache result
        self._add_to_cache(npi, provider_data)

        return provider_data

    async def validate_npi(self, npi: str) -> Dict[str, Any]:
        """
        Validate NPI and return validation result.

        Args:
            npi: NPI number to validate

        Returns:
            Dict with validation result:
            {
                "is_valid": bool,
                "npi": str,
                "exists": bool,
                "is_active": bool,
                "provider_type": str,
                "confidence": float (0.0-1.0)
            }
        """
        # Basic format validation
        if not npi or len(npi) != 10 or not npi.isdigit():
            return {
                "is_valid": False,
                "npi": npi,
                "exists": False,
                "is_active": False,
                "provider_type": None,
                "confidence": 0.0
            }

        try:
            provider_data = await self.lookup_npi(npi)

            is_active = provider_data.get("status", "").upper() == "A"

            return {
                "is_valid": True,
                "npi": npi,
                "exists": True,
                "is_active": is_active,
                "provider_type": provider_data.get("provider_type"),
                "confidence": 1.0 if is_active else 0.7
            }
        except NPIRegistryError:
            return {
                "is_valid": False,
                "npi": npi,
                "exists": False,
                "is_active": False,
                "provider_type": None,
                "confidence": 0.0
            }

    async def batch_validate_npi(self, npis: list[str]) -> list[Dict[str, Any]]:
        """
        Validate multiple NPIs concurrently.

        Args:
            npis: List of NPI numbers

        Returns:
            List of validation results
        """
        tasks = [self.validate_npi(npi) for npi in npis]
        return await asyncio.gather(*tasks, return_exceptions=False)
