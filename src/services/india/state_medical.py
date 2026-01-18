"""
India State Medical Council client for license verification.
Each state in India has its own medical council (e.g., Maharashtra, Karnataka, Tamil Nadu).
"""

import httpx
import asyncio
import logging
from typing import Optional, List, Dict, Any
from enum import Enum

from cache.base import BaseCacheClient
from services.base import BaseLicenseValidator, LicenseData, LicenseValidationResult


logger = logging.getLogger(__name__)


class LicenseStatus(str, Enum):
    """License status values."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    EXPIRED = "expired"
    SUSPENDED = "suspended"
    REVOKED = "revoked"
    UNKNOWN = "unknown"


class IndiaStateMedicalError(Exception):
    """India State Medical Council API error."""
    pass


class IndiaStateMedicalClient(BaseLicenseValidator):
    """
    India State Medical Council client.

    Features:
    - Multi-council license validation
    - Redis caching with 7-day TTL
    - Automatic retries with exponential backoff
    - Inherits from BaseLicenseValidator

    Note: Each state medical council has different systems.
    This is a generic implementation that can be extended per council.
    """

    CACHE_PREFIX = "india:medical_council"
    CACHE_TTL = 604800  # 7 days
    DEFAULT_TIMEOUT = 15.0
    MAX_RETRIES = 3

    # State medical councils
    STATE_COUNCILS = {
        "MH": "Maharashtra Medical Council",
        "KA": "Karnataka Medical Council",
        "TN": "Tamil Nadu Medical Council",
        "DL": "Delhi Medical Council",
        "GJ": "Gujarat Medical Council",
        "RJ": "Rajasthan Medical Council",
        "UP": "Uttar Pradesh Medical Council",
        "WB": "West Bengal Medical Council",
        "AP": "Andhra Pradesh Medical Council",
        "TG": "Telangana State Medical Council",
    }

    def __init__(self, cache: BaseCacheClient, timeout: float = DEFAULT_TIMEOUT):
        """
        Initialize India State Medical Council client.

        Args:
            cache: Cache client (Redis or Memory)
            timeout: HTTP request timeout in seconds
        """
        super().__init__(cache)
        self.timeout = timeout

    def _get_cache_key(self, license_number: str, region: str) -> str:
        """Generate cache key for license."""
        return f"{self.CACHE_PREFIX}:{region}:{license_number}"

    def _parse_license_status(self, status_str: str) -> LicenseStatus:
        """Parse license status string to enum."""
        status_lower = status_str.lower().strip()
        if "active" in status_lower or "current" in status_lower or "valid" in status_lower:
            return LicenseStatus.ACTIVE
        elif "expired" in status_lower or "lapsed" in status_lower:
            return LicenseStatus.EXPIRED
        elif "suspend" in status_lower:
            return LicenseStatus.SUSPENDED
        elif "cancel" in status_lower or "revok" in status_lower:
            return LicenseStatus.REVOKED
        elif "inactive" in status_lower:
            return LicenseStatus.INACTIVE
        return LicenseStatus.UNKNOWN

    async def _make_request(
        self,
        url: str,
        params: dict,
        retry_count: int = 0
    ) -> dict:
        """Make HTTP request with retry logic."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            if retry_count < self.MAX_RETRIES:
                wait_time = 2 ** retry_count
                logger.warning(
                    f"State medical council API request failed (attempt {retry_count + 1}/{self.MAX_RETRIES}). "
                    f"Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                return await self._make_request(url, params, retry_count + 1)
            raise IndiaStateMedicalError(f"State Medical Council API error: {str(e)}")

    async def lookup_license(
        self,
        license_number: str,
        region: str,
        provider_name: Optional[str] = None
    ) -> LicenseData:
        """
        Look up license information by state medical council.

        Args:
            license_number: Medical council registration number
            region: Two-letter state/council code (e.g., "MH", "KA")
            provider_name: Optional provider name for validation

        Returns:
            LicenseData with license information

        Raises:
            IndiaStateMedicalError: If API call fails or license not found
        """
        # Check cache first
        cache_key = self._get_cache_key(license_number, region)
        cached = await self.cache.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for license: {region}:{license_number}")
            return LicenseData(**cached)

        # Check if council is supported
        if region not in self.STATE_COUNCILS:
            raise IndiaStateMedicalError(
                f"No API available for state medical council: {region}. "
                f"Supported councils: {', '.join(self.STATE_COUNCILS.keys())}"
            )

        # NOTE: This is a placeholder implementation
        # In production, call actual state medical council API or portal
        # Each state council has different systems (some have APIs, others require scraping)

        # Simulated license data (replace with actual API call)
        license_data = LicenseData(
            license_number=license_number,
            region=region,
            region_type="council",
            status="ACTIVE",  # Would come from actual API
            issue_date="2020-01-01",
            expiration_date=None,  # Many Indian licenses don't expire
            provider_name=provider_name or "Unknown",
            license_type="Registered Medical Practitioner",
            disciplinary_actions=[],
            additional_data={
                "council_name": self.STATE_COUNCILS[region],
                "registration_type": "Permanent"  # or "Temporary"
            }
        )

        # Cache result
        await self.cache.set(cache_key, license_data.model_dump(), self.CACHE_TTL)
        logger.info(f"Cached license data for: {region}:{license_number}")

        return license_data

    async def validate_license(
        self,
        license_number: str,
        region: str,
        provider_name: Optional[str] = None
    ) -> LicenseValidationResult:
        """
        Validate license and return validation result.

        Args:
            license_number: Registration number to validate
            region: Two-letter state/council code
            provider_name: Optional provider name for matching

        Returns:
            LicenseValidationResult with confidence score
        """
        # Basic validation
        if not license_number or not region:
            return LicenseValidationResult(
                is_valid=False,
                license_number=license_number,
                region=region,
                region_type="council",
                exists=False,
                is_active=False,
                is_expired=False,
                has_disciplinary_actions=False,
                name_matches=False,
                confidence=0.0,
                error="Missing license number or council code"
            )

        try:
            license_data = await self.lookup_license(license_number, region, provider_name)

            status = self._parse_license_status(license_data.status)
            is_active = status == LicenseStatus.ACTIVE
            is_expired = status == LicenseStatus.EXPIRED
            has_disciplinary = len(license_data.disciplinary_actions) > 0

            # Check name match if provided
            name_matches = None
            if provider_name and license_data.provider_name:
                name_matches = provider_name.lower() in license_data.provider_name.lower()

            # Calculate confidence score
            confidence = 0.0
            if is_active:
                confidence = 1.0
                if has_disciplinary:
                    confidence = 0.7
                if name_matches is False:
                    confidence *= 0.8
            elif is_expired:
                confidence = 0.3
            else:
                confidence = 0.1

            return LicenseValidationResult(
                is_valid=is_active and (name_matches is not False),
                license_number=license_number,
                region=region,
                region_type="council",
                exists=True,
                is_active=is_active,
                is_expired=is_expired,
                has_disciplinary_actions=has_disciplinary,
                name_matches=name_matches,
                confidence=confidence
            )

        except IndiaStateMedicalError as e:
            logger.error(f"License validation error for {region}:{license_number}: {str(e)}")
            return LicenseValidationResult(
                is_valid=False,
                license_number=license_number,
                region=region,
                region_type="council",
                exists=False,
                is_active=False,
                is_expired=False,
                has_disciplinary_actions=False,
                name_matches=False,
                confidence=0.0,
                error=str(e)
            )

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
        tasks = [
            self.validate_license(
                lic.get("license_number", ""),
                lic.get("region", ""),
                lic.get("provider_name")
            )
            for lic in licenses
        ]
        return await asyncio.gather(*tasks, return_exceptions=False)