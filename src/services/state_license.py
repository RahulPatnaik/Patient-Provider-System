"""
State License API client for license verification.

Note: Each state has different APIs. This is a generic implementation
that can be extended with state-specific APIs.
"""

import httpx
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from enum import Enum


class LicenseStatus(str, Enum):
    """License status values."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    EXPIRED = "expired"
    SUSPENDED = "suspended"
    REVOKED = "revoked"
    UNKNOWN = "unknown"


class StateLicenseError(Exception):
    """State License API error."""
    pass


class StateLicenseClient:
    """
    Async client for State License verification.

    Features:
    - Multi-state license validation
    - Automatic retries with exponential backoff
    - Response caching (7 day TTL)
    - Error handling and timeout management

    Note: This is a generic implementation. In production, you would integrate
    with specific state APIs (e.g., California Medical Board, Texas Medical Board, etc.)
    """

    DEFAULT_TIMEOUT = 15.0
    MAX_RETRIES = 3

    # State API endpoints (placeholder - would be actual state APIs in production)
    STATE_APIS = {
        "CA": "https://www.mbc.ca.gov/breeze/",
        "TX": "https://profile.tmb.state.tx.us/",
        "NY": "https://www.nysed.gov/",
        "FL": "https://mqa-internet.doh.state.fl.us/",
    }

    def __init__(self, timeout: float = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _get_cache_key(self, license_number: str, state: str) -> str:
        """Generate cache key."""
        return f"{state}:{license_number}"

    def _get_from_cache(self, license_number: str, state: str) -> Optional[Dict[str, Any]]:
        """Get cached result if exists and not expired."""
        key = self._get_cache_key(license_number, state)
        if key in self._cache:
            cached = self._cache[key]
            if datetime.now() < cached["expires_at"]:
                return cached["data"]
            else:
                del self._cache[key]
        return None

    def _add_to_cache(self, license_number: str, state: str, data: Dict[str, Any]):
        """Add result to cache with 7 day TTL."""
        key = self._get_cache_key(license_number, state)
        self._cache[key] = {
            "data": data,
            "expires_at": datetime.now() + timedelta(days=7)
        }

    async def _make_request(
        self,
        url: str,
        params: Dict[str, Any],
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            if retry_count < self.MAX_RETRIES:
                wait_time = 2 ** retry_count
                await asyncio.sleep(wait_time)
                return await self._make_request(url, params, retry_count + 1)
            raise StateLicenseError(f"State License API error: {str(e)}")

    def _parse_license_status(self, status_str: str) -> LicenseStatus:
        """Parse license status string to enum."""
        status_lower = status_str.lower().strip()
        if "active" in status_lower or "current" in status_lower:
            return LicenseStatus.ACTIVE
        elif "expired" in status_lower:
            return LicenseStatus.EXPIRED
        elif "suspend" in status_lower:
            return LicenseStatus.SUSPENDED
        elif "revok" in status_lower:
            return LicenseStatus.REVOKED
        elif "inactive" in status_lower:
            return LicenseStatus.INACTIVE
        return LicenseStatus.UNKNOWN

    async def lookup_license(
        self,
        license_number: str,
        state: str,
        provider_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Look up license information by state and license number.

        Args:
            license_number: State license number
            state: Two-letter state code (e.g., "CA", "TX")
            provider_name: Optional provider name for additional validation

        Returns:
            Dict with license information:
            {
                "license_number": str,
                "state": str,
                "status": str,
                "issue_date": str,
                "expiration_date": str,
                "provider_name": str,
                "license_type": str,
                "disciplinary_actions": list
            }

        Raises:
            StateLicenseError: If API call fails or license not found
        """
        # Check cache first
        cached = self._get_from_cache(license_number, state)
        if cached:
            return cached

        # Check if state API is available
        if state not in self.STATE_APIS:
            raise StateLicenseError(f"No API available for state: {state}")

        # NOTE: This is a placeholder implementation
        # In production, you would call the actual state API here
        # Each state has different API formats and authentication requirements

        # Simulate API call (in production, replace with actual API call)
        # For now, we'll return a mock response structure
        license_data = {
            "license_number": license_number,
            "state": state,
            "status": "ACTIVE",  # Would come from actual API
            "issue_date": "2020-01-01",
            "expiration_date": "2025-12-31",
            "provider_name": provider_name or "Unknown",
            "license_type": "Physician and Surgeon",
            "disciplinary_actions": []
        }

        # Cache result
        self._add_to_cache(license_number, state, license_data)

        return license_data

    async def validate_license(
        self,
        license_number: str,
        state: str,
        provider_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate license and return validation result.

        Args:
            license_number: License number to validate
            state: Two-letter state code
            provider_name: Optional provider name for matching

        Returns:
            Dict with validation result:
            {
                "is_valid": bool,
                "license_number": str,
                "state": str,
                "exists": bool,
                "is_active": bool,
                "is_expired": bool,
                "has_disciplinary_actions": bool,
                "name_matches": bool,
                "confidence": float (0.0-1.0)
            }
        """
        # Basic validation
        if not license_number or not state:
            return {
                "is_valid": False,
                "license_number": license_number,
                "state": state,
                "exists": False,
                "is_active": False,
                "is_expired": False,
                "has_disciplinary_actions": False,
                "name_matches": False,
                "confidence": 0.0
            }

        try:
            license_data = await self.lookup_license(license_number, state, provider_name)

            status = self._parse_license_status(license_data.get("status", ""))
            is_active = status == LicenseStatus.ACTIVE
            is_expired = status == LicenseStatus.EXPIRED
            has_disciplinary = len(license_data.get("disciplinary_actions", [])) > 0

            # Check name match if provided
            name_matches = True
            if provider_name and license_data.get("provider_name"):
                name_matches = provider_name.lower() in license_data["provider_name"].lower()

            # Calculate confidence score
            confidence = 0.0
            if is_active:
                confidence = 1.0
                if has_disciplinary:
                    confidence = 0.7
                if not name_matches:
                    confidence *= 0.8
            elif is_expired:
                confidence = 0.3
            else:
                confidence = 0.1

            return {
                "is_valid": is_active and name_matches,
                "license_number": license_number,
                "state": state,
                "exists": True,
                "is_active": is_active,
                "is_expired": is_expired,
                "has_disciplinary_actions": has_disciplinary,
                "name_matches": name_matches,
                "confidence": confidence
            }
        except StateLicenseError:
            return {
                "is_valid": False,
                "license_number": license_number,
                "state": state,
                "exists": False,
                "is_active": False,
                "is_expired": False,
                "has_disciplinary_actions": False,
                "name_matches": False,
                "confidence": 0.0
            }

    async def validate_multiple_licenses(
        self,
        licenses: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Validate multiple licenses concurrently.

        Args:
            licenses: List of dicts with keys: license_number, state, provider_name (optional)

        Returns:
            List of validation results
        """
        tasks = [
            self.validate_license(
                lic.get("license_number", ""),
                lic.get("state", ""),
                lic.get("provider_name")
            )
            for lic in licenses
        ]
        return await asyncio.gather(*tasks, return_exceptions=False)
