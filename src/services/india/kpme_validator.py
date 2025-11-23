"""
KPME Karnataka Validator - Uses KPME database for provider validation

Validates Karnataka healthcare providers against KPME establishment database.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
import re

from services.base import BaseLicenseValidator, LicenseValidationResult, LicenseData
from database.kpme_db import get_kpme_db
from cache.factory import get_cache_instance


class KPMEValidator(BaseLicenseValidator):
    """
    KPME Karnataka establishment validator.

    Validates providers against Karnataka Private Medical Establishments database.
    """

    def __init__(self, use_cache: bool = True):
        """
        Initialize KPME validator.

        Args:
            use_cache: Whether to use caching (default: True)
        """
        cache = get_cache_instance() if use_cache else None
        super().__init__(cache)
        self.db = get_kpme_db()
        self.use_cache = use_cache

    def _get_cache_key(self, license_number: str, region: str) -> str:
        """Generate cache key for KPME certificate."""
        return f"kpme:{region}:{license_number}"

    async def lookup_license(
        self,
        license_number: str,
        region: str,
        provider_name: Optional[str] = None
    ) -> LicenseData:
        """
        Look up KPME certificate details.

        Args:
            license_number: KPME certificate number
            region: District name
            provider_name: Establishment name

        Returns:
            License data

        Raises:
            Exception: If license not found
        """
        establishment = self.db.get_establishment_by_certificate(license_number)

        if not establishment:
            raise Exception(f"KPME certificate {license_number} not found")

        return LicenseData(
            license_number=establishment.get('certificate_number', ''),
            region=establishment.get('district', region),
            region_type='kpme_establishment',
            status='active' if establishment.get('certificate_validity') else 'unknown',
            issue_date=None,
            expiration_date=establishment.get('certificate_validity'),
            provider_name=establishment.get('establishment_name'),
            license_type=establishment.get('category'),
            disciplinary_actions=[],
            additional_data={
                'system_of_medicine': establishment.get('system_of_medicine'),
                'address': establishment.get('address'),
                'phone': establishment.get('phone'),
                'email': establishment.get('email'),
                'latitude': establishment.get('latitude'),
                'longitude': establishment.get('longitude'),
                'num_beds': establishment.get('num_beds')
            }
        )

    async def validate_license(
        self,
        license_number: str,
        region: str,
        provider_name: Optional[str] = None
    ) -> LicenseValidationResult:
        """
        Validate KPME certificate.

        Args:
            license_number: KPME certificate number
            region: District name
            provider_name: Establishment name

        Returns:
            Validation result
        """
        license_data = LicenseData(
            license_number=license_number,
            region=region,
            region_type='kpme_establishment',
            status='unknown',
            provider_name=provider_name
        )

        return await self.validate(license_data)

    async def validate(self, license_data: LicenseData) -> LicenseValidationResult:
        """
        Validate KPME certificate/license.

        Args:
            license_data: License data with certificate_number, provider_name, etc.

        Returns:
            Validation result
        """
        cert_number = license_data.license_number
        provider_name = license_data.provider_name
        region = license_data.region  # Karnataka district

        # Check cache
        if self.cache:
            cache_key = f"kpme:{cert_number}"
            cached = await self.cache.get(cache_key)
            if cached:
                return LicenseValidationResult(**cached)

        # Search KPME database
        establishment = None

        # Try exact certificate match first
        if cert_number:
            establishment = self.db.get_establishment_by_certificate(cert_number)

        # If not found by cert, try name search
        if not establishment and provider_name:
            matches = self.db.search_establishment_by_name(provider_name, limit=5)
            if matches:
                # Use best match (exact or closest)
                for match in matches:
                    if match['establishment_name'].lower() == provider_name.lower():
                        establishment = match
                        break
                if not establishment:
                    establishment = matches[0]  # Use first match

        # Build validation result
        if establishment:
            is_valid = True
            exists = True
            confidence = 0.9

            # Check certificate validity
            is_expired = False
            cert_validity = establishment.get('certificate_validity', '')
            if cert_validity:
                try:
                    # Parse validity date (format: "31 Dec 2025")
                    validity_date = datetime.strptime(cert_validity, "%d %b %Y")
                    is_expired = validity_date < datetime.now()
                    if is_expired:
                        confidence = 0.5
                except:
                    pass

            # Check district match
            if region:
                est_district = establishment.get('district', '')
                if est_district and est_district.upper() != region.upper():
                    confidence *= 0.8  # Reduce confidence if district mismatch

            # Check name match
            name_matches = None
            if provider_name and establishment.get('establishment_name'):
                name_matches = provider_name.lower() in establishment.get('establishment_name', '').lower()

            result = LicenseValidationResult(
                is_valid=is_valid and not is_expired,
                license_number=cert_number or establishment.get('certificate_number', ''),
                region=region or establishment.get('district', ''),
                region_type='kpme_establishment',
                exists=exists,
                is_active=not is_expired,
                is_expired=is_expired,
                has_disciplinary_actions=False,  # KPME doesn't provide this
                name_matches=name_matches,
                confidence=confidence,
                error=None
            )
        else:
            # Not found in KPME database
            result = LicenseValidationResult(
                is_valid=False,
                license_number=cert_number or '',
                region=region or 'Karnataka',
                region_type='kpme_establishment',
                exists=False,
                is_active=False,
                is_expired=False,
                has_disciplinary_actions=False,
                name_matches=None,
                confidence=0.0,
                error='Not found in KPME database'
            )

        # Cache result
        if self.cache:
            await self.cache.set(cache_key, result.model_dump(), ttl=604800)  # 7 days

        return result

    async def validate_multiple(self, licenses: List[Dict[str, str]]) -> List[LicenseValidationResult]:
        """
        Validate multiple KPME certificates.

        Args:
            licenses: List of dicts with keys: license_number, region, provider_name

        Returns:
            List of validation results
        """
        results = []

        for lic_dict in licenses:
            license_data = LicenseData(
                license_number=lic_dict.get('license_number', ''),
                region=lic_dict.get('region', 'Karnataka'),
                provider_name=lic_dict.get('provider_name', '')
            )

            result = await self.validate(license_data)
            results.append(result)

        return results

    def search_by_phone(self, phone: str) -> List[Dict[str, Any]]:
        """
        Search establishments by phone number.

        Args:
            phone: Phone number

        Returns:
            List of matching establishments
        """
        return self.db.get_establishment_by_phone(phone)

    def search_by_name(self, name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search establishments by name.

        Args:
            name: Establishment name
            limit: Maximum results

        Returns:
            List of matching establishments
        """
        return self.db.search_establishment_by_name(name, limit)

    def search_staff_by_registration(self, reg_no: str) -> Optional[Dict[str, Any]]:
        """
        Search staff by registration number.

        Args:
            reg_no: Medical council registration number

        Returns:
            Staff record with establishment info or None
        """
        return self.db.search_staff_by_registration(reg_no)

    def get_establishment_details(self, establishment_id: int) -> Dict[str, Any]:
        """
        Get complete establishment details.

        Args:
            establishment_id: Establishment ID

        Returns:
            Complete establishment data with staff, specialties, etc.
        """
        return self.db.get_establishment_with_staff(establishment_id)
