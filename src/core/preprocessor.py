"""
Data Preprocessor - Cleans and normalizes provider input data (Deterministic).

KPME-focused (EL Branch):
- Deterministic data cleaning (no AI)
- Phone number normalization
- Address standardization
- Field validation and formatting
"""

import re
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DataPreprocessor:
    """
    Preprocessor for KPME provider data.

    FULLY DETERMINISTIC - No AI/LLM calls.
    """

    @staticmethod
    def normalize_phone(phone: Optional[str]) -> Optional[str]:
        """
        Normalize phone number to standard format.

        Args:
            phone: Raw phone number string

        Returns:
            Normalized phone number or None
        """
        if not phone:
            return None

        # Convert to string and remove all non-numeric characters
        cleaned = str(phone).replace(" ", "").replace("-", "").replace("+", "") \
                          .replace("(", "").replace(")", "").replace(".", "")

        # Remove country code if present
        if cleaned.startswith("91") and len(cleaned) == 12:
            cleaned = cleaned[2:]

        # Validate length (10 digits for Indian numbers)
        if len(cleaned) < 10:
            return None

        # Return first 10 digits
        return cleaned[:10]

    @staticmethod
    def normalize_email(email: Optional[str]) -> Optional[str]:
        """
        Normalize email address.

        Args:
            email: Raw email string

        Returns:
            Normalized email or None
        """
        if not email:
            return None

        email = str(email).strip().lower()

        # Basic email validation
        if "@" not in email or "." not in email:
            return None

        return email

    @staticmethod
    def normalize_certificate_number(cert: Optional[str]) -> Optional[str]:
        """
        Normalize KPME certificate number.

        Args:
            cert: Raw certificate number

        Returns:
            Normalized certificate number or None
        """
        if not cert:
            return None

        # Remove whitespace and convert to uppercase
        cert = str(cert).strip().upper()

        # Validate format: starts with letters, contains numbers
        if not re.match(r"^[A-Z]{2,4}\d+", cert):
            return None

        return cert

    @staticmethod
    def normalize_district(district: Optional[str]) -> Optional[str]:
        """
        Normalize district name to uppercase.

        Args:
            district: Raw district name

        Returns:
            Normalized district name or None
        """
        if not district:
            return None

        return str(district).strip().upper()

    @staticmethod
    def normalize_pin_code(pin_code: Optional[str]) -> Optional[str]:
        """
        Normalize PIN code.

        Args:
            pin_code: Raw PIN code

        Returns:
            Normalized PIN code or None
        """
        if not pin_code:
            return None

        # Remove all non-numeric characters
        cleaned = re.sub(r'\D', '', str(pin_code))

        # Validate length (6 digits for Indian PIN codes)
        if len(cleaned) != 6:
            return None

        return cleaned

    @staticmethod
    def normalize_establishment_name(name: Optional[str]) -> Optional[str]:
        """
        Normalize establishment name.

        Args:
            name: Raw establishment name

        Returns:
            Normalized name or None
        """
        if not name:
            return None

        # Strip whitespace and normalize case
        name = str(name).strip()

        # Remove extra spaces
        name = re.sub(r'\s+', ' ', name)

        return name if len(name) >= 2 else None

    @staticmethod
    def normalize_address(address: Optional[str]) -> Optional[str]:
        """
        Normalize address string.

        Args:
            address: Raw address

        Returns:
            Normalized address or None
        """
        if not address:
            return None

        # Strip whitespace
        address = str(address).strip()

        # Remove extra spaces
        address = re.sub(r'\s+', ' ', address)

        return address if len(address) >= 5 else None

    @staticmethod
    def normalize_num_beds(num_beds: Any) -> Optional[int]:
        """
        Normalize number of beds.

        Args:
            num_beds: Raw beds value

        Returns:
            Normalized integer or None
        """
        if num_beds is None:
            return None

        try:
            beds = int(float(num_beds))
            return beds if beds >= 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_coordinates(lat: Any, lon: Any) -> tuple[Optional[float], Optional[float]]:
        """
        Normalize GPS coordinates.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Tuple of (normalized_lat, normalized_lon)
        """
        try:
            lat_norm = float(lat) if lat else None
            lon_norm = float(lon) if lon else None

            # Validate ranges
            if lat_norm and (lat_norm < -90 or lat_norm > 90):
                lat_norm = None
            if lon_norm and (lon_norm < -180 or lon_norm > 180):
                lon_norm = None

            return lat_norm, lon_norm
        except (ValueError, TypeError):
            return None, None

    def preprocess(self, provider_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preprocess provider data (deterministic cleaning and normalization).

        Args:
            provider_data: Raw provider data dictionary

        Returns:
            Preprocessed and normalized data dictionary
        """
        logger.info("Starting data preprocessing (deterministic)")

        preprocessed = provider_data.copy()

        # Normalize establishment name
        if 'establishment_name' in preprocessed:
            preprocessed['establishment_name'] = self.normalize_establishment_name(
                preprocessed.get('establishment_name')
            )

        # Normalize certificate number
        if 'certificate_number' in preprocessed:
            preprocessed['certificate_number'] = self.normalize_certificate_number(
                preprocessed.get('certificate_number')
            )

        # Normalize phone
        if 'phone' in preprocessed:
            preprocessed['phone'] = self.normalize_phone(
                preprocessed.get('phone')
            )

        # Normalize email
        if 'email' in preprocessed:
            preprocessed['email'] = self.normalize_email(
                preprocessed.get('email')
            )

        # Normalize address
        if 'address' in preprocessed:
            preprocessed['address'] = self.normalize_address(
                preprocessed.get('address')
            )

        # Normalize district
        if 'district' in preprocessed:
            preprocessed['district'] = self.normalize_district(
                preprocessed.get('district')
            )

        # Normalize PIN code
        if 'pin_code' in preprocessed:
            preprocessed['pin_code'] = self.normalize_pin_code(
                preprocessed.get('pin_code')
            )

        # Normalize number of beds
        if 'num_beds' in preprocessed:
            preprocessed['num_beds'] = self.normalize_num_beds(
                preprocessed.get('num_beds')
            )

        # Normalize GPS coordinates
        if 'latitude' in preprocessed or 'longitude' in preprocessed:
            lat, lon = self.normalize_coordinates(
                preprocessed.get('latitude'),
                preprocessed.get('longitude')
            )
            preprocessed['latitude'] = lat
            preprocessed['longitude'] = lon

        # Normalize system of medicine (uppercase)
        if 'system_of_medicine' in preprocessed and preprocessed['system_of_medicine']:
            preprocessed['system_of_medicine'] = str(preprocessed['system_of_medicine']).strip()

        logger.info("Data preprocessing completed successfully")

        return preprocessed

    def validate_minimal_requirements(self, provider_data: Dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate minimal data requirements for processing.

        Args:
            provider_data: Provider data dictionary

        Returns:
            Tuple of (is_valid, list_of_missing_fields)
        """
        required_fields = ['establishment_name']
        optional_but_recommended = ['certificate_number', 'phone', 'district']

        missing = []
        warnings = []

        # Check required fields
        for field in required_fields:
            if not provider_data.get(field):
                missing.append(field)

        # Check recommended fields
        for field in optional_but_recommended:
            if not provider_data.get(field):
                warnings.append(field)

        is_valid = len(missing) == 0

        if warnings:
            logger.warning(f"Missing recommended fields: {', '.join(warnings)}")

        return is_valid, missing


# Singleton instance
_preprocessor_instance: Optional[DataPreprocessor] = None


def get_preprocessor() -> DataPreprocessor:
    """Get singleton preprocessor instance."""
    global _preprocessor_instance
    if _preprocessor_instance is None:
        _preprocessor_instance = DataPreprocessor()
    return _preprocessor_instance
