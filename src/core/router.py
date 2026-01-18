"""
Router Decision Logic - Determines validation path (simple vs complex).

KPME-focused (EL Branch):
- Deterministic routing logic (no AI)
- Analyzes data completeness and complexity
- Routes to simple path (Fast Validator) or complex path (full validation)
"""

from typing import Dict, Any, Literal
import logging

logger = logging.getLogger(__name__)


PathType = Literal["simple", "complex"]


class ValidationRouter:
    """
    Router for KPME provider validation.

    FULLY DETERMINISTIC - No AI/LLM calls.

    Routing Logic:
    - Simple path: Certificate number provided + no red flags → Fast Validator
    - Complex path: Missing data, expired certificate, or special cases → Full validation
    """

    def __init__(
        self,
        min_completeness_for_simple: float = 0.6,
        require_certificate_for_simple: bool = True
    ):
        """
        Initialize router with configuration.

        Args:
            min_completeness_for_simple: Minimum data completeness for simple path (0-1)
            require_certificate_for_simple: Whether certificate is required for simple path
        """
        self.min_completeness_for_simple = min_completeness_for_simple
        self.require_certificate_for_simple = require_certificate_for_simple

        logger.info(
            f"ValidationRouter initialized: "
            f"min_completeness={min_completeness_for_simple}, "
            f"require_certificate={require_certificate_for_simple}"
        )

    def calculate_completeness(self, provider_data: Dict[str, Any]) -> float:
        """
        Calculate data completeness score (deterministic).

        Args:
            provider_data: Provider data dictionary

        Returns:
            Completeness score (0.0 to 1.0)
        """
        # Key fields for KPME providers
        key_fields = [
            'establishment_name',
            'certificate_number',
            'phone',
            'address',
            'district',
        ]

        # Additional fields
        additional_fields = [
            'email',
            'category',
            'system_of_medicine',
            'latitude',
            'longitude',
            'num_beds'
        ]

        # Count present key fields (weighted more)
        key_present = sum(1 for field in key_fields if provider_data.get(field))
        key_score = key_present / len(key_fields) if key_fields else 0

        # Count present additional fields
        additional_present = sum(1 for field in additional_fields if provider_data.get(field))
        additional_score = additional_present / len(additional_fields) if additional_fields else 0

        # Weighted completeness (key fields 70%, additional 30%)
        completeness = (key_score * 0.7) + (additional_score * 0.3)

        logger.debug(
            f"Data completeness: {completeness:.2f} "
            f"(key: {key_present}/{len(key_fields)}, "
            f"additional: {additional_present}/{len(additional_fields)})"
        )

        return completeness

    def detect_red_flags(self, provider_data: Dict[str, Any]) -> list[str]:
        """
        Detect red flags that require complex validation (deterministic).

        Args:
            provider_data: Provider data dictionary

        Returns:
            List of red flag descriptions
        """
        red_flags = []

        # Missing critical information
        if not provider_data.get('establishment_name'):
            red_flags.append("Missing establishment name")

        # Phone number issues
        phone = provider_data.get('phone')
        if phone and len(str(phone)) < 10:
            red_flags.append("Invalid phone number format")

        # Email issues
        email = provider_data.get('email')
        if email and '@' not in str(email):
            red_flags.append("Invalid email format")

        # Num beds issues
        num_beds = provider_data.get('num_beds')
        if num_beds is not None:
            try:
                beds = int(float(num_beds))
                if beds < 0:
                    red_flags.append("Invalid number of beds (negative)")
                elif beds > 2000:
                    red_flags.append("Suspiciously high number of beds (> 2000)")
            except (ValueError, TypeError):
                red_flags.append("Invalid number of beds format")

        # GPS coordinate issues
        lat = provider_data.get('latitude')
        lon = provider_data.get('longitude')
        if lat or lon:
            try:
                if lat and (float(lat) < -90 or float(lat) > 90):
                    red_flags.append("Invalid latitude (out of range)")
                if lon and (float(lon) < -180 or float(lon) > 180):
                    red_flags.append("Invalid longitude (out of range)")
            except (ValueError, TypeError):
                red_flags.append("Invalid GPS coordinates format")

        if red_flags:
            logger.warning(f"Detected {len(red_flags)} red flags: {red_flags}")

        return red_flags

    def determine_path(self, provider_data: Dict[str, Any]) -> tuple[PathType, str]:
        """
        Determine validation path based on data analysis (deterministic).

        Args:
            provider_data: Preprocessed provider data

        Returns:
            Tuple of (path_type, reasoning)
        """
        logger.info("Determining validation path...")

        # Check for certificate number (required for simple path)
        has_certificate = bool(provider_data.get('certificate_number'))

        if self.require_certificate_for_simple and not has_certificate:
            reason = "Missing certificate number - routing to complex path for thorough validation"
            logger.info(f"Path: COMPLEX - {reason}")
            return "complex", reason

        # Calculate data completeness
        completeness = self.calculate_completeness(provider_data)

        if completeness < self.min_completeness_for_simple:
            reason = (
                f"Low data completeness ({completeness:.0%}) - "
                f"routing to complex path for data enrichment"
            )
            logger.info(f"Path: COMPLEX - {reason}")
            return "complex", reason

        # Check for red flags
        red_flags = self.detect_red_flags(provider_data)

        if red_flags:
            reason = f"Detected {len(red_flags)} red flags - routing to complex path for verification"
            logger.info(f"Path: COMPLEX - {reason}")
            return "complex", reason

        # All checks passed - use simple path
        reason = (
            f"High data completeness ({completeness:.0%}), "
            f"valid certificate, no red flags - routing to fast validator"
        )
        logger.info(f"Path: SIMPLE - {reason}")
        return "simple", reason

    def should_use_web_scraper(self, provider_data: Dict[str, Any]) -> bool:
        """
        Determine if web scraper should be used (deterministic).

        Args:
            provider_data: Provider data

        Returns:
            True if web scraper should be used
        """
        # Use web scraper if:
        # 1. Missing address or GPS coordinates
        # 2. Website provided but no verification data
        # 3. Phone/email missing

        missing_address = not provider_data.get('address')
        missing_gps = not (provider_data.get('latitude') and provider_data.get('longitude'))
        has_website = bool(provider_data.get('website'))
        missing_contact = not (provider_data.get('phone') or provider_data.get('email'))

        use_scraper = missing_address or missing_gps or (has_website) or missing_contact

        logger.debug(f"Web scraper needed: {use_scraper}")
        return use_scraper

    def should_use_enrichment(self, provider_data: Dict[str, Any]) -> bool:
        """
        Determine if enrichment agent should be used (deterministic).

        Args:
            provider_data: Provider data

        Returns:
            True if enrichment should be used
        """
        # Use enrichment if:
        # 1. Missing specialty information
        # 2. Missing system of medicine
        # 3. Missing category
        # 4. Low data completeness

        completeness = self.calculate_completeness(provider_data)

        missing_specialty = not provider_data.get('specialties')
        missing_system = not provider_data.get('system_of_medicine')
        missing_category = not provider_data.get('category')
        low_completeness = completeness < 0.7

        use_enrichment = missing_specialty or missing_system or missing_category or low_completeness

        logger.debug(f"Enrichment needed: {use_enrichment}")
        return use_enrichment

    def get_routing_decision(
        self,
        provider_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get complete routing decision with recommendations.

        Args:
            provider_data: Preprocessed provider data

        Returns:
            Dictionary with path, reasoning, and agent recommendations
        """
        path, reasoning = self.determine_path(provider_data)
        completeness = self.calculate_completeness(provider_data)
        red_flags = self.detect_red_flags(provider_data)

        # For complex path, determine which agents to use
        use_web_scraper = False
        use_enrichment = False
        use_compliance = True  # Always use compliance for complex path

        if path == "complex":
            use_web_scraper = self.should_use_web_scraper(provider_data)
            use_enrichment = self.should_use_enrichment(provider_data)

        decision = {
            "path": path,
            "reasoning": reasoning,
            "completeness": completeness,
            "red_flags": red_flags,
            "red_flag_count": len(red_flags),
            "agent_recommendations": {
                "use_fast_validator": path == "simple",
                "use_data_validator": path == "complex",
                "use_web_scraper": use_web_scraper,
                "use_enrichment": use_enrichment,
                "use_compliance": use_compliance
            }
        }

        logger.info(
            f"Routing decision: {path.upper()} path, "
            f"completeness={completeness:.0%}, "
            f"red_flags={len(red_flags)}"
        )

        return decision


# Singleton instance
_router_instance: ValidationRouter | None = None


def get_router() -> ValidationRouter:
    """Get singleton router instance."""
    global _router_instance
    if _router_instance is None:
        _router_instance = ValidationRouter()
    return _router_instance
