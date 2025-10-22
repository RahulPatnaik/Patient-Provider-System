"""
Data Validation Agent - Multi-region provider validation (USA & India).
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext

from agents.base import BaseAgent, AgentName, AgentValidationError
from config.regions import Region
from services.base import (
    BaseProviderRegistry,
    BaseLicenseValidator,
    ProviderValidationResult,
    LicenseValidationResult
)


# ============================================================================
# Response Models
# ============================================================================


class DataQualityResult(BaseModel):
    """Data quality assessment result."""
    completeness_score: float = Field(ge=0.0, le=1.0)
    accuracy_score: float = Field(ge=0.0, le=1.0)
    overall_score: float = Field(ge=0.0, le=1.0)
    missing_fields: List[str]
    issues: List[str]


class DataValidatorResponse(BaseModel):
    """Complete Data Validator Agent response (region-agnostic)."""
    provider_validation: ProviderValidationResult
    license_validations: List[LicenseValidationResult]
    data_quality: DataQualityResult
    overall_confidence: float = Field(ge=0.0, le=1.0)
    is_valid: bool
    region: str
    validation_timestamp: str


# ============================================================================
# Agent Dependencies
# ============================================================================


class DataValidatorDeps(BaseModel):
    """Dependencies for Data Validator Agent."""
    provider_registry: BaseProviderRegistry
    license_validator: BaseLicenseValidator
    provider_data: Dict[str, Any]
    region: Region

    class Config:
        arbitrary_types_allowed = True


# ============================================================================
# Data Validator Agent Class
# ============================================================================


class DataValidatorAgent(BaseAgent):
    """
    Multi-region Data Validator Agent using Pydantic AI.

    Supports both USA and India provider validation through
    region-specific service implementations.

    Features:
    - Region-aware validation (NPI/NMR, State/Council licenses)
    - Pydantic AI with tool use
    - Data quality assessment
    - Confidence scoring
    - Execution time tracking
    """

    def __init__(
        self,
        region: Region,
        provider_registry: BaseProviderRegistry,
        license_validator: BaseLicenseValidator
    ):
        """
        Initialize Data Validator Agent.

        Args:
            region: Region enum (USA or INDIA)
            provider_registry: Provider registry client (NPI or NMC)
            license_validator: License validator client (State or Council)
        """
        super().__init__(AgentName.DATA_VALIDATOR)

        self.region = region
        self.provider_registry = provider_registry
        self.license_validator = license_validator

        # Get API key from environment
        api_key = self.get_env("GEMINI_API_KEY")

        # Create region-aware system prompt
        if region == Region.USA:
            identifier_name = "NPI"
            license_name = "state medical license"
        else:
            identifier_name = "NMR ID"
            license_name = "state medical council registration"

        # Create Pydantic AI agent
        self.agent = Agent(
            "gemini-2.0-flash-exp",
            deps_type=DataValidatorDeps,
            system_prompt=f"""You are a Data Validation Agent for healthcare provider verification ({region.value.upper()}).

Your responsibilities:
1. Validate {identifier_name} against the provider registry
2. Validate {license_name}s
3. Assess data quality and completeness
4. Calculate confidence scores

Use the provided tools to validate data and return structured results.
Be thorough and accurate in your validation."""
        )

        # Register tools
        self._register_tools()

        self.logger.info(f"Initialized Data Validator Agent for region: {region.value.upper()}")

    def _register_tools(self):
        """Register Pydantic AI tools."""

        @self.agent.tool
        async def validate_provider_identifier(
            ctx: RunContext[DataValidatorDeps],
            identifier: str
        ) -> Dict[str, Any]:
            """
            Validate provider identifier (NPI or NMR ID).

            Args:
                ctx: Runtime context with dependencies
                identifier: Provider identifier to validate

            Returns:
                Validation result with confidence score
            """
            try:
                result = await ctx.deps.provider_registry.validate_provider(identifier)
                return result.model_dump()
            except Exception as e:
                return {
                    "is_valid": False,
                    "identifier": identifier,
                    "identifier_type": "npi" if ctx.deps.region == Region.USA else "nmr",
                    "exists": False,
                    "is_active": False,
                    "provider_type": None,
                    "confidence": 0.0,
                    "error": str(e)
                }

        @self.agent.tool
        async def validate_licenses(
            ctx: RunContext[DataValidatorDeps],
            licenses: List[Dict[str, str]]
        ) -> List[Dict[str, Any]]:
            """
            Validate multiple licenses (state or council).

            Args:
                ctx: Runtime context with dependencies
                licenses: List of license dicts with keys: license_number, region, provider_name

            Returns:
                List of validation results with confidence scores
            """
            try:
                results = await ctx.deps.license_validator.validate_multiple(licenses)
                return [r.model_dump() for r in results]
            except Exception as e:
                return [{
                    "is_valid": False,
                    "license_number": lic.get("license_number", ""),
                    "region": lic.get("region", ""),
                    "region_type": "state" if ctx.deps.region == Region.USA else "council",
                    "exists": False,
                    "is_active": False,
                    "is_expired": False,
                    "has_disciplinary_actions": False,
                    "confidence": 0.0,
                    "error": str(e)
                } for lic in licenses]

        @self.agent.tool
        def calculate_data_quality(
            ctx: RunContext[DataValidatorDeps]
        ) -> Dict[str, Any]:
            """
            Calculate data quality metrics for provider data.

            Args:
                ctx: Runtime context with provider data

            Returns:
                Data quality assessment with scores and issues
            """
            data = ctx.deps.provider_data

            # Required fields (region-agnostic)
            required_fields = [
                "identifier",  # NPI or NMR ID
                "first_name",
                "last_name",
                "specialty",
                "address",
                "city",
                "state",
                "zip_code",
                "phone"
            ]

            # Check completeness
            missing_fields = [f for f in required_fields if not data.get(f)]
            completeness = 1.0 - (len(missing_fields) / len(required_fields))

            # Check data accuracy (basic validation)
            issues = []

            # Identifier format check
            identifier = data.get("identifier", "")
            if ctx.deps.region == Region.USA:
                # NPI validation (10 digits)
                if identifier and (len(identifier) != 10 or not identifier.isdigit()):
                    issues.append("Invalid NPI format")
            else:
                # NMR ID validation (minimum length)
                if identifier and len(identifier) < 5:
                    issues.append("Invalid NMR ID format")

            # Phone format check
            phone = data.get("phone", "")
            if phone:
                digits = phone.replace("-", "").replace(" ", "").replace("+", "")
                if len(digits) < 10:
                    issues.append("Invalid phone format")

            # Zip code format check (region-specific)
            zip_code = data.get("zip_code", "")
            if zip_code:
                if ctx.deps.region == Region.USA:
                    if len(str(zip_code)) not in [5, 9]:
                        issues.append("Invalid US zip code format")
                else:
                    if len(str(zip_code)) != 6:
                        issues.append("Invalid Indian PIN code format")

            # Calculate accuracy score
            accuracy = 1.0 - (len(issues) / 10)  # Normalize to 0-1

            # Overall quality score
            overall = (completeness * 0.6 + accuracy * 0.4)

            return {
                "completeness_score": completeness,
                "accuracy_score": accuracy,
                "overall_score": overall,
                "missing_fields": missing_fields,
                "issues": issues
            }

    async def validate(self, provider_data: Dict[str, Any]) -> DataValidatorResponse:
        """
        Validate provider data.

        Args:
            provider_data: Provider information to validate
                Required keys: identifier, first_name, last_name, region, licenses

        Returns:
            Complete validation result with confidence scores

        Raises:
            AgentValidationError: If validation fails
        """
        identifier = provider_data.get("identifier") or provider_data.get("npi") or provider_data.get("nmr_id")
        self.logger.info(f"Starting data validation for identifier: {identifier} (Region: {self.region.value.upper()})")

        async with self.track_time_async() as timer:
            try:
                # Create dependencies
                deps = DataValidatorDeps(
                    provider_registry=self.provider_registry,
                    license_validator=self.license_validator,
                    provider_data=provider_data,
                    region=self.region
                )

                # Build region-aware validation prompt
                identifier_name = "NPI" if self.region == Region.USA else "NMR ID"
                license_name = "state licenses" if self.region == Region.USA else "medical council registrations"

                prompt = f"""Validate this provider data ({self.region.value.upper()}):

{identifier_name}: {identifier}
Name: {provider_data.get('first_name')} {provider_data.get('last_name')}
Specialty: {provider_data.get('specialty')}
Region: {provider_data.get('region') or provider_data.get('state')}
Licenses: {provider_data.get('licenses', [])}

Use the tools to:
1. Validate the provider identifier ({identifier_name})
2. Validate all {license_name}
3. Calculate data quality

Return a complete validation result with region={self.region.value} and validation_timestamp={datetime.utcnow().isoformat()}Z."""

                # Run agent
                result = await self.agent.run(prompt, deps=deps)

                execution_time = timer.get("execution_time_ms", 0)
                self.logger.info(
                    f"Data validation completed in {execution_time}ms. "
                    f"Confidence: {result.data.overall_confidence:.2f} "
                    f"Valid: {result.data.is_valid}"
                )

                return result.data

            except Exception as e:
                self.logger.error(f"Data validation failed: {str(e)}")
                raise AgentValidationError(f"Data validation failed: {str(e)}")

    def calculate_confidence(
        self,
        provider_result: ProviderValidationResult,
        license_results: List[LicenseValidationResult],
        data_quality: DataQualityResult
    ) -> float:
        """
        Calculate overall confidence score.

        Weights:
        - Provider validation: 40%
        - License validation: 40%
        - Data quality: 20%

        Args:
            provider_result: Provider validation result
            license_results: List of license validation results
            data_quality: Data quality assessment

        Returns:
            Overall confidence score (0.0-1.0)
        """
        # Provider confidence (40%)
        provider_confidence = provider_result.confidence * 0.4

        # License confidence (40%)
        if license_results:
            avg_license_confidence = sum(r.confidence for r in license_results) / len(license_results)
            license_confidence = avg_license_confidence * 0.4
        else:
            license_confidence = 0.0

        # Data quality (20%)
        quality_confidence = data_quality.overall_score * 0.2

        overall = provider_confidence + license_confidence + quality_confidence

        return min(1.0, max(0.0, overall))  # Clamp to 0-1
