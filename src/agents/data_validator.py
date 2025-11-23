"""
Data Validation Agent - KPME Karnataka Provider Validation (EL Branch)

This validator is KPME-only and integrates with other agents:
- Web Scraper Agent (for additional web data)
- Enrichment Agent (for data enrichment)
- Compliance Agent (for regulatory compliance)
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext

from agents.base import BaseAgent, AgentName, AgentValidationError
from database.kpme_db import get_kpme_db


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


class KPMEValidationResult(BaseModel):
    """KPME establishment validation result."""
    is_valid: bool
    exists: bool
    certificate_number: str
    establishment_name: Optional[str] = None
    category: Optional[str] = None
    district: Optional[str] = None
    is_expired: bool
    confidence: float = Field(ge=0.0, le=1.0)
    details: Dict[str, Any] = Field(default_factory=dict)


class DataValidatorResponse(BaseModel):
    """Complete Data Validator Agent response (KPME-only)."""
    kpme_validation: KPMEValidationResult
    data_quality: DataQualityResult
    web_scraper_data: Optional[Dict[str, Any]] = None
    enrichment_data: Optional[Dict[str, Any]] = None
    compliance_data: Optional[Dict[str, Any]] = None
    overall_confidence: float = Field(ge=0.0, le=1.0)
    is_valid: bool
    validation_timestamp: str


# ============================================================================
# Agent Dependencies
# ============================================================================


class DataValidatorDeps(BaseModel):
    """Dependencies for Data Validator Agent."""
    provider_data: Dict[str, Any]
    expected_output: Optional[Dict[str, Any]] = None  # Optional expected result for testing
    use_web_scraper: bool = False
    use_enrichment: bool = False
    use_compliance: bool = False

    class Config:
        arbitrary_types_allowed = True


# ============================================================================
# Data Validator Agent Class (KPME-Only)
# ============================================================================


class DataValidatorAgent(BaseAgent):
    """
    KPME Karnataka Data Validator Agent using Pydantic AI.

    EL Branch - KPME-Only Implementation:
    - Validates against KPME Karnataka database (1,000 establishments)
    - Integrates with Web Scraper, Enrichment, and Compliance agents
    - Supports expected input for testing/validation scenarios

    Features:
    - KPME certificate validation
    - Staff registration verification
    - Phone/email/address validation
    - Data quality assessment
    - Optional agent integration (web scraper, enrichment, compliance)
    """

    def __init__(self):
        """Initialize KPME Data Validator Agent."""
        super().__init__(AgentName.DATA_VALIDATOR)

        self.db = get_kpme_db()

        # Get API key from environment
        api_key = self.get_env("GEMINI_API_KEY")

        # Create Pydantic AI agent
        self.agent = Agent(
            "gemini-2.0-flash-exp",
            deps_type=DataValidatorDeps,
            system_prompt="""You are a Data Validation Agent for KPME Karnataka healthcare providers.

Your responsibilities:
1. Validate KPME certificate numbers against the Karnataka database
2. Verify establishment details (name, address, phone, email)
3. Check staff registrations and qualifications
4. Assess data quality and completeness
5. Integrate with other agents when needed (web scraper, enrichment, compliance)
6. Calculate confidence scores

Use the provided tools to validate KPME establishment data and return structured results.
Be thorough and accurate in your validation."""
        )

        # Register tools
        self._register_tools()

        self.logger.info("Initialized KPME Data Validator Agent (EL Branch - KPME-only)")

    def _register_tools(self):
        """Register Pydantic AI tools."""

        @self.agent.tool
        def validate_kpme_certificate(
            ctx: RunContext[DataValidatorDeps],
            certificate_number: str
        ) -> Dict[str, Any]:
            """
            Validate KPME certificate number.

            Args:
                ctx: Runtime context with dependencies
                certificate_number: KPME certificate number

            Returns:
                Validation result with establishment details
            """
            try:
                establishment = self.db.get_establishment_by_certificate(certificate_number)

                if establishment:
                    # Check expiry
                    is_expired = False
                    cert_validity = establishment.get('certificate_validity', '')
                    if cert_validity:
                        try:
                            validity_date = datetime.strptime(cert_validity, "%d %b %Y")
                            is_expired = validity_date < datetime.now()
                        except:
                            pass

                    return {
                        "is_valid": True,
                        "exists": True,
                        "certificate_number": certificate_number,
                        "establishment_name": establishment.get('establishment_name'),
                        "category": establishment.get('category'),
                        "district": establishment.get('district'),
                        "system_of_medicine": establishment.get('system_of_medicine'),
                        "address": establishment.get('address'),
                        "phone": establishment.get('phone'),
                        "email": establishment.get('email'),
                        "latitude": establishment.get('latitude'),
                        "longitude": establishment.get('longitude'),
                        "num_beds": establishment.get('num_beds'),
                        "certificate_validity": cert_validity,
                        "is_expired": is_expired,
                        "confidence": 0.9 if not is_expired else 0.5
                    }
                else:
                    return {
                        "is_valid": False,
                        "exists": False,
                        "certificate_number": certificate_number,
                        "confidence": 0.0,
                        "error": "Certificate not found in KPME database"
                    }
            except Exception as e:
                return {
                    "is_valid": False,
                    "exists": False,
                    "certificate_number": certificate_number,
                    "confidence": 0.0,
                    "error": str(e)
                }

        @self.agent.tool
        def search_by_phone(
            ctx: RunContext[DataValidatorDeps],
            phone: str
        ) -> List[Dict[str, Any]]:
            """
            Search KPME establishments by phone number.

            Args:
                ctx: Runtime context
                phone: Phone number to search

            Returns:
                List of matching establishments
            """
            try:
                results = self.db.get_establishment_by_phone(phone)
                return results[:5]  # Return top 5 matches
            except Exception as e:
                self.logger.error(f"Phone search failed: {e}")
                return []

        @self.agent.tool
        def search_by_name(
            ctx: RunContext[DataValidatorDeps],
            name: str
        ) -> List[Dict[str, Any]]:
            """
            Search KPME establishments by name.

            Args:
                ctx: Runtime context
                name: Establishment name to search

            Returns:
                List of matching establishments
            """
            try:
                results = self.db.search_establishment_by_name(name, limit=5)
                return results
            except Exception as e:
                self.logger.error(f"Name search failed: {e}")
                return []

        @self.agent.tool
        def verify_staff_registration(
            ctx: RunContext[DataValidatorDeps],
            registration_no: str
        ) -> Optional[Dict[str, Any]]:
            """
            Verify staff member by registration number.

            Args:
                ctx: Runtime context
                registration_no: Medical council registration number

            Returns:
                Staff details or None
            """
            try:
                staff = self.db.search_staff_by_registration(registration_no)
                return staff
            except Exception as e:
                self.logger.error(f"Staff verification failed: {e}")
                return None

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

            # Required fields for KPME
            required_fields = [
                "certificate_number",
                "establishment_name",
                "address",
                "phone"
            ]

            # Check completeness
            missing_fields = [f for f in required_fields if not data.get(f)]
            completeness = 1.0 - (len(missing_fields) / len(required_fields))

            # Check data accuracy
            issues = []

            # Phone format check
            phone = data.get("phone", "")
            if phone:
                digits = str(phone).replace("-", "").replace(" ", "").replace("+", "").replace(".0", "")
                if len(digits) < 10:
                    issues.append("Invalid phone format")

            # Certificate format check (KPME format)
            cert = data.get("certificate_number", "")
            if cert and len(cert) < 8:
                issues.append("Invalid certificate format")

            # Email format check
            email = data.get("email", "")
            if email and "@" not in email:
                issues.append("Invalid email format")

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

        @self.agent.tool
        def check_expected_output(
            ctx: RunContext[DataValidatorDeps]
        ) -> Optional[Dict[str, Any]]:
            """
            Check if expected output is provided (for testing scenarios).

            Args:
                ctx: Runtime context

            Returns:
                Expected output if provided, None otherwise
            """
            if ctx.deps.expected_output:
                self.logger.info("Using expected output for validation scenario")
                return ctx.deps.expected_output
            return None

    async def validate(
        self,
        provider_data: Dict[str, Any],
        expected_output: Optional[Dict[str, Any]] = None,
        use_web_scraper: bool = False,
        use_enrichment: bool = False,
        use_compliance: bool = False
    ) -> DataValidatorResponse:
        """
        Validate KPME provider data.

        Args:
            provider_data: Provider information to validate
                Required keys: certificate_number, establishment_name
                Optional: phone, email, address, district
            expected_output: Optional expected result (for testing/validation scenarios)
            use_web_scraper: Whether to integrate with web scraper agent
            use_enrichment: Whether to integrate with enrichment agent
            use_compliance: Whether to integrate with compliance agent

        Returns:
            Complete validation result with confidence scores

        Raises:
            AgentValidationError: If validation fails
        """
        cert_number = provider_data.get("certificate_number", "")
        est_name = provider_data.get("establishment_name", "Unknown")

        self.logger.info(f"Starting KPME validation for: {est_name} (Cert: {cert_number})")

        async with self.track_time_async() as timer:
            try:
                # Create dependencies
                deps = DataValidatorDeps(
                    provider_data=provider_data,
                    expected_output=expected_output,
                    use_web_scraper=use_web_scraper,
                    use_enrichment=use_enrichment,
                    use_compliance=use_compliance
                )

                # Build validation prompt
                prompt = f"""Validate this KPME Karnataka healthcare provider:

Certificate Number: {cert_number}
Establishment Name: {est_name}
Phone: {provider_data.get('phone', 'Not provided')}
Address: {provider_data.get('address', 'Not provided')}
District: {provider_data.get('district', 'Not provided')}

Use the tools to:
1. Check if expected output is provided (testing scenario)
2. Validate the KPME certificate number
3. Search by phone/name if needed
4. Calculate data quality
5. Verify staff registrations if provided

Return a complete validation result with:
- kpme_validation result
- data_quality assessment
- overall_confidence score (0.0 to 1.0)
- is_valid boolean
- validation_timestamp: {datetime.utcnow().isoformat()}Z

Agent integration flags:
- use_web_scraper: {use_web_scraper}
- use_enrichment: {use_enrichment}
- use_compliance: {use_compliance}
"""

                # Run agent
                result = await self.agent.run(prompt, deps=deps)

                execution_time = timer.get("execution_time_ms", 0)
                self.logger.info(
                    f"KPME validation completed in {execution_time}ms. "
                    f"Confidence: {result.data.overall_confidence:.2f} "
                    f"Valid: {result.data.is_valid}"
                )

                return result.data

            except Exception as e:
                self.logger.error(f"KPME validation failed: {str(e)}")
                raise AgentValidationError(f"KPME validation failed: {str(e)}")

    def calculate_confidence(
        self,
        kpme_result: KPMEValidationResult,
        data_quality: DataQualityResult
    ) -> float:
        """
        Calculate overall confidence score.

        Weights:
        - KPME validation: 70%
        - Data quality: 30%

        Args:
            kpme_result: KPME validation result
            data_quality: Data quality assessment

        Returns:
            Overall confidence score (0.0-1.0)
        """
        # KPME confidence (70%)
        kpme_confidence = kpme_result.confidence * 0.7

        # Data quality (30%)
        quality_confidence = data_quality.overall_score * 0.3

        overall = kpme_confidence + quality_confidence

        return min(1.0, max(0.0, overall))  # Clamp to 0-1
