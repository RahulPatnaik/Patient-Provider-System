"""
Fast Validator Agent - Quick validation for simple cases (India KPME support)

Handles:
- Cache checking
- Quick KPME database lookups (India)
- NPI quick verify (USA)
- Fast-path validation logic
"""

from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext

from agents.base import BaseAgent, AgentName, AgentValidationError
from config.regions import Region
from cache.factory import get_cache_instance


# ============================================================================
# Response Models
# ============================================================================


class FastValidationResult(BaseModel):
    """Fast validation result."""
    is_valid: bool
    provider_found: bool
    cache_hit: bool
    validation_source: str
    confidence: float = Field(ge=0.0, le=1.0)
    provider_name: Optional[str] = None
    provider_type: Optional[str] = None
    region: str
    timestamp: str
    details: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# Agent Dependencies
# ============================================================================


class FastValidatorDeps(BaseModel):
    """Dependencies for Fast Validator Agent."""
    region: Region
    identifier: str  # NPI, NMR ID, or KPME certificate number
    provider_name: Optional[str] = None
    phone: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


# ============================================================================
# Fast Validator Agent
# ============================================================================


class FastValidatorAgent(BaseAgent):
    """
    Fast Validator Agent - Quick validation path.

    Features:
    - Cache-first validation
    - KPME database quick lookup (India - Karnataka)
    - NPI quick verify (USA)
    - Minimal API calls
    - Sub-second response times
    """

    def __init__(self, region: Region):
        """
        Initialize Fast Validator Agent.

        Args:
            region: Region enum (USA or INDIA)
        """
        super().__init__(AgentName.FAST_VALIDATOR)

        self.region = region
        self.cache = get_cache_instance()

        # Get API key
        api_key = self.get_env("GEMINI_API_KEY")

        # Create Pydantic AI agent
        self.agent = Agent(
            "gemini-2.0-flash-exp",
            deps_type=FastValidatorDeps,
            system_prompt=f"""You are a Fast Validator Agent for {region.value.upper()} healthcare providers.

Your goal is SPEED - provide quick validation results using:
1. Cache lookups (fastest)
2. Local database queries (KPME for Karnataka, India)
3. Minimal external API calls

Be fast and decisive. Return structured validation results."""
        )

        # Register tools
        self._register_tools()

        self.logger.info(f"Initialized Fast Validator Agent for region: {region.value.upper()}")

    def _register_tools(self):
        """Register Pydantic AI tools."""

        @self.agent.tool
        async def check_cache(
            ctx: RunContext[FastValidatorDeps]
        ) -> Optional[Dict[str, Any]]:
            """
            Check cache for previous validation.

            Args:
                ctx: Runtime context

            Returns:
                Cached result or None
            """
            try:
                cache_key = f"fast_val:{ctx.deps.region.value}:{ctx.deps.identifier}"
                cached = await self.cache.get(cache_key)

                if cached:
                    self.logger.info(f"Cache hit for {ctx.deps.identifier}")
                    return cached

                self.logger.info(f"Cache miss for {ctx.deps.identifier}")
                return None
            except Exception as e:
                self.logger.warning(f"Cache check failed: {e}")
                return None

        @self.agent.tool
        async def quick_kpme_lookup(
            ctx: RunContext[FastValidatorDeps]
        ) -> Optional[Dict[str, Any]]:
            """
            Quick KPME database lookup for India/Karnataka providers.

            Args:
                ctx: Runtime context

            Returns:
                KPME establishment data or None
            """
            if ctx.deps.region != Region.INDIA:
                return None

            try:
                from services.india.kpme_validator import KPMEValidator

                validator = KPMEValidator()

                # Try certificate number first
                if ctx.deps.identifier:
                    from database.kpme_db import get_kpme_db
                    db = get_kpme_db()
                    est = db.get_establishment_by_certificate(ctx.deps.identifier)

                    if est:
                        self.logger.info(f"Found KPME establishment by cert: {ctx.deps.identifier}")
                        return {
                            'found': True,
                            'source': 'KPME Database',
                            'establishment_name': est.get('establishment_name'),
                            'category': est.get('category'),
                            'certificate_number': est.get('certificate_number'),
                            'certificate_validity': est.get('certificate_validity'),
                            'district': est.get('district'),
                            'phone': est.get('phone'),
                            'email': est.get('email'),
                            'address': est.get('address'),
                            'system_of_medicine': est.get('system_of_medicine')
                        }

                # Try phone number
                if ctx.deps.phone:
                    from database.kpme_db import get_kpme_db
                    db = get_kpme_db()
                    matches = db.get_establishment_by_phone(ctx.deps.phone)

                    if matches:
                        est = matches[0]
                        self.logger.info(f"Found KPME establishment by phone: {ctx.deps.phone}")
                        return {
                            'found': True,
                            'source': 'KPME Database (Phone Match)',
                            'establishment_name': est.get('establishment_name'),
                            'category': est.get('category'),
                            'certificate_number': est.get('certificate_number'),
                            'district': est.get('district'),
                            'phone': est.get('phone')
                        }

                # Try name
                if ctx.deps.provider_name:
                    from database.kpme_db import get_kpme_db
                    db = get_kpme_db()
                    matches = db.search_establishment_by_name(ctx.deps.provider_name, limit=1)

                    if matches:
                        est = matches[0]
                        self.logger.info(f"Found KPME establishment by name: {ctx.deps.provider_name}")
                        return {
                            'found': True,
                            'source': 'KPME Database (Name Match)',
                            'establishment_name': est.get('establishment_name'),
                            'category': est.get('category'),
                            'certificate_number': est.get('certificate_number'),
                            'district': est.get('district')
                        }

                self.logger.info("No KPME match found")
                return None

            except Exception as e:
                self.logger.error(f"KPME lookup failed: {e}")
                return None

        @self.agent.tool
        async def save_to_cache(
            ctx: RunContext[FastValidatorDeps],
            result: Dict[str, Any]
        ) -> bool:
            """
            Save validation result to cache.

            Args:
                ctx: Runtime context
                result: Validation result to cache

            Returns:
                True if saved successfully
            """
            try:
                cache_key = f"fast_val:{ctx.deps.region.value}:{ctx.deps.identifier}"
                await self.cache.set(cache_key, result, ttl=86400)  # 24 hours
                self.logger.info(f"Cached result for {ctx.deps.identifier}")
                return True
            except Exception as e:
                self.logger.warning(f"Cache save failed: {e}")
                return False

    async def validate_fast(
        self,
        identifier: str,
        provider_name: Optional[str] = None,
        phone: Optional[str] = None
    ) -> FastValidationResult:
        """
        Perform fast validation.

        Args:
            identifier: Provider identifier (NPI, NMR ID, or KPME cert)
            provider_name: Provider/establishment name (optional)
            phone: Phone number (optional)

        Returns:
            Fast validation result

        Raises:
            AgentValidationError: If validation fails
        """
        self.logger.info(f"Starting fast validation for: {identifier} (Region: {self.region.value.upper()})")

        async with self.track_time_async() as timer:
            try:
                # Create dependencies
                deps = FastValidatorDeps(
                    region=self.region,
                    identifier=identifier,
                    provider_name=provider_name,
                    phone=phone
                )

                # Build prompt
                prompt = f"""Perform FAST validation for provider ({self.region.value.upper()}):

Identifier: {identifier}
Name: {provider_name or 'Unknown'}
Phone: {phone or 'Unknown'}

Steps:
1. Check cache first (fastest)
2. If cache miss, do quick database lookup (KPME for India/Karnataka)
3. Return validation result with confidence score

Be FAST - prefer cached/local results over external API calls."""

                # Run agent
                result = await self.agent.run(prompt, deps=deps)

                execution_time = timer.get("execution_time_ms", 0)
                self.logger.info(
                    f"Fast validation completed in {execution_time}ms. "
                    f"Valid: {result.data.is_valid}"
                )

                return result.data

            except Exception as e:
                self.logger.error(f"Fast validation failed: {str(e)}")
                raise AgentValidationError(f"Fast validation failed: {str(e)}")

    async def quick_kpme_check(
        self,
        certificate_number: Optional[str] = None,
        phone: Optional[str] = None,
        name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Direct KPME database check (bypasses agent).

        Args:
            certificate_number: KPME certificate number
            phone: Phone number
            name: Establishment name

        Returns:
            Dictionary with match details or empty dict
        """
        if self.region != Region.INDIA:
            return {}

        try:
            from database.kpme_db import get_kpme_db
            db = get_kpme_db()

            # Try certificate
            if certificate_number:
                est = db.get_establishment_by_certificate(certificate_number)
                if est:
                    return est

            # Try phone
            if phone:
                matches = db.get_establishment_by_phone(phone)
                if matches:
                    return matches[0]

            # Try name
            if name:
                matches = db.search_establishment_by_name(name, limit=1)
                if matches:
                    return matches[0]

            return {}

        except Exception as e:
            self.logger.error(f"KPME quick check failed: {e}")
            return {}
