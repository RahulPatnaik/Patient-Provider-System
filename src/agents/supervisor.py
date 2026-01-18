"""
Supervisor Agent - Central orchestrator for the KPME validation system.

Coordinates the entire validation workflow:
1. Data preprocessing
2. Path routing (simple vs complex)
3. Agent execution (Fast Validator OR full validation with all agents)
4. Confidence scoring
5. Final decision making
"""

from typing import Dict, Any
import asyncio
import logging
from datetime import datetime

from agents.base import BaseAgent, AgentName
from agents.fast_validator import FastValidatorAgent
from agents.data_validator import DataValidatorAgent
from agents.web_scraper import get_web_scraper
from agents.enrichment import get_enrichment
from core.preprocessor import get_preprocessor
from core.router import get_router
from core.orchestrator import get_orchestrator
from core.scorer import get_scorer
from core.decision import get_decision_engine
from agents.compliance import check_compliance
from models.provider import ProviderInputModel, EnrichedProviderModel, ProviderOutputModel, ValidationStatus, Decision

logger = logging.getLogger(__name__)


class SupervisorAgent(BaseAgent):
    """
    Supervisor Agent - Main orchestrator for KPME provider validation.

    Workflow:
    1. Preprocess input data
    2. Route to simple or complex path
    3. Execute validation agents
    4. Score confidence
    5. Make final decision
    6. Return complete validation result
    """

    def __init__(self):
        """Initialize Supervisor Agent."""
        super().__init__(AgentName.SUPERVISOR)

        # Initialize components
        self.preprocessor = get_preprocessor()
        self.router = get_router()
        self.orchestrator = get_orchestrator()
        self.scorer = get_scorer()
        self.decision_engine = get_decision_engine()

        # Initialize agents
        self.fast_validator = FastValidatorAgent()
        self.data_validator = DataValidatorAgent()
        self.web_scraper = get_web_scraper()
        self.enrichment = get_enrichment()

        self.logger.info("Supervisor Agent initialized (EL Branch - KPME Validation)")

    async def validate_provider(
        self,
        provider_data: Dict[str, Any]
    ) -> ProviderOutputModel:
        """
        Main validation workflow.

        Args:
            provider_data: Provider data to validate (dictionary or ProviderInputModel)

        Returns:
            Complete validation result (ProviderOutputModel)
        """
        self.logger.info("="*80)
        self.logger.info("STARTING PROVIDER VALIDATION")
        self.logger.info("="*80)

        start_time = datetime.utcnow()

        try:
            # STEP 1: Preprocess data
            self.logger.info("STEP 1: Preprocessing data...")
            preprocessed = self.preprocessor.preprocess(provider_data)

            # Validate minimal requirements
            is_valid, missing = self.preprocessor.validate_minimal_requirements(preprocessed)
            if not is_valid:
                self.logger.error(f"Missing required fields: {missing}")
                return self._create_error_result(
                    provider_data,
                    f"Missing required fields: {', '.join(missing)}"
                )

            # STEP 2: Route to validation path
            self.logger.info("STEP 2: Determining validation path...")
            routing_decision = self.router.get_routing_decision(preprocessed)
            path = routing_decision['path']
            recommendations = routing_decision['agent_recommendations']

            self.logger.info(f"Selected path: {path.upper()}")
            self.logger.info(f"Routing reasoning: {routing_decision['reasoning']}")

            # STEP 3: Execute validation based on path
            if path == "simple":
                result = await self._execute_simple_path(preprocessed)
            else:
                result = await self._execute_complex_path(preprocessed, recommendations)

            # STEP 4: Calculate overall confidence
            self.logger.info("STEP 4: Calculating overall confidence...")
            confidence_analysis = self.scorer.get_confidence_analysis(result['validation_results'])
            overall_confidence = confidence_analysis['overall_confidence']

            # STEP 5: Make final decision
            self.logger.info("STEP 5: Making final decision...")
            decision_result = self.decision_engine.make_decision(
                overall_confidence,
                result['validation_results']
            )

            # STEP 6: Build final output
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            self.logger.info(f"Validation completed in {execution_time:.2f}s")

            output = self._build_output_model(
                provider_data,
                preprocessed,
                result,
                confidence_analysis,
                decision_result,
                path
            )

            self.logger.info("="*80)
            self.logger.info(f"VALIDATION COMPLETE: {decision_result['decision'].upper()}")
            self.logger.info(f"Confidence: {overall_confidence:.0%}")
            self.logger.info("="*80)

            return output

        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}", exc_info=True)
            return self._create_error_result(provider_data, str(e))

    async def _execute_simple_path(self, provider_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute simple validation path (Fast Validator only)."""
        self.logger.info("EXECUTING SIMPLE PATH (Fast Validator)")

        # Run fast validator
        fast_result = await self.fast_validator.validate_fast(
            certificate_number=provider_data.get('certificate_number'),
            provider_name=provider_data.get('establishment_name'),
            phone=provider_data.get('phone')
        )

        # Build validation results
        validation_results = {
            'kpme_validated': fast_result.is_valid and fast_result.provider_found,
            'certificate_valid': fast_result.is_valid,
            'certificate_expired': fast_result.is_expired,
            'data_quality_passed': True,  # Assumed for simple path
            'compliance_passed': not fast_result.is_expired,  # Simple compliance check

            'kpme_confidence': fast_result.confidence,
            'data_quality_confidence': 0.8 if fast_result.is_valid else 0.3,
            'compliance_confidence': 0.9 if not fast_result.is_expired else 0.4,
            'web_scraper_confidence': 0.0,  # Not used in simple path
            'enrichment_confidence': 0.0,  # Not used in simple path

            'kpme_data': fast_result.model_dump(),
            'compliance_data': {'decision': 'PASS' if not fast_result.is_expired else 'REVIEW'},
            'web_scraper_data': None,
            'enrichment_data': None
        }

        return {
            'path': 'simple',
            'validation_results': validation_results
        }

    async def _execute_complex_path(
        self,
        provider_data: Dict[str, Any],
        recommendations: Dict[str, bool]
    ) -> Dict[str, Any]:
        """Execute complex validation path (all agents in parallel)."""
        self.logger.info("EXECUTING COMPLEX PATH (Full Validation)")

        # Prepare tasks for parallel execution
        tasks = []

        # Always run data validator
        tasks.append({
            'name': 'Data Validator',
            'func': self.data_validator.validate,
            'kwargs': {
                'provider_data': provider_data,
                'use_web_scraper': recommendations['use_web_scraper'],
                'use_enrichment': recommendations['use_enrichment'],
                'use_compliance': False  # We'll run compliance separately
            }
        })

        # Run compliance (wrap in async function)
        async def run_compliance():
            return check_compliance(provider_data)

        tasks.append({
            'name': 'Compliance',
            'func': run_compliance,
            'kwargs': {}
        })

        # Optional: Web scraper (if recommended)
        if recommendations['use_web_scraper']:
            async def run_web_scraper():
                return self.web_scraper.scrape(provider_data)

            tasks.append({
                'name': 'Web Scraper',
                'func': run_web_scraper,
                'kwargs': {}
            })

        # Optional: Enrichment (if recommended)
        if recommendations['use_enrichment']:
            async def run_enrichment():
                return self.enrichment.enrich(provider_data)

            tasks.append({
                'name': 'Enrichment',
                'func': run_enrichment,
                'kwargs': {}
            })

        # Execute all tasks in parallel
        parallel_results = await self.orchestrator.run_parallel(tasks)

        # Extract results
        data_validator_result = parallel_results['results'].get('Data Validator', {}).get('result')
        compliance_result = parallel_results['results'].get('Compliance', {}).get('result')
        web_scraper_result = parallel_results['results'].get('Web Scraper', {}).get('result')
        enrichment_result = parallel_results['results'].get('Enrichment', {}).get('result')

        # Build validation results
        validation_results = {
            'kpme_validated': False,
            'certificate_valid': False,
            'certificate_expired': False,
            'data_quality_passed': False,
            'compliance_passed': False,

            'kpme_confidence': 0.0,
            'data_quality_confidence': 0.0,
            'compliance_confidence': 0.0,
            'web_scraper_confidence': 0.0,
            'enrichment_confidence': 0.0,

            'kpme_data': None,
            'compliance_data': compliance_result,
            'web_scraper_data': web_scraper_result,
            'enrichment_data': enrichment_result
        }

        # Extract data validator results
        if data_validator_result:
            validation_results['kpme_validated'] = data_validator_result.kpme_validation.is_valid
            validation_results['certificate_valid'] = data_validator_result.kpme_validation.is_valid
            validation_results['certificate_expired'] = data_validator_result.kpme_validation.is_expired
            validation_results['data_quality_passed'] = data_validator_result.data_quality.overall_score >= 0.6
            validation_results['kpme_confidence'] = data_validator_result.kpme_validation.confidence
            validation_results['data_quality_confidence'] = data_validator_result.data_quality.overall_score
            validation_results['kpme_data'] = data_validator_result.kpme_validation.model_dump()

        # Extract compliance results
        if compliance_result:
            comp_decision = compliance_result.get('decision', 'REJECT')
            validation_results['compliance_passed'] = comp_decision in ['PASS', 'REVIEW']
            validation_results['compliance_confidence'] = compliance_result.get('readiness_score', 0.0)

        # Extract web scraper confidence
        if web_scraper_result:
            validation_results['web_scraper_confidence'] = web_scraper_result.get('confidence', 0.0)

        # Extract enrichment confidence
        if enrichment_result:
            validation_results['enrichment_confidence'] = enrichment_result.get('confidence', 0.0)

        return {
            'path': 'complex',
            'validation_results': validation_results
        }

    def _build_output_model(
        self,
        original_data: Dict[str, Any],
        preprocessed_data: Dict[str, Any],
        validation_result: Dict[str, Any],
        confidence_analysis: Dict[str, Any],
        decision_result: Dict[str, Any],
        path: str
    ) -> ProviderOutputModel:
        """Build final output model."""

        # Create provider input model
        provider_input = ProviderInputModel(**preprocessed_data)

        # Create enriched model
        enriched = EnrichedProviderModel(
            provider_input=provider_input,
            **validation_result['validation_results'],
            overall_confidence=confidence_analysis['overall_confidence'],
            validation_path=path,
            data_sources=["KPME Database", "Compliance Agent"]
        )

        # Map decision string to Decision enum
        decision_map = {
            'auto_approved': Decision.AUTO_APPROVED,
            'manual_review': Decision.MANUAL_REVIEW,
            'auto_rejected': Decision.AUTO_REJECTED
        }

        # Create output model
        output = ProviderOutputModel(
            provider_data=enriched,
            validation_status=ValidationStatus.VALIDATED,
            decision=decision_map.get(decision_result['decision'], Decision.MANUAL_REVIEW),
            decision_reasoning=decision_result['reasoning'],
            final_confidence=confidence_analysis['overall_confidence'],
            issues=[],
            warnings=[]
        )

        return output

    def _create_error_result(
        self,
        provider_data: Dict[str, Any],
        error_message: str
    ) -> ProviderOutputModel:
        """Create error result."""
        provider_input = ProviderInputModel(
            establishment_name=provider_data.get('establishment_name', 'Unknown'),
            **{k: v for k, v in provider_data.items() if k != 'establishment_name'}
        )

        enriched = EnrichedProviderModel(
            provider_input=provider_input,
            overall_confidence=0.0
        )

        return ProviderOutputModel(
            provider_data=enriched,
            validation_status=ValidationStatus.FAILED,
            decision=Decision.AUTO_REJECTED,
            decision_reasoning=[error_message],
            final_confidence=0.0
        )


# Singleton instance
_supervisor_instance: SupervisorAgent | None = None


def get_supervisor() -> SupervisorAgent:
    """Get singleton supervisor instance."""
    global _supervisor_instance
    if _supervisor_instance is None:
        _supervisor_instance = SupervisorAgent()
    return _supervisor_instance
