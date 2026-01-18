"""
Final Decision Logic - Makes final decision on provider validation (Deterministic).

KPME-focused (EL Branch):
- Threshold-based decision making (no AI Router for deterministic mode)
- Auto-approve, manual review, or auto-reject
- Decision reasoning generation
"""

from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


class DecisionEngine:
    """
    Makes final validation decision based on confidence scores and validation results.

    FULLY DETERMINISTIC - No AI/LLM calls.

    Decision Thresholds:
    - Auto-Approve: confidence >= 0.85 AND all critical validations passed
    - Auto-Reject: confidence < 0.40 OR critical validation failed
    - Manual Review: everything in between
    """

    def __init__(
        self,
        auto_approve_threshold: float = 0.85,
        auto_reject_threshold: float = 0.40
    ):
        """
        Initialize decision engine.

        Args:
            auto_approve_threshold: Minimum confidence for auto-approval (0-1)
            auto_reject_threshold: Maximum confidence for auto-rejection (0-1)
        """
        self.auto_approve_threshold = auto_approve_threshold
        self.auto_reject_threshold = auto_reject_threshold

        logger.info(
            f"DecisionEngine initialized: "
            f"auto_approve>={auto_approve_threshold}, "
            f"auto_reject<{auto_reject_threshold}"
        )

    def check_critical_failures(
        self,
        validation_results: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Check for critical validation failures.

        Args:
            validation_results: Validation results dictionary

        Returns:
            Tuple of (has_critical_failure, list_of_failures)
        """
        failures = []

        # Check KPME validation
        if not validation_results.get('kpme_validated', False):
            failures.append("KPME certificate not validated")

        # Check if certificate is expired
        if validation_results.get('certificate_expired', False):
            failures.append("Certificate expired")

        # Check compliance
        compliance_data = validation_results.get('compliance_data', {})
        if isinstance(compliance_data, dict):
            decision = compliance_data.get('decision', '')
            if decision == 'REJECT':
                failures.append("Compliance check failed (regulatory rejection)")

        # Check data quality
        if not validation_results.get('data_quality_passed', False):
            data_quality_conf = validation_results.get('data_quality_confidence', 0.0)
            if data_quality_conf < 0.3:
                failures.append("Data quality too low (critical fields missing)")

        has_failure = len(failures) > 0

        if has_failure:
            logger.warning(f"Critical failures detected: {failures}")

        return has_failure, failures

    def generate_reasoning(
        self,
        decision: str,
        confidence: float,
        validation_results: Dict[str, Any],
        critical_failures: List[str]
    ) -> List[str]:
        """
        Generate reasoning for decision.

        Args:
            decision: Final decision ("auto_approved", "manual_review", "auto_rejected")
            confidence: Overall confidence score
            validation_results: Validation results
            critical_failures: List of critical failures

        Returns:
            List of reasoning statements
        """
        reasoning = []

        if decision == "auto_approved":
            reasoning.append(f"High confidence score ({confidence:.0%})")

            if validation_results.get('kpme_validated'):
                reasoning.append("KPME certificate successfully validated")

            if validation_results.get('compliance_passed'):
                reasoning.append("Passed all compliance checks")

            if validation_results.get('data_quality_passed'):
                reasoning.append("Data quality meets standards")

            reasoning.append("All critical validations passed - safe for auto-approval")

        elif decision == "auto_rejected":
            reasoning.append(f"Low confidence score ({confidence:.0%})")

            if critical_failures:
                for failure in critical_failures:
                    reasoning.append(f"Critical failure: {failure}")

            reasoning.append("Failed critical validations - automatic rejection")

        else:  # manual_review
            reasoning.append(f"Moderate confidence score ({confidence:.0%})")

            if critical_failures:
                for failure in critical_failures:
                    reasoning.append(f"Issue detected: {failure}")
            else:
                reasoning.append("No critical failures, but confidence not high enough for auto-approval")

            reasoning.append("Requires human review for final decision")

        return reasoning

    def make_decision(
        self,
        confidence: float,
        validation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Make final validation decision.

        Args:
            confidence: Overall confidence score (0-1)
            validation_results: Validation results from all agents

        Returns:
            Dictionary with decision, reasoning, and metadata
        """
        logger.info(f"Making final decision (confidence={confidence:.2f})...")

        # Check for critical failures
        has_critical_failure, critical_failures = self.check_critical_failures(validation_results)

        # Determine decision
        if has_critical_failure or confidence < self.auto_reject_threshold:
            decision = "auto_rejected"
            logger.info("Decision: AUTO-REJECT (critical failure or low confidence)")

        elif confidence >= self.auto_approve_threshold:
            decision = "auto_approved"
            logger.info("Decision: AUTO-APPROVE (high confidence, no critical failures)")

        else:
            decision = "manual_review"
            logger.info("Decision: MANUAL REVIEW (moderate confidence)")

        # Generate reasoning
        reasoning = self.generate_reasoning(
            decision,
            confidence,
            validation_results,
            critical_failures
        )

        result = {
            "decision": decision,
            "confidence": confidence,
            "reasoning": reasoning,
            "critical_failures": critical_failures,
            "has_critical_failure": has_critical_failure,
            "thresholds": {
                "auto_approve": self.auto_approve_threshold,
                "auto_reject": self.auto_reject_threshold
            }
        }

        logger.info(
            f"Final decision: {decision.upper()} "
            f"(confidence={confidence:.0%}, "
            f"critical_failures={len(critical_failures)})"
        )

        return result


# Singleton instance
_decision_engine_instance: DecisionEngine | None = None


def get_decision_engine() -> DecisionEngine:
    """Get singleton decision engine instance."""
    global _decision_engine_instance
    if _decision_engine_instance is None:
        _decision_engine_instance = DecisionEngine()
    return _decision_engine_instance
