"""
Confidence Scorer - Calculates confidence scores from validation results (Deterministic).

KPME-focused (EL Branch):
- Aggregates confidence scores from multiple agents
- Weighted scoring based on agent importance
- Deterministic confidence calculation (no AI)
"""

from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class ConfidenceScorer:
    """
    Calculates overall confidence score from agent results.

    FULLY DETERMINISTIC - No AI/LLM calls.

    Weighting:
    - KPME Validation: 40% (most important for establishment validation)
    - Compliance: 30% (regulatory requirements)
    - Data Quality: 15% (completeness and accuracy)
    - Web Scraper: 10% (enrichment data)
    - Enrichment: 5% (additional context)
    """

    def __init__(self):
        """Initialize confidence scorer."""
        # Agent weights (must sum to 1.0)
        self.weights = {
            'kpme_validation': 0.40,
            'compliance': 0.30,
            'data_quality': 0.15,
            'web_scraper': 0.10,
            'enrichment': 0.05
        }

        logger.info(f"ConfidenceScorer initialized with weights: {self.weights}")

    def calculate_overall_confidence(
        self,
        agent_results: Dict[str, Any]
    ) -> float:
        """
        Calculate overall confidence score from agent results.

        Args:
            agent_results: Dictionary with agent results
                Expected keys:
                - kpme_confidence: float (0-1)
                - compliance_confidence: float (0-1)
                - data_quality_confidence: float (0-1)
                - web_scraper_confidence: float (0-1)
                - enrichment_confidence: float (0-1)

        Returns:
            Overall confidence score (0.0 to 1.0)
        """
        logger.info("Calculating overall confidence score...")

        # Extract individual confidences (with defaults)
        kpme_conf = agent_results.get('kpme_confidence', 0.0)
        compliance_conf = agent_results.get('compliance_confidence', 0.0)
        data_quality_conf = agent_results.get('data_quality_confidence', 0.0)
        web_scraper_conf = agent_results.get('web_scraper_confidence', 0.0)
        enrichment_conf = agent_results.get('enrichment_confidence', 0.0)

        # Calculate weighted average
        overall = (
            (kpme_conf * self.weights['kpme_validation']) +
            (compliance_conf * self.weights['compliance']) +
            (data_quality_conf * self.weights['data_quality']) +
            (web_scraper_conf * self.weights['web_scraper']) +
            (enrichment_conf * self.weights['enrichment'])
        )

        # Clamp to 0-1 range
        overall = max(0.0, min(1.0, overall))

        logger.info(
            f"Confidence scores: "
            f"KPME={kpme_conf:.2f}, "
            f"Compliance={compliance_conf:.2f}, "
            f"DataQuality={data_quality_conf:.2f}, "
            f"WebScraper={web_scraper_conf:.2f}, "
            f"Enrichment={enrichment_conf:.2f} "
            f"→ Overall={overall:.2f}"
        )

        return overall

    def classify_confidence_level(self, confidence: float) -> str:
        """
        Classify confidence score into level.

        Args:
            confidence: Confidence score (0-1)

        Returns:
            Confidence level: "high", "medium", "low", "very_low"
        """
        if confidence >= 0.85:
            return "high"
        elif confidence >= 0.70:
            return "medium"
        elif confidence >= 0.50:
            return "low"
        else:
            return "very_low"

    def get_confidence_analysis(
        self,
        agent_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get detailed confidence analysis.

        Args:
            agent_results: Dictionary with agent results

        Returns:
            Dictionary with overall confidence, level, and breakdown
        """
        overall = self.calculate_overall_confidence(agent_results)
        level = self.classify_confidence_level(overall)

        analysis = {
            "overall_confidence": overall,
            "confidence_level": level,
            "breakdown": {
                "kpme_validation": {
                    "score": agent_results.get('kpme_confidence', 0.0),
                    "weight": self.weights['kpme_validation'],
                    "contribution": agent_results.get('kpme_confidence', 0.0) * self.weights['kpme_validation']
                },
                "compliance": {
                    "score": agent_results.get('compliance_confidence', 0.0),
                    "weight": self.weights['compliance'],
                    "contribution": agent_results.get('compliance_confidence', 0.0) * self.weights['compliance']
                },
                "data_quality": {
                    "score": agent_results.get('data_quality_confidence', 0.0),
                    "weight": self.weights['data_quality'],
                    "contribution": agent_results.get('data_quality_confidence', 0.0) * self.weights['data_quality']
                },
                "web_scraper": {
                    "score": agent_results.get('web_scraper_confidence', 0.0),
                    "weight": self.weights['web_scraper'],
                    "contribution": agent_results.get('web_scraper_confidence', 0.0) * self.weights['web_scraper']
                },
                "enrichment": {
                    "score": agent_results.get('enrichment_confidence', 0.0),
                    "weight": self.weights['enrichment'],
                    "contribution": agent_results.get('enrichment_confidence', 0.0) * self.weights['enrichment']
                }
            }
        }

        return analysis


# Singleton instance
_scorer_instance: ConfidenceScorer | None = None


def get_scorer() -> ConfidenceScorer:
    """Get singleton confidence scorer instance."""
    global _scorer_instance
    if _scorer_instance is None:
        _scorer_instance = ConfidenceScorer()
    return _scorer_instance
