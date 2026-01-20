"""
Mistral AI Explainer Service

Generates human-readable explanations for validation results using Mistral AI.
Provides transparency and explainability for both full and fast validation paths.
"""

import os
import logging
from typing import Dict, Any, List
from mistralai import Mistral

logger = logging.getLogger(__name__)


class MistralExplainer:
    """
    Mistral AI service for generating validation explanations.
    
    Uses Mistral's mistral-large-latest model to provide:
    - Overall decision reasoning
    - Individual agent contribution breakdown
    - Confidence score explanations
    - Clear, human-readable explanations
    """

    def __init__(self):
        self.api_key = os.getenv("MISTRAL_API_KEY")
        if not self.api_key:
            logger.error("MISTRAL_API_KEY not configured")
            self.client = None
        else:
            self.client = Mistral(api_key=self.api_key)
            logger.info("Mistral Explainer initialized with mistral-large-latest")

    async def explain_full_validation(
        self,
        validation_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate explanation for full validation result.
        
        Args:
            validation_result: Complete validation result from API
            
        Returns:
            Dictionary with explanations
        """
        if not self.client:
            return self._fallback_explanation(validation_result, "full")

        try:
            # Extract key information
            decision = validation_result.get("decision", "unknown")
            confidence = validation_result.get("final_confidence", 0.0)
            confidence_level = validation_result.get("confidence_level", "unknown")
            validation_path = validation_result.get("validation_path", "unknown")
            reasoning = validation_result.get("reasoning", [])
            
            # Extract agent data
            kpme_data = validation_result.get("kpme_data", {})
            data_quality = validation_result.get("data_quality", {})
            compliance = validation_result.get("compliance", {})
            provider_data = validation_result.get("provider_data", {})

            # Build prompt for Mistral
            prompt = f"""You are an expert AI assistant explaining healthcare provider validation results. Your goal is to provide clear, transparent explanations of automated validation decisions.

**Validation Result:**
- Decision: {decision.upper().replace('_', ' ')}
- Overall Confidence: {confidence * 100:.1f}% ({confidence_level})
- Validation Path: {validation_path.upper()}
- Provider: {provider_data.get('establishment_name', 'Unknown')}

**Agent Results:**

1. **KPME Database Validation:**
   - Valid: {kpme_data.get('is_valid', False) if kpme_data else 'N/A'}
   - Expired: {kpme_data.get('is_expired', False) if kpme_data else 'N/A'}
   - Certificate: {kpme_data.get('certificate_number', 'N/A') if kpme_data else 'N/A'}
   - Confidence: {kpme_data.get('confidence', 0) * 100:.1f}% if kpme_data else 'N/A'

2. **Data Quality Assessment:**
   - Completeness: {data_quality.get('completeness_score', 0) * 100:.1f}% if data_quality else 'N/A'
   - Accuracy: {data_quality.get('accuracy_score', 0) * 100:.1f}% if data_quality else 'N/A'
   - Overall: {data_quality.get('overall_score', 0) * 100:.1f}% if data_quality else 'N/A'

3. **Compliance Check:**
   - Compliant: {compliance.get('is_compliant', False) if compliance else 'N/A'}
   - Checks Passed: {compliance.get('checks_passed', 0) if compliance else 'N/A'}
   - Checks Failed: {compliance.get('checks_failed', 0) if compliance else 'N/A'}

**System Reasoning:**
{chr(10).join('- ' + r for r in reasoning) if reasoning else '- No specific reasoning provided'}

**Task:**
Provide a clear, conversational explanation in the following format:

1. **Overall Decision Explanation** (2-3 sentences):
   - Why was this decision made?
   - What were the key factors?

2. **Agent Contributions Breakdown**:
   For each agent (KPME, Data Quality, Compliance), explain:
   - What it checked
   - What it found
   - How it contributed to the final confidence score
   - Use percentages and specific findings

3. **Confidence Score Breakdown** (1-2 sentences):
   - How was the {confidence * 100:.1f}% confidence calculated?
   - Which agents had the most impact?

Keep explanations:
- Clear and non-technical
- Specific to this validation
- Focused on the "why" behind the decision
- Transparent about limitations

Do NOT use markdown formatting. Use plain text with clear section labels."""

            # Call Mistral API
            response = self.client.chat.complete(
                model="mistral-large-latest",
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,  # Low temperature for consistent explanations
                max_tokens=1500
            )

            explanation_text = response.choices[0].message.content.strip()

            # Parse the explanation into sections
            sections = self._parse_explanation_sections(explanation_text)

            return {
                "overall_explanation": sections.get("overall", explanation_text),
                "agent_breakdown": sections.get("agents", []),
                "confidence_explanation": sections.get("confidence", ""),
                "full_text": explanation_text,
                "success": True
            }

        except Exception as e:
            logger.error(f"Mistral explanation failed: {e}", exc_info=True)
            return self._fallback_explanation(validation_result, "full")

    async def explain_fast_validation(
        self,
        validation_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate explanation for fast validation result.
        
        Args:
            validation_result: Fast validation result from API
            
        Returns:
            Dictionary with explanations
        """
        if not self.client:
            return self._fallback_explanation(validation_result, "fast")

        try:
            # Extract key information
            is_valid = validation_result.get("is_valid", False)
            certificate_number = validation_result.get("certificate_number", "N/A")
            establishment_name = validation_result.get("establishment_name", "N/A")
            is_expired = validation_result.get("is_expired", False)
            confidence = validation_result.get("confidence", 0.0)
            cache_hit = validation_result.get("cache_hit", False)

            # Build prompt for Mistral
            prompt = f"""You are an expert AI assistant explaining fast KPME certificate validation results.

**Validation Result:**
- Valid: {is_valid}
- Certificate Number: {certificate_number}
- Establishment: {establishment_name}
- Expired: {is_expired}
- Confidence: {confidence * 100:.1f}%
- Cache Hit: {cache_hit}

**Task:**
Provide a clear, brief explanation (3-4 sentences) covering:

1. **What was validated**: The KPME certificate lookup
2. **The result**: Whether the certificate was found and is valid
3. **Confidence reasoning**: Why the confidence is {confidence * 100:.1f}%
4. **Any concerns**: Expiry status or other issues

Keep it:
- Very concise (3-4 sentences max)
- Clear and specific to this validation
- Non-technical
- Focused on the database lookup result

Do NOT use markdown formatting. Use plain text."""

            # Call Mistral API
            response = self.client.chat.complete(
                model="mistral-large-latest",
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=500
            )

            explanation_text = response.choices[0].message.content.strip()

            return {
                "explanation": explanation_text,
                "success": True
            }

        except Exception as e:
            logger.error(f"Mistral fast explanation failed: {e}", exc_info=True)
            return self._fallback_explanation(validation_result, "fast")

    def _parse_explanation_sections(self, text: str) -> Dict[str, Any]:
        """Parse explanation text into structured sections."""
        sections = {
            "overall": "",
            "agents": [],
            "confidence": ""
        }

        try:
            # Simple parsing based on numbered sections
            lines = text.split('\n')
            current_section = None
            current_text = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Check for section headers
                if "Overall Decision" in line or line.startswith("1."):
                    if current_text and current_section:
                        sections[current_section] = '\n'.join(current_text)
                    current_section = "overall"
                    current_text = []
                elif "Agent Contributions" in line or line.startswith("2."):
                    if current_text and current_section == "overall":
                        sections["overall"] = '\n'.join(current_text)
                    current_section = "agents"
                    current_text = []
                elif "Confidence Score" in line or line.startswith("3."):
                    if current_text and current_section == "agents":
                        sections["agents"] = [{'text': '\n'.join(current_text)}]
                    current_section = "confidence"
                    current_text = []
                else:
                    current_text.append(line)

            # Add remaining text
            if current_text:
                if current_section == "overall":
                    sections["overall"] = '\n'.join(current_text)
                elif current_section == "agents":
                    sections["agents"] = [{'text': '\n'.join(current_text)}]
                elif current_section == "confidence":
                    sections["confidence"] = '\n'.join(current_text)

        except Exception as e:
            logger.warning(f"Failed to parse explanation sections: {e}")
            sections["overall"] = text

        return sections

    def _fallback_explanation(
        self,
        validation_result: Dict[str, Any],
        validation_type: str
    ) -> Dict[str, Any]:
        """Generate fallback explanation when Mistral API is unavailable."""
        
        if validation_type == "fast":
            is_valid = validation_result.get("is_valid", False)
            is_expired = validation_result.get("is_expired", False)
            
            if is_valid and not is_expired:
                explanation = "This certificate was found in the KPME Karnataka database and is currently valid. The validation was performed through a direct database lookup with high confidence."
            elif is_valid and is_expired:
                explanation = "This certificate was found in the KPME database but has expired. While the certificate exists, it is no longer active, resulting in reduced confidence."
            else:
                explanation = "This certificate was not found in the KPME Karnataka database. It may be invalid, incorrectly entered, or from a different region."
            
            return {
                "explanation": explanation,
                "success": False,
                "fallback": True
            }
        
        else:  # full validation
            decision = validation_result.get("decision", "unknown")
            confidence = validation_result.get("final_confidence", 0.0)
            
            if decision == "auto_approved":
                explanation = f"This provider was automatically approved with {confidence * 100:.1f}% confidence based on successful KPME validation, good data quality, and compliance checks."
            elif decision == "auto_rejected":
                explanation = f"This provider was automatically rejected with {confidence * 100:.1f}% confidence due to failed validation checks or missing critical information."
            else:
                explanation = f"This provider requires manual review with {confidence * 100:.1f}% confidence. The automated system could not make a definitive decision."
            
            return {
                "overall_explanation": explanation,
                "agent_breakdown": [],
                "confidence_explanation": f"Confidence was calculated by combining scores from multiple validation agents.",
                "full_text": explanation,
                "success": False,
                "fallback": True
            }


# Singleton instance
_mistral_explainer = None


def get_mistral_explainer() -> MistralExplainer:
    """Get or create Mistral Explainer singleton."""
    global _mistral_explainer
    if _mistral_explainer is None:
        _mistral_explainer = MistralExplainer()
    return _mistral_explainer
