"""
Unit tests for Data Validator Agent (KPME-only)
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from agents.data_validator import (
    DataValidatorAgent,
    DataValidatorResponse,
    KPMEValidationResult,
    DataQualityResult
)


@pytest.fixture
def agent():
    """Create Data Validator Agent instance."""
    return DataValidatorAgent()


@pytest.fixture
def valid_provider_data():
    """Valid KPME provider data."""
    return {
        "certificate_number": "KPME/2024/12345",
        "establishment_name": "Test Hospital",
        "address": "123 Test Street, Bangalore",
        "phone": "9876543210",
        "email": "test@hospital.com",
        "district": "BANGALORE URBAN"
    }


@pytest.fixture
def incomplete_provider_data():
    """Incomplete provider data (missing fields)."""
    return {
        "certificate_number": "KPME/2024/99999",
        "establishment_name": "Incomplete Hospital"
        # Missing address, phone
    }


class TestDataValidatorAgent:
    """Test suite for Data Validator Agent."""

    def test_agent_initialization(self, agent):
        """Test agent initializes correctly."""
        assert agent is not None
        assert agent.db is not None
        assert agent.agent is not None

    @pytest.mark.asyncio
    async def test_validate_with_expected_output(self, agent, valid_provider_data):
        """Test validation with expected output (testing scenario)."""
        expected = {
            "kpme_validation": {
                "is_valid": True,
                "exists": True,
                "certificate_number": "KPME/2024/12345",
                "establishment_name": "Test Hospital",
                "category": "Hospital",
                "district": "BANGALORE URBAN",
                "is_expired": False,
                "confidence": 0.9,
                "details": {}
            },
            "data_quality": {
                "completeness_score": 1.0,
                "accuracy_score": 1.0,
                "overall_score": 1.0,
                "missing_fields": [],
                "issues": []
            },
            "overall_confidence": 0.95,
            "is_valid": True
        }

        result = await agent.validate(
            provider_data=valid_provider_data,
            expected_output=expected
        )

        assert isinstance(result, DataValidatorResponse)
        assert result.is_valid is True
        assert result.overall_confidence > 0.0

    @pytest.mark.asyncio
    async def test_validate_complete_data(self, agent, valid_provider_data):
        """Test validation with complete provider data."""
        result = await agent.validate(provider_data=valid_provider_data)

        assert isinstance(result, DataValidatorResponse)
        assert isinstance(result.kpme_validation, KPMEValidationResult)
        assert isinstance(result.data_quality, DataQualityResult)
        assert result.validation_timestamp is not None

    @pytest.mark.asyncio
    async def test_validate_incomplete_data(self, agent, incomplete_provider_data):
        """Test validation with incomplete data."""
        result = await agent.validate(provider_data=incomplete_provider_data)

        assert isinstance(result, DataValidatorResponse)
        assert result.data_quality.completeness_score < 1.0
        assert len(result.data_quality.missing_fields) > 0

    @pytest.mark.asyncio
    async def test_validate_with_agent_integration_flags(self, agent, valid_provider_data):
        """Test validation with agent integration flags."""
        result = await agent.validate(
            provider_data=valid_provider_data,
            use_web_scraper=True,
            use_enrichment=True,
            use_compliance=True
        )

        assert isinstance(result, DataValidatorResponse)
        # Agent integration data will be None unless actually integrated
        assert result.web_scraper_data is None or isinstance(result.web_scraper_data, dict)
        assert result.enrichment_data is None or isinstance(result.enrichment_data, dict)
        assert result.compliance_data is None or isinstance(result.compliance_data, dict)

    @pytest.mark.asyncio
    async def test_validate_invalid_certificate(self, agent):
        """Test validation with invalid certificate number."""
        invalid_data = {
            "certificate_number": "INVALID123",
            "establishment_name": "Test",
            "address": "Test Address",
            "phone": "1234567890"
        }

        result = await agent.validate(provider_data=invalid_data)

        assert isinstance(result, DataValidatorResponse)
        # May or may not be valid depending on KPME database
        assert result.kpme_validation.certificate_number == "INVALID123"

    def test_calculate_confidence(self, agent):
        """Test confidence calculation."""
        kpme_result = KPMEValidationResult(
            is_valid=True,
            exists=True,
            certificate_number="TEST123",
            is_expired=False,
            confidence=0.9
        )

        data_quality = DataQualityResult(
            completeness_score=1.0,
            accuracy_score=1.0,
            overall_score=1.0,
            missing_fields=[],
            issues=[]
        )

        confidence = agent.calculate_confidence(kpme_result, data_quality)

        assert 0.0 <= confidence <= 1.0
        # KPME 70% + Quality 30% = 0.9*0.7 + 1.0*0.3 = 0.63 + 0.3 = 0.93
        assert confidence == pytest.approx(0.93, rel=0.01)

    def test_calculate_confidence_low_quality(self, agent):
        """Test confidence with low data quality."""
        kpme_result = KPMEValidationResult(
            is_valid=True,
            exists=True,
            certificate_number="TEST123",
            is_expired=False,
            confidence=0.9
        )

        data_quality = DataQualityResult(
            completeness_score=0.5,
            accuracy_score=0.6,
            overall_score=0.54,  # 0.5*0.6 + 0.6*0.4
            missing_fields=["phone", "email"],
            issues=["Invalid phone format"]
        )

        confidence = agent.calculate_confidence(kpme_result, data_quality)

        assert 0.0 <= confidence <= 1.0
        # KPME 70% + Quality 30% = 0.9*0.7 + 0.54*0.3 = 0.63 + 0.162 = 0.792
        assert confidence < 0.93  # Lower than perfect quality


@pytest.mark.asyncio
async def test_full_validation_workflow(agent, valid_provider_data):
    """Test complete validation workflow."""
    result = await agent.validate(
        provider_data=valid_provider_data,
        use_web_scraper=False,
        use_enrichment=False,
        use_compliance=False
    )

    # Validate response structure
    assert isinstance(result, DataValidatorResponse)
    assert hasattr(result, 'kpme_validation')
    assert hasattr(result, 'data_quality')
    assert hasattr(result, 'overall_confidence')
    assert hasattr(result, 'is_valid')
    assert hasattr(result, 'validation_timestamp')

    # Validate timestamp format
    try:
        datetime.fromisoformat(result.validation_timestamp.rstrip('Z'))
    except ValueError:
        pytest.fail("Invalid timestamp format")

    # Validate confidence range
    assert 0.0 <= result.overall_confidence <= 1.0

    # Validate KPME result
    assert isinstance(result.kpme_validation.confidence, float)
    assert 0.0 <= result.kpme_validation.confidence <= 1.0

    # Validate data quality
    assert 0.0 <= result.data_quality.completeness_score <= 1.0
    assert 0.0 <= result.data_quality.accuracy_score <= 1.0
    assert 0.0 <= result.data_quality.overall_score <= 1.0
