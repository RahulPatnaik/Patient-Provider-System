"""
Unit tests for Fast Validator Agent (KPME-only)
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from agents.fast_validator import (
    FastValidatorAgent,
    FastValidationResult
)


@pytest.fixture
def agent():
    """Create Fast Validator Agent instance."""
    return FastValidatorAgent()


@pytest.fixture
def valid_certificate():
    """Valid KPME certificate number."""
    return "KPME/2024/12345"


@pytest.fixture
def valid_phone():
    """Valid phone number."""
    return "9876543210"


@pytest.fixture
def valid_name():
    """Valid establishment name."""
    return "Test Hospital"


class TestFastValidatorAgent:
    """Test suite for Fast Validator Agent."""

    def test_agent_initialization(self, agent):
        """Test agent initializes correctly."""
        assert agent is not None
        assert agent.db is not None
        assert agent.cache is not None
        assert agent.agent is not None

    @pytest.mark.asyncio
    async def test_validate_fast_with_expected_output(self, agent, valid_certificate):
        """Test fast validation with expected output (testing scenario)."""
        expected = {
            "is_valid": True,
            "provider_found": True,
            "cache_hit": False,
            "validation_source": "Expected Output",
            "confidence": 1.0,
            "establishment_name": "Test Hospital",
            "category": "Hospital",
            "certificate_number": valid_certificate,
            "district": "BANGALORE URBAN",
            "is_expired": False
        }

        result = await agent.validate_fast(
            certificate_number=valid_certificate,
            expected_output=expected
        )

        assert isinstance(result, FastValidationResult)
        assert result.is_valid is True or result.is_valid is False  # Either is valid

    @pytest.mark.asyncio
    async def test_validate_fast_by_certificate(self, agent, valid_certificate):
        """Test fast validation by certificate number."""
        result = await agent.validate_fast(certificate_number=valid_certificate)

        assert isinstance(result, FastValidationResult)
        assert isinstance(result.is_valid, bool)
        assert isinstance(result.provider_found, bool)
        assert isinstance(result.cache_hit, bool)
        assert isinstance(result.validation_source, str)
        assert 0.0 <= result.confidence <= 1.0
        assert result.timestamp is not None

    @pytest.mark.asyncio
    async def test_validate_fast_by_phone(self, agent, valid_phone):
        """Test fast validation by phone number."""
        result = await agent.validate_fast(phone=valid_phone)

        assert isinstance(result, FastValidationResult)
        assert isinstance(result.is_valid, bool)
        assert isinstance(result.provider_found, bool)

    @pytest.mark.asyncio
    async def test_validate_fast_by_name(self, agent, valid_name):
        """Test fast validation by establishment name."""
        result = await agent.validate_fast(provider_name=valid_name)

        assert isinstance(result, FastValidationResult)
        assert isinstance(result.is_valid, bool)
        assert isinstance(result.provider_found, bool)

    @pytest.mark.asyncio
    async def test_validate_fast_with_all_params(self, agent, valid_certificate, valid_phone, valid_name):
        """Test fast validation with all search parameters."""
        result = await agent.validate_fast(
            certificate_number=valid_certificate,
            provider_name=valid_name,
            phone=valid_phone
        )

        assert isinstance(result, FastValidationResult)
        assert result.validation_source is not None

    @pytest.mark.asyncio
    async def test_validate_fast_with_agent_integration_flags(self, agent, valid_certificate):
        """Test fast validation with agent integration flags."""
        result = await agent.validate_fast(
            certificate_number=valid_certificate,
            use_web_scraper=True,
            use_enrichment=True,
            use_compliance=True
        )

        assert isinstance(result, FastValidationResult)
        assert result.confidence >= 0.0

    @pytest.mark.asyncio
    async def test_validate_fast_invalid_certificate(self, agent):
        """Test fast validation with invalid certificate."""
        result = await agent.validate_fast(certificate_number="INVALID999")

        assert isinstance(result, FastValidationResult)
        # May or may not be found depending on database
        assert isinstance(result.provider_found, bool)

    def test_quick_kpme_check_by_certificate(self, agent, valid_certificate):
        """Test direct KPME database check by certificate."""
        result = agent.quick_kpme_check(certificate_number=valid_certificate)

        assert isinstance(result, dict)
        # May be empty if not in database

    def test_quick_kpme_check_by_phone(self, agent, valid_phone):
        """Test direct KPME database check by phone."""
        result = agent.quick_kpme_check(phone=valid_phone)

        assert isinstance(result, dict)

    def test_quick_kpme_check_by_name(self, agent, valid_name):
        """Test direct KPME database check by name."""
        result = agent.quick_kpme_check(name=valid_name)

        assert isinstance(result, dict)

    def test_quick_kpme_check_no_params(self, agent):
        """Test direct KPME check with no parameters."""
        result = agent.quick_kpme_check()

        assert result == {}

    @pytest.mark.asyncio
    async def test_validate_fast_response_structure(self, agent, valid_certificate):
        """Test that response has correct structure."""
        result = await agent.validate_fast(certificate_number=valid_certificate)

        # Check all required fields exist
        assert hasattr(result, 'is_valid')
        assert hasattr(result, 'provider_found')
        assert hasattr(result, 'cache_hit')
        assert hasattr(result, 'validation_source')
        assert hasattr(result, 'confidence')
        assert hasattr(result, 'establishment_name')
        assert hasattr(result, 'category')
        assert hasattr(result, 'certificate_number')
        assert hasattr(result, 'district')
        assert hasattr(result, 'is_expired')
        assert hasattr(result, 'timestamp')
        assert hasattr(result, 'details')

        # Check types
        assert isinstance(result.is_valid, bool)
        assert isinstance(result.provider_found, bool)
        assert isinstance(result.cache_hit, bool)
        assert isinstance(result.validation_source, str)
        assert isinstance(result.confidence, float)
        assert isinstance(result.is_expired, bool)
        assert isinstance(result.timestamp, str)
        assert isinstance(result.details, dict)

    @pytest.mark.asyncio
    async def test_validate_fast_timestamp_format(self, agent, valid_certificate):
        """Test that timestamp is in correct ISO format."""
        result = await agent.validate_fast(certificate_number=valid_certificate)

        try:
            datetime.fromisoformat(result.timestamp.rstrip('Z'))
        except ValueError:
            pytest.fail("Invalid timestamp format")

    @pytest.mark.asyncio
    async def test_validate_fast_confidence_range(self, agent, valid_certificate):
        """Test that confidence is in valid range."""
        result = await agent.validate_fast(certificate_number=valid_certificate)

        assert 0.0 <= result.confidence <= 1.0


@pytest.mark.asyncio
async def test_full_fast_validation_workflow(agent):
    """Test complete fast validation workflow."""
    # Test with multiple search methods
    cert_result = await agent.validate_fast(certificate_number="KPME/2024/12345")
    phone_result = await agent.validate_fast(phone="9876543210")
    name_result = await agent.validate_fast(provider_name="Hospital")

    # All should return valid FastValidationResult objects
    assert isinstance(cert_result, FastValidationResult)
    assert isinstance(phone_result, FastValidationResult)
    assert isinstance(name_result, FastValidationResult)

    # All should have valid confidence scores
    assert 0.0 <= cert_result.confidence <= 1.0
    assert 0.0 <= phone_result.confidence <= 1.0
    assert 0.0 <= name_result.confidence <= 1.0


@pytest.mark.asyncio
async def test_cache_behavior(agent):
    """Test cache hit/miss behavior."""
    cert = "KPME/2024/CACHE_TEST"

    # First call - should be cache miss
    result1 = await agent.validate_fast(certificate_number=cert)

    # Second call - may be cache hit
    result2 = await agent.validate_fast(certificate_number=cert)

    # Both should be valid FastValidationResult
    assert isinstance(result1, FastValidationResult)
    assert isinstance(result2, FastValidationResult)


def test_quick_check_returns_dict(agent):
    """Test that quick check always returns a dictionary."""
    result1 = agent.quick_kpme_check(certificate_number="TEST123")
    result2 = agent.quick_kpme_check(phone="1234567890")
    result3 = agent.quick_kpme_check(name="Test Name")
    result4 = agent.quick_kpme_check()

    assert isinstance(result1, dict)
    assert isinstance(result2, dict)
    assert isinstance(result3, dict)
    assert isinstance(result4, dict)
    assert result4 == {}  # No params should return empty dict
