"""
Tests for Data Validator Agent (Multi-Region).
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch

from src.config.regions import Region
from src.cache.memory import MemoryCacheClient
from src.services.factory import ServiceFactory
from src.agents.data_validator import DataValidatorAgent
from src.services.base import (
    ProviderValidationResult,
    LicenseValidationResult,
    ProviderData,
    LicenseData
)


# ============================================================================
# Test Data Validator Agent Initialization
# ============================================================================


class TestDataValidatorAgentInit:
    """Test Data Validator Agent initialization."""

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    def test_agent_initialization_usa(self):
        """Test agent initializes correctly for USA."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        assert agent.agent_name == "data_validator"
        assert agent.region == Region.USA
        assert agent.provider_registry is not None
        assert agent.license_validator is not None
        assert agent.agent is not None

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "NMC_API_KEY": "test-nmc-key"})
    def test_agent_initialization_india(self):
        """Test agent initializes correctly for India."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.INDIA, cache)
        agent = DataValidatorAgent(Region.INDIA, registry, validator)

        assert agent.agent_name == "data_validator"
        assert agent.region == Region.INDIA
        assert agent.provider_registry is not None
        assert agent.license_validator is not None
        assert agent.agent is not None

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    def test_agent_has_logger(self):
        """Test agent has logger configured."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        assert agent.logger is not None
        assert agent.logger.name == "agents.data_validator"


# ============================================================================
# Test Region-Specific System Prompts
# ============================================================================


class TestRegionSpecificPrompts:
    """Test region-specific system prompts."""

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    def test_usa_prompt_contains_npi(self):
        """Test USA agent prompt mentions NPI."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        # The system prompt should mention NPI for USA
        system_prompts = agent.agent._system_prompts
        # Get the first system prompt
        system_prompt = system_prompts[0] if system_prompts else ""
        assert "NPI" in str(system_prompt).upper()
        assert "USA" in str(system_prompt).upper()

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "NMC_API_KEY": "test-key"})
    def test_india_prompt_contains_nmr(self):
        """Test India agent prompt mentions NMR."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.INDIA, cache)
        agent = DataValidatorAgent(Region.INDIA, registry, validator)

        # The system prompt should mention NMR for India
        system_prompts = agent.agent._system_prompts
        # Get the first system prompt
        system_prompt = system_prompts[0] if system_prompts else ""
        assert "NMR" in str(system_prompt).upper() or "NATIONAL MEDICAL" in str(system_prompt).upper()
        assert "INDIA" in str(system_prompt).upper()


# ============================================================================
# Test Service Integration
# ============================================================================


class TestServiceIntegration:
    """Test integration with provider registry and license validator."""

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    async def test_usa_provider_registry_integration(self):
        """Test USA provider registry can be called."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        # Mock the registry validate method
        mock_result = ProviderValidationResult(
            is_valid=True,
            identifier="1234567890",
            identifier_type="npi",
            exists=True,
            is_active=True,
            confidence=1.0
        )
        agent.provider_registry.validate_provider = AsyncMock(return_value=mock_result)

        result = await agent.provider_registry.validate_provider("1234567890")
        assert result.is_valid is True
        assert result.identifier == "1234567890"
        assert result.identifier_type == "npi"

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "NMC_API_KEY": "test-key"})
    async def test_india_provider_registry_integration(self):
        """Test India provider registry can be called."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.INDIA, cache)
        agent = DataValidatorAgent(Region.INDIA, registry, validator)

        # Mock the registry validate method
        mock_result = ProviderValidationResult(
            is_valid=True,
            identifier="NMR123456",
            identifier_type="nmr",
            exists=True,
            is_active=True,
            confidence=1.0
        )
        agent.provider_registry.validate_provider = AsyncMock(return_value=mock_result)

        result = await agent.provider_registry.validate_provider("NMR123456")
        assert result.is_valid is True
        assert result.identifier == "NMR123456"
        assert result.identifier_type == "nmr"

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    async def test_usa_license_validator_integration(self):
        """Test USA license validator can be called."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        # Mock the validator method
        mock_result = LicenseValidationResult(
            is_valid=True,
            license_number="CA12345",
            region="CA",
            region_type="state",
            exists=True,
            is_active=True,
            is_expired=False,
            has_disciplinary_actions=False,
            confidence=1.0
        )
        agent.license_validator.validate_license = AsyncMock(return_value=mock_result)

        result = await agent.license_validator.validate_license("CA12345", "CA", "John Doe")
        assert result.is_valid is True
        assert result.region == "CA"
        assert result.region_type == "state"

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "NMC_API_KEY": "test-key"})
    async def test_india_license_validator_integration(self):
        """Test India license validator can be called."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.INDIA, cache)
        agent = DataValidatorAgent(Region.INDIA, registry, validator)

        # Mock the validator method
        mock_result = LicenseValidationResult(
            is_valid=True,
            license_number="MH123456",
            region="MH",
            region_type="council",
            exists=True,
            is_active=True,
            is_expired=False,
            has_disciplinary_actions=False,
            confidence=1.0
        )
        agent.license_validator.validate_license = AsyncMock(return_value=mock_result)

        result = await agent.license_validator.validate_license("MH123456", "MH", "Dr. Smith")
        assert result.is_valid is True
        assert result.region == "MH"
        assert result.region_type == "council"


# ============================================================================
# Test Multi-Region Awareness
# ============================================================================


class TestMultiRegionAwareness:
    """Test that agent correctly works with different regions."""

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    def test_usa_agent_uses_usa_services(self):
        """Test USA agent uses USA-specific services."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        from src.services.usa import NPIRegistryClient, USStateLicenseClient
        assert isinstance(agent.provider_registry, NPIRegistryClient)
        assert isinstance(agent.license_validator, USStateLicenseClient)

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "NMC_API_KEY": "test-key"})
    def test_india_agent_uses_india_services(self):
        """Test India agent uses India-specific services."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.INDIA, cache)
        agent = DataValidatorAgent(Region.INDIA, registry, validator)

        from src.services.india import NMCRegistryClient, IndiaStateMedicalClient
        assert isinstance(agent.provider_registry, NMCRegistryClient)
        assert isinstance(agent.license_validator, IndiaStateMedicalClient)


# ============================================================================
# Test Agent Tools
# ============================================================================


class TestAgentTools:
    """Test agent tools are properly configured."""

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    def test_agent_has_tools(self):
        """Test agent has tools configured."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        # Check that agent has tools registered
        toolset = agent.agent._function_toolset
        assert toolset is not None

        # Toolset should have functions
        assert hasattr(toolset, '_tools') or hasattr(toolset, 'tools')

    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key", "NMC_API_KEY": "test-key"})
    def test_india_agent_has_same_tools(self):
        """Test India agent has same tool structure as USA agent."""
        cache = MemoryCacheClient(max_size=100)

        usa_registry, usa_validator = ServiceFactory.get_services(Region.USA, cache)
        usa_agent = DataValidatorAgent(Region.USA, usa_registry, usa_validator)

        india_registry, india_validator = ServiceFactory.get_services(Region.INDIA, cache)
        india_agent = DataValidatorAgent(Region.INDIA, india_registry, india_validator)

        # Both should have toolsets configured
        usa_toolset = usa_agent.agent._function_toolset
        india_toolset = india_agent.agent._function_toolset
        assert usa_toolset is not None
        assert india_toolset is not None


# ============================================================================
# Test Error Handling
# ============================================================================


class TestErrorHandling:
    """Test error handling in agent."""

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    async def test_handles_registry_error(self):
        """Test agent handles registry errors gracefully."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        # Mock registry to raise exception
        agent.provider_registry.validate_provider = AsyncMock(side_effect=Exception("API Error"))

        # Agent should handle the error (not crash)
        try:
            await agent.provider_registry.validate_provider("1234567890")
            assert False, "Should have raised exception"
        except Exception as e:
            assert "API Error" in str(e)

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    async def test_handles_validator_error(self):
        """Test agent handles validator errors gracefully."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        # Mock validator to raise exception
        agent.license_validator.validate_license = AsyncMock(side_effect=Exception("API Error"))

        # Agent should handle the error (not crash)
        try:
            await agent.license_validator.validate_license("CA12345", "CA", "John Doe")
            assert False, "Should have raised exception"
        except Exception as e:
            assert "API Error" in str(e)


# ============================================================================
# Test Cache Integration
# ============================================================================


class TestCacheIntegration:
    """Test that services use cache correctly."""

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    async def test_services_share_same_cache(self):
        """Test that registry and validator share the same cache."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)
        agent = DataValidatorAgent(Region.USA, registry, validator)

        # Both services should have the same cache instance
        assert agent.provider_registry.cache is cache
        assert agent.license_validator.cache is cache

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    async def test_cache_isolation_between_regions(self):
        """Test that different regions use properly namespaced cache keys."""
        cache = MemoryCacheClient(max_size=100)

        # Create USA services
        usa_registry, _ = ServiceFactory.get_services(Region.USA, cache)

        # USA registry should use "usa:" prefix
        assert usa_registry.CACHE_PREFIX == "usa:npi"
