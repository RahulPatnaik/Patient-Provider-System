"""
Tests for service factory.
"""

import pytest
from unittest.mock import patch

from src.config.regions import Region
from src.cache.memory import MemoryCacheClient
from src.services.factory import ServiceFactory
from src.services.usa import NPIRegistryClient, USStateLicenseClient
from src.services.india import NMCRegistryClient, IndiaStateMedicalClient
from src.services.base import BaseProviderRegistry, BaseLicenseValidator


class TestServiceFactory:
    """Test ServiceFactory."""

    def test_get_provider_registry_usa(self):
        """Test creating USA provider registry (NPI)."""
        cache = MemoryCacheClient(max_size=100)
        registry = ServiceFactory.get_provider_registry(Region.USA, cache)

        assert isinstance(registry, NPIRegistryClient)
        assert isinstance(registry, BaseProviderRegistry)
        assert registry.cache is cache

    @patch.dict("os.environ", {"NMC_API_KEY": "test-key"})
    def test_get_provider_registry_india(self):
        """Test creating India provider registry (NMC)."""
        cache = MemoryCacheClient(max_size=100)
        registry = ServiceFactory.get_provider_registry(Region.INDIA, cache)

        assert isinstance(registry, NMCRegistryClient)
        assert isinstance(registry, BaseProviderRegistry)
        assert registry.cache is cache

    def test_get_license_validator_usa(self):
        """Test creating USA license validator (State)."""
        cache = MemoryCacheClient(max_size=100)
        validator = ServiceFactory.get_license_validator(Region.USA, cache)

        assert isinstance(validator, USStateLicenseClient)
        assert isinstance(validator, BaseLicenseValidator)
        assert validator.cache is cache

    def test_get_license_validator_india(self):
        """Test creating India license validator (Council)."""
        cache = MemoryCacheClient(max_size=100)
        validator = ServiceFactory.get_license_validator(Region.INDIA, cache)

        assert isinstance(validator, IndiaStateMedicalClient)
        assert isinstance(validator, BaseLicenseValidator)
        assert validator.cache is cache

    def test_get_services_usa(self):
        """Test getting both services for USA."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.USA, cache)

        assert isinstance(registry, NPIRegistryClient)
        assert isinstance(validator, USStateLicenseClient)

    def test_get_services_india(self):
        """Test getting both services for India."""
        cache = MemoryCacheClient(max_size=100)
        registry, validator = ServiceFactory.get_services(Region.INDIA, cache)

        assert isinstance(registry, NMCRegistryClient)
        assert isinstance(validator, IndiaStateMedicalClient)

    def test_get_provider_registry_auto_cache(self):
        """Test getting provider registry with auto cache creation."""
        # Should auto-create cache if not provided
        registry = ServiceFactory.get_provider_registry(Region.USA)

        assert isinstance(registry, NPIRegistryClient)
        assert registry.cache is not None

    def test_get_license_validator_auto_cache(self):
        """Test getting license validator with auto cache creation."""
        # Should auto-create cache if not provided
        validator = ServiceFactory.get_license_validator(Region.USA)

        assert isinstance(validator, USStateLicenseClient)
        assert validator.cache is not None
