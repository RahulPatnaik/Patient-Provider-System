"""
Tests for region configuration.
"""

import pytest
import os
from unittest.mock import patch

from src.config.regions import (
    Region,
    RegionConfig,
    get_region_from_env,
    get_region_config,
    USA_CONFIG,
    INDIA_CONFIG
)


class TestRegionEnum:
    """Test Region enum."""

    def test_region_values(self):
        """Test Region enum values."""
        assert Region.USA.value == "usa"
        assert Region.INDIA.value == "india"

    def test_region_from_string(self):
        """Test creating Region from string."""
        assert Region("usa") == Region.USA
        assert Region("india") == Region.INDIA


class TestRegionConfig:
    """Test RegionConfig model."""

    def test_usa_config(self):
        """Test USA configuration."""
        assert USA_CONFIG.region == Region.USA
        assert USA_CONFIG.provider_registry_name == "NPI Registry"
        assert USA_CONFIG.provider_identifier_name == "NPI"
        assert USA_CONFIG.license_authority_name == "State Medical Board"
        assert USA_CONFIG.license_region_name == "State"

    def test_india_config(self):
        """Test India configuration."""
        assert INDIA_CONFIG.region == Region.INDIA
        assert INDIA_CONFIG.provider_registry_name == "National Medical Commission"
        assert INDIA_CONFIG.provider_identifier_name == "NMR ID"
        assert INDIA_CONFIG.license_authority_name == "State Medical Council"
        assert INDIA_CONFIG.license_region_name == "Council"


class TestGetRegionFromEnv:
    """Test get_region_from_env function."""

    @patch.dict(os.environ, {"PROVIDER_REGION": "usa"})
    def test_get_usa_from_env(self):
        """Test getting USA from environment."""
        region = get_region_from_env()
        assert region == Region.USA

    @patch.dict(os.environ, {"PROVIDER_REGION": "india"})
    def test_get_india_from_env(self):
        """Test getting India from environment."""
        region = get_region_from_env()
        assert region == Region.INDIA

    @patch.dict(os.environ, {"PROVIDER_REGION": "USA"})
    def test_get_usa_case_insensitive(self):
        """Test case-insensitive region from env."""
        region = get_region_from_env()
        assert region == Region.USA

    @patch.dict(os.environ, {}, clear=True)
    def test_get_region_not_set(self):
        """Test when PROVIDER_REGION not set."""
        region = get_region_from_env()
        assert region is None

    @patch.dict(os.environ, {"PROVIDER_REGION": "invalid"})
    def test_get_invalid_region(self):
        """Test invalid region value."""
        region = get_region_from_env()
        assert region is None


class TestGetRegionConfig:
    """Test get_region_config function."""

    def test_get_usa_config(self):
        """Test getting USA configuration."""
        config = get_region_config(Region.USA)
        assert config == USA_CONFIG
        assert config.region == Region.USA

    def test_get_india_config(self):
        """Test getting India configuration."""
        config = get_region_config(Region.INDIA)
        assert config == INDIA_CONFIG
        assert config.region == Region.INDIA
