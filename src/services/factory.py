"""
Service factory for creating region-specific provider validation services.
"""

import os
import logging
from typing import Optional

from config.regions import Region
from cache.base import BaseCacheClient
from cache.factory import get_cache_client
from services.base import BaseProviderRegistry, BaseLicenseValidator
from services.usa import NPIRegistryClient, USStateLicenseClient
from services.india import NMCRegistryClient, IndiaStateMedicalClient


logger = logging.getLogger(__name__)


class ServiceFactory:
    """
    Factory class for creating region-specific services.

    This factory abstracts away the complexity of choosing the right
    service implementation based on the selected region (USA or India).
    """

    @staticmethod
    def get_provider_registry(
        region: Region,
        cache: Optional[BaseCacheClient] = None
    ) -> BaseProviderRegistry:
        """
        Create provider registry client for the specified region.

        Args:
            region: Region enum (USA or INDIA)
            cache: Cache client (defaults to auto-detected Redis/Memory)

        Returns:
            BaseProviderRegistry: Region-specific provider registry client
                - NPIRegistryClient for USA
                - NMCRegistryClient for India

        Raises:
            ValueError: If region is not supported

        Example:
            >>> cache = get_cache_client()
            >>> registry = ServiceFactory.get_provider_registry(Region.USA, cache)
            >>> result = await registry.validate_provider("1234567890")
        """
        if cache is None:
            cache = get_cache_client()

        if region == Region.USA:
            logger.info("Creating NPI Registry client for USA")
            return NPIRegistryClient(cache)

        elif region == Region.INDIA:
            logger.info("Creating NMC Registry client for India")
            # Get NMC API key from environment if available
            nmc_api_key = os.getenv("NMC_API_KEY")
            return NMCRegistryClient(cache, api_key=nmc_api_key)

        else:
            raise ValueError(
                f"Unsupported region: {region}. "
                f"Supported regions: {Region.USA.value}, {Region.INDIA.value}"
            )

    @staticmethod
    def get_license_validator(
        region: Region,
        cache: Optional[BaseCacheClient] = None
    ) -> BaseLicenseValidator:
        """
        Create license validator client for the specified region.

        Args:
            region: Region enum (USA or INDIA)
            cache: Cache client (defaults to auto-detected Redis/Memory)

        Returns:
            BaseLicenseValidator: Region-specific license validator client
                - USStateLicenseClient for USA
                - IndiaStateMedicalClient for India

        Raises:
            ValueError: If region is not supported

        Example:
            >>> cache = get_cache_client()
            >>> validator = ServiceFactory.get_license_validator(Region.USA, cache)
            >>> result = await validator.validate_license("CA12345", "CA")
        """
        if cache is None:
            cache = get_cache_client()

        if region == Region.USA:
            logger.info("Creating US State License client for USA")
            return USStateLicenseClient(cache)

        elif region == Region.INDIA:
            logger.info("Creating India State Medical client for India")
            return IndiaStateMedicalClient(cache)

        else:
            raise ValueError(
                f"Unsupported region: {region}. "
                f"Supported regions: {Region.USA.value}, {Region.INDIA.value}"
            )

    @staticmethod
    def get_services(
        region: Region,
        cache: Optional[BaseCacheClient] = None
    ) -> tuple[BaseProviderRegistry, BaseLicenseValidator]:
        """
        Create both provider registry and license validator for the specified region.

        This is a convenience method that creates both services at once.

        Args:
            region: Region enum (USA or INDIA)
            cache: Cache client (defaults to auto-detected Redis/Memory)

        Returns:
            Tuple of (provider_registry, license_validator)

        Example:
            >>> registry, validator = ServiceFactory.get_services(Region.USA)
            >>> npi_result = await registry.validate_provider("1234567890")
            >>> license_result = await validator.validate_license("CA12345", "CA")
        """
        if cache is None:
            cache = get_cache_client()

        provider_registry = ServiceFactory.get_provider_registry(region, cache)
        license_validator = ServiceFactory.get_license_validator(region, cache)

        logger.info(f"Created provider validation services for region: {region.value.upper()}")

        return provider_registry, license_validator