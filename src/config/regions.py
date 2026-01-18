"""
Region configuration for multi-region provider validation system.
"""

import os
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class Region(str, Enum):
    """Supported regions for provider validation."""
    USA = "usa"
    INDIA = "india"


class RegionConfig(BaseModel):
    """Configuration for a specific region."""
    region: Region
    provider_registry_name: str  # "NPI Registry" or "NMC Registry"
    provider_identifier_name: str  # "NPI" or "NMR ID"
    license_authority_name: str  # "State Medical Board" or "State Medical Council"
    license_region_name: str  # "State" or "Council"


# Region-specific configurations
USA_CONFIG = RegionConfig(
    region=Region.USA,
    provider_registry_name="NPI Registry",
    provider_identifier_name="NPI",
    license_authority_name="State Medical Board",
    license_region_name="State"
)

INDIA_CONFIG = RegionConfig(
    region=Region.INDIA,
    provider_registry_name="National Medical Commission",
    provider_identifier_name="NMR ID",
    license_authority_name="State Medical Council",
    license_region_name="Council"
)

# Region configuration mapping
REGION_CONFIGS = {
    Region.USA: USA_CONFIG,
    Region.INDIA: INDIA_CONFIG
}


def get_region_from_env() -> Optional[Region]:
    """
    Get region from environment variable.

    Returns:
        Region enum or None if not set
    """
    region_str = os.getenv("PROVIDER_REGION", "").lower()

    if not region_str:
        return None

    try:
        return Region(region_str)
    except ValueError:
        return None


def get_region_config(region: Region) -> RegionConfig:
    """
    Get configuration for a specific region.

    Args:
        region: Region enum

    Returns:
        RegionConfig for the specified region

    Raises:
        ValueError: If region not supported
    """
    if region not in REGION_CONFIGS:
        raise ValueError(
            f"Unsupported region: {region}. "
            f"Supported regions: {', '.join([r.value for r in Region])}"
        )

    return REGION_CONFIGS[region]


def prompt_region_selection() -> Region:
    """
    Prompt user to select region interactively.

    Returns:
        Selected Region enum
    """
    print("\n" + "="*50)
    print("PROVIDER VALIDATION SYSTEM - REGION SELECTION")
    print("="*50)
    print("\nSelect the region for provider validation:")
    print("1. USA (NPI Registry + State Medical Boards)")
    print("2. India (NMC Registry + State Medical Councils)")
    print()

    while True:
        try:
            choice = input("Enter your choice (1 or 2): ").strip()

            if choice == "1":
                print("\n Selected: USA")
                return Region.USA
            elif choice == "2":
                print("\n Selected: India")
                return Region.INDIA
            else:
                print("L Invalid choice. Please enter 1 or 2.")
        except (KeyboardInterrupt, EOFError):
            print("\n\nL Region selection cancelled.")
            raise SystemExit(1)


def get_or_prompt_region() -> Region:
    """
    Get region from environment or prompt user.

    Priority:
    1. Check PROVIDER_REGION environment variable
    2. Prompt user for interactive selection

    Returns:
        Selected Region enum
    """
    # Try to get from environment
    region = get_region_from_env()

    if region:
        print(f"\n Using region from environment: {region.value.upper()}")
        return region

    # Prompt user
    return prompt_region_selection()