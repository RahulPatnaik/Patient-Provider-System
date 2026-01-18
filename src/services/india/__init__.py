"""
India service implementations.
"""

from services.india.nmc_registry import NMCRegistryClient, NMCRegistryError
from services.india.state_medical import IndiaStateMedicalClient, IndiaStateMedicalError


__all__ = [
    "NMCRegistryClient",
    "NMCRegistryError",
    "IndiaStateMedicalClient",
    "IndiaStateMedicalError",
]