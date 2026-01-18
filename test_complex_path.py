"""Quick test of complex path validation after fixes."""

import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from agents.supervisor import get_supervisor


async def test_complex_path():
    """Test complex path validation."""
    print("Testing complex path validation...")

    supervisor = get_supervisor()

    # Test data that triggers complex path (incomplete data)
    test_data = {
        "establishment_name": "TEST HOSPITAL",
        "phone": "9876543210"
    }

    print(f"\nTest data: {test_data}")
    print("\nRunning validation...")

    result = await supervisor.validate_provider(test_data)

    print(f"\n✅ SUCCESS!")
    print(f"Decision: {result.decision.value}")
    print(f"Confidence: {result.final_confidence:.0%}")
    print(f"Path: {result.provider_data.validation_path}")

    if result.provider_data.kpme_data:
        print(f"KPME validation worked: {result.provider_data.kpme_validated}")

    return result


if __name__ == "__main__":
    result = asyncio.run(test_complex_path())
