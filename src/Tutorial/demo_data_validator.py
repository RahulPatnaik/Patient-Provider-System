"""
Data Validator Agent Demo - KPME Karnataka

Shows the hybrid architecture:
1. Deterministic KPME validation (no AI)
2. Deterministic data quality checks (no AI)
3. AI synthesis for final decision (only 1 API call)

Note: Requires Gemini API quota for the AI synthesis step.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.data_validator import DataValidatorAgent
from database.kpme_db import get_kpme_db


async def demo_data_validator():
    """Demonstrate Data Validator Agent."""

    print("=" * 80)
    print("DATA VALIDATOR AGENT DEMO - KPME Karnataka")
    print("=" * 80)
    print("\nArchitecture:")
    print("  1. Deterministic KPME validation (no AI)")
    print("  2. Deterministic data quality checks (no AI)")
    print("  3. AI synthesis for final confidence (1 API call)")
    print("=" * 80)

    # Initialize
    print("\n[1] Initializing Data Validator Agent...")
    agent = DataValidatorAgent()
    print("✓ Agent initialized")

    # Get sample data
    print("\n[2] Fetching sample establishment from KPME database...")
    db = get_kpme_db()
    establishments = db.search_establishment_by_name("hospital", limit=1)

    if not establishments:
        print("✗ No establishments found")
        return

    sample = establishments[0]
    print(f"✓ Found: {sample['establishment_name']}")
    print(f"  Certificate: {sample['certificate_number']}")
    print(f"  District: {sample['district']}")

    # Prepare provider data
    provider_data = {
        "certificate_number": sample['certificate_number'],
        "establishment_name": sample['establishment_name'],
        "address": sample.get('address', ''),
        "phone": sample.get('phone', ''),
        "email": sample.get('email', ''),
        "district": sample.get('district', '')
    }

    # Demo 1: Full Validation
    print("\n" + "=" * 80)
    print("DEMO 1: Full KPME Validation (Hybrid Architecture)")
    print("=" * 80)

    print("\nValidating provider data...")
    print("  [Step 1] Running deterministic KPME validation...")
    print("  [Step 2] Running deterministic data quality checks...")
    print("  [Step 3] Using AI to synthesize results...")

    try:
        result = await agent.validate(provider_data=provider_data)

        print("\n✅ Validation completed!")
        print(f"\n📊 KPME Validation:")
        print(f"    Valid: {result.kpme_validation.is_valid}")
        print(f"    Exists: {result.kpme_validation.exists}")
        print(f"    Certificate: {result.kpme_validation.certificate_number}")
        print(f"    Name: {result.kpme_validation.establishment_name}")
        print(f"    Category: {result.kpme_validation.category}")
        print(f"    District: {result.kpme_validation.district}")
        print(f"    Expired: {result.kpme_validation.is_expired}")
        print(f"    Confidence: {result.kpme_validation.confidence:.2%}")

        print(f"\n📈 Data Quality:")
        print(f"    Completeness: {result.data_quality.completeness_score:.2%}")
        print(f"    Accuracy: {result.data_quality.accuracy_score:.2%}")
        print(f"    Overall Score: {result.data_quality.overall_score:.2%}")
        print(f"    Missing Fields: {result.data_quality.missing_fields}")
        print(f"    Issues: {result.data_quality.issues}")

        print(f"\n🎯 Final Decision (AI Synthesis):")
        print(f"    Is Valid: {result.is_valid}")
        print(f"    Overall Confidence: {result.overall_confidence:.2%}")
        print(f"    Timestamp: {result.validation_timestamp}")

    except Exception as e:
        print(f"\n❌ Validation failed: {str(e)}")
        print("\nNote: This requires Gemini API quota for the AI synthesis step.")
        print("If quota is exhausted, the deterministic steps still completed successfully.")

    # Demo 2: With Incomplete Data
    print("\n" + "=" * 80)
    print("DEMO 2: Validation with Incomplete Data")
    print("=" * 80)

    incomplete_data = {
        "certificate_number": sample['certificate_number'],
        "establishment_name": sample['establishment_name']
        # Missing address, phone, email
    }

    print("\nValidating incomplete provider data (missing address, phone, email)...")

    try:
        result2 = await agent.validate(provider_data=incomplete_data)

        print("\n✅ Validation completed!")
        print(f"    Completeness: {result2.data_quality.completeness_score:.2%}")
        print(f"    Missing Fields: {result2.data_quality.missing_fields}")
        print(f"    Overall Confidence: {result2.overall_confidence:.2%}")

    except Exception as e:
        print(f"\n❌ Validation failed: {str(e)}")

    # Demo 3: With Agent Integration Flags
    print("\n" + "=" * 80)
    print("DEMO 3: Validation with Agent Integration Flags")
    print("=" * 80)

    print("\nValidating with web scraper, enrichment, compliance flags...")
    print("(These agents are not yet implemented, but the architecture supports them)")

    try:
        result3 = await agent.validate(
            provider_data=provider_data,
            use_web_scraper=True,
            use_enrichment=True,
            use_compliance=True
        )

        print("\n✅ Validation completed with agent flags!")
        print(f"    Overall Confidence: {result3.overall_confidence:.2%}")

    except Exception as e:
        print(f"\n❌ Validation failed: {str(e)}")

    print("\n" + "=" * 80)
    print("✅ DATA VALIDATOR AGENT DEMO COMPLETED!")
    print("=" * 80)
    print("\n🎯 Architecture Summary:")
    print("  ✓ Deterministic KPME validation (no AI, fast)")
    print("  ✓ Deterministic data quality checks (no AI, fast)")
    print("  ✓ AI synthesis for final decision (1 API call)")
    print("  ✓ Ready for multi-agent integration")


if __name__ == "__main__":
    asyncio.run(demo_data_validator())
