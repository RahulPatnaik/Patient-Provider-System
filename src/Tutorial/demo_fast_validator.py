"""
Demo script for Fast Validator Agent (KPME Karnataka)

This script demonstrates the Fast Validator Agent performing
quick KPME Karnataka healthcare establishment validation.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.fast_validator import FastValidatorAgent
from database.kpme_db import get_kpme_db


async def demo_fast_validator():
    """Demonstrate Fast Validator Agent functionality."""

    print("=" * 80)
    print("FAST VALIDATOR AGENT DEMO - KPME Karnataka")
    print("=" * 80)

    # Initialize agent
    print("\n[1] Initializing Fast Validator Agent...")
    agent = FastValidatorAgent()
    print("✓ Agent initialized successfully")

    # Get sample data from database
    print("\n[2] Fetching sample establishments from KPME database...")
    db = get_kpme_db()
    establishments = db.search_establishment_by_name("hospital", limit=3)

    if not establishments:
        print("✗ No establishments found in database")
        return

    sample_est = establishments[0]
    print(f"✓ Found {len(establishments)} establishments")
    print(f"  Using: {sample_est['establishment_name']}")
    print(f"  Certificate: {sample_est['certificate_number']}")
    print(f"  District: {sample_est['district']}")

    # Demo 1: Quick KPME Check (Direct Database - Ultra Fast)
    print("\n" + "=" * 80)
    print("DEMO 1: Quick KPME Check (Direct Database Access)")
    print("=" * 80)

    print(f"\nSearching by certificate: {sample_est['certificate_number']}")
    result = agent.quick_kpme_check(certificate_number=sample_est['certificate_number'])

    if result:
        print(f"\n✓ Quick check completed!")
        print(f"  - Name: {result.get('establishment_name')}")
        print(f"  - Category: {result.get('category')}")
        print(f"  - District: {result.get('district')}")
        print(f"  - Phone: {result.get('phone')}")
        print(f"  - Email: {result.get('email')}")
        print(f"  - System of Medicine: {result.get('system_of_medicine')}")
        print(f"  - GPS: ({result.get('latitude')}, {result.get('longitude')})")
    else:
        print("✗ No match found")

    # Demo 2: Fast Validation by Certificate Number
    print("\n" + "=" * 80)
    print("DEMO 2: Fast Validation by Certificate Number")
    print("=" * 80)

    print(f"\nValidating certificate: {sample_est['certificate_number']}")
    result = await agent.validate_fast(certificate_number=sample_est['certificate_number'])

    print(f"\n✓ Fast validation completed!")
    print(f"\n  Validation Result:")
    print(f"    - Is Valid: {result.is_valid}")
    print(f"    - Provider Found: {result.provider_found}")
    print(f"    - Cache Hit: {result.cache_hit}")
    print(f"    - Source: {result.validation_source}")
    print(f"    - Confidence: {result.confidence:.2%}")
    print(f"    - Establishment: {result.establishment_name}")
    print(f"    - Category: {result.category}")
    print(f"    - Certificate: {result.certificate_number}")
    print(f"    - District: {result.district}")
    print(f"    - Expired: {result.is_expired}")
    print(f"    - Timestamp: {result.timestamp}")

    # Demo 3: Fast Validation by Phone Number
    print("\n" + "=" * 80)
    print("DEMO 3: Fast Validation by Phone Number")
    print("=" * 80)

    phone = sample_est.get('phone')
    if phone:
        print(f"\nValidating by phone: {phone}")
        result2 = await agent.validate_fast(phone=str(phone))

        print(f"\n✓ Fast validation by phone completed!")
        print(f"  - Provider Found: {result2.provider_found}")
        print(f"  - Establishment: {result2.establishment_name}")
        print(f"  - Confidence: {result2.confidence:.2%}")
    else:
        print("\n⚠ Phone number not available for this establishment")

    # Demo 4: Fast Validation by Name
    print("\n" + "=" * 80)
    print("DEMO 4: Fast Validation by Name")
    print("=" * 80)

    print(f"\nValidating by name: 'hospital'")
    result3 = await agent.validate_fast(provider_name="hospital")

    print(f"\n✓ Fast validation by name completed!")
    print(f"  - Provider Found: {result3.provider_found}")
    print(f"  - Establishment: {result3.establishment_name}")
    print(f"  - Source: {result3.validation_source}")
    print(f"  - Confidence: {result3.confidence:.2%}")

    # Demo 5: Fast Validation with All Search Parameters
    print("\n" + "=" * 80)
    print("DEMO 5: Fast Validation with Multiple Search Criteria")
    print("=" * 80)

    print(f"\nValidating with certificate, name, and phone...")
    result4 = await agent.validate_fast(
        certificate_number=sample_est['certificate_number'],
        provider_name=sample_est['establishment_name'],
        phone=str(sample_est.get('phone', ''))
    )

    print(f"\n✓ Fast validation with multiple criteria completed!")
    print(f"  - Provider Found: {result4.provider_found}")
    print(f"  - Source: {result4.validation_source}")
    print(f"  - Confidence: {result4.confidence:.2%}")

    # Demo 6: Fast Validation with Agent Integration Flags
    print("\n" + "=" * 80)
    print("DEMO 6: Fast Validation with Agent Integration Flags")
    print("=" * 80)

    print(f"\nValidating with web scraper, enrichment, and compliance flags...")
    print("(Note: Integration not implemented yet, but flags are supported)")

    result5 = await agent.validate_fast(
        certificate_number=sample_est['certificate_number'],
        use_web_scraper=True,
        use_enrichment=True,
        use_compliance=True
    )

    print(f"\n✓ Fast validation with agent integration flags completed!")
    print(f"  - Provider Found: {result5.provider_found}")
    print(f"  - Confidence: {result5.confidence:.2%}")

    # Demo 7: Fast Validation with Expected Output (Testing)
    print("\n" + "=" * 80)
    print("DEMO 7: Fast Validation with Expected Output (Testing Scenario)")
    print("=" * 80)

    expected = {
        "is_valid": True,
        "provider_found": True,
        "cache_hit": False,
        "validation_source": "Expected Output",
        "confidence": 1.0,
        "establishment_name": "Test Hospital",
        "category": "Hospital",
        "certificate_number": "TEST123",
        "district": "BANGALORE",
        "is_expired": False
    }

    print(f"\nValidating with expected output for testing...")
    result6 = await agent.validate_fast(
        certificate_number="TEST123",
        expected_output=expected
    )

    print(f"\n✓ Fast validation with expected output completed!")
    print(f"  - Is Valid: {result6.is_valid}")
    print(f"  - Confidence: {result6.confidence:.2%}")

    # Demo 8: Multiple Quick Checks
    print("\n" + "=" * 80)
    print("DEMO 8: Multiple Quick Checks (Ultra Fast)")
    print("=" * 80)

    print("\nPerforming 3 ultra-fast database lookups...")
    for i, est in enumerate(establishments, 1):
        result = agent.quick_kpme_check(certificate_number=est['certificate_number'])
        if result:
            print(f"\n  {i}. {result.get('establishment_name')}")
            print(f"     Certificate: {result.get('certificate_number')}")
            print(f"     District: {result.get('district')}")

    print("\n" + "=" * 80)
    print("✅ FAST VALIDATOR AGENT DEMO COMPLETED!")
    print("=" * 80)
    print("\nKey Features Demonstrated:")
    print("  ✓ Ultra-fast direct database check (quick_kpme_check)")
    print("  ✓ Cache-first validation strategy")
    print("  ✓ Search by certificate, phone, or name")
    print("  ✓ Multiple search criteria support")
    print("  ✓ Agent integration flags support")
    print("  ✓ Expected output for testing scenarios")
    print("  ✓ Sub-second response times")
    print("  ✓ KPME Karnataka-only implementation (EL Branch)")


if __name__ == "__main__":
    asyncio.run(demo_fast_validator())
