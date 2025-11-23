"""
Demo script for Data Validator Agent (KPME Karnataka)

This script demonstrates the Data Validator Agent validating
KPME Karnataka healthcare establishment data.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.data_validator import DataValidatorAgent
from database.kpme_db import get_kpme_db


async def demo_data_validator():
    """Demonstrate Data Validator Agent functionality."""

    print("=" * 80)
    print("DATA VALIDATOR AGENT DEMO - KPME Karnataka")
    print("=" * 80)

    # Initialize agent
    print("\n[1] Initializing Data Validator Agent...")
    agent = DataValidatorAgent()
    print("✓ Agent initialized successfully")

    # Get sample data from database
    print("\n[2] Fetching sample establishment from KPME database...")
    db = get_kpme_db()
    establishments = db.search_establishment_by_name("hospital", limit=1)

    if not establishments:
        print("✗ No establishments found in database")
        return

    sample_est = establishments[0]
    print(f"✓ Found: {sample_est['establishment_name']}")
    print(f"  Certificate: {sample_est['certificate_number']}")
    print(f"  District: {sample_est['district']}")

    # Prepare provider data
    provider_data = {
        "certificate_number": sample_est['certificate_number'],
        "establishment_name": sample_est['establishment_name'],
        "address": sample_est.get('address', ''),
        "phone": sample_est.get('phone', ''),
        "email": sample_est.get('email', ''),
        "district": sample_est.get('district', '')
    }

    # Demo 1: Basic Validation
    print("\n" + "=" * 80)
    print("DEMO 1: Basic KPME Validation")
    print("=" * 80)

    print("\nValidating provider data...")
    result = await agent.validate(provider_data=provider_data)

    print(f"\n✓ Validation completed!")
    print(f"\n  KPME Validation Result:")
    print(f"    - Valid: {result.kpme_validation.is_valid}")
    print(f"    - Exists: {result.kpme_validation.exists}")
    print(f"    - Certificate: {result.kpme_validation.certificate_number}")
    print(f"    - Name: {result.kpme_validation.establishment_name}")
    print(f"    - Category: {result.kpme_validation.category}")
    print(f"    - District: {result.kpme_validation.district}")
    print(f"    - Expired: {result.kpme_validation.is_expired}")
    print(f"    - Confidence: {result.kpme_validation.confidence:.2f}")

    print(f"\n  Data Quality Assessment:")
    print(f"    - Completeness: {result.data_quality.completeness_score:.2%}")
    print(f"    - Accuracy: {result.data_quality.accuracy_score:.2%}")
    print(f"    - Overall Score: {result.data_quality.overall_score:.2%}")
    print(f"    - Missing Fields: {result.data_quality.missing_fields}")
    print(f"    - Issues: {result.data_quality.issues}")

    print(f"\n  Overall:")
    print(f"    - Is Valid: {result.is_valid}")
    print(f"    - Overall Confidence: {result.overall_confidence:.2%}")
    print(f"    - Timestamp: {result.validation_timestamp}")

    # Demo 2: Validation with incomplete data
    print("\n" + "=" * 80)
    print("DEMO 2: Validation with Incomplete Data")
    print("=" * 80)

    incomplete_data = {
        "certificate_number": sample_est['certificate_number'],
        "establishment_name": sample_est['establishment_name']
        # Missing address, phone, email
    }

    print("\nValidating incomplete provider data (missing address, phone, email)...")
    result2 = await agent.validate(provider_data=incomplete_data)

    print(f"\n✓ Validation completed!")
    print(f"  - Completeness: {result2.data_quality.completeness_score:.2%}")
    print(f"  - Missing Fields: {result2.data_quality.missing_fields}")
    print(f"  - Overall Confidence: {result2.overall_confidence:.2%}")

    # Demo 3: Validation with agent integration flags
    print("\n" + "=" * 80)
    print("DEMO 3: Validation with Agent Integration Flags")
    print("=" * 80)

    print("\nValidating with web scraper, enrichment, and compliance flags enabled...")
    print("(Note: Integration not implemented yet, but flags are supported)")

    result3 = await agent.validate(
        provider_data=provider_data,
        use_web_scraper=True,
        use_enrichment=True,
        use_compliance=True
    )

    print(f"\n✓ Validation completed with agent integration flags!")
    print(f"  - Web Scraper Data: {result3.web_scraper_data}")
    print(f"  - Enrichment Data: {result3.enrichment_data}")
    print(f"  - Compliance Data: {result3.compliance_data}")
    print(f"  - Overall Confidence: {result3.overall_confidence:.2%}")

    # Demo 4: Validation with expected output (testing scenario)
    print("\n" + "=" * 80)
    print("DEMO 4: Validation with Expected Output (Testing Scenario)")
    print("=" * 80)

    expected_output = {
        "kpme_validation": {
            "is_valid": True,
            "exists": True,
            "certificate_number": "TEST123",
            "establishment_name": "Test Hospital",
            "category": "Hospital",
            "district": "BANGALORE",
            "is_expired": False,
            "confidence": 0.95,
            "details": {}
        },
        "data_quality": {
            "completeness_score": 1.0,
            "accuracy_score": 1.0,
            "overall_score": 1.0,
            "missing_fields": [],
            "issues": []
        },
        "overall_confidence": 0.95,
        "is_valid": True
    }

    print("\nValidating with expected output for testing...")
    result4 = await agent.validate(
        provider_data={"certificate_number": "TEST123", "establishment_name": "Test Hospital", "address": "Test", "phone": "1234567890"},
        expected_output=expected_output
    )

    print(f"\n✓ Validation completed with expected output!")
    print(f"  - Is Valid: {result4.is_valid}")
    print(f"  - Overall Confidence: {result4.overall_confidence:.2%}")

    print("\n" + "=" * 80)
    print("✅ DATA VALIDATOR AGENT DEMO COMPLETED!")
    print("=" * 80)
    print("\nKey Features Demonstrated:")
    print("  ✓ KPME certificate validation")
    print("  ✓ Data quality assessment")
    print("  ✓ Confidence scoring (KPME 70% + Quality 30%)")
    print("  ✓ Agent integration flags support")
    print("  ✓ Expected output for testing scenarios")
    print("  ✓ KPME Karnataka-only implementation (EL Branch)")


if __name__ == "__main__":
    asyncio.run(demo_data_validator())
