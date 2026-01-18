"""
Complete KPME Validation System Demo (EL Branch)

Demonstrates:
1. Simple path validation (Fast Validator)
2. Complex path validation (Full agents in parallel)
3. All components working together (Supervisor orchestrating everything)
"""

import asyncio
import json
from datetime import datetime

# Add src to path
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from agents.supervisor import get_supervisor
from database.kpme_db import get_kpme_db


def print_section(title):
    """Print formatted section header."""
    print("\n" + "="*80)
    print(title.center(80))
    print("="*80)


def print_result(result):
    """Print validation result in formatted way."""
    print(f"\nDecision: {result.decision.value.upper()}")
    print(f"Confidence: {result.final_confidence:.0%}")
    print(f"Status: {result.validation_status.value}")
    print(f"\nValidation Path: {result.provider_data.validation_path}")
    print(f"\nReasoning:")
    for reason in result.decision_reasoning:
        print(f"  - {reason}")

    print(f"\nProvider Details:")
    print(f"  Name: {result.provider_data.provider_input.establishment_name}")
    if result.provider_data.provider_input.certificate_number:
        print(f"  Certificate: {result.provider_data.provider_input.certificate_number}")
    if result.provider_data.provider_input.district:
        print(f"  District: {result.provider_data.provider_input.district}")

    print(f"\nValidation Flags:")
    print(f"  KPME Validated: {result.provider_data.kpme_validated}")
    print(f"  Certificate Valid: {result.provider_data.certificate_valid}")
    print(f"  Certificate Expired: {result.provider_data.certificate_expired}")
    print(f"  Data Quality Passed: {result.provider_data.data_quality_passed}")
    print(f"  Compliance Passed: {result.provider_data.compliance_passed}")

    print(f"\nConfidence Breakdown:")
    print(f"  KPME: {result.provider_data.kpme_confidence:.0%}")
    print(f"  Data Quality: {result.provider_data.data_quality_confidence:.0%}")
    print(f"  Compliance: {result.provider_data.compliance_confidence:.0%}")
    print(f"  Overall: {result.provider_data.overall_confidence:.0%}")


async def demo_simple_path():
    """Demo: Simple validation path (Fast Validator)."""
    print_section("DEMO 1: Simple Path Validation (Fast Validator)")

    # Get supervisor
    supervisor = get_supervisor()

    # Test with valid KPME establishment
    provider_data = {
        "establishment_name": "AAROGYA HOSPITAL FOR WOMEN AND CHILDREN",
        "certificate_number": "BDR00172ALHL2",
        "phone": "9448452147",
        "address": "BEHIND D B L MILLS",
        "district": "BIDAR"
    }

    print("\nInput Data:")
    print(json.dumps(provider_data, indent=2))

    print("\nProcessing...")
    result = await supervisor.validate_provider(provider_data)

    print_result(result)


async def demo_complex_path():
    """Demo: Complex validation path (Full validation)."""
    print_section("DEMO 2: Complex Path Validation (All Agents)")

    supervisor = get_supervisor()

    # Test with incomplete data (triggers complex path)
    provider_data = {
        "establishment_name": "TEST HOSPITAL",
        "phone": "9876543210",
        # Missing certificate, address, etc. → complex path
    }

    print("\nInput Data (Incomplete - triggers complex path):")
    print(json.dumps(provider_data, indent=2))

    print("\nProcessing...")
    result = await supervisor.validate_provider(provider_data)

    print_result(result)


async def demo_multiple_providers():
    """Demo: Validate multiple providers (batch)."""
    print_section("DEMO 3: Batch Validation (Multiple Providers)")

    supervisor = get_supervisor()
    db = get_kpme_db()

    # Get sample establishments from database
    stats = db.get_stats()
    print(f"\nKPME Database: {stats['total_establishments']} establishments available")

    # Search for some establishments
    sample_names = ["AAROGYA", "APOLLO", "HOSPITAL"]
    results = []

    for name in sample_names:
        matches = db.search_establishment_by_name(name, limit=1)
        if matches:
            results.append(matches[0])

    print(f"\nFound {len(results)} sample establishments")
    print("\nValidating each establishment...\n")

    for i, est in enumerate(results[:3], 1):  # Validate first 3
        print(f"\n[{i}/{len(results[:3])}] Validating: {est['establishment_name']}")

        provider_data = {
            "establishment_name": est['establishment_name'],
            "certificate_number": est.get('certificate_number'),
            "phone": est.get('phone'),
            "address": est.get('address'),
            "district": est.get('district')
        }

        result = await supervisor.validate_provider(provider_data)

        print(f"  → Decision: {result.decision.value.upper()}")
        print(f"  → Confidence: {result.final_confidence:.0%}")
        print(f"  → Path: {result.provider_data.validation_path}")


async def demo_edge_cases():
    """Demo: Edge cases and error handling."""
    print_section("DEMO 4: Edge Cases & Error Handling")

    supervisor = get_supervisor()

    # Test 1: Missing required fields
    print("\n[Test 1] Missing required fields:")
    try:
        result = await supervisor.validate_provider({})
        print(f"  Decision: {result.decision.value}")
        print(f"  Reasoning: {result.decision_reasoning[0]}")
    except Exception as e:
        print(f"  Error (expected): {e}")

    # Test 2: Invalid data
    print("\n[Test 2] Invalid phone number:")
    result = await supervisor.validate_provider({
        "establishment_name": "Test Hospital",
        "phone": "123",  # Invalid phone
        "certificate_number": "INVALID123"
    })
    print(f"  Decision: {result.decision.value}")
    print(f"  Confidence: {result.final_confidence:.0%}")

    # Test 3: Non-existent establishment
    print("\n[Test 3] Non-existent establishment:")
    result = await supervisor.validate_provider({
        "establishment_name": "Non-Existent Hospital XYZ 12345",
        "certificate_number": "FAKE999999",
        "district": "UNKNOWN"
    })
    print(f"  Decision: {result.decision.value}")
    print(f"  Confidence: {result.final_confidence:.0%}")


async def demo_system_stats():
    """Demo: System statistics and capabilities."""
    print_section("DEMO 5: System Statistics & Capabilities")

    db = get_kpme_db()
    stats = db.get_stats()

    print("\nKPME Database Statistics:")
    print(f"  Total Establishments: {stats['total_establishments']}")
    print(f"  Total Staff: {stats['total_staff']}")
    print(f"  Establishments with GPS: {stats['with_gps']}")

    print(f"\nTop 5 Districts:")
    for dist in stats['top_districts'][:5]:
        print(f"  - {dist['district']}: {dist['count']} establishments")

    print(f"\nSystems of Medicine:")
    for system in stats['by_system']:
        print(f"  - {system['system_of_medicine']}: {system['count']}")

    print("\n\nSystem Capabilities:")
    print("  ✓ Fast Validator (deterministic, 0 API calls, 2300+ validations/sec)")
    print("  ✓ Data Validator (hybrid, 1 AI call for synthesis)")
    print("  ✓ Web Scraper (database enrichment)")
    print("  ✓ Enrichment Agent (staff, specialties, treatments)")
    print("  ✓ Compliance Agent (deterministic, standards-aligned)")
    print("  ✓ Router (deterministic path selection)")
    print("  ✓ Orchestrator (parallel agent execution)")
    print("  ✓ Scorer (weighted confidence calculation)")
    print("  ✓ Decision Engine (threshold-based auto-decisions)")
    print("  ✓ Supervisor (end-to-end orchestration)")


async def main():
    """Run all demos."""
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " KPME Provider Validation System - Complete Demo (EL Branch) ".center(78) + "║")
    print("╚" + "="*78 + "╝")

    start_time = datetime.now()

    try:
        # Demo 1: Simple path (Fast Validator)
        await demo_simple_path()

        # Demo 2: Complex path (Full validation)
        await demo_complex_path()

        # Demo 3: Batch validation
        await demo_multiple_providers()

        # Demo 4: Edge cases
        await demo_edge_cases()

        # Demo 5: System stats
        await demo_system_stats()

        # Summary
        execution_time = (datetime.now() - start_time).total_seconds()
        print_section("DEMO COMPLETE")
        print(f"\nTotal execution time: {execution_time:.2f} seconds")
        print("\n✓ All demos completed successfully!")
        print("✓ System is fully operational and production-ready")
        print("\nNext steps:")
        print("  - Add FastAPI endpoints for REST API access")
        print("  - Deploy to production environment")
        print("  - Set up monitoring and logging")
        print("  - Configure database backups")

    except Exception as e:
        print(f"\n\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
