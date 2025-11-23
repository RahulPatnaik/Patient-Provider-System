"""
Quick Demo - KPME Validator Agents (No API Required)

This script demonstrates the validator agents using direct database
access, bypassing the Gemini API to avoid quota issues.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.fast_validator import FastValidatorAgent
from database.kpme_db import get_kpme_db


def demo_quick_validators():
    """Quick demo of KPME validators using direct database access."""

    print("=" * 80)
    print("KPME VALIDATOR AGENTS - QUICK DEMO")
    print("(Using direct database access - No API quota required)")
    print("=" * 80)

    # Initialize
    print("\n[1] Initializing components...")
    db = get_kpme_db()
    fast_validator = FastValidatorAgent()
    print("✓ Database and Fast Validator initialized")

    # Get sample data
    print("\n[2] Fetching sample establishments...")
    hospitals = db.search_establishment_by_name("hospital", limit=5)
    print(f"✓ Found {len(hospitals)} hospitals in KPME database")

    # Demo 1: Database Direct Search
    print("\n" + "=" * 80)
    print("DEMO 1: Direct Database Search (Multiple Methods)")
    print("=" * 80)

    sample = hospitals[0]
    print(f"\nUsing establishment: {sample['establishment_name']}")
    print(f"Certificate: {sample['certificate_number']}")
    print(f"District: {sample['district']}")

    # Search by certificate
    print(f"\n[A] Search by Certificate Number:")
    result = db.get_establishment_by_certificate(sample['certificate_number'])
    if result:
        print(f"  ✓ Found: {result['establishment_name']}")
        print(f"    Category: {result['category']}")
        print(f"    Phone: {result.get('phone', 'N/A')}")
        print(f"    Email: {result.get('email', 'N/A')}")
        print(f"    GPS: ({result.get('latitude')}, {result.get('longitude')})")

    # Search by phone
    if sample.get('phone'):
        print(f"\n[B] Search by Phone Number:")
        phone_results = db.get_establishment_by_phone(str(sample['phone']))
        print(f"  ✓ Found {len(phone_results)} match(es)")
        for r in phone_results[:2]:
            print(f"    - {r['establishment_name']} ({r['district']})")

    # Search by name
    print(f"\n[C] Search by Name (partial match):")
    name_results = db.search_establishment_by_name("clinic", limit=3)
    print(f"  ✓ Found {len(name_results)} clinics")
    for i, r in enumerate(name_results, 1):
        print(f"    {i}. {r['establishment_name']}")
        print(f"       District: {r['district']}, Cert: {r['certificate_number']}")

    # Demo 2: Fast Validator Quick Check
    print("\n" + "=" * 80)
    print("DEMO 2: Fast Validator - Ultra Quick Check")
    print("=" * 80)

    print(f"\nPerforming ultra-fast lookups for 5 establishments...")
    for i, hospital in enumerate(hospitals[:5], 1):
        result = fast_validator.quick_kpme_check(
            certificate_number=hospital['certificate_number']
        )

        if result:
            print(f"\n  {i}. {result['establishment_name']}")
            print(f"     Certificate: {result['certificate_number']}")
            print(f"     Category: {result['category']}")
            print(f"     District: {result['district']}")
            print(f"     System: {result.get('system_of_medicine', 'N/A')}")
            print(f"     Beds: {result.get('num_beds', 'N/A')}")

    # Demo 3: Search by District
    print("\n" + "=" * 80)
    print("DEMO 3: Search by District")
    print("=" * 80)

    districts = ["BANGALORE", "MYSORE", "MANGALORE"]
    for district in districts:
        results = db.get_establishments_by_district(district)
        print(f"\n  {district}: {len(results)} establishments")
        if results:
            print(f"    Sample: {results[0]['establishment_name']}")

    # Demo 4: Staff Search
    print("\n" + "=" * 80)
    print("DEMO 4: Staff Search by Name and Registration")
    print("=" * 80)

    print(f"\nSearching for staff with 'doctor' in name...")
    staff_results = db.search_staff_by_name("doctor", limit=3)
    print(f"✓ Found {len(staff_results)} staff members")

    for i, staff in enumerate(staff_results, 1):
        print(f"\n  {i}. {staff.get('name', 'N/A')}")
        print(f"     Qualification: {staff.get('qualification', 'N/A')}")
        print(f"     Registration: {staff.get('registration_no', 'N/A')}")
        print(f"     Establishment: {staff.get('establishment_name', 'N/A')}")
        print(f"     Consultation Fee: ₹{staff.get('consultation_fee', 'N/A')}")

    # Demo 5: Database Statistics
    print("\n" + "=" * 80)
    print("DEMO 5: KPME Database Statistics")
    print("=" * 80)

    stats = db.get_stats()
    print(f"\n📊 Database Overview:")
    print(f"  Total Establishments: {stats['total_establishments']}")
    print(f"  Total Staff: {stats['total_staff']}")
    print(f"  Establishments with GPS: {stats['with_gps']}")

    print(f"\n📍 Top 5 Districts:")
    for i, district in enumerate(stats['top_districts'][:5], 1):
        print(f"  {i}. {district['district']}: {district['count']} establishments")

    print(f"\n💊 By System of Medicine:")
    for system in stats['by_system']:
        print(f"  - {system['system_of_medicine']}: {system['count']}")

    # Demo 6: Complete Establishment Details
    print("\n" + "=" * 80)
    print("DEMO 6: Complete Establishment Details (with Staff)")
    print("=" * 80)

    # Get an establishment with staff
    sample_with_id = hospitals[0]
    if 'id' in sample_with_id:
        complete = db.get_establishment_with_staff(sample_with_id['id'])

        print(f"\n🏥 {complete.get('establishment_name', 'N/A')}")
        print(f"   Certificate: {complete.get('certificate_number', 'N/A')}")
        print(f"   Category: {complete.get('category', 'N/A')}")
        print(f"   District: {complete.get('district', 'N/A')}")
        print(f"   Address: {complete.get('address', 'N/A')}")
        print(f"   Phone: {complete.get('phone', 'N/A')}")
        print(f"   Email: {complete.get('email', 'N/A')}")

        staff = complete.get('staff', [])
        print(f"\n   👥 Staff Members: {len(staff)}")
        for s in staff[:3]:
            print(f"      - {s.get('name', 'N/A')} ({s.get('qualification', 'N/A')})")

        specialties = complete.get('specialties', [])
        print(f"\n   🩺 Specialties: {len(specialties)}")
        for spec in specialties[:3]:
            print(f"      - {spec.get('specialty_name', 'N/A')}")

    print("\n" + "=" * 80)
    print("✅ QUICK DEMO COMPLETED!")
    print("=" * 80)
    print("\n📝 Summary:")
    print("  ✓ Direct database queries (ultra-fast, no API)")
    print("  ✓ Search by certificate, phone, name, district")
    print("  ✓ Staff search by name and registration")
    print("  ✓ Complete establishment details with staff")
    print("  ✓ Database statistics and analytics")
    print("\n💡 Features:")
    print("  • 1,000 KPME establishments in Karnataka")
    print("  • 4,483 staff records with qualifications")
    print("  • 899 establishments with GPS coordinates")
    print("  • Fast indexed queries on certificate, phone, name")
    print("  • EL Branch - KPME-only implementation")
    print("\n🎯 Next Steps:")
    print("  → For full agent validation (requires Gemini API):")
    print("     python src/Tutorial/demo_data_validator.py")
    print("     python src/Tutorial/demo_fast_validator.py")


if __name__ == "__main__":
    demo_quick_validators()
