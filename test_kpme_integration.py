"""
Test script for KPME database integration with validators
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from database.kpme_db import get_kpme_db as get_db
from services.india.kpme_validator import KPMEValidator
from agents.fast_validator import FastValidatorAgent
from config.regions import Region


async def test_kpme_database():
    """Test KPME database loading and queries."""
    print("=" * 80)
    print("TEST 1: KPME Database Loading")
    print("=" * 80)

    db = get_db(force_reload=True)

    # Get stats
    stats = db.get_stats()
    print(f"\n✓ Database loaded successfully!")
    print(f"  Total establishments: {stats['total_establishments']}")
    print(f"  Total staff: {stats['total_staff']}")
    print(f"  Establishments with GPS: {stats['with_gps']}")

    print(f"\n  Top 5 districts:")
    for dist in stats['top_districts'][:5]:
        print(f"    - {dist['district']}: {dist['count']} establishments")

    print(f"\n  By system of medicine:")
    for system in stats['by_system']:
        print(f"    - {system['system_of_medicine']}: {system['count']}")

    print("\n" + "=" * 80)
    print("TEST 2: Search by Name")
    print("=" * 80)

    # Search for a hospital
    results = db.search_establishment_by_name("hospital", limit=3)
    print(f"\n✓ Found {len(results)} establishments matching 'hospital':")
    for i, est in enumerate(results[:3], 1):
        print(f"\n  {i}. {est['establishment_name']}")
        print(f"     Category: {est['category']}")
        print(f"     District: {est['district']}")
        print(f"     Certificate: {est['certificate_number']}")

    print("\n" + "=" * 80)
    print("TEST 3: KPME Validator Service")
    print("=" * 80)

    validator = KPMEValidator()

    # Get a certificate number from the first result
    if results:
        cert_num = results[0]['certificate_number']
        print(f"\n✓ Testing validation with certificate: {cert_num}")

        from services.base import LicenseData
        license_data = LicenseData(
            license_number=cert_num,
            region="Karnataka",
            region_type="kpme_establishment",
            status="active",
            provider_name=results[0]['establishment_name']
        )

        result = await validator.validate(license_data)

        print(f"\n  Validation Result:")
        print(f"    Valid: {result.is_valid}")
        print(f"    Exists: {result.exists}")
        print(f"    Active: {result.is_active}")
        print(f"    Expired: {result.is_expired}")
        print(f"    Confidence: {result.confidence:.2f}")
        print(f"    Region: {result.region}")
        print(f"    Region Type: {result.region_type}")

    print("\n" + "=" * 80)
    print("TEST 4: Fast Validator Agent (Quick Check)")
    print("=" * 80)

    fast_validator = FastValidatorAgent()

    # Use the same certificate
    if results:
        cert_num = results[0]['certificate_number']
        print(f"\n✓ Testing fast validation with certificate: {cert_num}")

        result = fast_validator.quick_kpme_check(
            certificate_number=cert_num
        )

        if result:
            print(f"\n  Quick Check Result:")
            print(f"    Name: {result.get('establishment_name')}")
            print(f"    Category: {result.get('category')}")
            print(f"    District: {result.get('district')}")
            print(f"    Phone: {result.get('phone')}")
            print(f"    Email: {result.get('email')}")
            print(f"    Latitude: {result.get('latitude')}")
            print(f"    Longitude: {result.get('longitude')}")

    print("\n" + "=" * 80)
    print("TEST 5: Search Staff by Registration")
    print("=" * 80)

    # Get staff from database
    db = get_db()
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM staff WHERE registration_no IS NOT NULL AND registration_no != '' LIMIT 3")
    staff_records = [dict(row) for row in cursor.fetchall()]

    if staff_records:
        print(f"\n✓ Found {len(staff_records)} staff records:")
        for i, staff in enumerate(staff_records, 1):
            print(f"\n  {i}. {staff.get('name')}")
            print(f"     Registration: {staff.get('registration_no')}")
            print(f"     Qualification: {staff.get('qualification')}")
            print(f"     Consultation Fee: ₹{staff.get('consultation_fee')}")
            print(f"     Establishment: {staff.get('establishment_name')}")

    print("\n" + "=" * 80)
    print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
    print("=" * 80)

    db.close()


if __name__ == "__main__":
    asyncio.run(test_kpme_database())
