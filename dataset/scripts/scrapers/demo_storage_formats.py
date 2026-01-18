"""
Demo: Show both storage formats for KPME full data
"""

import json
import pandas as pd

# Sample extracted data for 2 establishments
sample_data = [
    {
        "establishment_name": "ABC Hospital",
        "category": "Hospital (Level 2)",
        "system_of_medicine": "Allopathy",
        "address": "123 Main St, Bangalore",
        "certificate_number": "BLU001",
        "certificate_validity": "31 Dec 2025",
        "latitude": "12.9716",
        "longitude": "77.5946",
        "email": "abc@hospital.com",
        "phone": "9876543210",
        "num_beds": "50",
        "land_area_sqft": "5000",
        "building_area_sqft": "3000",
        "specialties": [
            {"Specialties": "MD Cardiology", "Type": "Specialties"},
            {"Specialties": "MS Orthopaedics", "Type": "Super Specialties"}
        ],
        "administrator": [
            {
                "Name": "Mr Admin",
                "Gender": "Male",
                "Age": "45",
                "Mobile No": "9999999999",
                "Email ID": "admin@abc.com",
                "Designation": "Administrator"
            }
        ],
        "staff_details": [
            {
                "Registration No.": "12345",
                "Name": "Dr John Doe",
                "Qualification": "MBBS, MD",
                "JobType": "FullTime",
                "Mobile No.": "9876543211"
            },
            {
                "Registration No.": "12346",
                "Name": "Dr Jane Smith",
                "Qualification": "MBBS, MS",
                "JobType": "PartTime",
                "Mobile No.": "9876543212"
            }
        ],
        "consultation_fees": [
            {"Registration No.": "12345", "Name": "Dr John Doe", "Consultation Fee (Rs.)": "300"},
            {"Registration No.": "12346", "Name": "Dr Jane Smith", "Consultation Fee (Rs.)": "500"}
        ],
        "surgery_fees": [
            {"Registration No.": "12345", "Name": "Dr John Doe", "Surgery Fees": "10000"},
            {"Registration No.": "12346", "Name": "Dr Jane Smith", "Surgery Fees": "15000"}
        ],
        "certificates": [
            {"Attachment Name": "Fire Safety Certificate", "Expiry Date": "31/12/2025"},
            {"Attachment Name": "PCB Certificate", "Expiry Date": "30/06/2026"}
        ],
        "treatment_charges": [
            {"Treatment Name": "X-Ray", "Treatment Code": "XRAY001", "Amount": "500"},
            {"Treatment Name": "ECG", "Treatment Code": "ECG001", "Amount": "300"}
        ]
    },
    {
        "establishment_name": "XYZ Clinic",
        "category": "Clinic/Polyclinic Only Consultation",
        "system_of_medicine": "Ayurveda",
        "address": "456 Park Rd, Mysore",
        "certificate_number": "MYS002",
        "certificate_validity": "15 Jun 2026",
        "latitude": "12.2958",
        "longitude": "76.6394",
        "email": "xyz@clinic.com",
        "phone": "9876543213",
        "num_beds": "10",
        "land_area_sqft": "1000",
        "building_area_sqft": "800",
        "specialties": [
            {"Specialties": "Panchakarma", "Type": "Specialties"}
        ],
        "administrator": [
            {
                "Name": "Dr Owner",
                "Gender": "Female",
                "Age": "38",
                "Mobile No": "9999999998",
                "Email ID": "owner@xyz.com",
                "Designation": "Owner"
            }
        ],
        "staff_details": [
            {
                "Registration No.": "67890",
                "Name": "Dr Ayur Veda",
                "Qualification": "BAMS",
                "JobType": "FullTime",
                "Mobile No.": "9876543214"
            }
        ],
        "consultation_fees": [
            {"Registration No.": "67890", "Name": "Dr Ayur Veda", "Consultation Fee (Rs.)": "200"}
        ],
        "surgery_fees": [],
        "certificates": [
            {"Attachment Name": "Trade License", "Expiry Date": "31/03/2026"}
        ],
        "treatment_charges": [
            {"Treatment Name": "Consultation", "Treatment Code": "-", "Amount": "200"}
        ]
    }
]

print("=" * 100)
print("STORAGE FORMAT DEMO - KPME FULL DATA")
print("=" * 100)

# ============================================================================
# APPROACH 1: Single Flattened CSV with JSON columns
# ============================================================================
print("\n" + "=" * 100)
print("APPROACH 1: Single Flattened CSV (with JSON columns for lists)")
print("=" * 100)

flattened_data = []

for est in sample_data:
    flat_row = {
        # Basic fields
        'establishment_name': est['establishment_name'],
        'category': est['category'],
        'system_of_medicine': est['system_of_medicine'],
        'address': est['address'],
        'certificate_number': est['certificate_number'],
        'certificate_validity': est['certificate_validity'],

        # GPS & Contact
        'latitude': est['latitude'],
        'longitude': est['longitude'],
        'email': est['email'],
        'phone': est['phone'],

        # Infrastructure
        'num_beds': est['num_beds'],
        'land_area_sqft': est['land_area_sqft'],
        'building_area_sqft': est['building_area_sqft'],

        # Complex data as JSON strings
        'specialties_json': json.dumps(est['specialties']),
        'administrator_json': json.dumps(est['administrator']),
        'staff_details_json': json.dumps(est['staff_details']),
        'consultation_fees_json': json.dumps(est['consultation_fees']),
        'surgery_fees_json': json.dumps(est['surgery_fees']),
        'certificates_json': json.dumps(est['certificates']),
        'treatment_charges_json': json.dumps(est['treatment_charges']),

        # Counts for easy filtering
        'num_specialties': len(est['specialties']),
        'num_staff': len(est['staff_details']),
        'num_certificates': len(est['certificates']),
        'num_treatments': len(est['treatment_charges'])
    }

    flattened_data.append(flat_row)

df_flat = pd.DataFrame(flattened_data)
df_flat.to_csv('/tmp/KPME_APPROACH1_FLATTENED.csv', index=False)

print("\n✓ Created: /tmp/KPME_APPROACH1_FLATTENED.csv")
print(f"  Rows: {len(df_flat)}")
print(f"  Columns: {len(df_flat.columns)}")
print(f"\nColumn names:")
for col in df_flat.columns:
    print(f"  - {col}")

print(f"\nSample row (first establishment):")
print(f"  Name: {df_flat.iloc[0]['establishment_name']}")
print(f"  GPS: {df_flat.iloc[0]['latitude']}, {df_flat.iloc[0]['longitude']}")
print(f"  Staff: {df_flat.iloc[0]['num_staff']} people")
print(f"  Specialties JSON (truncated): {df_flat.iloc[0]['specialties_json'][:80]}...")

# ============================================================================
# APPROACH 2: Normalized tables (main + related tables)
# ============================================================================
print("\n" + "=" * 100)
print("APPROACH 2: Normalized Tables (Main + Related CSVs)")
print("=" * 100)

# Main establishments table
main_data = []
for idx, est in enumerate(sample_data, 1):
    main_data.append({
        'id': idx,
        'establishment_name': est['establishment_name'],
        'category': est['category'],
        'system_of_medicine': est['system_of_medicine'],
        'address': est['address'],
        'certificate_number': est['certificate_number'],
        'certificate_validity': est['certificate_validity'],
        'latitude': est['latitude'],
        'longitude': est['longitude'],
        'email': est['email'],
        'phone': est['phone'],
        'num_beds': est['num_beds'],
        'land_area_sqft': est['land_area_sqft'],
        'building_area_sqft': est['building_area_sqft'],
    })

df_main = pd.DataFrame(main_data)
df_main.to_csv('/tmp/KPME_APPROACH2_MAIN.csv', index=False)
print(f"\n✓ Main table: /tmp/KPME_APPROACH2_MAIN.csv ({len(df_main)} rows)")

# Specialties table
specialties_data = []
for idx, est in enumerate(sample_data, 1):
    for spec in est['specialties']:
        specialties_data.append({
            'establishment_id': idx,
            'establishment_name': est['establishment_name'],
            'specialty': spec['Specialties'],
            'type': spec['Type']
        })

df_specialties = pd.DataFrame(specialties_data)
df_specialties.to_csv('/tmp/KPME_APPROACH2_SPECIALTIES.csv', index=False)
print(f"✓ Specialties table: /tmp/KPME_APPROACH2_SPECIALTIES.csv ({len(df_specialties)} rows)")

# Staff table
staff_data = []
for idx, est in enumerate(sample_data, 1):
    for staff in est['staff_details']:
        # Find consultation and surgery fees for this staff member
        reg_no = staff.get('Registration No.', '')

        consult_fee = next(
            (f['Consultation Fee (Rs.)'] for f in est.get('consultation_fees', [])
             if f.get('Registration No.') == reg_no),
            'N/A'
        )

        surgery_fee = next(
            (f['Surgery Fees'] for f in est.get('surgery_fees', [])
             if f.get('Registration No.') == reg_no),
            'N/A'
        )

        staff_data.append({
            'establishment_id': idx,
            'establishment_name': est['establishment_name'],
            'registration_no': reg_no,
            'name': staff.get('Name', ''),
            'qualification': staff.get('Qualification', ''),
            'job_type': staff.get('JobType', ''),
            'mobile': staff.get('Mobile No.', ''),
            'consultation_fee': consult_fee,
            'surgery_fee': surgery_fee
        })

df_staff = pd.DataFrame(staff_data)
df_staff.to_csv('/tmp/KPME_APPROACH2_STAFF.csv', index=False)
print(f"✓ Staff table: /tmp/KPME_APPROACH2_STAFF.csv ({len(df_staff)} rows)")

# Certificates table
certificates_data = []
for idx, est in enumerate(sample_data, 1):
    for cert in est['certificates']:
        certificates_data.append({
            'establishment_id': idx,
            'establishment_name': est['establishment_name'],
            'certificate_name': cert['Attachment Name'],
            'expiry_date': cert['Expiry Date']
        })

df_certificates = pd.DataFrame(certificates_data)
df_certificates.to_csv('/tmp/KPME_APPROACH2_CERTIFICATES.csv', index=False)
print(f"✓ Certificates table: /tmp/KPME_APPROACH2_CERTIFICATES.csv ({len(df_certificates)} rows)")

# Treatment charges table
treatments_data = []
for idx, est in enumerate(sample_data, 1):
    for treatment in est['treatment_charges']:
        treatments_data.append({
            'establishment_id': idx,
            'establishment_name': est['establishment_name'],
            'treatment_name': treatment['Treatment Name'],
            'treatment_code': treatment['Treatment Code'],
            'amount': treatment['Amount']
        })

df_treatments = pd.DataFrame(treatments_data)
df_treatments.to_csv('/tmp/KPME_APPROACH2_TREATMENTS.csv', index=False)
print(f"✓ Treatments table: /tmp/KPME_APPROACH2_TREATMENTS.csv ({len(df_treatments)} rows)")

# ============================================================================
# COMPARISON
# ============================================================================
print("\n" + "=" * 100)
print("COMPARISON OF APPROACHES")
print("=" * 100)

print("\n📊 APPROACH 1 (Flattened CSV):")
print("  ✅ Pros:")
print("     - Single file - easy to share/download")
print("     - All data in one place")
print("     - Easy to filter by establishment")
print("  ❌ Cons:")
print("     - JSON columns harder to query")
print("     - Larger file size")
print("     - Need to parse JSON to access nested data")
print(f"\n  Total: 1 file with {len(df_flat.columns)} columns")

print("\n📊 APPROACH 2 (Normalized Tables):")
print("  ✅ Pros:")
print("     - Easy to query (e.g., 'all doctors with consultation fee > 400')")
print("     - Smaller individual files")
print("     - Standard relational database structure")
print("     - Can load only tables you need")
print("  ❌ Cons:")
print("     - Multiple files to manage")
print("     - Need joins to get complete picture")
print(f"\n  Total: 5 files")
print(f"    - Main: {len(df_main)} establishments")
print(f"    - Specialties: {len(df_specialties)} records")
print(f"    - Staff: {len(df_staff)} records")
print(f"    - Certificates: {len(df_certificates)} records")
print(f"    - Treatments: {len(df_treatments)} records")

print("\n" + "=" * 100)
print("RECOMMENDATION:")
print("=" * 100)
print("\n💡 I recommend APPROACH 2 (Normalized Tables) because:")
print("   1. Easier to query (e.g., 'show all doctors in Bangalore')")
print("   2. Standard database structure")
print("   3. Can import into any database easily")
print("   4. Easier to analyze (e.g., average consultation fees by specialty)")
print("\n   But I can generate BOTH formats if you want!")
print("=" * 100)

# Show sample queries
print("\n" + "=" * 100)
print("SAMPLE QUERIES (Approach 2):")
print("=" * 100)

print("\n1️⃣  Find all staff with consultation fee > 250:")
high_fee_staff = df_staff[pd.to_numeric(df_staff['consultation_fee'].replace('N/A', '0'), errors='coerce') > 250]
print(f"   Result: {len(high_fee_staff)} staff members")
print(high_fee_staff[['name', 'establishment_name', 'consultation_fee']].to_string(index=False))

print("\n2️⃣  Count specialties by type:")
print(df_specialties['type'].value_counts().to_string())

print("\n3️⃣  Establishments with expired certificates:")
# (In real scenario, we'd parse dates)
print("   (Would check expiry_date < today)")

print("\n" + "=" * 100)
