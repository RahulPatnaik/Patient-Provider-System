"""
Create comprehensive master CSV combining all Karnataka healthcare data
"""

import json
import csv
from pathlib import Path
import pandas as pd

def create_master_csv():
    """Create master CSV with all healthcare data"""

    # Load all data
    print("Loading data files...")

    # Reference data
    with open('dataset/processed/reference/karnataka_reference_data.json') as f:
        ref_data = json.load(f)

    # Facility data
    with open('dataset/raw/osm/karnataka_health_osm.json') as f:
        facility_data = json.load(f)

    # KPME data
    kpme_df = None
    kpme_path = Path('dataset/KPME_DATA.csv')
    if kpme_path.exists():
        print("Loading KPME data...")
        kpme_df = pd.read_csv(kpme_path)

    # Prepare master dataset
    master_data = []

    # Section 1: Karnataka Districts Reference
    print("Processing district reference data...")
    for district in ref_data['karnataka_districts']:
        master_data.append({
            'data_type': 'DISTRICT_REFERENCE',
            'district_id': district['district_id'],
            'district_name': district['district_name'],
            'district_iso_code': district['district_iso_code'],
            'state': district['state'],
            'state_iso_code': district['state_iso_code'],
            'facility_type': '',
            'facility_name': '',
            'latitude': '',
            'longitude': '',
            'phone': '',
            'address': '',
            'specialization': '',
            'degree': '',
            'council': '',
            'system_of_medicine': '',
            'description': f'Official Karnataka District Reference - ID {district["district_id"]}'
        })

    # Section 2: Healthcare Facilities
    print("Processing facility data...")
    for facility in facility_data['facilities']:
        location = facility.get('location', {})
        contact = facility.get('contact', {})
        address = facility.get('address', {})

        # Find district reference
        district_name = facility.get('queried_district', '').upper()
        district_id = ''
        district_iso = ''

        for dist in ref_data['karnataka_districts']:
            if dist['district_name'] == district_name or district_name in dist['district_name']:
                district_id = dist['district_id']
                district_iso = dist['district_iso_code']
                break

        # Build address string
        addr_parts = []
        if address.get('street'):
            addr_parts.append(address['street'])
        if address.get('city'):
            addr_parts.append(address['city'])
        if address.get('postcode'):
            addr_parts.append(address['postcode'])
        addr_string = ', '.join(addr_parts) if addr_parts else ''

        master_data.append({
            'data_type': 'HEALTHCARE_FACILITY',
            'district_id': district_id,
            'district_name': district_name,
            'district_iso_code': district_iso,
            'state': 'KARNATAKA',
            'state_iso_code': '29',
            'facility_type': (facility.get('amenity') or 'unknown').upper(),
            'facility_name': facility.get('name', ''),
            'latitude': location.get('latitude', ''),
            'longitude': location.get('longitude', ''),
            'phone': contact.get('phone', ''),
            'address': addr_string,
            'specialization': facility.get('healthcare_speciality', ''),
            'degree': '',
            'council': '',
            'system_of_medicine': '',
            'description': f"OSM ID: {facility.get('osm_id', '')}, Type: {facility.get('osm_type', '')}"
        })

    # Section 3: Medical Specializations Reference
    print("Processing specialization reference data...")
    for system, specializations in ref_data['medical_specializations'].items():
        for idx, spec in enumerate(specializations, 1):
            master_data.append({
                'data_type': 'SPECIALIZATION_REFERENCE',
                'district_id': '',
                'district_name': '',
                'district_iso_code': '',
                'state': 'KARNATAKA',
                'state_iso_code': '29',
                'facility_type': '',
                'facility_name': '',
                'latitude': '',
                'longitude': '',
                'phone': '',
                'address': '',
                'specialization': spec,
                'degree': '',
                'council': '',
                'system_of_medicine': system,
                'description': f'{system} specialization #{idx}'
            })

    # Section 4: Medical Degrees Reference
    print("Processing degree reference data...")
    for system, degrees in ref_data['medical_degrees'].items():
        for idx, degree in enumerate(degrees, 1):
            master_data.append({
                'data_type': 'DEGREE_REFERENCE',
                'district_id': '',
                'district_name': '',
                'district_iso_code': '',
                'state': 'KARNATAKA',
                'state_iso_code': '29',
                'facility_type': '',
                'facility_name': '',
                'latitude': '',
                'longitude': '',
                'phone': '',
                'address': '',
                'specialization': '',
                'degree': degree,
                'council': '',
                'system_of_medicine': system,
                'description': f'{system} degree #{idx}'
            })

    # Section 5: Medical Councils Reference
    print("Processing medical council reference data...")
    for system, councils in ref_data['medical_councils'].items():
        for idx, council in enumerate(councils, 1):
            master_data.append({
                'data_type': 'COUNCIL_REFERENCE',
                'district_id': '',
                'district_name': '',
                'district_iso_code': '',
                'state': 'KARNATAKA',
                'state_iso_code': '29',
                'facility_type': '',
                'facility_name': '',
                'latitude': '',
                'longitude': '',
                'phone': '',
                'address': '',
                'specialization': '',
                'degree': '',
                'council': council,
                'system_of_medicine': system,
                'description': f'{system} medical council #{idx}'
            })

    # Section 6: KPME Establishments
    if kpme_df is not None and len(kpme_df) > 0:
        print(f"Processing KPME establishment data ({len(kpme_df)} establishments)...")
        for _, row in kpme_df.iterrows():
            # Find district reference
            district_name = row.get('district', '').upper() if pd.notna(row.get('district')) else ''
            district_id = ''
            district_iso = ''

            for dist in ref_data['karnataka_districts']:
                if district_name and (dist['district_name'] == district_name or district_name in dist['district_name']):
                    district_id = dist['district_id']
                    district_iso = dist['district_iso_code']
                    break

            master_data.append({
                'data_type': 'KPME_ESTABLISHMENT',
                'district_id': district_id,
                'district_name': district_name,
                'district_iso_code': district_iso,
                'state': 'KARNATAKA',
                'state_iso_code': '29',
                'facility_type': row.get('category', '').upper() if pd.notna(row.get('category')) else '',
                'facility_name': row.get('establishment_name', '') if pd.notna(row.get('establishment_name')) else '',
                'latitude': '',
                'longitude': '',
                'phone': '',
                'address': row.get('address', '') if pd.notna(row.get('address')) else '',
                'specialization': '',
                'degree': '',
                'council': '',
                'system_of_medicine': row.get('system_of_medicine', '') if pd.notna(row.get('system_of_medicine')) else '',
                'description': f"KPME Cert: {row.get('certificate_number', '')}, Valid: {row.get('certificate_validity', '')}"
            })

    # Convert to DataFrame
    print("Creating master CSV...")
    df = pd.DataFrame(master_data)

    # Sort by data type and name
    df = df.sort_values(['data_type', 'district_name', 'facility_name'])

    # Save to CSV
    output_path = 'dataset/karnataka_healthcare_master.csv'
    df.to_csv(output_path, index=False, encoding='utf-8')

    print(f"\n{'='*70}")
    print("MASTER CSV CREATED SUCCESSFULLY")
    print(f"{'='*70}")
    print(f"Location: {output_path}")
    print(f"Total Records: {len(df):,}")
    print(f"\nBreakdown by Data Type:")
    print(df['data_type'].value_counts().to_string())
    print(f"\n{'='*70}")

    # Create summary CSV
    kpme_count = len(kpme_df) if kpme_df is not None else 0

    summary_data = {
        'Metric': [
            'Karnataka Districts',
            'Healthcare Facilities (OSM)',
            'KPME Establishments',
            'Medical Specializations',
            'Medical Degrees',
            'Medical Councils',
            'Facilities with GPS',
            'Districts Covered',
            'Hospitals',
            'Clinics',
            'Pharmacies'
        ],
        'Count': [
            len(ref_data['karnataka_districts']),
            len([r for r in master_data if r['data_type'] == 'HEALTHCARE_FACILITY']),
            kpme_count,
            sum(len(v) for v in ref_data['medical_specializations'].values()),
            sum(len(v) for v in ref_data['medical_degrees'].values()),
            sum(len(v) for v in ref_data['medical_councils'].values()),
            len([f for f in facility_data['facilities'] if f.get('location', {}).get('latitude')]),
            len(set(f.get('queried_district') for f in facility_data['facilities'] if f.get('queried_district'))),
            len([f for f in facility_data['facilities'] if f.get('amenity') == 'hospital']),
            len([f for f in facility_data['facilities'] if f.get('amenity') == 'clinic']),
            len([f for f in facility_data['facilities'] if f.get('amenity') == 'pharmacy'])
        ]
    }

    summary_df = pd.DataFrame(summary_data)
    summary_path = 'dataset/karnataka_healthcare_summary.csv'
    summary_df.to_csv(summary_path, index=False)

    print(f"\nSummary CSV: {summary_path}")
    print(f"\n{summary_df.to_string(index=False)}")
    print(f"\n{'='*70}")

    return output_path, summary_path

if __name__ == '__main__':
    create_master_csv()
