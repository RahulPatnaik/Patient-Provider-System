"""
Reference Data Loader

Loads master reference data from ABDM Excel file and ABDM APIs
into standardized JSON format for validation and enrichment.

Input Sources:
- Master_Data_Production.xlsx (21 sheets of reference data)
- ABDM Master Data APIs (via abdm_master_data.py)

Output:
- Standardized reference data JSON files
- District mappings, specializations, degrees, councils, etc.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReferenceDataLoader:
    """Loads and processes reference data from Excel and APIs"""

    def __init__(
        self,
        excel_path: str = "dataset/Master_Data_Production_d7d5a5d974.xlsx",
        output_dir: str = "dataset/processed/reference"
    ):
        self.excel_path = Path(excel_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.reference_data: Dict[str, Any] = {}

    def load_karnataka_districts(self) -> List[Dict[str, Any]]:
        """
        Load Karnataka district reference data from Excel

        Returns:
            List of district records with IDs, names, and ISO codes
        """
        logger.info("Loading Karnataka districts from Excel...")

        df = pd.read_excel(self.excel_path, sheet_name="StateUT")

        # Filter for Karnataka districts (state_id = 15, state_iso_code = 29)
        karnataka = df[df['state_id'] == 15].copy()

        districts = []
        for _, row in karnataka.iterrows():
            district = {
                "identifier": int(row['district_id']),
                "name": str(row['district_name']).strip().upper(),
                "district_iso": str(row['district_iso_code']).strip(),
                "state": "KARNATAKA",
                "state_iso": "29",
                "country": "India"
            }
            districts.append(district)

        # Sort by identifier
        districts.sort(key=lambda x: x['identifier'])

        logger.info(f"Loaded {len(districts)} Karnataka districts")

        self.reference_data['karnataka_districts'] = districts
        return districts

    def load_medical_specializations(self) -> List[Dict[str, Any]]:
        """
        Load medical specializations from Excel

        Returns:
            List of specialization records
        """
        logger.info("Loading medical specializations...")

        df = pd.read_excel(self.excel_path, sheet_name="Doctor Specialities")

        specializations = []
        for _, row in df.iterrows():
            spec = {
                "identifier": int(row['identifier']),
                "name": str(row['name']).strip(),
                "system_of_medicine": int(row.get('systemOfMedicine', 1)),
                "is_super_specialty": bool(row.get('isSuperSpeciality', False))
            }
            specializations.append(spec)

        logger.info(f"Loaded {len(specializations)} medical specializations")

        self.reference_data['medical_specializations'] = specializations
        return specializations

    def load_medical_degrees(self) -> List[Dict[str, Any]]:
        """
        Load medical degrees/qualifications from Excel

        Returns:
            List of degree records
        """
        logger.info("Loading medical degrees...")

        df = pd.read_excel(self.excel_path, sheet_name="Doctor Degrees")

        degrees = []
        for _, row in df.iterrows():
            degree = {
                "identifier": int(row['identifier']),
                "name": str(row['name']).strip(),
                "broad_specialty": str(row.get('broadSpeciality', '')).strip() if pd.notna(row.get('broadSpeciality')) else None,
                "system_of_medicine": int(row.get('systemOfMedicine', 1)),
                "degree_type": str(row.get('degreeType', '')).strip() if pd.notna(row.get('degreeType')) else None
            }
            degrees.append(degree)

        logger.info(f"Loaded {len(degrees)} medical degrees")

        self.reference_data['medical_degrees'] = degrees
        return degrees

    def load_medical_councils(self) -> List[Dict[str, Any]]:
        """
        Load medical council data from Excel

        Returns:
            List of medical council records
        """
        logger.info("Loading medical councils...")

        df = pd.read_excel(self.excel_path, sheet_name="Doctor Reg with council")

        councils = []
        seen_identifiers = set()

        for _, row in df.iterrows():
            identifier = int(row['identifier'])

            # Skip duplicates
            if identifier in seen_identifiers:
                continue
            seen_identifiers.add(identifier)

            council = {
                "identifier": identifier,
                "name": str(row['name']).strip(),
                "system_of_medicine": int(row.get('systemOfMedicine', 1))
            }
            councils.append(council)

        logger.info(f"Loaded {len(councils)} medical councils")

        # Find Karnataka Medical Council
        kmc = next((c for c in councils if "karnataka" in c['name'].lower()), None)
        if kmc:
            logger.info(f"✓ Karnataka Medical Council: ID {kmc['identifier']}")

        self.reference_data['medical_councils'] = councils
        return councils

    def load_systems_of_medicine(self) -> List[Dict[str, Any]]:
        """
        Load systems of medicine from Excel

        Returns:
            List of system records
        """
        logger.info("Loading systems of medicine...")

        df = pd.read_excel(self.excel_path, sheet_name="HPR Sys of medicine")

        systems = []
        for _, row in df.iterrows():
            system = {
                "identifier": int(row['identifier']),
                "name": str(row['name']).strip(),
                "code": str(row['code']).strip() if pd.notna(row.get('code')) else None
            }
            systems.append(system)

        logger.info(f"Loaded {len(systems)} systems of medicine")

        self.reference_data['systems_of_medicine'] = systems
        return systems

    def load_languages(self) -> List[Dict[str, Any]]:
        """
        Load spoken languages from Excel

        Returns:
            List of language records
        """
        logger.info("Loading languages...")

        df = pd.read_excel(self.excel_path, sheet_name="Language Spoken")

        languages = []
        for _, row in df.iterrows():
            language = {
                "identifier": int(row['identifier']),
                "name": str(row['name']).strip(),
                "code": str(row.get('code', '')).strip() if pd.notna(row.get('code')) else None
            }
            languages.append(language)

        logger.info(f"Loaded {len(languages)} languages")

        self.reference_data['languages'] = languages
        return languages

    def load_facility_types(self) -> List[Dict[str, Any]]:
        """
        Load facility types from Excel

        Returns:
            List of facility type records
        """
        logger.info("Loading facility types...")

        try:
            df = pd.read_excel(self.excel_path, sheet_name="Facility Type")

            facility_types = []
            for _, row in df.iterrows():
                facility_type = {
                    "identifier": int(row['identifier']),
                    "name": str(row['name']).strip(),
                    "code": str(row.get('code', '')).strip() if pd.notna(row.get('code')) else None,
                    "system_of_medicine": int(row.get('systemOfMedicine', 1)) if pd.notna(row.get('systemOfMedicine')) else None
                }
                facility_types.append(facility_type)

            logger.info(f"Loaded {len(facility_types)} facility types")

            self.reference_data['facility_types'] = facility_types
            return facility_types

        except Exception as e:
            logger.warning(f"Could not load facility types: {e}")
            return []

    def load_all_reference_data(self) -> Dict[str, Any]:
        """
        Load all reference data from Excel

        Returns:
            Dictionary with all reference data
        """
        logger.info("="*70)
        logger.info("Loading Reference Data from Excel")
        logger.info(f"Source: {self.excel_path}")
        logger.info("="*70)

        # Load all sheets
        self.load_karnataka_districts()
        self.load_medical_specializations()
        self.load_medical_degrees()
        self.load_medical_councils()
        self.load_systems_of_medicine()
        self.load_languages()
        self.load_facility_types()

        # Add metadata
        self.reference_data['metadata'] = {
            "source_file": str(self.excel_path.name),
            "source_type": "ABDM Master Data Production",
            "region": "Karnataka, India",
            "loaded_at": pd.Timestamp.now().isoformat()
        }

        # Summary
        logger.info("="*70)
        logger.info("Reference Data Loading Summary:")
        logger.info(f"  Karnataka Districts: {len(self.reference_data.get('karnataka_districts', []))}")
        logger.info(f"  Medical Specializations: {len(self.reference_data.get('medical_specializations', []))}")
        logger.info(f"  Medical Degrees: {len(self.reference_data.get('medical_degrees', []))}")
        logger.info(f"  Medical Councils: {len(self.reference_data.get('medical_councils', []))}")
        logger.info(f"  Systems of Medicine: {len(self.reference_data.get('systems_of_medicine', []))}")
        logger.info(f"  Languages: {len(self.reference_data.get('languages', []))}")
        logger.info(f"  Facility Types: {len(self.reference_data.get('facility_types', []))}")
        logger.info("="*70)

        return self.reference_data

    def create_lookup_tables(self) -> Dict[str, Dict]:
        """
        Create fast lookup tables from reference data

        Returns:
            Dictionary of lookup tables (by ID, by name, etc.)
        """
        logger.info("Creating lookup tables...")

        lookups = {}

        # District lookups
        if 'karnataka_districts' in self.reference_data:
            districts = self.reference_data['karnataka_districts']
            lookups['district_by_id'] = {d['identifier']: d for d in districts}
            lookups['district_by_name'] = {d['name']: d for d in districts}
            lookups['district_by_iso'] = {d['district_iso']: d for d in districts}

        # Specialization lookups
        if 'medical_specializations' in self.reference_data:
            specs = self.reference_data['medical_specializations']
            lookups['specialization_by_id'] = {s['identifier']: s for s in specs}
            lookups['specialization_by_name'] = {s['name'].lower(): s for s in specs}

        # Degree lookups
        if 'medical_degrees' in self.reference_data:
            degrees = self.reference_data['medical_degrees']
            lookups['degree_by_id'] = {d['identifier']: d for d in degrees}
            lookups['degree_by_name'] = {d['name'].lower(): d for d in degrees}

        # Council lookups
        if 'medical_councils' in self.reference_data:
            councils = self.reference_data['medical_councils']
            lookups['council_by_id'] = {c['identifier']: c for c in councils}
            lookups['council_by_name'] = {c['name'].lower(): c for c in councils}

        logger.info(f"Created {len(lookups)} lookup tables")
        return lookups

    def save_reference_data(self, filename: str = "karnataka_reference_data.json"):
        """Save reference data to JSON"""
        output_path = self.output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.reference_data, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved reference data to {output_path}")

    def save_lookup_tables(self, lookups: Dict[str, Dict], filename: str = "lookup_tables.json"):
        """Save lookup tables to JSON"""
        output_path = self.output_dir / filename

        # Convert to serializable format
        serializable_lookups = {}
        for key, lookup in lookups.items():
            serializable_lookups[key] = {str(k): v for k, v in lookup.items()}

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(serializable_lookups, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved lookup tables to {output_path}")

    def save_karnataka_districts_csv(self):
        """Save Karnataka districts to CSV for easy reference"""
        if 'karnataka_districts' not in self.reference_data:
            return

        df = pd.DataFrame(self.reference_data['karnataka_districts'])
        output_path = self.output_dir / "karnataka_districts.csv"

        df.to_csv(output_path, index=False)
        logger.info(f"✓ Saved Karnataka districts to {output_path}")


def main():
    """Main execution"""
    logger.info("Reference Data Loader - Karnataka")
    logger.info("="*70)

    # Initialize loader
    loader = ReferenceDataLoader()

    # Load all data
    reference_data = loader.load_all_reference_data()

    # Create lookup tables
    lookups = loader.create_lookup_tables()

    # Save everything
    loader.save_reference_data()
    loader.save_lookup_tables(lookups)
    loader.save_karnataka_districts_csv()

    logger.info("")
    logger.info("✓ Reference data loading completed successfully!")
    logger.info(f"  Output directory: {loader.output_dir}")


if __name__ == "__main__":
    main()
