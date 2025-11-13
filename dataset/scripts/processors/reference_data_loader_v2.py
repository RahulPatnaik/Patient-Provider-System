"""
Reference Data Loader V2

Simplified version that works with actual Excel structure.
Loads master reference data from ABDM Excel file.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReferenceDataLoader:
    """Loads and processes reference data from Excel"""

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
        """Load Karnataka district reference data"""
        logger.info("Loading Karnataka districts...")

        df = pd.read_excel(self.excel_path, sheet_name="StateUT")

        # Filter for Karnataka (state_id = 15)
        karnataka = df[df['state_id'] == 15].copy()

        districts = []
        for _, row in karnataka.iterrows():
            district = {
                "district_id": int(row['district_id']),
                "district_name": str(row['district_name']).strip().upper(),
                "district_iso_code": str(row['district_iso_code']).strip(),
                "state": "KARNATAKA",
                "state_iso_code": "29"
            }
            districts.append(district)

        districts.sort(key=lambda x: x['district_id'])
        logger.info(f"✓ Loaded {len(districts)} Karnataka districts")

        self.reference_data['karnataka_districts'] = districts
        return districts

    def load_specializations(self) -> Dict[str, List[str]]:
        """Load medical specializations by system"""
        logger.info("Loading medical specializations...")

        df = pd.read_excel(self.excel_path, sheet_name="Doctor Specialities")

        specializations = {}
        systems = ['Modern Medicine', 'Dentistry', 'Ayurveda', 'Unani', 'Siddha', 'Sowa-Rigpa', 'Homeopathy']

        for system in systems:
            if system in df.columns:
                specs = df[system].dropna().tolist()
                specs = [str(s).strip() for s in specs if str(s).strip()]
                specializations[system] = specs
                logger.info(f"  {system}: {len(specs)} specializations")

        total = sum(len(v) for v in specializations.values())
        logger.info(f"✓ Loaded {total} total specializations across {len(systems)} systems")

        self.reference_data['medical_specializations'] = specializations
        return specializations

    def load_degrees(self) -> Dict[str, List[str]]:
        """Load medical degrees by system"""
        logger.info("Loading medical degrees...")

        df = pd.read_excel(self.excel_path, sheet_name="Doctor Degrees ")

        degrees = {}
        systems = ['Modern Medicine', 'Dentistry', 'Ayurveda', 'Unani', 'Siddha', 'Sowa-Rigpa', 'Homeopathy']

        for system in systems:
            if system in df.columns:
                degs = df[system].dropna().tolist()
                degs = [str(d).strip() for d in degs if str(d).strip()]
                degrees[system] = degs
                logger.info(f"  {system}: {len(degs)} degrees")

        total = sum(len(v) for v in degrees.values())
        logger.info(f"✓ Loaded {total} total degrees across {len(systems)} systems")

        self.reference_data['medical_degrees'] = degrees
        return degrees

    def load_medical_councils(self) -> Dict[str, List[str]]:
        """Load medical councils by system"""
        logger.info("Loading medical councils...")

        df = pd.read_excel(self.excel_path, sheet_name="Doctor Reg with council")

        councils = {}
        systems = ['Modern Medicine', 'Dentistry', 'Ayurveda', 'Unani', 'Siddha', 'Sowa-Rigpa', 'Homeopathy']

        for system in systems:
            if system in df.columns:
                cncs = df[system].dropna().tolist()
                cncs = [str(c).strip() for c in cncs if str(c).strip()]
                councils[system] = cncs
                logger.info(f"  {system}: {len(cncs)} councils")

        # Find Karnataka Medical Council
        for system, council_list in councils.items():
            for council in council_list:
                if 'karnataka' in council.lower():
                    logger.info(f"  ✓ Found: {council} ({system})")

        total = sum(len(v) for v in councils.values())
        logger.info(f"✓ Loaded {total} total councils")

        self.reference_data['medical_councils'] = councils
        return councils

    def load_systems_of_medicine(self) -> List[Dict[str, str]]:
        """Load systems of medicine"""
        logger.info("Loading systems of medicine...")

        df = pd.read_excel(self.excel_path, sheet_name="HPR Sys of medicine")

        systems = []
        for _, row in df.iterrows():
            if pd.notna(row['System of Medicine']):
                system = {
                    "name": str(row['System of Medicine']).strip()
                }
                systems.append(system)

        logger.info(f"✓ Loaded {len(systems)} systems of medicine")

        self.reference_data['systems_of_medicine'] = systems
        return systems

    def load_languages(self) -> List[Dict[str, Any]]:
        """Load spoken languages"""
        logger.info("Loading languages...")

        df = pd.read_excel(self.excel_path, sheet_name="Language Spoken")

        languages = []
        for _, row in df.iterrows():
            if pd.notna(row.get('Language spoken')):
                language = {
                    "name": str(row['Language spoken']).strip()
                }
                if pd.notna(row.get('id')):
                    language['id'] = int(row['id'])
                languages.append(language)

        logger.info(f"✓ Loaded {len(languages)} languages")

        self.reference_data['languages'] = languages
        return languages

    def load_facility_types(self) -> List[str]:
        """Load facility types"""
        logger.info("Loading facility types...")

        df = pd.read_excel(self.excel_path, sheet_name="Facility Type")

        facility_types = []
        if 'Facility Type' in df.columns:
            types = df['Facility Type'].dropna().tolist()
            facility_types = [str(t).strip() for t in types if str(t).strip()]

        logger.info(f"✓ Loaded {len(facility_types)} facility types")

        self.reference_data['facility_types'] = facility_types
        return facility_types

    def load_all_reference_data(self) -> Dict[str, Any]:
        """Load all reference data"""
        logger.info("="*70)
        logger.info("Loading Reference Data from Excel")
        logger.info(f"Source: {self.excel_path}")
        logger.info("="*70)

        # Load all data
        self.load_karnataka_districts()
        self.load_specializations()
        self.load_degrees()
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

        total_specs = sum(len(v) for v in self.reference_data.get('medical_specializations', {}).values())
        logger.info(f"  Medical Specializations: {total_specs}")

        total_degrees = sum(len(v) for v in self.reference_data.get('medical_degrees', {}).values())
        logger.info(f"  Medical Degrees: {total_degrees}")

        total_councils = sum(len(v) for v in self.reference_data.get('medical_councils', {}).values())
        logger.info(f"  Medical Councils: {total_councils}")

        logger.info(f"  Systems of Medicine: {len(self.reference_data.get('systems_of_medicine', []))}")
        logger.info(f"  Languages: {len(self.reference_data.get('languages', []))}")
        logger.info(f"  Facility Types: {len(self.reference_data.get('facility_types', []))}")
        logger.info("="*70)

        return self.reference_data

    def create_lookup_tables(self) -> Dict[str, Dict]:
        """Create fast lookup tables"""
        logger.info("Creating lookup tables...")

        lookups = {}

        # District lookups
        if 'karnataka_districts' in self.reference_data:
            districts = self.reference_data['karnataka_districts']
            lookups['district_by_id'] = {d['district_id']: d for d in districts}
            lookups['district_by_name'] = {d['district_name']: d for d in districts}
            lookups['district_by_iso'] = {d['district_iso_code']: d for d in districts}

        # Specialization lookups (flattened)
        if 'medical_specializations' in self.reference_data:
            all_specs = []
            for system, specs in self.reference_data['medical_specializations'].items():
                for spec in specs:
                    all_specs.append({'name': spec, 'system': system})

            lookups['specialization_by_name'] = {s['name'].lower(): s for s in all_specs}

        # Degree lookups (flattened)
        if 'medical_degrees' in self.reference_data:
            all_degrees = []
            for system, degrees in self.reference_data['medical_degrees'].items():
                for degree in degrees:
                    all_degrees.append({'name': degree, 'system': system})

            lookups['degree_by_name'] = {d['name'].lower(): d for d in all_degrees}

        # Council lookups (flattened)
        if 'medical_councils' in self.reference_data:
            all_councils = []
            for system, councils in self.reference_data['medical_councils'].items():
                for council in councils:
                    all_councils.append({'name': council, 'system': system})

            lookups['council_by_name'] = {c['name'].lower(): c for c in all_councils}

        logger.info(f"✓ Created {len(lookups)} lookup tables")
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

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(lookups, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved lookup tables to {output_path}")

    def save_karnataka_districts_csv(self):
        """Save Karnataka districts to CSV"""
        if 'karnataka_districts' not in self.reference_data:
            return

        df = pd.DataFrame(self.reference_data['karnataka_districts'])
        output_path = self.output_dir / "karnataka_districts.csv"

        df.to_csv(output_path, index=False)
        logger.info(f"✓ Saved Karnataka districts to {output_path}")


def main():
    """Main execution"""
    logger.info("Reference Data Loader V2 - Karnataka")
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
