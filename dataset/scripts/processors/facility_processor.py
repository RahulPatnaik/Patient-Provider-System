"""
Facility Data Processor

Standardizes and cleans healthcare facility data from multiple sources into
a unified format following the facility schema.
"""

import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import uuid

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FacilityProcessor:
    """Process and standardize facility data"""

    def __init__(
        self,
        schema_path: str = "dataset/schemas/facility_schema.json",
        output_dir: str = "dataset/processed"
    ):
        self.schema_path = Path(schema_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Load schema
        with open(self.schema_path, "r") as f:
            self.schema = json.load(f)

        self.facilities: List[Dict[str, Any]] = []

    def generate_facility_id(self, facility_data: Dict[str, Any]) -> str:
        """
        Generate unique facility ID based on name and location

        Args:
            facility_data: Facility information

        Returns:
            Unique facility ID
        """
        # Create hash from name + district + location
        hash_input = (
            str(facility_data.get("name", "")).lower() +
            str(facility_data.get("address", {}).get("district", "")).lower() +
            str(facility_data.get("location", {}).get("latitude", "")) +
            str(facility_data.get("location", {}).get("longitude", ""))
        )

        hash_value = hashlib.md5(hash_input.encode()).hexdigest()[:12].upper()
        return f"FAC_{hash_value}"

    def process_osm_facility(self, osm_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert OSM facility data to standard schema

        Args:
            osm_data: Raw OSM facility data

        Returns:
            Standardized facility dict
        """
        # Map OSM amenity to facility type
        type_mapping = {
            "hospital": "Hospital",
            "clinic": "Clinic",
            "doctors": "Clinic",
            "pharmacy": "Pharmacy",
            "dentist": "Clinic"
        }

        facility_type = type_mapping.get(
            osm_data.get("amenity"),
            "Clinic" if osm_data.get("healthcare") else "Other"
        )

        facility = {
            "facility_id": "",  # Will be generated
            "source_ids": {
                "hfr_id": None,
                "osm_id": str(osm_data.get("osm_id")),
                "data_gov_id": None,
                "rhs_code": None,
                "nhm_code": None
            },
            "name": osm_data.get("name", "Unnamed Facility"),
            "type": facility_type,
            "subtype": osm_data.get("healthcare"),
            "ownership": "Private" if osm_data.get("operator_type") == "private" else "Unknown",
            "system_of_medicine": "Modern",
            "address": {
                "line1": osm_data.get("address", {}).get("street"),
                "line2": None,
                "village": None,
                "taluk": None,
                "district": osm_data.get("address", {}).get("district") or osm_data.get("queried_district"),
                "pincode": osm_data.get("address", {}).get("postcode"),
                "state": "Karnataka"
            },
            "location": osm_data.get("location"),
            "contact": osm_data.get("contact", {}),
            "capacity": {
                "beds": int(osm_data["beds"]) if osm_data.get("beds") and str(osm_data["beds"]).isdigit() else None,
                "doctors": None,
                "nurses": None,
                "staff_total": None
            },
            "operational_status": "24x7" if osm_data.get("opening_hours") == "24/7" else "Unknown",
            "services": [],
            "rural_urban": None,
            "nfhs_district_code": None,
            "data_sources": ["OSM"],
            "quality_score": 0.7,  # OSM data tends to be incomplete
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "last_verified": None,
            "notes": None
        }

        # Generate ID
        facility["facility_id"] = self.generate_facility_id(facility)

        return facility

    def process_hfr_facility(self, hfr_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert HFR facility data to standard schema"""
        facility = {
            "facility_id": "",
            "source_ids": {
                "hfr_id": hfr_data.get("facility_id") or hfr_data.get("hfr_id"),
                "osm_id": None,
                "data_gov_id": None,
                "rhs_code": None,
                "nhm_code": None
            },
            "name": hfr_data.get("facility_name") or hfr_data.get("name"),
            "type": hfr_data.get("facility_type", "Unknown"),
            "subtype": hfr_data.get("facility_subtype"),
            "ownership": hfr_data.get("ownership_type", "Unknown"),
            "system_of_medicine": hfr_data.get("system_of_medicine", "Modern"),
            "address": {
                "line1": hfr_data.get("address_line1"),
                "line2": hfr_data.get("address_line2"),
                "village": hfr_data.get("village"),
                "taluk": hfr_data.get("taluk"),
                "district": hfr_data.get("district"),
                "pincode": hfr_data.get("pincode"),
                "state": "Karnataka"
            },
            "location": {
                "latitude": hfr_data.get("latitude"),
                "longitude": hfr_data.get("longitude"),
                "accuracy": "exact"
            } if hfr_data.get("latitude") else None,
            "contact": {
                "phone": hfr_data.get("phone") or hfr_data.get("contact_number"),
                "email": hfr_data.get("email"),
                "website": hfr_data.get("website")
            },
            "capacity": {
                "beds": hfr_data.get("total_beds"),
                "doctors": hfr_data.get("total_doctors"),
                "nurses": hfr_data.get("total_nurses"),
                "staff_total": hfr_data.get("total_staff")
            },
            "operational_status": "24x7" if hfr_data.get("is_24x7") else "Regular",
            "services": hfr_data.get("services", []),
            "rural_urban": hfr_data.get("location_type"),
            "nfhs_district_code": None,
            "data_sources": ["HFR"],
            "quality_score": 0.95,  # HFR is official registry
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "last_verified": None,
            "notes": None
        }

        facility["facility_id"] = self.generate_facility_id(facility)
        return facility

    def process_rhs_facility(self, rhs_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert RHS district summary to facility records

        RHS data is typically aggregated, so this creates summary records
        """
        district = rhs_data.get("district")
        facilities = []

        # Create aggregate records for PHCs, CHCs, etc.
        facility_types = [
            ("phc", "PHC"),
            ("chc", "CHC"),
            ("sub_centres", "SC"),
            ("district_hospital", "Hospital"),
            ("sub_divisional_hospital", "Hospital")
        ]

        for field, fac_type in facility_types:
            count = rhs_data.get(field)
            if count and count > 0:
                facility = {
                    "facility_id": f"FAC_RHS_{district.upper()}_{fac_type}",
                    "source_ids": {
                        "hfr_id": None,
                        "osm_id": None,
                        "data_gov_id": None,
                        "rhs_code": f"RHS_{district}_{fac_type}",
                        "nhm_code": None
                    },
                    "name": f"{fac_type} - {district} District (Aggregated)",
                    "type": fac_type,
                    "subtype": None,
                    "ownership": "Public",
                    "system_of_medicine": "Modern",
                    "address": {
                        "line1": None,
                        "line2": None,
                        "village": None,
                        "taluk": None,
                        "district": district,
                        "pincode": None,
                        "state": "Karnataka"
                    },
                    "location": None,
                    "contact": {},
                    "capacity": {
                        "beds": rhs_data.get("beds"),
                        "doctors": rhs_data.get("doctors"),
                        "nurses": rhs_data.get("nurses"),
                        "staff_total": None
                    },
                    "operational_status": "Unknown",
                    "services": [],
                    "rural_urban": "Rural",
                    "nfhs_district_code": None,
                    "data_sources": ["RHS"],
                    "quality_score": 0.6,  # Aggregated data
                    "last_updated": datetime.now().strftime("%Y-%m-%d"),
                    "last_verified": None,
                    "notes": f"Aggregate count: {count} facilities in district"
                }
                facilities.append(facility)

        return facilities

    def validate_facility(self, facility: Dict[str, Any]) -> bool:
        """
        Validate facility against schema

        Args:
            facility: Facility dict

        Returns:
            True if valid
        """
        required_fields = ["facility_id", "name", "type", "address", "state"]

        for field in required_fields:
            if field not in facility or not facility[field]:
                logger.warning(f"Missing required field: {field}")
                return False

        # Validate address has at least district and state
        if not facility.get("address", {}).get("district"):
            logger.warning("Missing district in address")
            return False

        return True

    def save_facilities(self, filename: str = "facilities.csv"):
        """Save processed facilities"""
        if not self.facilities:
            logger.warning("No facilities to save")
            return

        # Convert to DataFrame
        df = pd.json_normalize(self.facilities)

        # Save CSV
        csv_path = self.output_dir / filename
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(df)} facilities to {csv_path}")

        # Save JSON
        json_path = self.output_dir / filename.replace(".csv", ".json")
        data = {
            "processed_at": datetime.now().isoformat(),
            "total_facilities": len(self.facilities),
            "facilities": self.facilities
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved JSON to {json_path}")


def main():
    """Main execution function"""
    logger.info("Starting facility data processing")

    processor = FacilityProcessor()

    # Example: Process OSM data
    osm_data_path = Path("dataset/raw/osm/karnataka_health_osm.json")
    if osm_data_path.exists():
        with open(osm_data_path, "r") as f:
            osm_data = json.load(f)

        for osm_facility in osm_data.get("facilities", []):
            facility = processor.process_osm_facility(osm_facility)
            if processor.validate_facility(facility):
                processor.facilities.append(facility)

        logger.info(f"Processed {len(processor.facilities)} OSM facilities")

    # Save processed facilities
    processor.save_facilities()

    logger.info("Facility processing completed")


if __name__ == "__main__":
    main()
