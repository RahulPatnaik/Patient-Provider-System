"""
OpenStreetMap (OSM) Healthcare Facilities Query for Karnataka

This script uses the Overpass API to extract all healthcare-related POIs
(hospitals, clinics, pharmacies, etc.) in Karnataka state.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
TIMEOUT = 300.0  # Longer timeout for large queries
MAX_RETRIES = 3


class OSMHealthcareQuery:
    """Query OpenStreetMap for healthcare facilities in Karnataka"""

    def __init__(self, output_dir: str = "dataset/raw/osm"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.facilities: List[Dict[str, Any]] = []

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=4, max=60)
    )
    async def query_overpass(self, query: str) -> Dict[str, Any]:
        """
        Execute Overpass API query

        Args:
            query: Overpass QL query string

        Returns:
            Query results as dictionary
        """
        logger.info("Executing Overpass query...")
        logger.debug(f"Query: {query}")

        async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
            response = await client.post(
                OVERPASS_API_URL,
                data={"data": query},
                headers={
                    "User-Agent": "Karnataka-Healthcare-Research/1.0",
                    "Accept": "application/json"
                }
            )
            response.raise_for_status()
            return response.json()

    def build_karnataka_healthcare_query(self) -> str:
        """
        Build Overpass query for all healthcare facilities in Karnataka

        Returns:
            Overpass QL query string
        """
        query = """
        [out:json][timeout:180];

        // Define Karnataka area
        area["name"="Karnataka"]["admin_level"=4]["boundary"="administrative"]->.karnataka;

        (
          // Hospitals
          node["amenity"="hospital"](area.karnataka);
          way["amenity"="hospital"](area.karnataka);
          relation["amenity"="hospital"](area.karnataka);

          // Clinics
          node["amenity"="clinic"](area.karnataka);
          way["amenity"="clinic"](area.karnataka);

          // Doctors
          node["amenity"="doctors"](area.karnataka);
          way["amenity"="doctors"](area.karnataka);

          // Pharmacies
          node["amenity"="pharmacy"](area.karnataka);
          way["amenity"="pharmacy"](area.karnataka);

          // Healthcare facilities (general)
          node["healthcare"](area.karnataka);
          way["healthcare"](area.karnataka);

          // Dentists
          node["amenity"="dentist"](area.karnataka);
          way["amenity"="dentist"](area.karnataka);
        );

        // Output with full details
        out body;
        >;
        out skel qt;
        """
        return query.strip()

    def build_district_query(self, district_name: str) -> str:
        """
        Build query for a specific district

        Args:
            district_name: Name of the district

        Returns:
            Overpass QL query string
        """
        query = f"""
        [out:json][timeout:90];

        // Define district area
        area["name"="{district_name}"]["admin_level"=5]["boundary"="administrative"]->.district;

        (
          node["amenity"="hospital"](area.district);
          way["amenity"="hospital"](area.district);
          node["amenity"="clinic"](area.district);
          way["amenity"="clinic"](area.district);
          node["amenity"="doctors"](area.district);
          way["amenity"="doctors"](area.district);
          node["amenity"="pharmacy"](area.district);
          way["amenity"="pharmacy"](area.district);
          node["healthcare"](area.district);
          way["healthcare"](area.district);
          node["amenity"="dentist"](area.district);
          way["amenity"="dentist"](area.district);
        );

        out body;
        >;
        out skel qt;
        """
        return query.strip()

    async def fetch_karnataka_healthcare(self) -> List[Dict[str, Any]]:
        """
        Fetch all healthcare facilities in Karnataka

        Returns:
            List of facility dictionaries
        """
        logger.info("Fetching Karnataka healthcare facilities from OSM")

        query = self.build_karnataka_healthcare_query()

        try:
            result = await self.query_overpass(query)
            elements = result.get("elements", [])

            # Filter and process elements
            facilities = self._process_osm_elements(elements)
            self.facilities = facilities

            logger.info(f"Found {len(facilities)} healthcare facilities")
            return facilities

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code}")
            if e.response.status_code == 429:
                logger.error("Rate limited. Please wait and try again later.")
            return []
        except Exception as e:
            logger.error(f"Error fetching OSM data: {e}")
            return []

    async def fetch_by_districts(
        self,
        districts: List[str],
        delay_between_queries: float = 5.0
    ) -> List[Dict[str, Any]]:
        """
        Fetch facilities district by district (to avoid timeout on large queries)

        Args:
            districts: List of district names
            delay_between_queries: Seconds to wait between queries

        Returns:
            Combined list of facilities
        """
        all_facilities = []

        for district in districts:
            logger.info(f"Querying {district} district...")

            query = self.build_district_query(district)

            try:
                result = await self.query_overpass(query)
                elements = result.get("elements", [])
                facilities = self._process_osm_elements(elements)

                logger.info(f"Found {len(facilities)} facilities in {district}")
                all_facilities.extend(facilities)

                # Add district info to each facility
                for facility in facilities:
                    facility["queried_district"] = district

                # Rate limiting
                await asyncio.sleep(delay_between_queries)

            except Exception as e:
                logger.error(f"Error querying {district}: {e}")
                continue

        self.facilities = all_facilities
        logger.info(f"Total facilities from all districts: {len(all_facilities)}")
        return all_facilities

    def _process_osm_elements(self, elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process and standardize OSM elements

        Args:
            elements: Raw OSM elements

        Returns:
            Processed facility list
        """
        facilities = []

        for element in elements:
            # Skip node geometry (we only want the POIs)
            if element.get("type") not in ["node", "way", "relation"]:
                continue

            tags = element.get("tags", {})

            # Skip if no name or healthcare-related tag
            if not tags or not self._is_healthcare_facility(tags):
                continue

            # Build standardized facility record
            facility = {
                "osm_id": element.get("id"),
                "osm_type": element.get("type"),
                "name": tags.get("name", "Unnamed"),
                "amenity": tags.get("amenity"),
                "healthcare": tags.get("healthcare"),
                "healthcare_speciality": tags.get("healthcare:speciality"),
                "operator": tags.get("operator"),
                "operator_type": tags.get("operator:type"),
                "address": {
                    "street": tags.get("addr:street"),
                    "housenumber": tags.get("addr:housenumber"),
                    "city": tags.get("addr:city"),
                    "district": tags.get("addr:district"),
                    "state": tags.get("addr:state"),
                    "postcode": tags.get("addr:postcode"),
                },
                "contact": {
                    "phone": tags.get("phone") or tags.get("contact:phone"),
                    "email": tags.get("email") or tags.get("contact:email"),
                    "website": tags.get("website") or tags.get("contact:website"),
                },
                "opening_hours": tags.get("opening_hours"),
                "beds": tags.get("beds"),
                "emergency": tags.get("emergency"),
                "wheelchair": tags.get("wheelchair"),
                "all_tags": tags
            }

            # Add coordinates
            if element.get("type") == "node":
                facility["location"] = {
                    "latitude": element.get("lat"),
                    "longitude": element.get("lon")
                }
            elif element.get("type") == "way" and element.get("center"):
                facility["location"] = {
                    "latitude": element["center"].get("lat"),
                    "longitude": element["center"].get("lon")
                }

            facilities.append(facility)

        return facilities

    def _is_healthcare_facility(self, tags: Dict[str, Any]) -> bool:
        """Check if OSM element is a healthcare facility"""
        healthcare_amenities = [
            "hospital", "clinic", "doctors", "pharmacy", "dentist",
            "health_centre", "nursing_home"
        ]

        return (
            tags.get("amenity") in healthcare_amenities or
            "healthcare" in tags or
            "medical" in tags
        )

    def save_raw_data(self, filename: str = "karnataka_health_osm.json"):
        """Save raw OSM data to JSON file"""
        output_path = self.output_dir / filename

        data = {
            "extracted_at": datetime.now().isoformat(),
            "source": "OpenStreetMap (Overpass API)",
            "state": "Karnataka",
            "total_facilities": len(self.facilities),
            "facilities": self.facilities
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(self.facilities)} facilities to {output_path}")

    def save_csv(self, filename: str = "karnataka_health_osm.csv"):
        """Save facilities to CSV format"""
        import csv

        if not self.facilities:
            logger.warning("No facilities to save")
            return

        output_path = self.output_dir / filename

        # Flatten facility data
        flat_facilities = []
        for facility in self.facilities:
            flat = {
                "osm_id": facility.get("osm_id"),
                "osm_type": facility.get("osm_type"),
                "name": facility.get("name"),
                "amenity": facility.get("amenity"),
                "healthcare": facility.get("healthcare"),
                "healthcare_speciality": facility.get("healthcare_speciality"),
                "operator": facility.get("operator"),
                "operator_type": facility.get("operator_type"),
                "latitude": facility.get("location", {}).get("latitude"),
                "longitude": facility.get("location", {}).get("longitude"),
                "street": facility.get("address", {}).get("street"),
                "city": facility.get("address", {}).get("city"),
                "district": facility.get("address", {}).get("district"),
                "postcode": facility.get("address", {}).get("postcode"),
                "phone": facility.get("contact", {}).get("phone"),
                "email": facility.get("contact", {}).get("email"),
                "website": facility.get("contact", {}).get("website"),
                "opening_hours": facility.get("opening_hours"),
                "beds": facility.get("beds"),
                "emergency": facility.get("emergency"),
                "wheelchair": facility.get("wheelchair"),
                "queried_district": facility.get("queried_district")
            }
            flat_facilities.append(flat)

        fieldnames = flat_facilities[0].keys() if flat_facilities else []

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_facilities)

        logger.info(f"Saved {len(flat_facilities)} facilities to {output_path}")


# Karnataka districts
KARNATAKA_DISTRICTS = [
    "Bagalkot", "Ballari", "Belagavi", "Bengaluru Rural", "Bengaluru Urban",
    "Bidar", "Chamarajanagar", "Chikballapur", "Chikkamagaluru", "Chitradurga",
    "Dakshina Kannada", "Davanagere", "Dharwad", "Gadag", "Hassan",
    "Haveri", "Kalaburagi", "Kodagu", "Kolar", "Koppal",
    "Mandya", "Mysuru", "Raichur", "Ramanagara", "Shivamogga",
    "Tumakuru", "Udupi", "Uttara Kannada", "Vijayapura", "Yadgir"
]


async def main():
    """Main execution function"""
    logger.info("Starting OSM healthcare query for Karnataka")

    query_tool = OSMHealthcareQuery()

    # Option 1: Query entire Karnataka (may timeout for large results)
    # facilities = await query_tool.fetch_karnataka_healthcare()

    # Option 2: Query district by district (recommended)
    facilities = await query_tool.fetch_by_districts(KARNATAKA_DISTRICTS, delay_between_queries=3.0)

    # Save data
    if facilities:
        query_tool.save_raw_data()
        query_tool.save_csv()
        logger.info("OSM query completed successfully")
    else:
        logger.warning("No facilities found")


if __name__ == "__main__":
    asyncio.run(main())
