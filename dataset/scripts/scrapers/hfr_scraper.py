"""
ABDM Health Facility Registry (HFR) API Scraper for Karnataka

This script queries the ABDM Health Facility Registry to fetch all healthcare
facilities in Karnataka state. Uses async HTTP requests with retry logic.
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
HFR_BASE_URL = "https://facility.abdm.gov.in"
HFR_API_URL = "https://facility.abdm.gov.in/nhrr"  # Public search endpoint
TIMEOUT = 30.0
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 2.0  # Seconds between requests


class HFRScraper:
    """Scraper for ABDM Health Facility Registry"""

    def __init__(self, output_dir: str = "dataset/raw/hfr"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client: Optional[httpx.AsyncClient] = None
        self.facilities: List[Dict[str, Any]] = []

    async def __aenter__(self):
        """Async context manager entry"""
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT),
            headers={
                "User-Agent": "Karnataka-Healthcare-Research/1.0",
                "Accept": "application/json"
            },
            follow_redirects=True
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.client:
            await self.client.aclose()

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> httpx.Response:
        """Make HTTP request with retry logic"""
        logger.debug(f"Making {method} request to {url}")
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    async def search_facilities_by_state(
        self,
        state_name: str = "Karnataka"
    ) -> List[Dict[str, Any]]:
        """
        Search facilities by state name

        Note: This is a placeholder implementation. The actual HFR API
        requires authentication and proper endpoints. This method needs
        to be updated based on actual API documentation.

        Args:
            state_name: Name of the state (default: Karnataka)

        Returns:
            List of facility dictionaries
        """
        logger.info(f"Searching facilities in {state_name}")

        # NOTE: This endpoint structure is hypothetical
        # Actual implementation requires:
        # 1. Authentication/API key
        # 2. Correct endpoint discovery
        # 3. Pagination handling

        search_url = f"{HFR_API_URL}/search"
        params = {
            "state": state_name,
            "limit": 100,  # Adjust based on actual API
            "offset": 0
        }

        try:
            response = await self._make_request("GET", search_url, params=params)
            data = response.json()

            # Extract facilities (structure depends on actual API response)
            if isinstance(data, dict):
                facilities = data.get("facilities", data.get("results", []))
            elif isinstance(data, list):
                facilities = data
            else:
                facilities = []

            logger.info(f"Found {len(facilities)} facilities")
            return facilities

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Error searching facilities: {e}")
            return []

    async def get_facility_details(self, facility_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific facility

        Args:
            facility_id: Facility ID from HFR

        Returns:
            Facility details dictionary or None
        """
        logger.debug(f"Fetching details for facility {facility_id}")

        detail_url = f"{HFR_API_URL}/facility/{facility_id}"

        try:
            response = await self._make_request("GET", detail_url)
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching facility {facility_id}: {e}")
            return None

    async def search_facilities_by_district(
        self,
        state: str = "Karnataka",
        district: str = None
    ) -> List[Dict[str, Any]]:
        """
        Search facilities by district

        Args:
            state: State name
            district: District name

        Returns:
            List of facilities
        """
        logger.info(f"Searching facilities in {district}, {state}")

        search_url = f"{HFR_API_URL}/search"
        params = {
            "state": state,
            "district": district
        }

        try:
            response = await self._make_request("GET", search_url, params=params)
            data = response.json()

            if isinstance(data, dict):
                return data.get("facilities", data.get("results", []))
            return data if isinstance(data, list) else []

        except Exception as e:
            logger.error(f"Error searching {district}: {e}")
            return []

    async def scrape_karnataka_facilities(
        self,
        districts: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape all facilities in Karnataka

        Args:
            districts: List of district names (if None, searches entire state)

        Returns:
            List of all facilities
        """
        all_facilities = []

        if districts:
            # Search district by district
            for district in districts:
                logger.info(f"Processing district: {district}")
                facilities = await self.search_facilities_by_district(
                    state="Karnataka",
                    district=district
                )
                all_facilities.extend(facilities)

                # Rate limiting
                await asyncio.sleep(RATE_LIMIT_DELAY)
        else:
            # Search entire state
            all_facilities = await self.search_facilities_by_state("Karnataka")

        self.facilities = all_facilities
        logger.info(f"Total facilities scraped: {len(all_facilities)}")
        return all_facilities

    def save_raw_data(self, filename: str = "karnataka_facilities.json"):
        """Save raw facility data to JSON file"""
        output_path = self.output_dir / filename

        data = {
            "scraped_at": datetime.now().isoformat(),
            "source": "ABDM Health Facility Registry",
            "state": "Karnataka",
            "total_facilities": len(self.facilities),
            "facilities": self.facilities
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(self.facilities)} facilities to {output_path}")

    def save_csv(self, filename: str = "karnataka_facilities.csv"):
        """Save facilities to CSV format"""
        import csv

        if not self.facilities:
            logger.warning("No facilities to save")
            return

        output_path = self.output_dir / filename

        # Flatten facility data for CSV
        fieldnames = set()
        for facility in self.facilities:
            fieldnames.update(self._flatten_dict(facility).keys())

        fieldnames = sorted(fieldnames)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for facility in self.facilities:
                flat_data = self._flatten_dict(facility)
                writer.writerow(flat_data)

        logger.info(f"Saved {len(self.facilities)} facilities to {output_path}")

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = "") -> Dict[str, Any]:
        """Flatten nested dictionary for CSV export"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)


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
    logger.info("Starting HFR scraper for Karnataka facilities")

    async with HFRScraper() as scraper:
        # Option 1: Search entire state
        facilities = await scraper.scrape_karnataka_facilities()

        # Option 2: Search district by district (uncomment to use)
        # facilities = await scraper.scrape_karnataka_facilities(KARNATAKA_DISTRICTS)

        # Save data
        if facilities:
            scraper.save_raw_data()
            scraper.save_csv()
            logger.info("HFR scraping completed successfully")
        else:
            logger.warning("No facilities found. Check API endpoints and authentication.")


if __name__ == "__main__":
    asyncio.run(main())
