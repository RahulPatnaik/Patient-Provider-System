"""
ABDM Healthcare Professionals Registry (HPR) Scraper for Karnataka

Fetches verified healthcare professional data from the official ABDM HPR portal.
Uses authenticated API with ABDM Gateway credentials.

Supports:
- Professional search by state/district
- HPR ID verification
- Multiple professional types (Doctor, Nurse, etc.)
- All systems of medicine
"""

import asyncio
import json
import logging
import os
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
ABDM_GATEWAY_URL = "https://live.abdm.gov.in/gateway/v0.5/sessions"
HPR_BASE_URL = "https://hpr.abdm.gov.in"
TIMEOUT = 30.0
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 2.0


class HPRScraper:
    """Scraper for ABDM Healthcare Professionals Registry"""

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        output_dir: str = "dataset/raw/hpr"
    ):
        self.client_id = client_id or os.getenv("ABDM_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("ABDM_CLIENT_SECRET")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.access_token: Optional[str] = None
        self.http_client: Optional[httpx.AsyncClient] = None
        self.providers: List[Dict[str, Any]] = []

    async def __aenter__(self):
        """Async context manager entry"""
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT),
            headers={
                "User-Agent": "Karnataka-Healthcare-Research/1.0",
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            follow_redirects=True
        )

        # Authenticate on entry
        if self.client_id and self.client_secret:
            await self.authenticate()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.http_client:
            await self.http_client.aclose()

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def authenticate(self) -> str:
        """
        Authenticate with ABDM Gateway to get access token

        Returns:
            Access token string
        """
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "ABDM credentials not provided. "
                "Set ABDM_CLIENT_ID and ABDM_CLIENT_SECRET environment variables "
                "or pass to constructor."
            )

        logger.info("Authenticating with ABDM Gateway...")

        payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "grantType": "client_credentials"
        }

        response = await self.http_client.post(ABDM_GATEWAY_URL, json=payload)
        response.raise_for_status()

        data = response.json()
        self.access_token = data["accessToken"]

        logger.info(f"Authentication successful. Token expires in {data['expiresIn']} seconds")
        return self.access_token

    def _get_headers(self) -> Dict[str, str]:
        """Get headers with Bearer token"""
        if not self.access_token:
            raise ValueError("Not authenticated. Call authenticate() first.")

        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

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
        """Make HTTP request with retry logic and authentication"""
        logger.debug(f"Making {method} request to {url}")

        # Add authentication headers
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"].update(self._get_headers())

        response = await self.http_client.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    async def search_professionals(
        self,
        state: str = "Karnataka",
        professional_type: str = "Doctor",
        system_of_medicine: str = None,
        district: str = None
    ) -> List[Dict[str, Any]]:
        """
        Search for healthcare professionals

        Args:
            state: State name (Karnataka)
            professional_type: Doctor, Nurse, etc.
            system_of_medicine: Modern Medicine, Ayurveda, etc.
            district: District name

        Returns:
            List of professionals
        """
        logger.info(f"Searching {professional_type}s in {state}")

        # Note: This is a hypothetical endpoint structure
        # Actual HPR API structure needs to be discovered from documentation
        # Visit: http://hpr.abdm.gov.in/apidocuments

        search_params = {
            "state": state,
            "professionalType": professional_type
        }

        if system_of_medicine:
            search_params["systemOfMedicine"] = system_of_medicine
        if district:
            search_params["district"] = district

        try:
            # Attempt 1: Try search endpoint
            response = await self._make_request(
                "GET",
                HPR_SEARCH_URL,
                params=search_params
            )
            data = response.json()

            if isinstance(data, dict):
                professionals = data.get("results", data.get("data", []))
            elif isinstance(data, list):
                professionals = data
            else:
                professionals = []

            logger.info(f"Found {len(professionals)} professionals")
            return professionals

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code}")
            logger.warning("HPR API endpoint may require authentication or different URL")
            return []
        except Exception as e:
            logger.error(f"Error searching HPR: {e}")
            return []

    async def verify_professional(
        self,
        hpr_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Verify and get details for a specific HPR ID

        Args:
            hpr_id: Healthcare Professional ID (14-digit)

        Returns:
            Professional details or None
        """
        logger.debug(f"Verifying HPR ID: {hpr_id}")

        try:
            response = await self._make_request(
                "GET",
                f"{HPR_VERIFY_URL}/{hpr_id}"
            )
            return response.json()
        except Exception as e:
            logger.error(f"Error verifying {hpr_id}: {e}")
            return None

    async def scrape_karnataka_professionals(
        self,
        districts: Optional[List[str]] = None,
        professional_types: List[str] = ["Doctor"]
    ) -> List[Dict[str, Any]]:
        """
        Scrape all healthcare professionals in Karnataka

        Args:
            districts: List of district names (if None, searches entire state)
            professional_types: Types to search (Doctor, Nurse, etc.)

        Returns:
            List of all professionals
        """
        all_professionals = []

        for prof_type in professional_types:
            if districts:
                # Search district by district
                for district in districts:
                    logger.info(f"Processing {prof_type}s in {district}")

                    professionals = await self.search_professionals(
                        state="Karnataka",
                        professional_type=prof_type,
                        district=district
                    )

                    all_professionals.extend(professionals)
                    await asyncio.sleep(RATE_LIMIT_DELAY)
            else:
                # Search entire state
                professionals = await self.search_professionals(
                    state="Karnataka",
                    professional_type=prof_type
                )
                all_professionals.extend(professionals)

            await asyncio.sleep(RATE_LIMIT_DELAY)

        self.providers = all_professionals
        logger.info(f"Total professionals scraped: {len(all_professionals)}")
        return all_professionals

    def save_raw_data(self, filename: str = "karnataka_healthcare_professionals.json"):
        """Save raw provider data to JSON file"""
        output_path = self.output_dir / filename

        data = {
            "scraped_at": datetime.now().isoformat(),
            "source": "ABDM Healthcare Professionals Registry (HPR)",
            "state": "Karnataka",
            "total_professionals": len(self.providers),
            "providers": self.providers
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(self.providers)} professionals to {output_path}")

    def save_csv(self, filename: str = "karnataka_healthcare_professionals.csv"):
        """Save providers to CSV format"""
        import csv

        if not self.providers:
            logger.warning("No providers to save")
            return

        output_path = self.output_dir / filename

        # Flatten provider data for CSV
        fieldnames = set()
        for provider in self.providers:
            fieldnames.update(self._flatten_dict(provider).keys())

        fieldnames = sorted(fieldnames)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for provider in self.providers:
                flat_data = self._flatten_dict(provider)
                writer.writerow(flat_data)

        logger.info(f"Saved {len(self.providers)} providers to {output_path}")

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
    logger.info("="*70)
    logger.info("ABDM HPR Professional Search - Karnataka")
    logger.info("="*70)

    # Check for credentials
    client_id = os.getenv("ABDM_CLIENT_ID")
    client_secret = os.getenv("ABDM_CLIENT_SECRET")

    if not client_id or not client_secret:
        logger.error("ABDM credentials not found!")
        logger.info("")
        logger.info("To use this scraper:")
        logger.info("1. Register at https://sandbox.abdm.gov.in")
        logger.info("2. Get your clientId and clientSecret")
        logger.info("3. Set environment variables:")
        logger.info("   export ABDM_CLIENT_ID='your_client_id'")
        logger.info("   export ABDM_CLIENT_SECRET='your_client_secret'")
        logger.info("")
        logger.info("Note: Professional search API endpoint needs to be discovered")
        logger.info("from http://hpr.abdm.gov.in/apidocuments")
        logger.info("")
        logger.info("Alternative: Use public 'Know Your Doctor' portal with Selenium")
        return

    async with HPRScraper(client_id, client_secret) as scraper:
        logger.info("Authentication successful!")
        logger.info("")
        logger.info("⚠️  Note: Professional search endpoint not yet discovered")
        logger.info("The Master Data APIs are implemented in abdm_master_data.py")
        logger.info("To search professionals, we need to discover the search endpoint")
        logger.info("from the full API documentation at:")
        logger.info("  http://hpr.abdm.gov.in/apidocuments")
        logger.info("")
        logger.info("Alternative approaches:")
        logger.info("1. Web scraping the public 'Know Your Doctor' portal")
        logger.info("2. Contact ABDM support for professional search API docs")
        logger.info("3. Use Practo/other sources (see PROVIDER_DATA_SOURCES.md)")


if __name__ == "__main__":
    asyncio.run(main())
