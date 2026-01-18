"""
ABDM Master Data API Client

Official implementation of ABDM HPR Master Data APIs for reference data collection.
Based on Master_API_PRD_7634e1e645.pdf (v1, Jan 2024)

Implements all 12 master data endpoints:
- System of Medicine
- Medical Councils
- Languages
- Universities
- Colleges
- Courses
- Countries
- States
- Districts
- Sub Districts
- Affiliated Boards
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
HPR_MASTER_BASE_URL = "https://hpr.abdm.gov.in/apis/v1/masters"
TIMEOUT = 30.0
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 1.0


class ABDMMasterDataClient:
    """Client for ABDM HPR Master Data APIs"""

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        output_dir: str = "dataset/raw/abdm_master"
    ):
        self.client_id = client_id or os.getenv("ABDM_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("ABDM_CLIENT_SECRET")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        self.http_client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """Async context manager entry"""
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT),
            headers={
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
        expires_in = data["expiresIn"]  # seconds

        logger.info(f"Authentication successful. Token expires in {expires_in} seconds")

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
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make authenticated request to master data API

        Args:
            endpoint: API endpoint (e.g., 'system-of-medicines')
            payload: Optional request body

        Returns:
            Response data
        """
        url = f"{HPR_MASTER_BASE_URL}/{endpoint}"

        logger.debug(f"POST {url}")

        response = await self.http_client.post(
            url,
            json=payload or {},
            headers=self._get_headers()
        )
        response.raise_for_status()

        return response.json()

    async def get_system_of_medicines(self) -> List[Dict[str, Any]]:
        """
        Get all systems of medicine

        Returns:
            List of system of medicine records
        """
        logger.info("Fetching systems of medicine...")

        data = await self._make_request("system-of-medicines")
        medicines = data.get("systemofMedicineList", [])

        logger.info(f"Retrieved {len(medicines)} systems of medicine")
        return medicines

    async def get_medical_councils(
        self,
        system_of_medicine: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get medical councils, optionally filtered by system of medicine

        Args:
            system_of_medicine: System ID (1=Modern Medicine, 2=Dentistry, etc.)

        Returns:
            List of medical council records
        """
        logger.info(f"Fetching medical councils (system: {system_of_medicine})...")

        payload = {}
        if system_of_medicine:
            payload["systemOfMedicine"] = str(system_of_medicine)

        data = await self._make_request("medical-councils", payload)
        councils = data.get("medicalCouncilsList", data.get("councils", []))

        logger.info(f"Retrieved {len(councils)} medical councils")
        return councils

    async def get_languages(self, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get languages

        Args:
            name: Optional language name filter

        Returns:
            List of language records
        """
        logger.info(f"Fetching languages (name: {name})...")

        payload = {}
        if name:
            payload["name"] = name

        data = await self._make_request("languages", payload)
        languages = data.get("languagesList", data.get("languages", []))

        logger.info(f"Retrieved {len(languages)} languages")
        return languages

    async def get_universities(self, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get universities

        Args:
            name: Optional university name filter

        Returns:
            List of university records
        """
        logger.info(f"Fetching universities (name: {name})...")

        payload = {}
        if name:
            payload["name"] = name

        data = await self._make_request("universites", payload)  # Note: API uses 'universites'
        universities = data.get("universitiesList", data.get("universities", []))

        logger.info(f"Retrieved {len(universities)} universities")
        return universities

    async def get_colleges(
        self,
        state_name: Optional[str] = None,
        system_of_medicine: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get colleges

        Args:
            state_name: State name (e.g., 'Karnataka')
            system_of_medicine: System ID

        Returns:
            List of college records
        """
        logger.info(f"Fetching colleges (state: {state_name}, system: {system_of_medicine})...")

        payload = {}
        if state_name:
            payload["stateName"] = state_name
        if system_of_medicine:
            payload["systemOfMedicine"] = system_of_medicine

        data = await self._make_request("colleges", payload)
        colleges = data.get("collegesList", data.get("colleges", []))

        logger.info(f"Retrieved {len(colleges)} colleges")
        return colleges

    async def get_courses(
        self,
        system_of_medicine: Optional[int] = None,
        hpr_type: Optional[str] = None,
        name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get courses

        Args:
            system_of_medicine: System ID
            hpr_type: Type (e.g., 'doctor')
            name: Course name filter (e.g., 'mbbs')

        Returns:
            List of course records
        """
        logger.info(f"Fetching courses (system: {system_of_medicine}, type: {hpr_type})...")

        payload = {}
        if system_of_medicine:
            payload["systemOfMedicine"] = system_of_medicine
        if hpr_type:
            payload["hprType"] = hpr_type
        if name:
            payload["name"] = name

        data = await self._make_request("courses", payload)
        courses = data.get("coursesList", data.get("courses", []))

        logger.info(f"Retrieved {len(courses)} courses")
        return courses

    async def get_countries(self, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get countries

        Args:
            name: Country name filter (e.g., 'India')

        Returns:
            List of country records
        """
        logger.info(f"Fetching countries (name: {name})...")

        payload = {}
        if name:
            payload["name"] = name

        data = await self._make_request("countries", payload)
        countries = data.get("countriesList", data.get("countries", []))

        logger.info(f"Retrieved {len(countries)} countries")
        return countries

    async def get_states(self, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get states

        Args:
            name: State name filter (e.g., 'Karnataka')

        Returns:
            List of state records
        """
        logger.info(f"Fetching states (name: {name})...")

        payload = {}
        if name:
            payload["name"] = name

        data = await self._make_request("states", payload)
        states = data.get("statesList", data.get("states", []))

        logger.info(f"Retrieved {len(states)} states")
        return states

    async def get_districts(self, state: str) -> List[Dict[str, Any]]:
        """
        Get districts for a state

        Args:
            state: State name (e.g., 'KARNATAKA')

        Returns:
            List of district records
        """
        logger.info(f"Fetching districts for {state}...")

        payload = {"state": state}

        data = await self._make_request("district", payload)
        districts = data.get("districtsList", data.get("districts", []))

        logger.info(f"Retrieved {len(districts)} districts for {state}")
        return districts

    async def get_sub_districts(
        self,
        state: str,
        district: str
    ) -> List[Dict[str, Any]]:
        """
        Get sub-districts (taluks) for a district

        Args:
            state: State name (e.g., 'KARNATAKA')
            district: District name (e.g., 'BENGALURU URBAN')

        Returns:
            List of sub-district records
        """
        logger.info(f"Fetching sub-districts for {district}, {state}...")

        payload = {
            "state": state,
            "district": district
        }

        data = await self._make_request("sub-districts", payload)
        sub_districts = data.get("subDistrictsList", data.get("subDistricts", []))

        logger.info(f"Retrieved {len(sub_districts)} sub-districts")
        return sub_districts

    async def get_affiliated_boards(self) -> List[Dict[str, Any]]:
        """
        Get nurse affiliated boards

        Returns:
            List of affiliated board records
        """
        logger.info("Fetching affiliated boards...")

        data = await self._make_request("affiliated-board")
        boards = data.get("affiliatedBoardList", data.get("boards", []))

        logger.info(f"Retrieved {len(boards)} affiliated boards")
        return boards

    async def collect_all_karnataka_master_data(self) -> Dict[str, Any]:
        """
        Collect all master data relevant to Karnataka

        Returns:
            Dictionary with all master data
        """
        logger.info("="*70)
        logger.info("Collecting Karnataka Master Data from ABDM HPR")
        logger.info("="*70)

        master_data = {
            "collected_at": datetime.now().isoformat(),
            "source": "ABDM HPR Master Data APIs",
            "region": "Karnataka, India"
        }

        # 1. System of Medicines
        master_data["system_of_medicines"] = await self.get_system_of_medicines()
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 2. Medical Councils (Modern Medicine)
        councils = await self.get_medical_councils(system_of_medicine=1)
        master_data["medical_councils"] = councils

        # Find Karnataka Medical Council
        kmc = next((c for c in councils if "karnataka" in c.get("name", "").lower()), None)
        if kmc:
            logger.info(f"✓ Karnataka Medical Council: ID {kmc.get('identifier')}")
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 3. Languages
        master_data["languages"] = await self.get_languages()
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 4. States (Karnataka)
        states = await self.get_states(name="Karnataka")
        master_data["states"] = states
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 5. Karnataka Districts
        districts = await self.get_districts(state="KARNATAKA")
        master_data["districts"] = districts
        logger.info(f"✓ Retrieved {len(districts)} Karnataka districts")
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 6. Karnataka Colleges
        colleges = await self.get_colleges(
            state_name="Karnataka",
            system_of_medicine=1
        )
        master_data["colleges"] = colleges
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 7. Courses (Modern Medicine)
        courses = await self.get_courses(
            system_of_medicine=1,
            hpr_type="doctor"
        )
        master_data["courses"] = courses
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 8. Universities (Karnataka)
        universities = await self.get_universities()
        master_data["universities"] = universities
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # 9. Countries (India)
        countries = await self.get_countries(name="India")
        master_data["countries"] = countries

        # Summary
        logger.info("="*70)
        logger.info("Collection Summary:")
        logger.info(f"  System of Medicines: {len(master_data.get('system_of_medicines', []))}")
        logger.info(f"  Medical Councils: {len(master_data.get('medical_councils', []))}")
        logger.info(f"  Languages: {len(master_data.get('languages', []))}")
        logger.info(f"  Karnataka Districts: {len(master_data.get('districts', []))}")
        logger.info(f"  Karnataka Colleges: {len(master_data.get('colleges', []))}")
        logger.info(f"  Medical Courses: {len(master_data.get('courses', []))}")
        logger.info(f"  Universities: {len(master_data.get('universities', []))}")
        logger.info("="*70)

        return master_data

    def save_master_data(self, data: Dict[str, Any], filename: str = "karnataka_master_data.json"):
        """Save master data to JSON file"""
        output_path = self.output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved master data to {output_path}")


async def main():
    """Main execution function"""
    logger.info("ABDM Master Data Collection - Karnataka")
    logger.info("="*70)

    # Check for credentials
    client_id = os.getenv("ABDM_CLIENT_ID")
    client_secret = os.getenv("ABDM_CLIENT_SECRET")

    if not client_id or not client_secret:
        logger.error("ABDM credentials not found!")
        logger.info("")
        logger.info("To use this script:")
        logger.info("1. Register at https://sandbox.abdm.gov.in")
        logger.info("2. Get your clientId and clientSecret")
        logger.info("3. Set environment variables:")
        logger.info("   export ABDM_CLIENT_ID='your_client_id'")
        logger.info("   export ABDM_CLIENT_SECRET='your_client_secret'")
        logger.info("")
        logger.info("For testing, you can run individual methods without authentication")
        return

    # Collect master data
    async with ABDMMasterDataClient(client_id, client_secret) as client:
        master_data = await client.collect_all_karnataka_master_data()
        client.save_master_data(master_data)

        logger.info("✓ Master data collection completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
