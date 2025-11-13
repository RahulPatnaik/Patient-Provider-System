"""
Practo Healthcare Provider Scraper for Karnataka

Scrapes doctor and healthcare provider listings from Practo.
Focuses on Karnataka cities (Bangalore, Mysore, Mangalore, etc.)

⚠️ IMPORTANT: Check Practo's Terms of Service before using.
This scraper is for research/educational purposes only.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PRACTO_BASE_URL = "https://www.practo.com"
TIMEOUT = 30.0
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 3.0  # Respectful delay


class PractoScraper:
    """Scraper for Practo doctor listings"""

    def __init__(self, output_dir: str = "dataset/raw/practo"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.client: Optional[httpx.AsyncClient] = None
        self.providers: List[Dict[str, Any]] = []

    async def __aenter__(self):
        """Async context manager entry"""
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT),
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                "Accept-Language": "en-US,en;q=0.5"
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
        wait=wait_exponential(multiplier=2, min=4, max=30)
    )
    async def _fetch_page(self, url: str) -> str:
        """Fetch page HTML with retry logic"""
        logger.debug(f"Fetching {url}")
        response = await self.client.get(url)
        response.raise_for_status()
        return response.text

    async def get_specialties(self, city: str = "bangalore") -> List[str]:
        """
        Get list of medical specialties available in the city

        Args:
            city: City name (bangalore, mysore, etc.)

        Returns:
            List of specialty slugs
        """
        url = f"{PRACTO_BASE_URL}/{city}"

        try:
            html = await self._fetch_page(url)
            soup = BeautifulSoup(html, 'html.parser')

            # Find specialty links (structure may vary)
            specialties = []

            # Common patterns for specialty links
            specialty_links = soup.find_all('a', href=lambda x: x and '/doctor/' in x)

            for link in specialty_links:
                href = link.get('href', '')
                if '/doctor/' in href:
                    # Extract specialty from URL pattern: /bangalore/doctor/[specialty]
                    parts = href.split('/')
                    if len(parts) >= 4:
                        specialty = parts[3]  # Usually the specialty slug
                        if specialty and specialty not in specialties:
                            specialties.append(specialty)

            logger.info(f"Found {len(specialties)} specialties in {city}")
            return specialties[:50]  # Limit to avoid overwhelming

        except Exception as e:
            logger.error(f"Error fetching specialties: {e}")
            # Return common specialties as fallback
            return [
                "general-physician", "dentist", "gynecologist", "pediatrician",
                "dermatologist", "cardiologist", "orthopedist", "ent-specialist",
                "ophthalmologist", "psychiatrist"
            ]

    async def scrape_doctors_by_specialty(
        self,
        city: str,
        specialty: str,
        max_pages: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Scrape doctors for a specific specialty in a city

        Args:
            city: City name
            specialty: Specialty slug
            max_pages: Maximum pages to scrape

        Returns:
            List of doctor dictionaries
        """
        doctors = []

        for page in range(1, max_pages + 1):
            url = f"{PRACTO_BASE_URL}/{city}/doctor/{specialty}"
            if page > 1:
                url += f"?page={page}"

            logger.info(f"Scraping {specialty} page {page} in {city}")

            try:
                html = await self._fetch_page(url)
                soup = BeautifulSoup(html, 'html.parser')

                # Parse doctor listings (structure depends on Practo's HTML)
                # This is a simplified example - actual structure may differ

                doctor_cards = soup.find_all('div', class_='info-section')

                if not doctor_cards:
                    logger.debug(f"No more doctors found on page {page}")
                    break

                for card in doctor_cards:
                    try:
                        doctor = self._parse_doctor_card(card, city, specialty)
                        if doctor:
                            doctors.append(doctor)
                    except Exception as e:
                        logger.warning(f"Error parsing doctor card: {e}")
                        continue

                # Rate limiting
                await asyncio.sleep(RATE_LIMIT_DELAY)

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.debug(f"No more pages for {specialty}")
                    break
                logger.error(f"HTTP error {e.response.status_code}")
                break
            except Exception as e:
                logger.error(f"Error scraping page: {e}")
                break

        logger.info(f"Scraped {len(doctors)} doctors for {specialty} in {city}")
        return doctors

    def _parse_doctor_card(
        self,
        card: Any,
        city: str,
        specialty: str
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a doctor card element

        Args:
            card: BeautifulSoup element
            city: City name
            specialty: Specialty

        Returns:
            Doctor dictionary or None
        """
        # Note: This is a template - actual parsing depends on Practo's HTML structure
        # You'll need to inspect the page and update selectors

        doctor = {
            "source": "Practo",
            "city": city,
            "specialty": specialty,
            "scraped_at": datetime.now().isoformat()
        }

        # Extract name (example selector - may need adjustment)
        name_elem = card.find('h2', class_='doctor-name') or card.find('a', class_='doctor-name')
        if name_elem:
            doctor["name"] = name_elem.text.strip()
        else:
            return None  # Skip if no name

        # Extract other fields
        # Qualification
        qual_elem = card.find('span', class_='qualification')
        if qual_elem:
            doctor["qualifications"] = qual_elem.text.strip()

        # Experience
        exp_elem = card.find('span', class_='experience')
        if exp_elem:
            doctor["experience"] = exp_elem.text.strip()

        # Clinic/Hospital
        clinic_elem = card.find('span', class_='clinic-name')
        if clinic_elem:
            doctor["clinic"] = clinic_elem.text.strip()

        # Location
        loc_elem = card.find('span', class_='location')
        if loc_elem:
            doctor["location"] = loc_elem.text.strip()

        # Consultation fee
        fee_elem = card.find('span', class_='fee')
        if fee_elem:
            doctor["consultation_fee"] = fee_elem.text.strip()

        # Profile URL
        profile_link = card.find('a', href=True)
        if profile_link:
            doctor["profile_url"] = PRACTO_BASE_URL + profile_link['href']

        return doctor

    async def scrape_karnataka_cities(
        self,
        cities: List[str] = ["bangalore", "mysore", "mangalore", "hubli"],
        max_specialties: int = 10,
        max_pages_per_specialty: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Scrape doctors from multiple Karnataka cities

        Args:
            cities: List of city names
            max_specialties: Maximum specialties to scrape per city
            max_pages_per_specialty: Maximum pages per specialty

        Returns:
            Combined list of all doctors
        """
        all_doctors = []

        for city in cities:
            logger.info(f"Processing city: {city}")

            # Get specialties for this city
            specialties = await self.get_specialties(city)
            specialties = specialties[:max_specialties]  # Limit

            for specialty in specialties:
                doctors = await self.scrape_doctors_by_specialty(
                    city,
                    specialty,
                    max_pages=max_pages_per_specialty
                )
                all_doctors.extend(doctors)

                # Rate limiting between specialties
                await asyncio.sleep(RATE_LIMIT_DELAY)

            logger.info(f"Completed {city}: {len(all_doctors)} total doctors")

        self.providers = all_doctors
        return all_doctors

    def save_raw_data(self, filename: str = "karnataka_practo_doctors.json"):
        """Save raw provider data"""
        output_path = self.output_dir / filename

        data = {
            "scraped_at": datetime.now().isoformat(),
            "source": "Practo",
            "state": "Karnataka",
            "total_providers": len(self.providers),
            "providers": self.providers
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(self.providers)} providers to {output_path}")

    def save_csv(self, filename: str = "karnataka_practo_doctors.csv"):
        """Save providers to CSV"""
        import csv

        if not self.providers:
            logger.warning("No providers to save")
            return

        output_path = self.output_dir / filename

        fieldnames = set()
        for provider in self.providers:
            fieldnames.update(provider.keys())

        fieldnames = sorted(fieldnames)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.providers)

        logger.info(f"Saved CSV to {output_path}")


async def main():
    """Main execution function"""
    logger.warning("="*70)
    logger.warning("⚠️  IMPORTANT: Practo Scraping Ethics Notice")
    logger.warning("="*70)
    logger.warning("Before running this scraper:")
    logger.warning("1. Check Practo's Terms of Service")
    logger.warning("2. Ensure compliance with data protection laws")
    logger.warning("3. Use for research/educational purposes only")
    logger.warning("4. Do NOT overload their servers (respect rate limits)")
    logger.warning("="*70)

    # Uncomment to proceed
    # proceed = input("\nDo you accept these terms? (yes/no): ")
    # if proceed.lower() != "yes":
    #     logger.info("Scraping cancelled")
    #     return

    logger.info("Starting Practo scraper for Karnataka")

    async with PractoScraper() as scraper:
        # Scrape major Karnataka cities
        doctors = await scraper.scrape_karnataka_cities(
            cities=["bangalore", "mysore", "mangalore"],
            max_specialties=5,  # Limit for testing
            max_pages_per_specialty=2  # Limit for testing
        )

        if doctors:
            scraper.save_raw_data()
            scraper.save_csv()
            logger.info(f"Scraping completed: {len(doctors)} doctors collected")
        else:
            logger.warning("No doctors collected. Check page structure and selectors.")


if __name__ == "__main__":
    logger.warning("\n⚠️  This scraper requires careful ethical consideration.")
    logger.warning("Consider using official APIs (HPR) instead.\n")

    # Uncomment to run
    # asyncio.run(main())

    logger.info("Scraper ready but not executed.")
    logger.info("Modify the code to enable execution after reviewing ToS.")
