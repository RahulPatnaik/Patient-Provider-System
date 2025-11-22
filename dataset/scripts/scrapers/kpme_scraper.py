"""
KPME Karnataka Portal Scraper

Scrapes data from Karnataka Private Medical Establishments portal
https://kpme.karnataka.gov.in

Collects:
- Approved medical establishments
- Diagnostic labs
- Hospital and clinic data
- Certificate information
"""

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KPME_BASE_URL = "https://kpme.karnataka.gov.in"
APPROVED_CERTS_URL = f"{KPME_BASE_URL}/AllapplicationList.aspx"
DIAGNOSTIC_LAB_URL = f"{KPME_BASE_URL}/AllapplicationListLabList.aspx"
TIMEOUT = 30.0
RATE_LIMIT_DELAY = 2.0


class KPMEScraper:
    """Scraper for KPME Karnataka portal"""

    def __init__(self, output_dir: str = "dataset/raw/kpme"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.http_client: httpx.AsyncClient = None
        self.establishments: List[Dict[str, Any]] = []

    async def __aenter__(self):
        """Async context manager entry"""
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
            verify=False  # Government sites may have cert issues
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.http_client:
            await self.http_client.aclose()

    def parse_establishment(self, row_html: str) -> Dict[str, Any]:
        """Parse establishment data from HTML row"""
        soup = BeautifulSoup(row_html, 'html.parser')

        establishment = {
            'system_of_medicine': '',
            'category': '',
            'establishment_name': '',
            'address': '',
            'district': '',
            'certificate_number': '',
            'certificate_validity': '',
            'website': '',
            'source': 'KPME Karnataka'
        }

        try:
            # Extract text from table cells
            cells = soup.find_all('td')

            if len(cells) >= 6:
                establishment['system_of_medicine'] = cells[0].get_text(strip=True)
                establishment['category'] = cells[1].get_text(strip=True)
                establishment['establishment_name'] = cells[2].get_text(strip=True)
                establishment['address'] = cells[3].get_text(strip=True)

                # Extract district from address
                address_upper = establishment['address'].upper()
                districts = [
                    'BAGALKOTE', 'BALLARI', 'BELAGAVI', 'BENGALURU', 'BIDAR',
                    'CHAMARAJANAGARA', 'CHIKKABALLAPURA', 'CHIKKAMAGALURU', 'CHITRADURGA',
                    'DAKSHINA KANNADA', 'DAVANAGERE', 'DHARWAD', 'GADAG', 'HASSAN',
                    'HAVERI', 'KALABURAGI', 'KODAGU', 'KOLAR', 'KOPPAL', 'MANDYA',
                    'MYSURU', 'RAICHUR', 'RAMANAGARA', 'SHIVAMOGGA', 'TUMAKURU',
                    'UDUPI', 'UTTARA KANNADA', 'VIJAYAPURA', 'YADGIR', 'BANGALORE',
                    'MANGALORE', 'HUBLI', 'MYSORE'
                ]

                for district in districts:
                    if district in address_upper:
                        establishment['district'] = district
                        break

                # Certificate validity
                validity_cell = cells[4].get_text(strip=True)
                establishment['certificate_validity'] = validity_cell

                # Certificate number
                cert_cell = cells[5].get_text(strip=True)
                establishment['certificate_number'] = cert_cell

                # Website if available
                if len(cells) > 6:
                    website_link = cells[6].find('a')
                    if website_link and website_link.get('href'):
                        establishment['website'] = website_link.get('href')

        except Exception as e:
            logger.warning(f"Error parsing establishment: {e}")

        return establishment

    async def scrape_approved_certificates(self, max_pages: int = 50) -> List[Dict[str, Any]]:
        """
        Scrape approved medical establishments

        Args:
            max_pages: Maximum pages to scrape

        Returns:
            List of establishment records
        """
        logger.info("Scraping approved medical establishments...")

        establishments = []
        page = 1

        try:
            # Initial request to get the page
            response = await self.http_client.get(APPROVED_CERTS_URL)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all table rows with establishment data
            table = soup.find('table', {'id': re.compile('.*GridView.*')})

            if table:
                rows = table.find_all('tr')[1:]  # Skip header row

                logger.info(f"Found {len(rows)} establishments on page {page}")

                for row in rows:
                    row_html = str(row)
                    establishment = self.parse_establishment(row_html)

                    if establishment['establishment_name']:
                        establishments.append(establishment)

                logger.info(f"Extracted {len(establishments)} establishments")

            else:
                logger.warning("Could not find data table on page")

        except Exception as e:
            logger.error(f"Error scraping approved certificates: {e}")

        return establishments

    async def scrape_diagnostic_labs(self, max_pages: int = 20) -> List[Dict[str, Any]]:
        """
        Scrape diagnostic lab list

        Args:
            max_pages: Maximum pages to scrape

        Returns:
            List of lab records
        """
        logger.info("Scraping diagnostic labs...")

        labs = []

        try:
            response = await self.http_client.get(DIAGNOSTIC_LAB_URL)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all table rows
            table = soup.find('table', {'id': re.compile('.*GridView.*')})

            if table:
                rows = table.find_all('tr')[1:]  # Skip header

                logger.info(f"Found {len(rows)} labs")

                for row in rows:
                    row_html = str(row)
                    lab = self.parse_establishment(row_html)

                    if lab['establishment_name']:
                        labs.append(lab)

                logger.info(f"Extracted {len(labs)} labs")

            else:
                logger.warning("Could not find lab data table")

        except Exception as e:
            logger.error(f"Error scraping diagnostic labs: {e}")

        return labs

    async def scrape_all_kpme_data(self) -> Dict[str, Any]:
        """Scrape all available KPME data"""
        logger.info("="*70)
        logger.info("Scraping KPME Karnataka Portal")
        logger.info("="*70)

        all_data = {
            "scraped_at": datetime.now().isoformat(),
            "source": "Karnataka Private Medical Establishments Portal",
            "url": KPME_BASE_URL
        }

        # Scrape approved establishments
        logger.info("\n1. Scraping approved medical establishments...")
        establishments = await self.scrape_approved_certificates()
        all_data['approved_establishments'] = establishments

        await asyncio.sleep(RATE_LIMIT_DELAY)

        # Scrape diagnostic labs
        logger.info("\n2. Scraping diagnostic labs...")
        labs = await self.scrape_diagnostic_labs()
        all_data['diagnostic_labs'] = labs

        # Summary
        logger.info("="*70)
        logger.info("Scraping Summary:")
        logger.info(f"  Approved Establishments: {len(establishments)}")
        logger.info(f"  Diagnostic Labs: {len(labs)}")
        logger.info(f"  Total Records: {len(establishments) + len(labs)}")
        logger.info("="*70)

        self.establishments = establishments + labs
        return all_data

    def save_json(self, data: Dict[str, Any], filename: str = "kpme_data.json"):
        """Save data to JSON"""
        output_path = self.output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved JSON to {output_path}")

    def save_csv(self, filename: str = "KPME_DATA.csv"):
        """Save data to CSV in dataset root"""
        if not self.establishments:
            logger.warning("No establishments to save")
            return

        # Save to dataset root directory
        output_path = Path("dataset") / filename

        df = pd.DataFrame(self.establishments)

        # Reorder columns for better readability
        column_order = [
            'establishment_name',
            'category',
            'system_of_medicine',
            'address',
            'district',
            'certificate_number',
            'certificate_validity',
            'website',
            'source'
        ]

        # Only include columns that exist
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]

        df.to_csv(output_path, index=False, encoding='utf-8')

        logger.info(f"✓ Saved CSV to {output_path}")
        logger.info(f"  Records: {len(df)}")
        logger.info(f"  Columns: {', '.join(df.columns.tolist())}")

        return output_path


async def main():
    """Main execution"""
    logger.info("KPME Karnataka Portal Scraper")
    logger.info("="*70)

    async with KPMEScraper() as scraper:
        # Scrape all data
        data = await scraper.scrape_all_kpme_data()

        # Save JSON
        scraper.save_json(data)

        # Save CSV to dataset root
        csv_path = scraper.save_csv()

        logger.info("")
        logger.info("✓ KPME scraping completed successfully!")
        logger.info(f"  CSV: {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
