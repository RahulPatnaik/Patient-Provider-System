"""
Data.gov.in Health Facilities Scraper for Karnataka

Downloads health facility datasets from India's Open Government Data Platform
focusing on Karnataka state PHC, CHC, and hospital data.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import httpx
import pandas as pd
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DATA_GOV_BASE_URL = "https://data.gov.in"
KARNATAKA_DATA_GOV_URL = "https://karnataka.data.gov.in"
TIMEOUT = 60.0


class DataGovScraper:
    """Scraper for data.gov.in health facility datasets"""

    def __init__(self, output_dir: str = "dataset/raw/data_gov_in"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.datasets: List[Dict[str, Any]] = []

    async def search_datasets(
        self,
        query: str = "Karnataka health facilities",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search for datasets on data.gov.in

        Args:
            query: Search query
            limit: Maximum number of results

        Returns:
            List of dataset metadata
        """
        logger.info(f"Searching for: {query}")

        search_url = f"{DATA_GOV_BASE_URL}/search"
        params = {
            "query": query,
            "limit": limit
        }

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
                response = await client.get(search_url, params=params)
                response.raise_for_status()

                # Parse HTML response
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract dataset links (structure may vary)
                datasets = []
                dataset_elements = soup.find_all('div', class_='dataset-item')

                for elem in dataset_elements:
                    title_elem = elem.find('a', class_='dataset-title')
                    if title_elem:
                        datasets.append({
                            "title": title_elem.text.strip(),
                            "url": DATA_GOV_BASE_URL + title_elem.get('href', ''),
                            "description": elem.find('div', class_='notes').text.strip() if elem.find('div', class_='notes') else ""
                        })

                logger.info(f"Found {len(datasets)} datasets")
                self.datasets = datasets
                return datasets

        except Exception as e:
            logger.error(f"Error searching datasets: {e}")
            return []

    async def download_csv_from_url(
        self,
        url: str,
        filename: str
    ) -> Optional[Path]:
        """
        Download CSV file from URL

        Args:
            url: URL of the CSV file
            filename: Local filename to save as

        Returns:
            Path to downloaded file or None
        """
        logger.info(f"Downloading CSV from {url}")

        output_path = self.output_dir / filename

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT), follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()

                with open(output_path, "wb") as f:
                    f.write(response.content)

            logger.info(f"Downloaded to {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

    async def download_karnataka_phc_data(self) -> Optional[Path]:
        """
        Download Karnataka PHC data from known sources

        Returns:
            Path to downloaded file or None
        """
        # Known dataset URLs (update with actual URLs)
        phc_urls = [
            "https://karnataka.data.gov.in/catalog/monthly-maximum-and-minimum-performing-public-health-facilities/export",
            # Add more known URLs
        ]

        for url in phc_urls:
            filepath = await self.download_csv_from_url(url, "karnataka_phc_data.csv")
            if filepath:
                return filepath

        return None

    async def download_karnataka_chc_data(self) -> Optional[Path]:
        """Download Karnataka CHC data"""
        # Placeholder - add actual URL when available
        logger.info("CHC data download not implemented - no known URL")
        return None

    async def download_karnataka_hospital_data(self) -> Optional[Path]:
        """Download Karnataka hospital list"""
        # Placeholder - add actual URL when available
        logger.info("Hospital data download not implemented - no known URL")
        return None

    def process_csv(
        self,
        csv_path: Path,
        dataset_type: str = "facility"
    ) -> pd.DataFrame:
        """
        Process downloaded CSV file

        Args:
            csv_path: Path to CSV file
            dataset_type: Type of dataset (facility, phc, chc, hospital)

        Returns:
            Processed DataFrame
        """
        logger.info(f"Processing {csv_path}")

        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")

            # Basic cleaning
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

            # Filter Karnataka if state column exists
            state_cols = [col for col in df.columns if 'state' in col]
            if state_cols:
                df = df[df[state_cols[0]].str.contains('Karnataka', case=False, na=False)]
                logger.info(f"Filtered to {len(df)} Karnataka records")

            return df

        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            return pd.DataFrame()

    def save_processed_data(
        self,
        df: pd.DataFrame,
        filename: str
    ):
        """Save processed data"""
        if df.empty:
            logger.warning("No data to save")
            return

        # Save CSV
        csv_path = self.output_dir / filename
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(df)} records to {csv_path}")

        # Save JSON
        json_path = self.output_dir / filename.replace('.csv', '.json')
        data = {
            "downloaded_at": datetime.now().isoformat(),
            "source": "data.gov.in",
            "state": "Karnataka",
            "total_records": len(df),
            "data": df.to_dict(orient="records")
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved JSON to {json_path}")

    def save_dataset_catalog(self):
        """Save catalog of found datasets"""
        catalog_path = self.output_dir / "dataset_catalog.json"

        catalog = {
            "cataloged_at": datetime.now().isoformat(),
            "source": "data.gov.in",
            "total_datasets": len(self.datasets),
            "datasets": self.datasets
        }

        with open(catalog_path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved dataset catalog to {catalog_path}")


async def main():
    """Main execution function"""
    logger.info("Starting data.gov.in scraper for Karnataka health facilities")

    scraper = DataGovScraper()

    # Search for datasets
    datasets = await scraper.search_datasets("Karnataka health facilities PHC CHC")
    scraper.save_dataset_catalog()

    # Download known datasets
    # PHC data
    phc_path = await scraper.download_karnataka_phc_data()
    if phc_path:
        phc_df = scraper.process_csv(phc_path, "phc")
        scraper.save_processed_data(phc_df, "karnataka_phc_processed.csv")

    # CHC data
    chc_path = await scraper.download_karnataka_chc_data()
    if chc_path:
        chc_df = scraper.process_csv(chc_path, "chc")
        scraper.save_processed_data(chc_df, "karnataka_chc_processed.csv")

    # Hospital data
    hospital_path = await scraper.download_karnataka_hospital_data()
    if hospital_path:
        hospital_df = scraper.process_csv(hospital_path, "hospital")
        scraper.save_processed_data(hospital_df, "karnataka_hospitals_processed.csv")

    logger.info("Data.gov.in scraping completed")


if __name__ == "__main__":
    asyncio.run(main())
