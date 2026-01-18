"""
NFHS (National Family Health Survey) Data Downloader for Karnataka

Downloads NFHS-5 district-level data for Karnataka from GitHub repositories
and official sources. This data provides health indicators and demographics
useful for facility context enrichment.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import httpx
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
NFHS5_GITHUB_REPO = "https://raw.githubusercontent.com/pratapvardhan/NFHS-5/master"
NFHS_DISTRICTS_CSV = f"{NFHS5_GITHUB_REPO}/NFHS-5-Districts.csv"
NFHS_STATES_CSV = f"{NFHS5_GITHUB_REPO}/NFHS-5-States.csv"
TIMEOUT = 60.0


class NFHSDownloader:
    """Downloader for NFHS data"""

    def __init__(self, output_dir: str = "dataset/raw/nfhs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.karnataka_data: Optional[pd.DataFrame] = None

    async def download_file(self, url: str, filename: str) -> Path:
        """
        Download file from URL

        Args:
            url: URL to download from
            filename: Local filename to save as

        Returns:
            Path to downloaded file
        """
        logger.info(f"Downloading {filename}...")

        output_path = self.output_dir / filename

        async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT)) as client:
            response = await client.get(url)
            response.raise_for_status()

            with open(output_path, "wb") as f:
                f.write(response.content)

        logger.info(f"Downloaded to {output_path}")
        return output_path

    async def download_nfhs5_districts(self) -> Path:
        """
        Download NFHS-5 district-level data

        Returns:
            Path to downloaded CSV
        """
        return await self.download_file(
            NFHS_DISTRICTS_CSV,
            "NFHS-5-Districts.csv"
        )

    async def download_nfhs5_states(self) -> Path:
        """
        Download NFHS-5 state-level data

        Returns:
            Path to downloaded CSV
        """
        return await self.download_file(
            NFHS_STATES_CSV,
            "NFHS-5-States.csv"
        )

    def filter_karnataka_districts(
        self,
        csv_path: Path
    ) -> pd.DataFrame:
        """
        Filter Karnataka districts from NFHS data

        Args:
            csv_path: Path to NFHS districts CSV

        Returns:
            DataFrame with Karnataka districts only
        """
        logger.info("Filtering Karnataka districts...")

        df = pd.read_csv(csv_path)

        # Filter by state name (check column names)
        state_column = None
        for col in df.columns:
            if 'state' in col.lower():
                state_column = col
                break

        if state_column:
            karnataka_df = df[df[state_column].str.contains('Karnataka', case=False, na=False)]
        else:
            logger.warning("State column not found. Returning all data.")
            karnataka_df = df

        logger.info(f"Found {len(karnataka_df)} Karnataka district records")
        self.karnataka_data = karnataka_df
        return karnataka_df

    def get_key_health_indicators(self) -> pd.DataFrame:
        """
        Extract key health indicators relevant to facility planning

        Returns:
            DataFrame with selected indicators
        """
        if self.karnataka_data is None:
            logger.error("No Karnataka data loaded")
            return pd.DataFrame()

        # Key indicators to extract (adjust based on actual column names)
        key_indicators = [
            'District',
            'Population',
            'Births',
            'Institutional births',
            'Healthcare facilities',
            'PHC',
            'CHC',
            'Hospitals'
        ]

        # Find matching columns
        available_columns = []
        for indicator in key_indicators:
            matching_cols = [col for col in self.karnataka_data.columns if indicator.lower() in col.lower()]
            available_columns.extend(matching_cols)

        if available_columns:
            return self.karnataka_data[available_columns]
        else:
            logger.warning("Key indicator columns not found")
            return self.karnataka_data

    def save_karnataka_data(self, filename: str = "karnataka_nfhs5.csv"):
        """Save Karnataka NFHS data"""
        if self.karnataka_data is None:
            logger.warning("No data to save")
            return

        output_path = self.output_dir / filename
        self.karnataka_data.to_csv(output_path, index=False)
        logger.info(f"Saved Karnataka NFHS data to {output_path}")

    def save_indicators_json(self, filename: str = "karnataka_health_indicators.json"):
        """Save health indicators as JSON"""
        if self.karnataka_data is None:
            logger.warning("No data to save")
            return

        output_path = self.output_dir / filename

        # Convert to JSON-friendly format
        data = {
            "extracted_at": datetime.now().isoformat(),
            "source": "NFHS-5 (2019-21)",
            "state": "Karnataka",
            "districts": self.karnataka_data.to_dict(orient="records")
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved health indicators to {output_path}")

    async def create_district_mapping(self) -> Dict[str, Any]:
        """
        Create mapping of NFHS district codes to names

        Returns:
            Dictionary mapping district codes to metadata
        """
        if self.karnataka_data is None:
            return {}

        mapping = {}

        for _, row in self.karnataka_data.iterrows():
            # Find district name and code columns
            district_name = None
            district_code = None

            for col in row.index:
                if 'district' in col.lower() and 'code' not in col.lower():
                    district_name = row[col]
                if 'code' in col.lower():
                    district_code = row[col]

            if district_name:
                mapping[str(district_code or district_name)] = {
                    "name": district_name,
                    "code": district_code,
                    "state": "Karnataka"
                }

        # Save mapping
        mapping_path = self.output_dir / "district_mapping.json"
        with open(mapping_path, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2)

        logger.info(f"Created district mapping with {len(mapping)} entries")
        return mapping

    def generate_summary_report(self) -> str:
        """Generate summary report of downloaded data"""
        if self.karnataka_data is None:
            return "No data loaded"

        report = f"""
NFHS-5 Karnataka Data Summary
==============================
Downloaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Districts: {len(self.karnataka_data)}
Indicators: {len(self.karnataka_data.columns)}

Sample Indicators:
{', '.join(list(self.karnataka_data.columns[:10]))}...

Data Shape: {self.karnataka_data.shape[0]} rows x {self.karnataka_data.shape[1]} columns
        """

        # Save report
        report_path = self.output_dir / "download_summary.txt"
        with open(report_path, "w") as f:
            f.write(report)

        return report


# Karnataka districts (for validation)
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
    logger.info("Starting NFHS data download for Karnataka")

    downloader = NFHSDownloader()

    try:
        # Download NFHS-5 district data
        csv_path = await downloader.download_nfhs5_districts()

        # Filter Karnataka data
        karnataka_df = downloader.filter_karnataka_districts(csv_path)

        # Save processed data
        downloader.save_karnataka_data()
        downloader.save_indicators_json()

        # Create district mapping
        await downloader.create_district_mapping()

        # Generate summary
        report = downloader.generate_summary_report()
        logger.info(report)

        logger.info("NFHS download completed successfully")

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
    except Exception as e:
        logger.error(f"Error downloading NFHS data: {e}")


if __name__ == "__main__":
    asyncio.run(main())
