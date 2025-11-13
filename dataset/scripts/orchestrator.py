"""
Data Collection Orchestrator

Main pipeline to run all scrapers and processors in sequence.
Coordinates data collection from all sources for Karnataka.
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from scrapers import (
    hfr_scraper,
    osm_query,
    nfhs_downloader,
    data_gov_scraper
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dataset/data_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataCollectionOrchestrator:
    """Orchestrate all data collection tasks"""

    def __init__(self):
        self.start_time = None
        self.results = {}

    async def run_hfr_scraper(self):
        """Run HFR scraper"""
        logger.info("="*60)
        logger.info("STEP 1: HFR (Health Facility Registry) Scraper")
        logger.info("="*60)

        try:
            async with hfr_scraper.HFRScraper() as scraper:
                facilities = await scraper.scrape_karnataka_facilities()

                if facilities:
                    scraper.save_raw_data()
                    scraper.save_csv()
                    self.results["hfr"] = {
                        "status": "success",
                        "count": len(facilities)
                    }
                else:
                    self.results["hfr"] = {
                        "status": "warning",
                        "message": "No facilities found - API may require authentication"
                    }

        except Exception as e:
            logger.error(f"HFR scraper failed: {e}")
            self.results["hfr"] = {
                "status": "error",
                "error": str(e)
            }

    async def run_osm_query(self):
        """Run OSM Overpass query"""
        logger.info("="*60)
        logger.info("STEP 2: OpenStreetMap Query")
        logger.info("="*60)

        try:
            query_tool = osm_query.OSMHealthcareQuery()

            # Query district by district (more reliable than full state query)
            facilities = await query_tool.fetch_by_districts(
                osm_query.KARNATAKA_DISTRICTS,
                delay_between_queries=3.0
            )

            if facilities:
                query_tool.save_raw_data()
                query_tool.save_csv()
                self.results["osm"] = {
                    "status": "success",
                    "count": len(facilities)
                }
            else:
                self.results["osm"] = {
                    "status": "warning",
                    "message": "No facilities found in OSM"
                }

        except Exception as e:
            logger.error(f"OSM query failed: {e}")
            self.results["osm"] = {
                "status": "error",
                "error": str(e)
            }

    async def run_nfhs_downloader(self):
        """Run NFHS downloader"""
        logger.info("="*60)
        logger.info("STEP 3: NFHS Data Download")
        logger.info("="*60)

        try:
            downloader = nfhs_downloader.NFHSDownloader()

            csv_path = await downloader.download_nfhs5_districts()
            karnataka_df = downloader.filter_karnataka_districts(csv_path)

            downloader.save_karnataka_data()
            downloader.save_indicators_json()
            await downloader.create_district_mapping()

            self.results["nfhs"] = {
                "status": "success",
                "count": len(karnataka_df)
            }

        except Exception as e:
            logger.error(f"NFHS downloader failed: {e}")
            self.results["nfhs"] = {
                "status": "error",
                "error": str(e)
            }

    async def run_data_gov_scraper(self):
        """Run data.gov.in scraper"""
        logger.info("="*60)
        logger.info("STEP 4: Data.gov.in Scraper")
        logger.info("="*60)

        try:
            scraper = data_gov_scraper.DataGovScraper()

            # Search for datasets
            datasets = await scraper.search_datasets("Karnataka health facilities")
            scraper.save_dataset_catalog()

            # Try to download known datasets
            phc_path = await scraper.download_karnataka_phc_data()

            self.results["data_gov"] = {
                "status": "success",
                "datasets_found": len(datasets),
                "downloaded": "phc_data" if phc_path else "none"
            }

        except Exception as e:
            logger.error(f"Data.gov.in scraper failed: {e}")
            self.results["data_gov"] = {
                "status": "error",
                "error": str(e)
            }

    async def run_all_scrapers(self):
        """Run all scrapers sequentially"""
        logger.info("\n" + "="*60)
        logger.info("STARTING KARNATAKA HEALTH DATA COLLECTION")
        logger.info("="*60 + "\n")

        self.start_time = datetime.now()

        # Run scrapers in sequence (to respect rate limits)
        await self.run_osm_query()  # Start with OSM (most reliable, free)

        await asyncio.sleep(5)  # Brief pause between sources

        await self.run_nfhs_downloader()  # NFHS (GitHub, fast)

        await asyncio.sleep(5)

        await self.run_data_gov_scraper()  # data.gov.in

        await asyncio.sleep(5)

        await self.run_hfr_scraper()  # HFR (may need auth)

        # Note: RHS parsing requires manual PDF download first
        logger.info("\n" + "="*60)
        logger.info("NOTE: RHS PDF Parser")
        logger.info("="*60)
        logger.info("To parse RHS data, first download the PDF manually from:")
        logger.info("https://main.mohfw.gov.in/sites/default/files/Final%20RHS%202018-19_0.pdf")
        logger.info("Then run: python dataset/scripts/scrapers/rhs_parser.py <path_to_pdf>")

    def print_summary(self):
        """Print collection summary"""
        end_time = datetime.now()
        duration = end_time - self.start_time

        logger.info("\n" + "="*60)
        logger.info("DATA COLLECTION SUMMARY")
        logger.info("="*60)

        for source, result in self.results.items():
            status = result.get("status", "unknown")
            logger.info(f"\n{source.upper()}:")
            logger.info(f"  Status: {status}")

            if status == "success":
                if "count" in result:
                    logger.info(f"  Records: {result['count']}")
                if "datasets_found" in result:
                    logger.info(f"  Datasets found: {result['datasets_found']}")
            elif status == "error":
                logger.info(f"  Error: {result.get('error', 'Unknown')}")
            elif status == "warning":
                logger.info(f"  Message: {result.get('message', 'Unknown')}")

        logger.info(f"\nTotal duration: {duration}")
        logger.info("="*60)

        # Save summary to file
        summary_path = Path("dataset/collection_summary.json")
        import json
        with open(summary_path, "w") as f:
            json.dump({
                "completed_at": end_time.isoformat(),
                "duration_seconds": duration.total_seconds(),
                "results": self.results
            }, f, indent=2)

        logger.info(f"\nSummary saved to: {summary_path}")


async def main():
    """Main execution function"""
    orchestrator = DataCollectionOrchestrator()

    try:
        await orchestrator.run_all_scrapers()
    except KeyboardInterrupt:
        logger.warning("\nData collection interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        orchestrator.print_summary()

        logger.info("\n" + "="*60)
        logger.info("NEXT STEPS:")
        logger.info("="*60)
        logger.info("1. Manually download RHS PDF and run rhs_parser.py")
        logger.info("2. Run data processors to standardize data:")
        logger.info("   python dataset/scripts/processors/facility_processor.py")
        logger.info("3. Run deduplication to merge records")
        logger.info("4. Review processed data in dataset/processed/")
        logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
