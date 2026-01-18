"""
Web Scraper Agent - Mock implementation for KPME (EL Branch).

DETERMINISTIC - Simulates web scraping with database enrichment.

For KPME local database, actual web scraping isn't needed.
This agent enriches missing data using KPME database lookups.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import logging

from agents.base import BaseAgent, AgentName
from database.kpme_db import get_kpme_db

logger = logging.getLogger(__name__)


class WebScraperResult:
    """Result from web scraper agent."""
    def __init__(
        self,
        found_data: bool,
        data_source: str,
        enriched_fields: Dict[str, Any],
        confidence: float
    ):
        self.found_data = found_data
        self.data_source = data_source
        self.enriched_fields = enriched_fields
        self.confidence = confidence
        self.timestamp = datetime.utcnow().isoformat() + "Z"


class WebScraperAgent(BaseAgent):
    """
    Web Scraper Agent for KPME (Mock/Deterministic).

    Instead of actual web scraping, uses KPME database
    to enrich missing fields.

    FULLY DETERMINISTIC - No AI/LLM calls.
    """

    def __init__(self):
        """Initialize Web Scraper Agent."""
        super().__init__(AgentName.WEB_SCRAPER)
        self.db = get_kpme_db()
        self.logger.info("Web Scraper Agent initialized (KPME database mode)")

    def scrape(
        self,
        provider_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        'Scrape' web data (actually: enrich from KPME database).

        Args:
            provider_data: Provider data with potential missing fields

        Returns:
            Enriched data dictionary
        """
        self.logger.info("Starting web scraping (KPME database enrichment)...")

        enriched = {}
        data_source = "KPME Database (Fallback Enrichment)"

        # Try to find establishment in database
        certificate = provider_data.get('certificate_number')
        phone = provider_data.get('phone')
        name = provider_data.get('establishment_name')

        establishment = None

        # Try certificate first
        if certificate:
            establishment = self.db.get_establishment_by_certificate(certificate)
            if establishment:
                data_source = "KPME Database (Certificate Match)"

        # Try phone if no certificate match
        if not establishment and phone:
            matches = self.db.get_establishment_by_phone(phone)
            if matches:
                establishment = matches[0]
                data_source = "KPME Database (Phone Match)"

        # Try name if still not found
        if not establishment and name:
            matches = self.db.search_establishment_by_name(name, limit=1)
            if matches:
                establishment = matches[0]
                data_source = "KPME Database (Name Match)"

        # Enrich missing fields from database
        if establishment:
            # Fill in missing address
            if not provider_data.get('address') and establishment.get('address'):
                enriched['address'] = establishment['address']

            # Fill in missing GPS coordinates
            if not provider_data.get('latitude') and establishment.get('latitude'):
                enriched['latitude'] = establishment['latitude']
            if not provider_data.get('longitude') and establishment.get('longitude'):
                enriched['longitude'] = establishment['longitude']

            # Fill in missing phone
            if not provider_data.get('phone') and establishment.get('phone'):
                enriched['phone'] = establishment['phone']

            # Fill in missing email
            if not provider_data.get('email') and establishment.get('email'):
                enriched['email'] = establishment['email']

            # Fill in missing district
            if not provider_data.get('district') and establishment.get('district'):
                enriched['district'] = establishment['district']

            # Fill in missing category
            if not provider_data.get('category') and establishment.get('category'):
                enriched['category'] = establishment['category']

            # Fill in missing system of medicine
            if not provider_data.get('system_of_medicine') and establishment.get('system_of_medicine'):
                enriched['system_of_medicine'] = establishment['system_of_medicine']

            # Fill in missing beds
            if not provider_data.get('num_beds') and establishment.get('num_beds'):
                enriched['num_beds'] = establishment['num_beds']

            confidence = 0.85  # High confidence from database match
            self.logger.info(f"Enriched {len(enriched)} fields from {data_source}")

        else:
            confidence = 0.0
            self.logger.warning("No establishment found in database for enrichment")

        result = {
            "found_data": len(enriched) > 0,
            "data_source": data_source,
            "enriched_fields": enriched,
            "fields_enriched": list(enriched.keys()),
            "fields_count": len(enriched),
            "confidence": confidence,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        self.logger.info(f"Web scraping completed: enriched {len(enriched)} fields")

        return result


# Singleton instance
_web_scraper_instance: Optional[WebScraperAgent] = None


def get_web_scraper() -> WebScraperAgent:
    """Get singleton web scraper instance."""
    global _web_scraper_instance
    if _web_scraper_instance is None:
        _web_scraper_instance = WebScraperAgent()
    return _web_scraper_instance
