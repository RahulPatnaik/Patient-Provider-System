"""
Enrichment Agent - Enriches provider data with additional information (KPME EL Branch).

DETERMINISTIC - Uses KPME database for enrichment.

Enriches with:
- Staff information
- Specialties
- Treatment/fee information
- Certificate details
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

from agents.base import BaseAgent, AgentName
from database.kpme_db import get_kpme_db

logger = logging.getLogger(__name__)


class EnrichmentAgent(BaseAgent):
    """
    Enrichment Agent for KPME providers.

    Enriches establishment data with:
    - Staff listings
    - Specialties offered
    - Treatment charges
    - Certificate information

    FULLY DETERMINISTIC - No AI/LLM calls.
    """

    def __init__(self):
        """Initialize Enrichment Agent."""
        super().__init__(AgentName.ENRICHMENT)
        self.db = get_kpme_db()
        self.logger.info("Enrichment Agent initialized (KPME database mode)")

    def get_staff_information(self, establishment_id: int) -> List[Dict[str, Any]]:
        """
        Get staff information for establishment.

        Args:
            establishment_id: Establishment ID from database

        Returns:
            List of staff records
        """
        try:
            conn = self.db.connect()
            cursor = conn.cursor()

            query = "SELECT * FROM staff WHERE establishment_id = ?"
            cursor.execute(query, (establishment_id,))

            staff = [dict(row) for row in cursor.fetchall()]
            self.logger.debug(f"Found {len(staff)} staff members for establishment {establishment_id}")

            return staff
        except Exception as e:
            self.logger.error(f"Error fetching staff: {e}")
            return []

    def get_specialties(self, establishment_id: int) -> List[Dict[str, Any]]:
        """
        Get specialties for establishment.

        Args:
            establishment_id: Establishment ID

        Returns:
            List of specialty records
        """
        try:
            conn = self.db.connect()
            cursor = conn.cursor()

            query = "SELECT * FROM specialties WHERE establishment_id = ?"
            cursor.execute(query, (establishment_id,))

            specialties = [dict(row) for row in cursor.fetchall()]
            self.logger.debug(f"Found {len(specialties)} specialties for establishment {establishment_id}")

            return specialties
        except Exception as e:
            self.logger.error(f"Error fetching specialties: {e}")
            return []

    def get_treatments(self, establishment_id: int) -> List[Dict[str, Any]]:
        """
        Get treatment/fee information for establishment.

        Args:
            establishment_id: Establishment ID

        Returns:
            List of treatment records
        """
        try:
            conn = self.db.connect()
            cursor = conn.cursor()

            query = "SELECT * FROM treatments WHERE establishment_id = ?"
            cursor.execute(query, (establishment_id,))

            treatments = [dict(row) for row in cursor.fetchall()]
            self.logger.debug(f"Found {len(treatments)} treatments for establishment {establishment_id}")

            return treatments
        except Exception as e:
            self.logger.error(f"Error fetching treatments: {e}")
            return []

    def get_certificates(self, establishment_id: int) -> List[Dict[str, Any]]:
        """
        Get certificate details for establishment.

        Args:
            establishment_id: Establishment ID

        Returns:
            List of certificate records
        """
        try:
            conn = self.db.connect()
            cursor = conn.cursor()

            query = "SELECT * FROM certificates WHERE establishment_id = ?"
            cursor.execute(query, (establishment_id,))

            certificates = [dict(row) for row in cursor.fetchall()]
            self.logger.debug(f"Found {len(certificates)} certificates for establishment {establishment_id}")

            return certificates
        except Exception as e:
            self.logger.error(f"Error fetching certificates: {e}")
            return []

    def find_establishment_id(self, provider_data: Dict[str, Any]) -> Optional[int]:
        """
        Find establishment ID from provider data.

        Args:
            provider_data: Provider data

        Returns:
            Establishment ID or None
        """
        certificate = provider_data.get('certificate_number')
        phone = provider_data.get('phone')
        name = provider_data.get('establishment_name')

        # Try certificate first
        if certificate:
            est = self.db.get_establishment_by_certificate(certificate)
            if est and est.get('id'):
                return est['id']

        # Try phone
        if phone:
            matches = self.db.get_establishment_by_phone(phone)
            if matches and matches[0].get('id'):
                return matches[0]['id']

        # Try name
        if name:
            matches = self.db.search_establishment_by_name(name, limit=1)
            if matches and matches[0].get('id'):
                return matches[0]['id']

        return None

    def enrich(self, provider_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich provider data with additional information.

        Args:
            provider_data: Provider data to enrich

        Returns:
            Enrichment result dictionary
        """
        self.logger.info("Starting data enrichment...")

        # Find establishment in database
        establishment_id = self.find_establishment_id(provider_data)

        if not establishment_id:
            self.logger.warning("Establishment not found in database - no enrichment possible")
            return {
                "enrichment_found": False,
                "data_source": "KPME Database",
                "staff_count": 0,
                "specialties_count": 0,
                "treatments_count": 0,
                "certificates_count": 0,
                "confidence": 0.0,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }

        # Fetch enrichment data
        staff = self.get_staff_information(establishment_id)
        specialties = self.get_specialties(establishment_id)
        treatments = self.get_treatments(establishment_id)
        certificates = self.get_certificates(establishment_id)

        # Calculate confidence based on data availability
        data_points = sum([
            len(staff) > 0,
            len(specialties) > 0,
            len(treatments) > 0,
            len(certificates) > 0
        ])
        confidence = data_points / 4.0  # 0.0 to 1.0

        enrichment_result = {
            "enrichment_found": True,
            "data_source": "KPME Database",
            "establishment_id": establishment_id,

            # Staff information
            "staff": staff,
            "staff_count": len(staff),

            # Specialties
            "specialties": specialties,
            "specialties_count": len(specialties),

            # Treatments/fees
            "treatments": treatments,
            "treatments_count": len(treatments),

            # Certificates
            "certificates": certificates,
            "certificates_count": len(certificates),

            # Summary
            "total_data_points": data_points,
            "confidence": confidence,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        self.logger.info(
            f"Enrichment completed: "
            f"{len(staff)} staff, {len(specialties)} specialties, "
            f"{len(treatments)} treatments, {len(certificates)} certificates"
        )

        return enrichment_result


# Singleton instance
_enrichment_instance: Optional[EnrichmentAgent] = None


def get_enrichment() -> EnrichmentAgent:
    """Get singleton enrichment agent instance."""
    global _enrichment_instance
    if _enrichment_instance is None:
        _enrichment_instance = EnrichmentAgent()
    return _enrichment_instance
