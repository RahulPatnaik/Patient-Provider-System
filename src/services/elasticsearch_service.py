"""
Elasticsearch Service for Patient Records
Provides full-text search capabilities with fallback to SQLite LIKE queries
"""

import os
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class ElasticsearchService:
    """
    Elasticsearch integration for patient records with SQLite fallback

    If Elasticsearch is not available, falls back to database LIKE queries
    """

    def __init__(self):
        self.es_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
        self.es_available = False
        self.client = None

        try:
            from elasticsearch import Elasticsearch
            self.client = Elasticsearch([self.es_url])
            # Test connection
            if self.client.ping():
                self.es_available = True
                logger.info(f"Elasticsearch connected: {self.es_url}")
                self._ensure_indices()
            else:
                logger.warning("Elasticsearch ping failed, using database fallback")
        except ImportError:
            logger.warning("elasticsearch-py not installed, using database fallback")
        except Exception as e:
            logger.warning(f"Elasticsearch connection failed: {e}, using database fallback")

    def _ensure_indices(self):
        """Create Elasticsearch indices if they don't exist"""
        if not self.es_available:
            return

        # Patient index
        patient_index = {
            "mappings": {
                "properties": {
                    "patient_id": {"type": "integer"},
                    "patient_name": {"type": "text", "analyzer": "standard"},
                    "phone": {"type": "keyword"},
                    "email": {"type": "keyword"},
                    "address": {"type": "text"},
                    "blood_group": {"type": "keyword"},
                    "allergies": {"type": "text"},
                    "chronic_conditions": {"type": "text"},
                    "created_at": {"type": "date"}
                }
            }
        }

        # Prescription index
        prescription_index = {
            "mappings": {
                "properties": {
                    "prescription_id": {"type": "integer"},
                    "patient_id": {"type": "integer"},
                    "patient_name": {"type": "text", "analyzer": "standard"},
                    "prescription_date": {"type": "date"},
                    "doctor_name": {"type": "text", "analyzer": "standard"},
                    "doctor_license": {"type": "keyword"},
                    "clinic_name": {"type": "text", "analyzer": "standard"},
                    "clinic_location": {"type": "text", "analyzer": "standard"},
                    "diagnosis": {"type": "text"},
                    "medications": {"type": "text"},
                    "extracted_text": {"type": "text"},
                    "created_at": {"type": "date"}
                }
            }
        }

        try:
            if not self.client.indices.exists(index="patients"):
                self.client.indices.create(index="patients", body=patient_index)
                logger.info("Created Elasticsearch index: patients")

            if not self.client.indices.exists(index="prescriptions"):
                self.client.indices.create(index="prescriptions", body=prescription_index)
                logger.info("Created Elasticsearch index: prescriptions")
        except Exception as e:
            logger.error(f"Failed to create Elasticsearch indices: {e}")

    # ==================== PATIENT INDEXING ====================

    def index_patient(self, patient_data: Dict[str, Any]):
        """Index patient record in Elasticsearch"""
        if not self.es_available:
            return

        try:
            self.client.index(
                index="patients",
                id=patient_data.get('id'),
                body={
                    "patient_id": patient_data.get('id'),
                    "patient_name": patient_data.get('patient_name'),
                    "phone": patient_data.get('phone'),
                    "email": patient_data.get('email'),
                    "address": patient_data.get('address'),
                    "blood_group": patient_data.get('blood_group'),
                    "allergies": patient_data.get('allergies'),
                    "chronic_conditions": patient_data.get('chronic_conditions'),
                    "created_at": patient_data.get('created_at')
                }
            )
            logger.debug(f"Indexed patient: {patient_data.get('id')}")
        except Exception as e:
            logger.error(f"Failed to index patient: {e}")

    def delete_patient(self, patient_id: int):
        """Remove patient from Elasticsearch index"""
        if not self.es_available:
            return

        try:
            self.client.delete(index="patients", id=patient_id, ignore=[404])
            logger.debug(f"Deleted patient from index: {patient_id}")
        except Exception as e:
            logger.error(f"Failed to delete patient from index: {e}")

    def search_patients(self, query: str, limit: int = 50) -> List[int]:
        """
        Search patients using Elasticsearch

        Returns list of patient IDs matching the query
        """
        if not self.es_available:
            return []  # Fallback to database search

        try:
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["patient_name^3", "phone^2", "email^2", "address", "allergies", "chronic_conditions"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                "size": limit
            }

            response = self.client.search(index="patients", body=search_body)
            patient_ids = [int(hit['_source']['patient_id']) for hit in response['hits']['hits']]

            logger.info(f"Elasticsearch patient search: '{query}' found {len(patient_ids)} results")
            return patient_ids
        except Exception as e:
            logger.error(f"Elasticsearch patient search failed: {e}")
            return []

    # ==================== PRESCRIPTION INDEXING ====================

    def index_prescription(self, prescription_data: Dict[str, Any]):
        """Index prescription record in Elasticsearch"""
        if not self.es_available:
            return

        try:
            self.client.index(
                index="prescriptions",
                id=prescription_data.get('id'),
                body={
                    "prescription_id": prescription_data.get('id'),
                    "patient_id": prescription_data.get('patient_id'),
                    "patient_name": prescription_data.get('patient_name', ''),
                    "prescription_date": prescription_data.get('prescription_date'),
                    "doctor_name": prescription_data.get('doctor_name'),
                    "doctor_license": prescription_data.get('doctor_license'),
                    "clinic_name": prescription_data.get('clinic_name'),
                    "clinic_location": prescription_data.get('clinic_location'),
                    "diagnosis": prescription_data.get('diagnosis'),
                    "medications": prescription_data.get('medications'),
                    "extracted_text": prescription_data.get('extracted_text'),
                    "created_at": prescription_data.get('created_at')
                }
            )
            logger.debug(f"Indexed prescription: {prescription_data.get('id')}")
        except Exception as e:
            logger.error(f"Failed to index prescription: {e}")

    def delete_prescription(self, prescription_id: int):
        """Remove prescription from Elasticsearch index"""
        if not self.es_available:
            return

        try:
            self.client.delete(index="prescriptions", id=prescription_id, ignore=[404])
            logger.debug(f"Deleted prescription from index: {prescription_id}")
        except Exception as e:
            logger.error(f"Failed to delete prescription from index: {e}")

    def search_prescriptions(
        self,
        query: str = None,
        doctor_name: str = None,
        clinic_name: str = None,
        clinic_location: str = None,
        limit: int = 100
    ) -> List[int]:
        """
        Search prescriptions using Elasticsearch

        Returns list of prescription IDs matching the criteria
        """
        if not self.es_available:
            return []  # Fallback to database search

        try:
            # Build query
            must_clauses = []

            if query:
                # Full-text search across all fields
                must_clauses.append({
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "doctor_name^3",
                            "clinic_name^3",
                            "clinic_location^2",
                            "diagnosis^2",
                            "medications^2",
                            "extracted_text"
                        ],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                })

            if doctor_name:
                must_clauses.append({
                    "match": {
                        "doctor_name": {
                            "query": doctor_name,
                            "fuzziness": "AUTO"
                        }
                    }
                })

            if clinic_name:
                must_clauses.append({
                    "match": {
                        "clinic_name": {
                            "query": clinic_name,
                            "fuzziness": "AUTO"
                        }
                    }
                })

            if clinic_location:
                must_clauses.append({
                    "match": {
                        "clinic_location": {
                            "query": clinic_location,
                            "fuzziness": "AUTO"
                        }
                    }
                })

            if not must_clauses:
                # Match all if no filters
                search_body = {
                    "query": {"match_all": {}},
                    "size": limit,
                    "sort": [{"prescription_date": {"order": "desc"}}]
                }
            else:
                search_body = {
                    "query": {
                        "bool": {
                            "must": must_clauses
                        }
                    },
                    "size": limit,
                    "sort": [{"prescription_date": {"order": "desc"}}]
                }

            response = self.client.search(index="prescriptions", body=search_body)
            prescription_ids = [int(hit['_source']['prescription_id']) for hit in response['hits']['hits']]

            logger.info(f"Elasticsearch prescription search found {len(prescription_ids)} results")
            return prescription_ids
        except Exception as e:
            logger.error(f"Elasticsearch prescription search failed: {e}")
            return []

    def bulk_reindex_patients(self, patients: List[Dict[str, Any]]):
        """Bulk reindex all patients"""
        if not self.es_available:
            return

        try:
            from elasticsearch.helpers import bulk

            actions = [
                {
                    "_index": "patients",
                    "_id": patient.get('id'),
                    "_source": {
                        "patient_id": patient.get('id'),
                        "patient_name": patient.get('patient_name'),
                        "phone": patient.get('phone'),
                        "email": patient.get('email'),
                        "address": patient.get('address'),
                        "blood_group": patient.get('blood_group'),
                        "allergies": patient.get('allergies'),
                        "chronic_conditions": patient.get('chronic_conditions'),
                        "created_at": patient.get('created_at')
                    }
                }
                for patient in patients
            ]

            success, failed = bulk(self.client, actions)
            logger.info(f"Bulk reindexed {success} patients, {failed} failed")
        except Exception as e:
            logger.error(f"Bulk reindex patients failed: {e}")

    def bulk_reindex_prescriptions(self, prescriptions: List[Dict[str, Any]]):
        """Bulk reindex all prescriptions"""
        if not self.es_available:
            return

        try:
            from elasticsearch.helpers import bulk

            actions = [
                {
                    "_index": "prescriptions",
                    "_id": prescription.get('id'),
                    "_source": {
                        "prescription_id": prescription.get('id'),
                        "patient_id": prescription.get('patient_id'),
                        "patient_name": prescription.get('patient_name', ''),
                        "prescription_date": prescription.get('prescription_date'),
                        "doctor_name": prescription.get('doctor_name'),
                        "doctor_license": prescription.get('doctor_license'),
                        "clinic_name": prescription.get('clinic_name'),
                        "clinic_location": prescription.get('clinic_location'),
                        "diagnosis": prescription.get('diagnosis'),
                        "medications": prescription.get('medications'),
                        "extracted_text": prescription.get('extracted_text'),
                        "created_at": prescription.get('created_at')
                    }
                }
                for prescription in prescriptions
            ]

            success, failed = bulk(self.client, actions)
            logger.info(f"Bulk reindexed {success} prescriptions, {failed} failed")
        except Exception as e:
            logger.error(f"Bulk reindex prescriptions failed: {e}")


# Singleton instance
_elasticsearch_service = None


def get_elasticsearch_service() -> ElasticsearchService:
    """Get or create Elasticsearch service singleton"""
    global _elasticsearch_service
    if _elasticsearch_service is None:
        _elasticsearch_service = ElasticsearchService()
    return _elasticsearch_service
