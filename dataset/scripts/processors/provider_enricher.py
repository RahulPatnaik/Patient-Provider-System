"""
Provider Data Enrichment Pipeline

Enriches healthcare provider data by:
1. Loading reference data for validation
2. Standardizing district names, specializations, degrees
3. Linking providers to facilities (OSM data)
4. Cross-referencing with multiple data sources
5. Adding confidence scores

Input:
- HPR provider data (ABDM official)
- OSM facility data
- Reference data (districts, specializations, etc.)
- Optional: Practo data for enrichment

Output:
- Validated and enriched provider records
- Provider-facility linkages
- Quality metrics
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProviderEnricher:
    """Enriches and validates provider data"""

    def __init__(
        self,
        reference_data_path: str = "dataset/processed/reference/karnataka_reference_data.json",
        lookup_tables_path: str = "dataset/processed/reference/lookup_tables.json",
        output_dir: str = "dataset/processed/providers"
    ):
        self.reference_data_path = Path(reference_data_path)
        self.lookup_tables_path = Path(lookup_tables_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.reference_data: Dict[str, Any] = {}
        self.lookups: Dict[str, Dict] = {}
        self.providers: List[Dict[str, Any]] = []

    def load_reference_data(self):
        """Load reference data and lookup tables"""
        logger.info("Loading reference data...")

        # Load full reference data
        with open(self.reference_data_path, 'r', encoding='utf-8') as f:
            self.reference_data = json.load(f)

        # Load lookup tables
        with open(self.lookup_tables_path, 'r', encoding='utf-8') as f:
            self.lookups = json.load(f)

        logger.info(f"✓ Loaded {len(self.reference_data)} reference data categories")
        logger.info(f"✓ Loaded {len(self.lookups)} lookup tables")

    def normalize_district_name(self, district: str) -> Optional[str]:
        """
        Normalize district name to match reference data

        Args:
            district: Input district name (any case, any format)

        Returns:
            Standardized district name or None
        """
        if not district:
            return None

        # Convert to uppercase and strip
        district_normalized = str(district).upper().strip()

        # Direct lookup
        if district_normalized in self.lookups.get('district_by_name', {}):
            return district_normalized

        # Try fuzzy matching
        district_lower = district.lower()
        for ref_name in self.lookups.get('district_by_name', {}).keys():
            if district_lower in ref_name.lower() or ref_name.lower() in district_lower:
                return ref_name

        # Common variations
        replacements = {
            'BANGALORE': 'BENGALURU',
            'MYSORE': 'MYSURU',
            'BELLARY': 'BALLARI',
            'GULBARGA': 'KALABURAGI',
            'SHIMOGA': 'SHIVAMOGGA',
            'BELGAUM': 'BELAGAVI',
            'BIJAPUR': 'VIJAYAPURA',
            'TUMKUR': 'TUMAKURU'
        }

        for old, new in replacements.items():
            if old in district_normalized:
                district_normalized = district_normalized.replace(old, new)
                if district_normalized in self.lookups.get('district_by_name', {}):
                    return district_normalized

        logger.warning(f"Could not normalize district: {district}")
        return None

    def validate_district(self, provider: Dict[str, Any]) -> Tuple[bool, Optional[Dict]]:
        """
        Validate and enrich district information

        Returns:
            (is_valid, district_data)
        """
        district_name = provider.get('district')
        if not district_name:
            return False, None

        normalized = self.normalize_district_name(district_name)
        if not normalized:
            return False, None

        district_data = self.lookups['district_by_name'].get(normalized)
        return True, district_data

    def normalize_specialization(self, specialization: str) -> Optional[str]:
        """Normalize specialization name"""
        if not specialization:
            return None

        spec_lower = str(specialization).lower().strip()

        # Direct lookup
        if spec_lower in self.lookups.get('specialization_by_name', {}):
            spec_data = self.lookups['specialization_by_name'][spec_lower]
            return spec_data['name']

        # Partial matching
        for ref_name in self.lookups.get('specialization_by_name', {}).keys():
            if spec_lower in ref_name or ref_name in spec_lower:
                spec_data = self.lookups['specialization_by_name'][ref_name]
                return spec_data['name']

        return None

    def validate_specialization(self, provider: Dict[str, Any]) -> Tuple[bool, Optional[Dict]]:
        """Validate and enrich specialization"""
        specialization = provider.get('specialization')
        if not specialization:
            return False, None

        normalized = self.normalize_specialization(specialization)
        if not normalized:
            return False, None

        # Find in lookup
        spec_data = self.lookups['specialization_by_name'].get(normalized.lower())
        return True, spec_data

    def parse_qualifications(self, qualification_str: str) -> List[str]:
        """
        Parse qualification string into individual degrees

        Args:
            qualification_str: "MBBS, MD, DM" or similar

        Returns:
            List of individual degrees
        """
        if not qualification_str:
            return []

        # Split by common separators
        degrees = re.split(r'[,;/]', qualification_str)

        # Clean and uppercase
        degrees = [d.strip().upper() for d in degrees if d.strip()]

        return degrees

    def validate_qualifications(self, provider: Dict[str, Any]) -> Tuple[bool, List[Dict]]:
        """Validate qualifications against reference data"""
        qualification_str = provider.get('qualification') or provider.get('qualifications')
        if not qualification_str:
            return False, []

        degrees = self.parse_qualifications(qualification_str)
        validated_degrees = []

        for degree in degrees:
            degree_lower = degree.lower()

            # Direct lookup
            if degree_lower in self.lookups.get('degree_by_name', {}):
                degree_data = self.lookups['degree_by_name'][degree_lower]
                validated_degrees.append(degree_data)
                continue

            # Partial matching
            for ref_name in self.lookups.get('degree_by_name', {}).keys():
                if degree_lower in ref_name or ref_name in degree_lower:
                    degree_data = self.lookups['degree_by_name'][ref_name]
                    validated_degrees.append(degree_data)
                    break

        return len(validated_degrees) > 0, validated_degrees

    def validate_council(self, provider: Dict[str, Any]) -> Tuple[bool, Optional[Dict]]:
        """Validate medical council"""
        council_name = provider.get('council') or provider.get('medical_council')
        if not council_name:
            return False, None

        council_lower = str(council_name).lower()

        # Direct lookup
        if council_lower in self.lookups.get('council_by_name', {}):
            council_data = self.lookups['council_by_name'][council_lower]
            return True, council_data

        # Partial matching
        for ref_name in self.lookups.get('council_by_name', {}).keys():
            if council_lower in ref_name or ref_name in council_lower:
                council_data = self.lookups['council_by_name'][ref_name]
                return True, council_data

        return False, None

    def calculate_confidence_score(self, validation_results: Dict[str, bool]) -> float:
        """
        Calculate overall confidence score based on validation results

        Returns:
            Confidence score 0.0 to 1.0
        """
        weights = {
            'district_valid': 0.20,
            'specialization_valid': 0.15,
            'qualification_valid': 0.20,
            'council_valid': 0.25,
            'has_hpr_id': 0.20
        }

        score = 0.0
        for key, weight in weights.items():
            if validation_results.get(key, False):
                score += weight

        return round(score, 3)

    def enrich_provider(self, provider: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single provider record with validation and standardization

        Args:
            provider: Raw provider data

        Returns:
            Enriched provider record
        """
        enriched = provider.copy()

        # Validation results
        validation = {}

        # 1. Validate District
        district_valid, district_data = self.validate_district(provider)
        validation['district_valid'] = district_valid
        if district_data:
            enriched['district_standardized'] = district_data['name']
            enriched['district_id'] = district_data['identifier']
            enriched['district_iso'] = district_data['district_iso']

        # 2. Validate Specialization
        spec_valid, spec_data = self.validate_specialization(provider)
        validation['specialization_valid'] = spec_valid
        if spec_data:
            enriched['specialization_standardized'] = spec_data['name']
            enriched['specialization_id'] = spec_data['identifier']
            enriched['is_super_specialty'] = spec_data.get('is_super_specialty', False)

        # 3. Validate Qualifications
        qual_valid, qual_data = self.validate_qualifications(provider)
        validation['qualification_valid'] = qual_valid
        if qual_data:
            enriched['qualifications_standardized'] = [d['name'] for d in qual_data]
            enriched['qualification_ids'] = [d['identifier'] for d in qual_data]

        # 4. Validate Council
        council_valid, council_data = self.validate_council(provider)
        validation['council_valid'] = council_valid
        if council_data:
            enriched['council_standardized'] = council_data['name']
            enriched['council_id'] = council_data['identifier']

        # 5. Check HPR ID
        has_hpr_id = bool(provider.get('hpr_id'))
        validation['has_hpr_id'] = has_hpr_id

        # Calculate confidence score
        confidence_score = self.calculate_confidence_score(validation)

        enriched['validation'] = validation
        enriched['confidence_score'] = confidence_score
        enriched['enriched_at'] = datetime.now().isoformat()

        return enriched

    def enrich_all_providers(
        self,
        providers: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Enrich all provider records

        Args:
            providers: List of raw provider data

        Returns:
            List of enriched providers
        """
        logger.info(f"Enriching {len(providers)} provider records...")

        enriched_providers = []
        stats = {
            'total': len(providers),
            'district_valid': 0,
            'specialization_valid': 0,
            'qualification_valid': 0,
            'council_valid': 0,
            'high_confidence': 0  # >= 0.8
        }

        for provider in providers:
            enriched = self.enrich_provider(provider)
            enriched_providers.append(enriched)

            # Update stats
            if enriched['validation'].get('district_valid'):
                stats['district_valid'] += 1
            if enriched['validation'].get('specialization_valid'):
                stats['specialization_valid'] += 1
            if enriched['validation'].get('qualification_valid'):
                stats['qualification_valid'] += 1
            if enriched['validation'].get('council_valid'):
                stats['council_valid'] += 1
            if enriched['confidence_score'] >= 0.8:
                stats['high_confidence'] += 1

        # Calculate percentages
        total = stats['total']
        logger.info("="*70)
        logger.info("Enrichment Statistics:")
        logger.info(f"  Total Providers: {total}")
        logger.info(f"  District Valid: {stats['district_valid']} ({stats['district_valid']/total*100:.1f}%)")
        logger.info(f"  Specialization Valid: {stats['specialization_valid']} ({stats['specialization_valid']/total*100:.1f}%)")
        logger.info(f"  Qualification Valid: {stats['qualification_valid']} ({stats['qualification_valid']/total*100:.1f}%)")
        logger.info(f"  Council Valid: {stats['council_valid']} ({stats['council_valid']/total*100:.1f}%)")
        logger.info(f"  High Confidence (≥0.8): {stats['high_confidence']} ({stats['high_confidence']/total*100:.1f}%)")
        logger.info("="*70)

        self.providers = enriched_providers
        return enriched_providers

    def filter_high_confidence(self, min_confidence: float = 0.8) -> List[Dict[str, Any]]:
        """Filter providers by minimum confidence score"""
        high_confidence = [
            p for p in self.providers
            if p.get('confidence_score', 0) >= min_confidence
        ]

        logger.info(f"Filtered to {len(high_confidence)} high-confidence providers (>= {min_confidence})")
        return high_confidence

    def save_enriched_data(self, filename: str = "enriched_providers.json"):
        """Save enriched provider data"""
        output_path = self.output_dir / filename

        data = {
            "enriched_at": datetime.now().isoformat(),
            "source": "ABDM HPR + Reference Data",
            "total_providers": len(self.providers),
            "providers": self.providers
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved enriched providers to {output_path}")

    def save_high_confidence(
        self,
        min_confidence: float = 0.8,
        filename: str = "high_confidence_providers.json"
    ):
        """Save only high-confidence providers"""
        high_confidence = self.filter_high_confidence(min_confidence)

        output_path = self.output_dir / filename

        data = {
            "enriched_at": datetime.now().isoformat(),
            "source": "ABDM HPR + Reference Data",
            "min_confidence_score": min_confidence,
            "total_providers": len(high_confidence),
            "providers": high_confidence
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✓ Saved {len(high_confidence)} high-confidence providers to {output_path}")

    def generate_quality_report(self) -> Dict[str, Any]:
        """Generate data quality report"""
        if not self.providers:
            return {}

        total = len(self.providers)

        # Calculate statistics
        district_valid = sum(1 for p in self.providers if p['validation'].get('district_valid'))
        spec_valid = sum(1 for p in self.providers if p['validation'].get('specialization_valid'))
        qual_valid = sum(1 for p in self.providers if p['validation'].get('qualification_valid'))
        council_valid = sum(1 for p in self.providers if p['validation'].get('council_valid'))
        has_hpr = sum(1 for p in self.providers if p['validation'].get('has_hpr_id'))

        # Confidence distribution
        confidence_scores = [p.get('confidence_score', 0) for p in self.providers]
        avg_confidence = sum(confidence_scores) / len(confidence_scores)

        confidence_buckets = {
            '0.0-0.2': sum(1 for s in confidence_scores if 0.0 <= s < 0.2),
            '0.2-0.4': sum(1 for s in confidence_scores if 0.2 <= s < 0.4),
            '0.4-0.6': sum(1 for s in confidence_scores if 0.4 <= s < 0.6),
            '0.6-0.8': sum(1 for s in confidence_scores if 0.6 <= s < 0.8),
            '0.8-1.0': sum(1 for s in confidence_scores if 0.8 <= s <= 1.0)
        }

        report = {
            "generated_at": datetime.now().isoformat(),
            "total_providers": total,
            "validation_rates": {
                "district": f"{district_valid}/{total} ({district_valid/total*100:.1f}%)",
                "specialization": f"{spec_valid}/{total} ({spec_valid/total*100:.1f}%)",
                "qualification": f"{qual_valid}/{total} ({qual_valid/total*100:.1f}%)",
                "council": f"{council_valid}/{total} ({council_valid/total*100:.1f}%)",
                "hpr_id": f"{has_hpr}/{total} ({has_hpr/total*100:.1f}%)"
            },
            "confidence_metrics": {
                "average_score": round(avg_confidence, 3),
                "distribution": confidence_buckets,
                "high_confidence_count": confidence_buckets['0.8-1.0'],
                "high_confidence_rate": f"{confidence_buckets['0.8-1.0']/total*100:.1f}%"
            }
        }

        return report

    def save_quality_report(self, filename: str = "quality_report.json"):
        """Save quality report"""
        report = self.generate_quality_report()

        output_path = self.output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        logger.info(f"✓ Saved quality report to {output_path}")

        # Also log to console
        logger.info("")
        logger.info("="*70)
        logger.info("DATA QUALITY REPORT")
        logger.info("="*70)
        logger.info(f"Total Providers: {report['total_providers']}")
        logger.info("")
        logger.info("Validation Rates:")
        for field, rate in report['validation_rates'].items():
            logger.info(f"  {field.title()}: {rate}")
        logger.info("")
        logger.info(f"Average Confidence Score: {report['confidence_metrics']['average_score']}")
        logger.info(f"High Confidence Rate: {report['confidence_metrics']['high_confidence_rate']}")
        logger.info("="*70)


def main():
    """Main execution"""
    logger.info("Provider Data Enrichment Pipeline")
    logger.info("="*70)

    # Initialize enricher
    enricher = ProviderEnricher()

    # Load reference data
    enricher.load_reference_data()

    # Example: Load provider data (replace with actual path)
    logger.info("")
    logger.info("Note: This is a template. To use:")
    logger.info("1. Collect provider data using hpr_scraper.py")
    logger.info("2. Load the provider data JSON file")
    logger.info("3. Run enrichment pipeline")
    logger.info("")
    logger.info("Example usage:")
    logger.info("""
    from provider_enricher import ProviderEnricher

    # Initialize
    enricher = ProviderEnricher()
    enricher.load_reference_data()

    # Load provider data
    with open('dataset/raw/hpr/karnataka_healthcare_professionals.json') as f:
        data = json.load(f)
        providers = data['providers']

    # Enrich
    enriched = enricher.enrich_all_providers(providers)

    # Save
    enricher.save_enriched_data()
    enricher.save_high_confidence()
    enricher.save_quality_report()
    """)


if __name__ == "__main__":
    main()
