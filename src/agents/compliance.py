# ------------------------------------------------------
# Standards-Aligned Compliance Agent (FINAL)
# ------------------------------------------------------
# Frameworks referenced:
# - ISO/IEC 25012 (Data Quality Model)
# - ISO 8000 (Master Data Quality)
# - WHO Digital Health Indicator Framework
#
# Key principle:
#   Regulatory Validity is a HARD GATE.
#   Readiness is scored only after legality is confirmed.
# ------------------------------------------------------

from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import json
import re

from pydantic import BaseModel


# ======================================================
# PATH RESOLUTION
# ======================================================
BASE_DIR = Path(__file__).resolve().parents[2]
DATASET_DIR = BASE_DIR / "dataset" / "eda_outputs"

if not DATASET_DIR.exists():
    raise RuntimeError(f"EDA outputs directory not found: {DATASET_DIR}")


# ======================================================
# LOAD POLICY ARTIFACTS (FROM EDA)
# ======================================================
with open(DATASET_DIR / "districts.json", "r", encoding="utf-8") as f:
    VALID_DISTRICTS = set(json.load(f))

with open(DATASET_DIR / "categories.json", "r", encoding="utf-8") as f:
    VALID_CATEGORIES = set(json.load(f))

with open(DATASET_DIR / "systems_of_medicine.json", "r", encoding="utf-8") as f:
    VALID_SYSTEMS = set(json.load(f))


CERTIFICATE_REGEX = re.compile(r"^[A-Z]{2,4}\d{3,6}[A-Z0-9]*$")
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DATE_FORMAT = "%d %b %Y"

MAX_BEDS_BY_CATEGORY = {
    "Hospital (Level 1A)": 50,
    "Hospital (Level 1B)": 65,
    "Hospital (Level 2)": 100,
    "Hospital (Level 3)(Non-Teaching with Super Specialty Services)": 350,
    "Hospital (Level 4)(Teaching)": 1000,
    "Specialty / Super-Specialty Specific Hospital": 800,
}


# ======================================================
# OUTPUT MODELS
# ======================================================

class FieldIssue(BaseModel):
    field: str
    issue: str
    description: str


class ComplianceResult(BaseModel):
    decision: str
    regulatory_validity: float
    temporal_validity: float
    completeness: float
    structural_accuracy: float
    operational_readiness: float
    readiness_score: float
    issues: List[FieldIssue]
    reasoning: List[str]


# ======================================================
# SAFE HELPERS
# ======================================================

def add_issue(issues, field, issue, description):
    issues.append(FieldIssue(
        field=field,
        issue=issue,
        description=description
    ))


def safe_str(val):
    return val.strip() if isinstance(val, str) and val.strip() else None


def safe_float(val):
    try:
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            cleaned = re.sub(r"[^\d.]", "", val)
            return float(cleaned) if cleaned else None
    except Exception:
        return None
    return None


def safe_date(val):
    try:
        if isinstance(val, str):
            return datetime.strptime(val.strip(), DATE_FORMAT)
    except Exception:
        return None
    return None


# ======================================================
# COMPLIANCE AGENT (STANDARDS-ALIGNED)
# ======================================================

class ComplianceAgent:
    """
    Gated compliance evaluation aligned with healthcare
    registry and ISO data quality frameworks.
    """

    def run(self, record: Dict) -> ComplianceResult:
        issues: List[FieldIssue] = []
        reasoning: List[str] = []

        # ==================================================
        # 1. REGULATORY VALIDITY (HARD GATE)
        # ==================================================
        rv_checks = 4
        rv_failures = 0

        if record.get("category") not in VALID_CATEGORIES:
            rv_failures += 1
            add_issue(issues, "category", "INVALID", "Invalid healthcare category")

        if record.get("system_of_medicine") not in VALID_SYSTEMS:
            rv_failures += 1
            add_issue(issues, "system_of_medicine", "INVALID", "Unsupported system of medicine")

        if not CERTIFICATE_REGEX.match(str(record.get("certificate_number", ""))):
            rv_failures += 1
            add_issue(issues, "certificate_number", "INVALID_FORMAT", "Invalid certificate number")

        beds = safe_float(record.get("num_beds"))
        category = record.get("category")
        if beds is not None and category in MAX_BEDS_BY_CATEGORY:
            if beds > MAX_BEDS_BY_CATEGORY[category]:
                rv_failures += 1
                add_issue(
                    issues,
                    "num_beds",
                    "INCONSISTENT",
                    f"Beds exceed permitted limit for {category}"
                )

        regulatory_validity = 1 - (rv_failures / rv_checks)

        if regulatory_validity < 0.75:
            reasoning.append("Failed regulatory validity gate")
            return ComplianceResult(
                decision="REJECT",
                regulatory_validity=round(regulatory_validity, 2),
                temporal_validity=0.0,
                completeness=0.0,
                structural_accuracy=0.0,
                operational_readiness=0.0,
                readiness_score=0.0,
                issues=issues,
                reasoning=reasoning,
            )

        # ==================================================
        # 2. TEMPORAL VALIDITY (ISO: Timeliness)
        # ==================================================
        expiry = safe_date(record.get("certificate_validity"))
        if expiry is None:
            temporal_validity = 0.0
            add_issue(issues, "certificate_validity", "INVALID_FORMAT", "Invalid expiry date")
        elif expiry < datetime.now():
            temporal_validity = 0.0
            add_issue(issues, "certificate_validity", "EXPIRED", "Registration expired")
        else:
            temporal_validity = 1.0

        # ==================================================
        # 3. COMPLETENESS (ISO: Completeness)
        # ==================================================
        required_fields = [
            "establishment_name",
            "address",
            "district",
            "phone",
        ]

        present = sum(1 for f in required_fields if safe_str(record.get(f)))
        completeness = present / len(required_fields)

        for f in required_fields:
            if not safe_str(record.get(f)):
                add_issue(issues, f, "MISSING", "Required field missing")

        # ==================================================
        # 4. STRUCTURAL ACCURACY (ISO: Accuracy + Consistency)
        # ==================================================
        sa_checks = 3
        sa_passed = 0

        # District validity
        district = safe_str(record.get("district"))
        if district and district.upper() in VALID_DISTRICTS:
            sa_passed += 1
        else:
            add_issue(issues, "district", "INVALID", "Invalid or missing district")

        # Geo
        lat = safe_float(record.get("latitude"))
        lon = safe_float(record.get("longitude"))
        if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
            sa_passed += 1
        else:
            add_issue(issues, "geo", "INVALID", "Invalid latitude/longitude")

        # Land vs Building
        land = safe_float(record.get("land_area_sqft"))
        building = safe_float(record.get("building_area_sqft"))
        if land is None or building is None or building <= land:
            sa_passed += 1
        else:
            add_issue(
                issues,
                "building_area_sqft",
                "INCONSISTENT",
                "Building area exceeds land area"
            )

        structural_accuracy = sa_passed / sa_checks

        # ==================================================
        # 5. OPERATIONAL READINESS (WHO / Registry Use)
        # ==================================================
        or_checks = 3
        or_passed = 0

        phone = safe_str(record.get("phone"))
        if phone and phone.isdigit() and len(phone) == 10:
            or_passed += 1
        else:
            add_issue(issues, "phone", "INVALID_FORMAT", "Invalid phone number")

        email = safe_str(record.get("email"))
        if email and EMAIL_REGEX.match(email):
            or_passed += 1
        else:
            add_issue(issues, "email", "INVALID_FORMAT", "Invalid or missing email")

        if safe_str(record.get("address")):
            or_passed += 1
        else:
            add_issue(issues, "address", "MISSING", "Address missing")

        operational_readiness = or_passed / or_checks

        # ==================================================
        # 6. READINESS SCORE (POST-GATE ONLY)
        # ==================================================
        readiness_score = (
            0.20 * completeness +
            0.20 * structural_accuracy +
            0.20 * temporal_validity +
            0.40 * operational_readiness
        )

        # ==================================================
        # 7. DECISION LOGIC
        # ==================================================
        if operational_readiness < 0.8:
            decision = "NEEDS_ENRICHMENT"
            reasoning.append("Insufficient operational readiness for directory use")
        elif readiness_score >= 0.85:
            decision = "AUTO_APPROVED"
            reasoning.append("Meets regulatory and directory readiness criteria")
        else:
            decision = "MANUAL_REVIEW"
            reasoning.append("Moderate readiness; manual verification recommended")

        return ComplianceResult(
            decision=decision,
            regulatory_validity=round(regulatory_validity, 2),
            temporal_validity=round(temporal_validity, 2),
            completeness=round(completeness, 2),
            structural_accuracy=round(structural_accuracy, 2),
            operational_readiness=round(operational_readiness, 2),
            readiness_score=round(readiness_score, 2),
            issues=issues,
            reasoning=reasoning,
        )


# ======================================================
# LOCAL TESTS
# ======================================================

if __name__ == "__main__":
    import json

    agent = ComplianceAgent()

    test_record_1 = {
        "establishment_name": "Aastrika Midwifery Centre",
        "category": "Hospital (Level 1B)",
        "system_of_medicine": "Allopathy",
        "address": "#15, Bengaluru",
        "district": "BENGALURU",
        "certificate_number": "BLU03175ALH1B",
        "certificate_validity": "18 Mar 2027",
        "latitude": 12.9031,
        "longitude": 77.5606,
        "email": "info@aastrika.com",
        "phone": "9886137004",
        "num_beds": 5,
        "land_area_sqft": 5300,
        "building_area_sqft": 5461,
    }

    test_record_2 = {
        "establishment_name": "GAJAKOSH HOSPITAL",
        "category": "Hospital (Level 2)",
        "system_of_medicine": "Allopathy",
        "address": "VIJAYAPUR ROAD INDI",
        "district": None,
        "certificate_number": "BIJ00191ALHL2",
        "certificate_validity": "02 Apr 2026",
        "latitude": 17.169593,
        "longitude": 75.955302,
        "email": "Phone:",
        "phone": "9901021781",
        "num_beds": 30,
        "land_area_sqft": "",
        "building_area_sqft": "Buliding Area(sq.ft):",
    }

    print("\n--- TEST RECORD 1 ---")
    print(json.dumps(agent.run(test_record_1).model_dump(), indent=2))

    print("\n--- TEST RECORD 2 ---")
    print(json.dumps(agent.run(test_record_2).model_dump(), indent=2))


# ======================================================
# HELPER FUNCTION FOR SUPERVISOR
# ======================================================

def check_compliance(provider_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Helper function to check compliance for a provider.

    Args:
        provider_data: Provider data dictionary

    Returns:
        Compliance result as dictionary
    """
    agent = ComplianceAgent()
    result = agent.run(provider_data)
    return result.model_dump()
