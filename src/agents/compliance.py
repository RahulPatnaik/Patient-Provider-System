# ------------------------------------------------------
# Deterministic Compliance Agent (HARDENED)
# Karnataka KPME Healthcare Establishments
# ------------------------------------------------------
# - NO LLMs
# - NO APIs
# - Handles dirty real-world data
# - Never crashes
# - Hard + Soft compliance checks
# ------------------------------------------------------

from pathlib import Path
from datetime import datetime
from typing import Dict, List
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
    compliance_score: float
    decision: str
    metrics: Dict[str, float]
    issues: List[FieldIssue]
    reasoning: List[str]


# ======================================================
# SAFE HELPERS (CRITICAL)
# ======================================================

def add_issue(issues: List[FieldIssue], field: str, issue: str, description: str):
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
# COMPLIANCE AGENT
# ======================================================

class ComplianceAgent:
    """
    Fully deterministic, defensive compliance validator.
    """

    def run(self, record: Dict) -> ComplianceResult:
        issues: List[FieldIssue] = []
        reasoning: List[str] = []

        # --------------------------------------------------
        # 1. COMPLETENESS (HARD)
        # --------------------------------------------------
        required_fields = [
            "establishment_name",
            "category",
            "system_of_medicine",
            "certificate_number",
            "certificate_validity",
        ]

        missing = [f for f in required_fields if not safe_str(record.get(f))]
        completeness = 1 - (len(missing) / len(required_fields))

        for f in missing:
            add_issue(issues, f, "MISSING", "Mandatory field missing")

        # --------------------------------------------------
        # 2. VALIDITY (HARD)
        # --------------------------------------------------
        validity_checks = 4
        validity_failures = 0

        district = safe_str(record.get("district"))
        if district and district.upper() not in VALID_DISTRICTS:
            validity_failures += 1
            add_issue(issues, "district", "INVALID", "Not a valid Karnataka district")

        if record.get("category") not in VALID_CATEGORIES:
            validity_failures += 1
            add_issue(issues, "category", "INVALID", "Unknown healthcare category")

        if record.get("system_of_medicine") not in VALID_SYSTEMS:
            validity_failures += 1
            add_issue(issues, "system_of_medicine", "INVALID", "Unsupported system of medicine")

        if not CERTIFICATE_REGEX.match(str(record.get("certificate_number", ""))):
            validity_failures += 1
            add_issue(issues, "certificate_number", "INVALID_FORMAT", "Invalid certificate format")

        validity = 1 - (validity_failures / validity_checks)

        # --------------------------------------------------
        # 3. AUTHENTICITY (HARD)
        # --------------------------------------------------
        expiry = safe_date(record.get("certificate_validity"))

        if expiry is None:
            authenticity = 0.0
            add_issue(issues, "certificate_validity", "INVALID_FORMAT", "Invalid certificate date")
        elif expiry < datetime.now():
            authenticity = 0.3
            add_issue(issues, "certificate_validity", "EXPIRED", "Certificate expired")
        else:
            authenticity = 1.0

        # --------------------------------------------------
        # 4. CONSISTENCY (HARD)
        # --------------------------------------------------
        consistency = 1.0
        beds = safe_float(record.get("num_beds"))
        category = record.get("category")

        if beds is not None and category in MAX_BEDS_BY_CATEGORY:
            if beds > MAX_BEDS_BY_CATEGORY[category]:
                consistency = 0.0
                add_issue(
                    issues,
                    "num_beds",
                    "INCONSISTENT",
                    f"Beds exceed allowed limit for {category}"
                )

        # --------------------------------------------------
        # 5. SOFT QUALITY CHECKS (DEFENSIVE)
        # --------------------------------------------------
        soft_checks = 0
        soft_passed = 0

        # Address
        soft_checks += 1
        if safe_str(record.get("address")):
            soft_passed += 1
        else:
            add_issue(issues, "address", "MISSING", "Address not provided")

        # Phone
        soft_checks += 1
        phone = safe_str(record.get("phone"))
        if phone and phone.isdigit() and len(phone) == 10:
            soft_passed += 1
        else:
            add_issue(issues, "phone", "INVALID_FORMAT", "Invalid or missing phone number")

        # Email
        soft_checks += 1
        email = safe_str(record.get("email"))
        if email and EMAIL_REGEX.match(email):
            soft_passed += 1
        else:
            add_issue(issues, "email", "INVALID_FORMAT", "Invalid or missing email")

        # Geo
        soft_checks += 1
        lat = safe_float(record.get("latitude"))
        lon = safe_float(record.get("longitude"))
        if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
            soft_passed += 1
        else:
            add_issue(issues, "geo", "INVALID", "Invalid or missing latitude/longitude")

        # Land vs Building area
        soft_checks += 1
        land = safe_float(record.get("land_area_sqft"))
        building = safe_float(record.get("building_area_sqft"))

        if land is None or building is None:
            soft_passed += 0.5  # neutral
        elif building <= land:
            soft_passed += 1
        else:
            add_issue(
                issues,
                "building_area_sqft",
                "INCONSISTENT",
                "Building area exceeds land area"
            )

        soft_quality = soft_passed / soft_checks

        # --------------------------------------------------
        # 6. FINAL SCORE
        # --------------------------------------------------
        score = (
            0.25 * completeness +
            0.25 * validity +
            0.25 * authenticity +
            0.15 * consistency +
            0.10 * soft_quality
        ) * 100

        # --------------------------------------------------
        # 7. DECISION
        # --------------------------------------------------
        if authenticity < 0.5:
            decision = "REJECT"
            reasoning.append("Invalid or expired legal registration")
        elif score >= 85:
            decision = "AUTO_APPROVED"
            reasoning.append("High compliance across hard and soft checks")
        elif score >= 65:
            decision = "NEEDS_ENRICHMENT"
            reasoning.append("Soft data quality issues detected")
        elif score >= 40:
            decision = "MANUAL_REVIEW"
            reasoning.append("Low confidence record")
        else:
            decision = "REJECT"
            reasoning.append("Insufficient compliance score")

        return ComplianceResult(
            compliance_score=round(score, 2),
            decision=decision,
            metrics={
                "completeness": round(completeness, 2),
                "validity": round(validity, 2),
                "authenticity": round(authenticity, 2),
                "consistency": round(consistency, 2),
                "soft_quality": round(soft_quality, 2),
            },
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
