"""
EDA for KPME Karnataka Dataset (CORRECTED VERSION)

Purpose:
- Discover valid enums (districts, categories, systems_of_medicine)
- Clean dirty numeric fields correctly
- Inspect numeric ranges and logical constraints
- Measure missingness
- Identify certificate number patterns

Schema (exact):
id, establishment_name, category, system_of_medicine, address, district,
certificate_number, certificate_validity, latitude, longitude, email, phone,
num_beds, land_area_sqft, building_area_sqft, page_number
"""

import pandas as pd
import json
import re
import numpy as np
from pathlib import Path


# =====================================================
# CONFIG
# =====================================================

CSV_PATH = "KPME_FULL_DATA.csv"
OUTPUT_DIR = Path("eda_outputs")
OUTPUT_DIR.mkdir(exist_ok=True)


# =====================================================
# LOAD DATA
# =====================================================

df = pd.read_csv(CSV_PATH)

print("=" * 80)
print("KPME DATASET LOADED")
print("=" * 80)
print(f"Rows    : {len(df)}")
print(f"Columns : {len(df.columns)}")
print("Column Names:")
print(list(df.columns))


# =====================================================
# NUMERIC CLEANING (CRITICAL FIX)
# =====================================================

def clean_numeric(val):
    """
    Convert numeric-looking values to float.
    Any non-numeric text (e.g. 'Building Area(sq.ft):') → NaN.
    """
    if pd.isna(val):
        return np.nan

    if isinstance(val, (int, float)):
        return float(val)

    val = str(val).strip()

    # Remove everything except digits and decimal point
    cleaned = re.sub(r"[^0-9.]", "", val)

    if cleaned == "":
        return np.nan

    try:
        return float(cleaned)
    except ValueError:
        return np.nan


numeric_cols = [
    "num_beds",
    "land_area_sqft",
    "building_area_sqft",
    "latitude",
    "longitude"
]

for col in numeric_cols:
    df[col] = df[col].apply(clean_numeric)

print("\nNumeric columns cleaned safely.")


# =====================================================
# 1. ENUM DISCOVERY
# =====================================================

print("\n" + "=" * 80)
print("ENUMERATIONS")
print("=" * 80)

districts = sorted(
    df["district"]
    .dropna()
    .astype(str)
    .str.upper()
    .str.strip()
    .unique()
)

categories = sorted(
    df["category"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
)

systems_of_medicine = sorted(
    df["system_of_medicine"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
)

print(f"Districts ({len(districts)}):")
print(districts)

print(f"\nCategories ({len(categories)}):")
print(categories)

print(f"\nSystems of Medicine ({len(systems_of_medicine)}):")
print(systems_of_medicine)

json.dump(districts, open(OUTPUT_DIR / "districts.json", "w"), indent=2)
json.dump(categories, open(OUTPUT_DIR / "categories.json", "w"), indent=2)
json.dump(systems_of_medicine, open(OUTPUT_DIR / "systems_of_medicine.json", "w"), indent=2)


# =====================================================
# 2. MISSING VALUE ANALYSIS
# =====================================================

print("\n" + "=" * 80)
print("MISSING VALUE ANALYSIS (%)")
print("=" * 80)

missing_pct = (df.isnull().mean() * 100).round(2).sort_values(ascending=False)
print(missing_pct)

missing_pct.to_csv(OUTPUT_DIR / "missing_percentages.csv")


# =====================================================
# 3. NUMERIC STATISTICS
# =====================================================

print("\n" + "=" * 80)
print("NUMERIC COLUMN STATISTICS")
print("=" * 80)

numeric_stats = df[numeric_cols].describe().round(2)
print(numeric_stats)

numeric_stats.to_csv(OUTPUT_DIR / "numeric_stats.csv")


# =====================================================
# 4. LOGICAL CONSISTENCY CHECKS
# =====================================================

print("\n" + "=" * 80)
print("LOGICAL CONSISTENCY CHECKS")
print("=" * 80)

# Only compare when BOTH values exist
area_conflicts = df[
    df["building_area_sqft"].notna() &
    df["land_area_sqft"].notna() &
    (df["building_area_sqft"] > df["land_area_sqft"])
]

print(f"Records where building_area_sqft > land_area_sqft: {len(area_conflicts)}")
area_conflicts.to_csv(OUTPUT_DIR / "area_conflicts.csv", index=False)

# Clinics with beds (beds should usually be 0 for clinics)
clinic_beds = df[
    df["category"].str.contains("Clinic", case=False, na=False) &
    (df["num_beds"].fillna(0) > 0)
]

print(f"Clinics with beds > 0: {len(clinic_beds)}")
clinic_beds.to_csv(OUTPUT_DIR / "clinic_beds_conflicts.csv", index=False)


# =====================================================
# 5. CERTIFICATE NUMBER PATTERN ANALYSIS
# =====================================================

print("\n" + "=" * 80)
print("CERTIFICATE NUMBER PATTERNS")
print("=" * 80)

certs = df["certificate_number"].dropna().astype(str)

print("\nSample certificate numbers:")
for c in certs.sample(min(10, len(certs)), random_state=42):
    print(" ", c)

pattern = re.compile(r"^[A-Z]{2,4}\d{3,6}[A-Z0-9]*$")
match_rate = certs.apply(lambda x: bool(pattern.match(x))).mean()

print(f"\nPattern match rate (heuristic): {match_rate:.2%}")

json.dump(
    {
        "regex": pattern.pattern,
        "approx_match_rate": round(match_rate, 4)
    },
    open(OUTPUT_DIR / "certificate_pattern.json", "w"),
    indent=2
)


# =====================================================
# 6. BEDS DISTRIBUTION BY CATEGORY
# =====================================================

print("\n" + "=" * 80)
print("BEDS DISTRIBUTION BY CATEGORY")
print("=" * 80)

beds_by_category = (
    df.groupby("category")["num_beds"]
    .describe()
    .round(2)
)

print(beds_by_category)
beds_by_category.to_csv(OUTPUT_DIR / "beds_by_category.csv")


# =====================================================
# 7. GEO COVERAGE
# =====================================================

print("\n" + "=" * 80)
print("GEO / LOCATION COVERAGE")
print("=" * 80)

geo_complete = df["latitude"].notna() & df["longitude"].notna()
print(
    f"Records with both latitude & longitude: "
    f"{geo_complete.sum()} ({geo_complete.mean() * 100:.2f}%)"
)

missing_geo = df[~geo_complete]
missing_geo.to_csv(OUTPUT_DIR / "missing_geo.csv", index=False)


# =====================================================
# DONE
# =====================================================

print("\n" + "=" * 80)
print("EDA COMPLETE")
print("=" * 80)
print(f"All outputs saved to: {OUTPUT_DIR.resolve()}")
