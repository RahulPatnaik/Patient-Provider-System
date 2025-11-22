# Karnataka Healthcare Data - Complete Summary

**Generated:** November 22, 2025
**Location:** `dataset/karnataka_healthcare_master.csv`

## 📊 Overview

**Total Records:** 6,605
**File Size:** 955 KB
**Format:** CSV (UTF-8)

## 📁 Data Composition

| Data Type | Count | Description |
|-----------|-------|-------------|
| **Healthcare Facilities (OSM)** | 4,219 | OpenStreetMap healthcare facilities with GPS coordinates |
| **Specialization Reference** | 748 | Medical specializations across all systems of medicine |
| **Degree Reference** | 389 | Recognized medical degrees by system |
| **Council Reference** | 198 | Medical council registrations |
| **KPME Establishments** | 1,020 | Licensed establishments from KPME Karnataka Portal |
| **District Reference** | 31 | Karnataka district master data |

## 🏥 KPME Establishments (1,020 Records)

**Source:** https://kpme.karnataka.gov.in/AllapplicationList.aspx
**Scraped:** November 22, 2025
**Pages:** 10 (all available pages)

### By System of Medicine
- **Allopathy:** 726 establishments (71.2%)
- **Ayurveda:** 210 establishments (20.6%)
- **Homeopathy:** 52 establishments (5.1%)
- **Integrated System:** 7 establishments (0.7%)
- **Unani:** 4 establishments (0.4%)
- **Yoga & Naturopathy:** 1 establishment (0.1%)

### Top Categories
1. **Clinic/Polyclinic Only Consultation** - 329 (32.3%)
2. **Clinic/Polyclinic with Dispensary** - 115 (11.3%)
3. **Dental Lab or Clinic** - 95 (9.3%)
4. **Clinic/Polyclinic with Diagnostic Support** - 94 (9.2%)
5. **Hospital (Level 1A)** - 88 (8.6%)
6. **Medical Diagnostic Laboratory** - 55 (5.4%)
7. **Hospital (Level 2)** - 53 (5.2%)
8. **Hospital (Level 1B)** - 50 (4.9%)
9. **Clinic/Polyclinic with Observation** - 49 (4.8%)
10. **Diagnostic Imaging Centre** - 30 (2.9%)

### Geographic Distribution
- **30 districts** identified from addresses
- **Top districts:** Bangalore (178), Bengaluru (58), Mysore (40), Mangalore (32), Udupi (31)

### Data Fields (KPME)
- Establishment name
- Category (facility type)
- System of medicine
- Full address
- Certificate number
- Certificate validity date
- District (extracted from address)
- Source metadata
- Scraped timestamp

## 🗺️ Healthcare Facilities (OSM - 4,219 Records)

**Source:** OpenStreetMap Overpass API
**Coverage:** 25 of 31 Karnataka districts

### Statistics
- **With GPS Coordinates:** 3,911 (92.7%)
- **Hospitals:** 1,968 (46.6%)
- **Clinics:** 1,043 (24.7%)
- **Pharmacies:** 174 (4.1%)
- **Other facilities:** 1,034 (24.5%)

### Data Fields (OSM)
- Facility name
- Facility type (hospital, clinic, pharmacy, etc.)
- GPS coordinates (latitude, longitude)
- Address (street, city, postcode)
- Phone number
- District information
- OSM ID and metadata
- Healthcare speciality (where available)

## 📚 Reference Data

### Districts (31 Records)
Complete Karnataka district master with:
- District ID, Name, ISO Code
- State information (Karnataka, ISO 29)

### Medical Specializations (748 Records)
Comprehensive list across systems:
- Modern Medicine
- Dentistry
- Ayurveda
- Homeopathy
- Unani
- Siddha
- Nursing

### Medical Degrees (389 Records)
Recognized degrees by system of medicine

### Medical Councils (198 Records)
Registration councils by system of medicine

## 📄 File Structure

### Column Schema
```
data_type              - Type of record (KPME_ESTABLISHMENT, HEALTHCARE_FACILITY, etc.)
district_id            - District identifier
district_name          - District name
district_iso_code      - ISO district code
state                  - State name (KARNATAKA)
state_iso_code         - State ISO code (29)
facility_type          - Type of healthcare facility
facility_name          - Name of establishment
latitude               - GPS latitude (OSM facilities only)
longitude              - GPS longitude (OSM facilities only)
phone                  - Contact phone number
address                - Full address
specialization         - Medical specialization
degree                 - Medical degree
council                - Medical council
system_of_medicine     - System (Allopathy, Ayurveda, etc.)
description            - Additional metadata
```

## 🔧 Data Processing Scripts

### `create_master_csv.py`
- Loads and combines all data sources
- Standardizes column structure
- Performs district matching
- Generates master CSV and summary

### KPME Scrapers
- `kpme_basic_scraper.py` - Table data extraction
- `kpme_detailed_scraper.py` - Detail popup extraction
- `kpme_debug_scraper.py` - Page structure analysis

## 📈 Data Quality

### Completeness
- **KPME:** 100% for name, category, system, certificate
- **OSM:** 92.7% GPS coverage, variable address completeness
- **Reference:** 100% complete for all fields

### Accuracy
- KPME data scraped directly from official government portal
- OSM data validated against Karnataka boundaries
- Reference data sourced from official documentation

### Freshness
- **KPME:** Scraped November 22, 2025
- **OSM:** Last updated in data collection
- **Reference:** Current as of data collection

## 🎯 Use Cases

1. **Provider Verification**
   - Cross-reference KPME certificate numbers
   - Validate establishment legitimacy
   - Check license validity dates

2. **Geographic Analysis**
   - Map healthcare facility distribution
   - Identify underserved districts
   - Calculate coverage metrics

3. **System of Medicine Analysis**
   - Compare Allopathy vs AYUSH presence
   - Track multi-system facilities
   - Analyze specialization distribution

4. **Compliance Checking**
   - Verify KPME registrations
   - Check certificate validity
   - Match against medical council data

## 📊 Additional Files

- `karnataka_healthcare_summary.csv` - Quick statistics summary
- `KPME_DATA.csv` - Raw KPME scraper output
- `ALTERNATIVE_DATA_SOURCES.txt` - Guide to other Karnataka data sources

## 🔄 Future Enhancements

Potential additions:
- HPR (ABDM) API integration (50,000+ professionals)
- NMC Registry data integration
- Data.gov.in hospital directories
- Practo/commercial directory integration
- Detailed establishment data from KPME popups

## 📞 Data Sources

1. **KPME Karnataka Portal**
   https://kpme.karnataka.gov.in/AllapplicationList.aspx

2. **OpenStreetMap**
   Overpass API - Karnataka healthcare facilities

3. **Government Reference Data**
   Official Karnataka districts and medical reference data

---

**Last Updated:** November 22, 2025
**Data Coverage:** Karnataka, India
**Total Healthcare Points:** 5,239 (1,020 KPME + 4,219 OSM facilities)
