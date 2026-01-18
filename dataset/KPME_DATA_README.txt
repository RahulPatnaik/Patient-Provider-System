================================================================================
KPME_DATA.csv - Karnataka Private Medical Establishments
================================================================================

SOURCE: Karnataka Private Medical Establishments Portal
        https://kpme.karnataka.gov.in

PURPOSE: Licensed private medical establishments registered with Karnataka
         government under KPME Act

================================================================================
CURRENT STATUS:
================================================================================

✓ Template created with sample data (2 real samples + 1 template)
⚠️ Full data collection requires:
   1. Portal authentication/login
   2. OR Selenium with proper form submission
   3. OR Manual data export from portal
   4. OR API access from KPME

================================================================================
SAMPLE DATA INCLUDED:
================================================================================

1. SRIHARI DIAGNOSTIC (Bagalkote)
   - Diagnostic Imaging Centre
   - Certificate: BGK00035ALDIC
   - Valid until: 20-Oct-2025

2. Lingaraj Dental Clinic (Haveri)
   - Dental Clinic
   - Certificate: HVR00618ALDEN
   - Valid until: 05-Feb-2029

================================================================================
CSV COLUMNS (24 total):
================================================================================

Basic Information:
  - establishment_name     : Name of medical establishment
  - category               : Hospital, Clinic, Diagnostic Centre, etc.
  - system_of_medicine     : Allopathy, Ayurveda, Homeopathy, etc.

Location:
  - address                : Full address
  - district               : Karnataka district
  - taluk                  : Taluk/Tehsil
  - pincode                : 6-digit pincode
  - latitude               : GPS coordinate
  - longitude              : GPS coordinate

Registration:
  - certificate_number     : KPME certificate number
  - certificate_validity   : Certificate expiry date
  - registration_date      : First registration date
  - last_renewed           : Last renewal date

Contact:
  - phone                  : Contact number
  - email                  : Email address
  - website                : Website URL

Establishment Details:
  - owner_name             : Owner/Director name
  - establishment_type     : Private, Trust, Charitable
  - bed_capacity           : Number of beds
  - specialties            : Medical specialties offered
  - facilities             : Available facilities/services

Metadata:
  - source                 : KPME Karnataka Portal
  - scraped_at             : Data collection timestamp
  - data_status            : Sample/Template/Scraped

================================================================================
PORTAL PAGES:
================================================================================

Approved Certificates:
  https://kpme.karnataka.gov.in/AllapplicationList.aspx

Diagnostic Lab List:
  https://kpme.karnataka.gov.in/AllapplicationListLabList.aspx

Main Portal:
  https://kpme.karnataka.gov.in

================================================================================
HOW TO COLLECT FULL DATA:
================================================================================

Option 1: Manual Export (Recommended)
  1. Visit https://kpme.karnataka.gov.in
  2. Navigate to "Approved Certificates" or "Diagnostic Lab List"
  3. If export button available, download data
  4. Convert to CSV matching this template

Option 2: Portal Account Access
  1. Create account on KPME portal
  2. Request data export or API access
  3. Contact: KPME Helpdesk via portal

Option 3: Selenium Automation (Advanced)
  1. Use Selenium WebDriver
  2. Handle JavaScript rendering
  3. Navigate pagination
  4. Extract all establishment data
  5. Script available: dataset/scripts/scrapers/kpme_selenium_scraper.py

Option 4: Contact Government
  1. Contact: Department of Health and Family Welfare, Karnataka
  2. Request: Bulk data export under RTI or for research
  3. Email: Available on portal contact page

================================================================================
EXPECTED DATA VOLUME:
================================================================================

Based on portal observations:
  - Approved Establishments: 100+ per page, multiple pages
  - Diagnostic Labs: 100+ per page, 11+ pages
  - Total Estimated: 1,000-5,000 licensed establishments
  - Coverage: All 31 Karnataka districts

================================================================================
DATA CATEGORIES:
================================================================================

Available on portal:
  - Hospitals (Multi-specialty, Specialty, General)
  - Clinics (General, Specialty)
  - Diagnostic Centres (Imaging, Laboratory)
  - Dental Clinics
  - Ayurveda Hospitals
  - Homeopathy Clinics
  - Other Traditional Medicine Establishments

================================================================================
INTEGRATION WITH MAIN DATASET:
================================================================================

This KPME data complements:
  - karnataka_healthcare_master.csv (4,219 OSM facilities)
  - ABDM reference data (districts, specializations)

Combined, these provide:
  ✓ OSM Facilities (geographic coverage, GPS coordinates)
  ✓ KPME Licensed Establishments (official registration, certificates)
  ✓ ABDM Reference Data (standardization, validation)

Use KPME certificate numbers to validate OSM facilities
Cross-reference addresses to link datasets

================================================================================
NOTES:
================================================================================

- KPME portal may update data periodically
- Certificate validity dates indicate active licenses
- Some establishments may appear in both OSM and KPME
- District names should match ABDM reference data
- For research/academic use, cite source properly

================================================================================
CONTACT:
================================================================================

KPME Portal: https://kpme.karnataka.gov.in
Department: Health and Family Welfare, Government of Karnataka

================================================================================
Last Updated: 2025-11-22
Version: 1.0 (Template with samples)
Status: Awaiting full data collection
================================================================================
