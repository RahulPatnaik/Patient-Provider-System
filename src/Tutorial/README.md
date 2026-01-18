# Tutorial - KPME Validator Agents Demo

This folder contains demo scripts showing the KPME Karnataka validator agents in action.

## Overview

The EL branch implements **KPME-only** (Karnataka Private Medical Establishments) validation through two specialized agents:

1. **Data Validator Agent** - Comprehensive validation with data quality assessment
2. **Fast Validator Agent** - Quick validation with cache-first strategy

## Demo Scripts

### 1. Data Validator Demo (`demo_data_validator.py`)

Demonstrates comprehensive KPME validation with data quality assessment.

**Run:**
```bash
python src/Tutorial/demo_data_validator.py
```

**What it shows:**
- Basic KPME certificate validation
- Data quality assessment (completeness, accuracy)
- Validation with incomplete data
- Agent integration flags (web scraper, enrichment, compliance)
- Expected output for testing scenarios
- Confidence scoring (KPME 70% + Quality 30%)

**Sample Output:**
```
KPME Validation Result:
  - Valid: True
  - Certificate: BDR00172ALHL2
  - Name: AAROGYA HOSPITAL FOR WOMEN AND CHILDREN
  - Confidence: 0.90

Data Quality Assessment:
  - Completeness: 100%
  - Accuracy: 100%
  - Overall Score: 100%

Overall Confidence: 93%
```

### 2. Fast Validator Demo (`demo_fast_validator.py`)

Demonstrates quick KPME validation with multiple search methods.

**Run:**
```bash
python src/Tutorial/demo_fast_validator.py
```

**What it shows:**
- Ultra-fast direct database check (bypasses agent)
- Fast validation by certificate number
- Fast validation by phone number
- Fast validation by establishment name
- Multiple search criteria
- Cache-first strategy
- Agent integration flags support
- Expected output for testing

**Sample Output:**
```
Fast Validation Result:
  - Provider Found: True
  - Cache Hit: False
  - Source: KPME Database
  - Confidence: 90%
  - Establishment: AAROGYA HOSPITAL FOR WOMEN AND CHILDREN
  - District: BIDAR
```

## Key Features

### Data Validator Agent
- ✅ KPME certificate validation with expiry checking
- ✅ Staff registration verification
- ✅ Phone/email/address validation
- ✅ Data quality assessment (completeness + accuracy)
- ✅ Confidence scoring with customizable weights
- ✅ Agent integration support (web scraper, enrichment, compliance)
- ✅ Expected output for testing scenarios

### Fast Validator Agent
- ✅ Cache-first validation strategy
- ✅ Ultra-fast direct database access
- ✅ Search by certificate, phone, or name
- ✅ Sub-second response times
- ✅ Multiple search criteria support
- ✅ Agent integration flags
- ✅ Expected output for testing

## Architecture

Both agents integrate with:

1. **KPME Database** (`src/database/kpme_db.py`)
   - SQLite database with 1,000 establishments
   - 4,483 staff records
   - 899 establishments with GPS coordinates
   - Fast indexed queries

2. **Agent Integration Flags**
   - `use_web_scraper`: Enable web scraper agent integration
   - `use_enrichment`: Enable enrichment agent integration
   - `use_compliance`: Enable compliance agent integration

3. **Testing Support**
   - `expected_output`: Provide expected results for testing scenarios
   - Enables validation without actual API calls

## KPME Database

The KPME database contains Karnataka healthcare establishments:

- **1,000 establishments** across Karnataka
- **Top districts**: Bangalore (157), Bengaluru (54), Kalaburagi (44)
- **Systems of medicine**: Allopathy (739), Ayurveda (199), Homeopathy (45)
- **Data fields**: Certificate number, name, category, address, phone, email, GPS coordinates, bed count

## EL Branch - KPME-Only

This is the **EL branch** implementation:
- ✅ KPME Karnataka-only (no USA NPI, no India NMC)
- ✅ Removed multi-region support
- ✅ Direct KPME database integration
- ✅ Agent integration architecture ready
- ✅ Testing scenario support

## Requirements

- Python 3.12+
- Gemini API key (set in `.env` as `GEMINI_API_KEY`)
- Dependencies: `pydantic-ai`, `sqlite3`, `pandas`

## Next Steps

These agents are ready to be integrated with:
1. **Web Scraper Agent** - Scrape additional provider data from web
2. **Enrichment Agent** - Enrich provider data with external sources
3. **Compliance Agent** - Check regulatory compliance

The architecture supports this integration through the agent integration flags.
