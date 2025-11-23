# KPME Validator Agents - Mentor Demo Guide

## Quick Start (No API Required) ⚡

```bash
python src/Tutorial/demo_quick_validators.py
```

This demo runs immediately and shows all validator capabilities without requiring Gemini API quota.

## What We've Built on EL Branch

### 1. Data Validator Agent (`src/agents/data_validator.py`)
**Comprehensive KPME Karnataka healthcare provider validation**

**Features:**
- ✅ KPME certificate validation with expiry checking
- ✅ Data quality assessment (completeness + accuracy scores)
- ✅ Staff registration verification
- ✅ Phone/email/address validation
- ✅ Confidence scoring: KPME validation (70%) + Data quality (30%)
- ✅ Agent integration support (web scraper, enrichment, compliance)
- ✅ Testing scenario support with expected output

**Key Response:**
```python
{
  "kpme_validation": {
    "is_valid": True,
    "certificate_number": "BDR00172ALHL2",
    "establishment_name": "AAROGYA HOSPITAL",
    "confidence": 0.90
  },
  "data_quality": {
    "completeness_score": 1.0,
    "accuracy_score": 1.0,
    "overall_score": 1.0
  },
  "overall_confidence": 0.93
}
```

### 2. Fast Validator Agent (`src/agents/fast_validator.py`)
**Quick KPME validation with cache-first strategy**

**Features:**
- ✅ Ultra-fast direct database lookup (bypasses agent)
- ✅ Cache-first validation strategy
- ✅ Search by certificate, phone, or name
- ✅ Sub-second response times
- ✅ Agent integration flags
- ✅ Testing scenario support

**Quick Check (Direct DB - No API):**
```python
result = agent.quick_kpme_check(certificate_number="BDR00172ALHL2")
# Returns complete establishment data instantly
```

### 3. KPME Database (`src/database/kpme_db.py`)
**SQLite database with 5 KPME Karnataka data tables**

**Database Contents:**
- 📊 **1,000 establishments** across Karnataka
- 👥 **4,483 staff records** with qualifications
- 📍 **899 establishments** with GPS coordinates
- 🏥 **Top districts**: Bangalore (157), Bengaluru (54), Kalaburagi (44)
- 💊 **Systems**: Allopathy (739), Ayurveda (199), Homeopathy (45)

**Tables:**
- `establishments` - Main KPME establishments
- `staff` - Medical staff with fees
- `specialties` - Specialty services
- `certificates` - Certificate details
- `treatments` - Treatment charges

**Indexed Queries:**
- Certificate number lookup
- Phone number search
- Name search (partial match)
- District filtering
- Staff registration search

## Demo Scripts

### 1. `demo_quick_validators.py` ⭐ **RECOMMENDED FOR MENTOR**
**No API required - runs immediately**

Shows:
- Direct database searches (certificate, phone, name, district)
- Ultra-fast validator quick checks
- Staff search by name and registration
- Complete establishment details with staff
- Database statistics and analytics

**Runtime:** ~2 seconds

### 2. `demo_data_validator.py`
**Requires Gemini API quota**

Demonstrates:
- Basic KPME validation
- Incomplete data validation
- Agent integration flags
- Expected output testing
- Confidence scoring

### 3. `demo_fast_validator.py`
**Requires Gemini API quota**

Demonstrates:
- Quick KPME check (no API)
- Fast validation by certificate
- Fast validation by phone/name
- Cache behavior
- Agent integration

## Architecture Highlights

### EL Branch - KPME-Only Implementation
- ✅ Removed multi-region support (USA/India)
- ✅ Pure KPME Karnataka focus
- ✅ Direct database integration
- ✅ Agent integration architecture ready
- ✅ Testing scenario support built-in

### Agent Integration Ready
Both validators support future integration with:
- **Web Scraper Agent** - Scrape additional provider data
- **Enrichment Agent** - Enrich with external sources
- **Compliance Agent** - Regulatory compliance checks

Integration via flags:
```python
result = await agent.validate(
    provider_data=data,
    use_web_scraper=True,
    use_enrichment=True,
    use_compliance=True
)
```

### Testing Support
Both validators support testing scenarios:
```python
expected_output = {
    "is_valid": True,
    "confidence": 0.95,
    # ... expected result
}

result = await agent.validate(
    provider_data=test_data,
    expected_output=expected_output  # Bypasses actual validation
)
```

## Sample Output (Quick Demo)

```
================================================================================
DEMO 1: Direct Database Search
================================================================================

Using establishment: AAROGYA HOSPITAL FOR WOMEN AND CHILDREN
Certificate: BDR00172ALHL2
District: BIDAR

[A] Search by Certificate Number:
  ✓ Found: AAROGYA HOSPITAL FOR WOMEN AND CHILDREN
    Category: Hospital (Level 2)
    Phone: 9448452147.0
    Email: ymb1922@gmail.com
    GPS: (17.76, 77.13)

================================================================================
DEMO 2: Fast Validator - Ultra Quick Check
================================================================================

  1. AAROGYA HOSPITAL FOR WOMEN AND CHILDREN
     Certificate: BDR00172ALHL2
     Category: Hospital (Level 2)
     District: BIDAR
     System: Allopathy
     Beds: 10.0

================================================================================
DEMO 5: Database Statistics
================================================================================

📊 Database Overview:
  Total Establishments: 1,000
  Total Staff: 4,483
  Establishments with GPS: 899

📍 Top 5 Districts:
  1. BANGALORE: 157 establishments
  2. BENGALURU: 54 establishments
  3. KALABURAGI: 44 establishments
  4. UDUPI: 39 establishments
  5. MANGALORE: 35 establishments
```

## Key Achievements

### ✅ Completed
1. KPME database integration (5 CSV files → SQLite)
2. Data Validator Agent (KPME-only)
3. Fast Validator Agent (KPME-only)
4. Agent integration architecture
5. Testing scenario support
6. Comprehensive unit tests (25 test cases)
7. Tutorial demos
8. Complete documentation

### 🎯 Ready For
- Integration with Web Scraper Agent
- Integration with Enrichment Agent
- Integration with Compliance Agent
- Production deployment

## Technical Stack

- **Database**: SQLite (1,000 establishments, indexed queries)
- **AI Framework**: Pydantic AI with Gemini 2.0 Flash
- **Agents**: Data Validator + Fast Validator
- **Cache**: Redis (with Memory fallback)
- **Testing**: Pytest with asyncio support

## EL Branch Commits

```
39ed1e9 Add Tutorial demos for KPME validator agents
f122c8d Refactor validators to KPME-only with agent integration and testing
bebccfd Add KPME Karnataka database integration for India validators
```

## Next Steps

1. Show mentor: `python src/Tutorial/demo_quick_validators.py`
2. Discuss agent integration strategy
3. Plan web scraper, enrichment, compliance agent implementations
4. Deploy to staging environment

---

**For Questions**: See `src/Tutorial/README.md` for detailed documentation
