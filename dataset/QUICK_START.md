# Quick Start - Karnataka Healthcare Data

## 📂 Main File
```
dataset/karnataka_healthcare_master.csv
```
**6,605 records | 955 KB | 17 columns**

## 🚀 Quick Usage

### Load in Python
```python
import pandas as pd

# Load entire dataset
df = pd.read_csv('dataset/karnataka_healthcare_master.csv')

# Filter by data type
kpme = df[df['data_type'] == 'KPME_ESTABLISHMENT']
facilities = df[df['data_type'] == 'HEALTHCARE_FACILITY']
districts = df[df['data_type'] == 'DISTRICT_REFERENCE']

# Search for a facility
result = df[df['facility_name'].str.contains('HOSPITAL', case=False, na=False)]

# Filter by district
bangalore = df[df['district_name'] == 'BANGALORE']

# Get facilities with GPS
with_gps = df[(df['latitude'] != '') & (df['longitude'] != '')]
```

### Filter Examples

#### Get all KPME hospitals
```python
kpme_hospitals = df[
    (df['data_type'] == 'KPME_ESTABLISHMENT') & 
    (df['facility_type'].str.contains('HOSPITAL', na=False))
]
```

#### Get Ayurveda establishments
```python
ayurveda = df[df['system_of_medicine'] == 'Ayurveda']
```

#### Get facilities in Mysuru district
```python
mysuru = df[df['district_name'] == 'MYSURU']
```

## 📊 Data Types

| Type | Count | Use For |
|------|-------|---------|
| `KPME_ESTABLISHMENT` | 1,020 | Licensed establishments with certificates |
| `HEALTHCARE_FACILITY` | 4,219 | Facilities with GPS coordinates |
| `DISTRICT_REFERENCE` | 31 | District master data |
| `SPECIALIZATION_REFERENCE` | 748 | Valid medical specializations |
| `DEGREE_REFERENCE` | 389 | Recognized medical degrees |
| `COUNCIL_REFERENCE` | 198 | Medical council data |

## 🔍 Common Queries

### Count by system of medicine
```python
df.groupby(['data_type', 'system_of_medicine']).size()
```

### Facilities per district
```python
df[df['data_type'] == 'HEALTHCARE_FACILITY'].groupby('district_name').size().sort_values(ascending=False)
```

### KPME establishments by category
```python
df[df['data_type'] == 'KPME_ESTABLISHMENT']['facility_type'].value_counts()
```

## 📁 Other Files

- `KPME_DATA.csv` - Raw KPME scraper output (1,020 records, 207 KB)
- `karnataka_healthcare_summary.csv` - Statistics summary
- `DATA_SUMMARY.md` - Detailed documentation
- `ALTERNATIVE_DATA_SOURCES.txt` - Other data source options

## 🔧 Scripts

```bash
# Regenerate master CSV
python dataset/scripts/create_master_csv.py

# Scrape fresh KPME data
python dataset/scripts/scrapers/kpme_basic_scraper.py
```

## 💡 Tips

1. **KPME data** has certificate numbers and validity dates - use for verification
2. **OSM facilities** have GPS coordinates - use for mapping
3. **District reference** helps standardize district names
4. Empty fields are represented as empty strings `''`
5. All text is uppercase for easier matching

## 🎯 Example Use Case

**Verify a clinic's KPME registration:**
```python
# Search by name
clinic = df[
    (df['data_type'] == 'KPME_ESTABLISHMENT') & 
    (df['facility_name'].str.contains('CLINIC NAME', case=False))
]

# Check certificate from description field
print(clinic['description'].values)
# Output: "KPME Cert: ABC12345, Valid: 31 Dec 2026"
```

---
For detailed documentation, see `DATA_SUMMARY.md`
