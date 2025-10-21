"""
Provider data models.

This file should contain:
- AddressModel - Pydantic model for address information (street, city, state, zip_code, country)
- LicenseModel - Pydantic model for license information (license_number, state, status, issue_date, expiration_date, verified)
- ProviderInputModel - Pydantic model for provider data input from user/API
  - Basic info: first_name, last_name, organization_name, provider_type
  - Contact: email, phone
  - Professional: NPI (10-digit), specialty
  - Address and licenses
  - Additional: website, practice_name
- EnrichedProviderModel - Pydantic model for enriched provider data after validation
  - Original provider input
  - Verification flags (verified_npi, verified_licenses, verified_address)
  - Enrichment data (google_maps_data, web_presence_data, compliance_data)
  - Metadata (timestamp, data_sources)
- ProviderOutputModel - Final provider output model for database storage
  - Provider data, validation status, decision, confidence score
  - Timestamps (created_at, updated_at)
"""
