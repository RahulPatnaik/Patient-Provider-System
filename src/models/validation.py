"""
Validation result models.

This file should contain:
- ValidationIssue - Model for individual validation issues (field, severity, message, suggestion)
- ValidationResult - Base validation result from a single check
  - is_valid, confidence, source, issues, metadata, timestamp
- NPIValidationResult - NPI-specific validation result (extends ValidationResult)
  - npi_found, npi_active, provider_name_match
- LicenseValidationResult - License-specific validation result
  - license_found, license_active, license_expiry_days
- AddressValidationResult - Address-specific validation result
  - address_found, coordinates, formatted_address
- WebPresenceValidationResult - Web presence validation result
  - website_active, social_media_found, reviews_count, average_rating
- ComplianceValidationResult - Compliance validation result
  - sanctions_found, malpractice_claims, regulatory_actions
- AggregatedValidationResult - Aggregated results from all validators
  - validation_path, status
  - Individual validation results (npi, license, address, web, compliance)
  - Aggregated metrics (overall_confidence, confidence_level, total_issues, critical_issues)
  - Timing metadata
- FinalDecisionResult - Final decision from LLM Router
  - decision, confidence_score, reasoning
  - Review information (requires_manual_review, review_priority, review_notes)
  - Decision metadata (timestamp, llm_model_used, processing_time_ms)
"""
