"""
Agent response models for Pydantic AI agents.

This file should contain:
- AgentResponse - Base agent response model
  - agent_type, success, confidence, message, error
  - Execution metadata (execution_time_ms, timestamp)
- SupervisorAgentResponse - Supervisor Agent specific response
  - assigned_path, reasoning
- FastValidatorAgentResponse - Fast Validator Agent response (simple path)
  - cache_hit, npi_quick_check_passed, validation_result
- DataValidatorAgentResponse - Data Validator Agent response
  - npi_verified, licenses_verified, validation_results, data_quality_score
- WebScraperAgentResponse - Web Scraper Agent response
  - sources_scraped, data_extracted, scraping_errors
- EnrichmentAgentResponse - Enrichment Agent response
  - enriched_fields, enrichment_data, data_sources_used
- ComplianceAgentResponse - Compliance Agent response
  - compliance_passed, sanctions_found, malpractice_claims, regulatory_issues
  - requires_manual_review
- ParallelAgentResults - Aggregated results from parallel agent execution
  - Individual agent results (data_validator, web_scraper, enrichment, compliance)
  - Aggregated metrics (all_successful, average_confidence, total_execution_time_ms)
"""
