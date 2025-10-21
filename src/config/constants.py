"""
Application constants.

This file should contain:
- API_RESPONSES - Dictionary of standard API response messages
- SIMPLE_PATH_CRITERIA - Criteria for routing to simple validation path
- DEFAULT_CONFIDENCE_THRESHOLDS - Default confidence thresholds for decisions
- AGENT_TIMEOUTS - Timeout settings for each agent type (in seconds)
- CACHE_KEY_TEMPLATES - String templates for cache keys
- CACHE_TTL_SECONDS - TTL values for different cache types
- NPI_REGISTRY_ENDPOINTS - API endpoint paths
- HTTP_TIMEOUTS - Connection and read timeout settings
- HTTP_RETRY_CONFIG - Retry configuration (max_retries, backoff_factor, status_forcelist)
- NPI_VALIDATION_RULES - Rules for NPI validation (length, numeric_only)
- LICENSE_VALIDATION_RULES - Rules for license validation (min/max length)
- US_STATES - List of US state codes
- PARALLEL_AGENT_CONFIG - Configuration for parallel execution (max_workers, timeout)
- LOG_FORMAT and LOG_DATE_FORMAT - Logging format strings
- DB_NAMING_CONVENTION - SQLAlchemy naming conventions for constraints
- SELENIUM_CONFIG - Selenium driver configuration (implicit_wait, page_load_timeout, window_size)
- USER_AGENTS - List of user agent strings for web scraping
- RATE_LIMIT_STORAGE_URL - Redis URL for rate limiting
- REVIEW_PRIORITY_LEVELS - Priority level constants
- FEEDBACK_LOOP_CONFIG - Feedback loop configuration (enabled, min_samples, update_frequency)
"""
