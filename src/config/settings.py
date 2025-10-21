"""
Application settings using Pydantic Settings.

This file should contain:
- Settings class (BaseSettings) that loads configuration from environment variables
- Application settings (app_name, version, environment, debug, log_level)
- API server settings (host, port, reload)
- Database configuration (database_url, connection pool settings)
- Redis cache configuration (redis_url, password, ttl, enabled)
- External API configurations:
  - NPI Registry API (base_url, version, api_key)
  - State License API (url, api_key)
  - Google Maps API (api_key, base_url)
  - Gemini API (api_key, model)
  - Grog API (url, api_key)
  - Regulatory Database (url, api_key)
- LLM configurations (OpenAI, Anthropic - api_keys, models, temperature, max_tokens)
- Selenium configuration (headless, timeout, driver_path, page_load_timeout)
- Celery configuration (broker_url, result_backend)
- Confidence scoring thresholds (auto_approve, auto_reject)
- Validation configuration (cache_expiry, parallel_agents, max_workers, timeout)
- Rate limiting settings
- CORS configuration (origins, credentials, methods, headers)
- Security settings (secret_key, algorithm, token_expire)
- Manual review settings (webhook_url, email settings)
- Monitoring settings (sentry_dsn, metrics enabled, metrics_port)
- Helper methods (e.g., cors_origins_list property)
- Global settings instance
"""
