"""
Cache factory to create appropriate cache client based on configuration.
"""

import os
import logging
from typing import Optional

from cache.base import BaseCacheClient
from cache.redis import RedisCacheClient
from cache.memory import MemoryCacheClient


logger = logging.getLogger(__name__)


def get_cache_client(
    redis_url: Optional[str] = None,
    redis_password: Optional[str] = None,
    cache_enabled: bool = True,
    fallback_to_memory: bool = True
) -> BaseCacheClient:
    """
    Factory function to create appropriate cache client.

    Priority:
    1. Redis (if enabled and available)
    2. Memory cache (if Redis fails and fallback_to_memory=True)

    Args:
        redis_url: Redis connection URL (defaults to REDIS_URL env var)
        redis_password: Redis password (defaults to REDIS_PASSWORD env var)
        cache_enabled: Whether caching is enabled (defaults to CACHE_ENABLED env var)
        fallback_to_memory: Whether to fallback to memory cache if Redis fails

    Returns:
        BaseCacheClient: Redis or Memory cache client
    """
    # Get configuration from environment if not provided
    if redis_url is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    if redis_password is None:
        redis_password = os.getenv("REDIS_PASSWORD", None)

    if not cache_enabled:
        cache_enabled = os.getenv("CACHE_ENABLED", "true").lower() == "true"

    # If caching is disabled, return memory cache (simpler than disabling completely)
    if not cache_enabled:
        logger.info("Caching is disabled. Using memory cache with minimal capacity.")
        return MemoryCacheClient(max_size=10)

    # Try to use Redis
    try:
        logger.info(f"Attempting to connect to Redis at {redis_url}")
        redis_client = RedisCacheClient(
            redis_url=redis_url,
            password=redis_password
        )

        # Test connection synchronously using asyncio
        import asyncio
        try:
            # Try to create an event loop or get existing one
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Test ping
            is_available = loop.run_until_complete(redis_client.ping())

            if is_available:
                logger.info("Successfully connected to Redis cache")
                return redis_client
            else:
                raise ConnectionError("Redis ping failed")

        except Exception as e:
            logger.warning(f"Redis connection test failed: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Failed to initialize Redis cache: {str(e)}")

        if fallback_to_memory:
            logger.info("Falling back to in-memory cache")
            return MemoryCacheClient()
        else:
            raise RuntimeError("Redis cache unavailable and fallback disabled")


# Singleton instance (optional - can be used for global cache)
_cache_instance: Optional[BaseCacheClient] = None


def get_cache_instance() -> BaseCacheClient:
    """
    Get singleton cache instance.

    Returns:
        BaseCacheClient: Singleton cache client
    """
    global _cache_instance

    if _cache_instance is None:
        _cache_instance = get_cache_client()

    return _cache_instance


def reset_cache_instance():
    """Reset singleton cache instance (useful for testing)."""
    global _cache_instance
    _cache_instance = None