"""
Redis cache implementation with connection pooling and retry logic.
"""

import json
import asyncio
import logging
from typing import Optional, Any
from redis import asyncio as aioredis
from redis.exceptions import RedisError, ConnectionError, TimeoutError

from cache.base import BaseCacheClient


logger = logging.getLogger(__name__)


class RedisCacheClient(BaseCacheClient):
    """
    Redis cache client with async support.

    Features:
    - Connection pooling
    - Automatic retry with exponential backoff
    - JSON serialization
    - Key prefixing for namespacing
    - Graceful error handling
    """

    def __init__(
        self,
        redis_url: str,
        password: Optional[str] = None,
        key_prefix: str = "pps",
        max_retries: int = 3,
        retry_delay: float = 0.1
    ):
        """
        Initialize Redis cache client.

        Args:
            redis_url: Redis connection URL (e.g., redis://localhost:6379/0)
            password: Redis password (optional)
            key_prefix: Prefix for all cache keys (default: "pps" for Patient-Provider-System)
            max_retries: Maximum number of retry attempts
            retry_delay: Initial retry delay in seconds (exponential backoff)
        """
        self.redis_url = redis_url
        self.password = password
        self.key_prefix = key_prefix
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._client: Optional[aioredis.Redis] = None
        self._connected = False

    async def _get_client(self) -> aioredis.Redis:
        """Get or create Redis client with connection pooling."""
        if self._client is None:
            self._client = await aioredis.from_url(
                self.redis_url,
                password=self.password,
                encoding="utf-8",
                decode_responses=True,
                max_connections=10,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            try:
                await self._client.ping()
                self._connected = True
                logger.info("Redis connection established successfully")
            except RedisError as e:
                self._connected = False
                logger.error(f"Failed to connect to Redis: {str(e)}")
                raise

        return self._client

    def _make_key(self, key: str) -> str:
        """Create namespaced key with prefix."""
        return f"{self.key_prefix}:{key}"

    async def _retry_operation(self, operation, *args, **kwargs) -> Any:
        """Execute operation with retry logic."""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return await operation(*args, **kwargs)
            except (ConnectionError, TimeoutError) as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Redis operation failed (attempt {attempt + 1}/{self.max_retries}). "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Redis operation failed after {self.max_retries} attempts")

        raise last_exception

    async def get(self, key: str) -> Optional[Any]:
        """Get value from Redis cache."""
        try:
            client = await self._get_client()
            namespaced_key = self._make_key(key)

            async def _get():
                value = await client.get(namespaced_key)
                if value is None:
                    return None
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    # Return as string if not JSON
                    return value

            return await self._retry_operation(_get)

        except RedisError as e:
            logger.error(f"Redis get error for key '{key}': {str(e)}")
            return None

    async def set(self, key: str, value: Any, ttl: int) -> bool:
        """Set value in Redis cache with TTL."""
        try:
            client = await self._get_client()
            namespaced_key = self._make_key(key)

            # Serialize value to JSON
            try:
                serialized_value = json.dumps(value)
            except (TypeError, ValueError) as e:
                logger.error(f"Failed to serialize value for key '{key}': {str(e)}")
                return False

            async def _set():
                return await client.setex(namespaced_key, ttl, serialized_value)

            result = await self._retry_operation(_set)
            return result is not None

        except RedisError as e:
            logger.error(f"Redis set error for key '{key}': {str(e)}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete key from Redis cache."""
        try:
            client = await self._get_client()
            namespaced_key = self._make_key(key)

            async def _delete():
                return await client.delete(namespaced_key)

            deleted_count = await self._retry_operation(_delete)
            return deleted_count > 0

        except RedisError as e:
            logger.error(f"Redis delete error for key '{key}': {str(e)}")
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists in Redis cache."""
        try:
            client = await self._get_client()
            namespaced_key = self._make_key(key)

            async def _exists():
                return await client.exists(namespaced_key)

            result = await self._retry_operation(_exists)
            return result > 0

        except RedisError as e:
            logger.error(f"Redis exists error for key '{key}': {str(e)}")
            return False

    async def clear(self) -> bool:
        """Clear all keys with our prefix from Redis."""
        try:
            client = await self._get_client()
            pattern = f"{self.key_prefix}:*"

            async def _clear():
                keys = await client.keys(pattern)
                if keys:
                    return await client.delete(*keys)
                return 0

            deleted_count = await self._retry_operation(_clear)
            logger.info(f"Cleared {deleted_count} keys from Redis cache")
            return True

        except RedisError as e:
            logger.error(f"Redis clear error: {str(e)}")
            return False

    async def ping(self) -> bool:
        """Check if Redis is available."""
        try:
            client = await self._get_client()

            async def _ping():
                return await client.ping()

            result = await self._retry_operation(_ping)
            return result is True

        except RedisError as e:
            logger.error(f"Redis ping error: {str(e)}")
            return False

    async def close(self):
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None
            self._connected = False
            logger.info("Redis connection closed")
