"""
Abstract base class for cache clients.
"""

from abc import ABC, abstractmethod
from typing import Optional, Any


class BaseCacheClient(ABC):
    """
    Abstract cache client interface.

    All cache implementations (Redis, Memory) must implement this interface.
    """

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: int) -> bool:
        """
        Set value in cache with TTL.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds

        Returns:
            True if successful
        """
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        Args:
            key: Cache key

        Returns:
            True if key existed and was deleted
        """
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Check if key exists in cache.

        Args:
            key: Cache key

        Returns:
            True if key exists and is not expired
        """
        pass

    @abstractmethod
    async def clear(self) -> bool:
        """
        Clear all keys from cache.

        Returns:
            True if successful
        """
        pass

    @abstractmethod
    async def ping(self) -> bool:
        """
        Check if cache is available.

        Returns:
            True if cache is reachable
        """
        pass
