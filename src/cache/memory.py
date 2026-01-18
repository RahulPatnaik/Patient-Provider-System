"""
In-memory cache implementation with TTL and LRU eviction.
"""

import time
import logging
import threading
from typing import Optional, Any, Dict
from collections import OrderedDict
from datetime import datetime, timedelta

from cache.base import BaseCacheClient


logger = logging.getLogger(__name__)


class MemoryCacheClient(BaseCacheClient):
    """
    In-memory cache client with TTL and LRU eviction.

    Features:
    - Thread-safe operations with locking
    - TTL (Time To Live) support
    - LRU (Least Recently Used) eviction
    - Automatic cleanup of expired entries
    - Fallback when Redis is unavailable
    """

    def __init__(self, max_size: int = 1000, cleanup_interval: int = 60):
        """
        Initialize memory cache client.

        Args:
            max_size: Maximum number of items in cache (LRU eviction)
            cleanup_interval: Interval in seconds to cleanup expired items
        """
        self.max_size = max_size
        self.cleanup_interval = cleanup_interval
        self._cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._lock = threading.Lock()
        self._last_cleanup = time.time()
        logger.info(f"Initialized in-memory cache with max_size={max_size}")

    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        """Check if cache entry is expired."""
        if "expires_at" not in entry:
            return False
        return datetime.now() > entry["expires_at"]

    def _cleanup_expired(self):
        """Remove expired entries from cache."""
        if time.time() - self._last_cleanup < self.cleanup_interval:
            return

        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if self._is_expired(entry)
            ]
            for key in expired_keys:
                del self._cache[key]

            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

            self._last_cleanup = time.time()

    def _evict_lru(self):
        """Evict least recently used item if cache is full."""
        if len(self._cache) >= self.max_size:
            # Remove oldest item (first item in OrderedDict)
            evicted_key, _ = self._cache.popitem(last=False)
            logger.debug(f"Evicted LRU cache entry: {evicted_key}")

    async def get(self, key: str) -> Optional[Any]:
        """Get value from memory cache."""
        self._cleanup_expired()

        with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]

            # Check if expired
            if self._is_expired(entry):
                del self._cache[key]
                return None

            # Move to end (mark as recently used)
            self._cache.move_to_end(key)

            return entry["value"]

    async def set(self, key: str, value: Any, ttl: int) -> bool:
        """Set value in memory cache with TTL."""
        try:
            self._cleanup_expired()

            with self._lock:
                # Evict LRU if needed
                if key not in self._cache:
                    self._evict_lru()

                # Calculate expiration time
                expires_at = datetime.now() + timedelta(seconds=ttl)

                # Store entry
                self._cache[key] = {
                    "value": value,
                    "expires_at": expires_at
                }

                # Move to end (mark as most recently used)
                self._cache.move_to_end(key)

            return True

        except Exception as e:
            logger.error(f"Memory cache set error for key '{key}': {str(e)}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete key from memory cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists in memory cache."""
        self._cleanup_expired()

        with self._lock:
            if key not in self._cache:
                return False

            entry = self._cache[key]

            # Check if expired
            if self._is_expired(entry):
                del self._cache[key]
                return False

            return True

    async def clear(self) -> bool:
        """Clear all entries from memory cache."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.info(f"Cleared {count} entries from memory cache")
            return True

    async def ping(self) -> bool:
        """Memory cache is always available."""
        return True

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_entries = len(self._cache)
            expired_entries = sum(
                1 for entry in self._cache.values()
                if self._is_expired(entry)
            )
            return {
                "total_entries": total_entries,
                "expired_entries": expired_entries,
                "active_entries": total_entries - expired_entries,
                "max_size": self.max_size,
                "fill_percentage": (total_entries / self.max_size) * 100
            }
