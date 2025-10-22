"""
Tests for memory cache implementation.
"""

import pytest
import asyncio
from datetime import datetime, timedelta

from src.cache.memory import MemoryCacheClient


class TestMemoryCacheClient:
    """Test MemoryCacheClient."""

    @pytest.mark.asyncio
    async def test_set_and_get(self):
        """Test basic set and get operations."""
        cache = MemoryCacheClient(max_size=100)

        # Set value
        success = await cache.set("test_key", {"data": "test_value"}, ttl=60)
        assert success is True

        # Get value
        value = await cache.get("test_key")
        assert value == {"data": "test_value"}

    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self):
        """Test getting non-existent key returns None."""
        cache = MemoryCacheClient(max_size=100)

        value = await cache.get("nonexistent")
        assert value is None

    @pytest.mark.asyncio
    async def test_ttl_expiration(self):
        """Test that entries expire after TTL."""
        cache = MemoryCacheClient(max_size=100)

        # Set with 1 second TTL
        await cache.set("test_key", "test_value", ttl=1)

        # Should exist immediately
        value = await cache.get("test_key")
        assert value == "test_value"

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Should be expired
        value = await cache.get("test_key")
        assert value is None

    @pytest.mark.asyncio
    async def test_delete(self):
        """Test delete operation."""
        cache = MemoryCacheClient(max_size=100)

        await cache.set("test_key", "test_value", ttl=60)
        assert await cache.exists("test_key") is True

        # Delete key
        deleted = await cache.delete("test_key")
        assert deleted is True

        # Should not exist
        assert await cache.exists("test_key") is False

    @pytest.mark.asyncio
    async def test_exists(self):
        """Test exists operation."""
        cache = MemoryCacheClient(max_size=100)

        # Non-existent key
        assert await cache.exists("test_key") is False

        # Set key
        await cache.set("test_key", "test_value", ttl=60)
        assert await cache.exists("test_key") is True

    @pytest.mark.asyncio
    async def test_clear(self):
        """Test clear operation."""
        cache = MemoryCacheClient(max_size=100)

        # Add multiple keys
        await cache.set("key1", "value1", ttl=60)
        await cache.set("key2", "value2", ttl=60)
        await cache.set("key3", "value3", ttl=60)

        # Clear cache
        success = await cache.clear()
        assert success is True

        # All keys should be gone
        assert await cache.exists("key1") is False
        assert await cache.exists("key2") is False
        assert await cache.exists("key3") is False

    @pytest.mark.asyncio
    async def test_lru_eviction(self):
        """Test LRU eviction when cache is full."""
        cache = MemoryCacheClient(max_size=3)

        # Fill cache to capacity
        await cache.set("key1", "value1", ttl=60)
        await cache.set("key2", "value2", ttl=60)
        await cache.set("key3", "value3", ttl=60)

        # Add 4th item - should evict key1 (least recently used)
        await cache.set("key4", "value4", ttl=60)

        # key1 should be evicted
        assert await cache.exists("key1") is False

        # Others should still exist
        assert await cache.exists("key2") is True
        assert await cache.exists("key3") is True
        assert await cache.exists("key4") is True

    @pytest.mark.asyncio
    async def test_ping(self):
        """Test ping operation (always available)."""
        cache = MemoryCacheClient(max_size=100)
        assert await cache.ping() is True

    @pytest.mark.asyncio
    async def test_update_existing_key(self):
        """Test updating existing key moves it to end (most recently used)."""
        cache = MemoryCacheClient(max_size=3)

        # Fill cache
        await cache.set("key1", "value1", ttl=60)
        await cache.set("key2", "value2", ttl=60)
        await cache.set("key3", "value3", ttl=60)

        # Update key1 (should move to end)
        await cache.set("key1", "updated_value1", ttl=60)

        # Add key4 - should evict key2 (now least recently used)
        await cache.set("key4", "value4", ttl=60)

        # key1 should still exist (was updated)
        assert await cache.get("key1") == "updated_value1"

        # key2 should be evicted
        assert await cache.exists("key2") is False

    def test_get_stats(self):
        """Test cache statistics."""
        cache = MemoryCacheClient(max_size=10)

        stats = cache.get_stats()
        assert stats["total_entries"] == 0
        assert stats["max_size"] == 10
        assert stats["fill_percentage"] == 0.0
