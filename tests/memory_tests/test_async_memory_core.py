"""Tests for async support in the memory core."""

import asyncio
from datetime import timedelta

import pytest

from cachier import cachier


@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_memory_core_allows_sync_and_async_functions():
    """Ensure memory core can decorate both sync and async functions."""
    sync_call_count = 0
    async_call_count = 0

    @cachier(backend="memory")
    def sync_memory_cached(x: int) -> int:
        nonlocal sync_call_count
        sync_call_count += 1
        return x + sync_call_count

    @cachier(backend="memory")
    async def async_memory_cached(x: int) -> int:
        nonlocal async_call_count
        async_call_count += 1
        await asyncio.sleep(0.01)
        return x + async_call_count

    sync_memory_cached.clear_cache()
    async_memory_cached.clear_cache()
    try:
        sync_val1 = sync_memory_cached(2)
        sync_val2 = sync_memory_cached(2)
        assert sync_val1 == sync_val2 == 3
        assert sync_call_count == 1

        async_val1 = await async_memory_cached(2)
        async_val2 = await async_memory_cached(2)
        assert async_val1 == async_val2 == 3
        assert async_call_count == 1
    finally:
        sync_memory_cached.clear_cache()
        async_memory_cached.clear_cache()


@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_memory_basic_caching():
    """Ensure async functions are cached by the memory backend."""
    call_count = 0

    @cachier(backend="memory")
    async def async_memory_cached(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return x + call_count

    async_memory_cached.clear_cache()
    try:
        value1 = await async_memory_cached(3)
        value2 = await async_memory_cached(3)
        assert value1 == value2 == 4
        assert call_count == 1
    finally:
        async_memory_cached.clear_cache()


@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_memory_next_time_returns_stale_then_updates():
    """Ensure next_time returns stale value and updates asynchronously."""
    call_count = 0

    @cachier(
        backend="memory",
        stale_after=timedelta(milliseconds=150),
        next_time=True,
    )
    async def async_memory_next_time(_: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return call_count

    async_memory_next_time.clear_cache()
    try:
        first = await async_memory_next_time(1)
        assert first == 1

        await asyncio.sleep(0.2)

        stale = await async_memory_next_time(1)
        assert stale == 1

        updated = stale
        for _ in range(10):
            await asyncio.sleep(0.05)
            updated = await async_memory_next_time(1)
            if updated > 1:
                break

        assert updated > 1
        assert call_count >= 2
        await asyncio.sleep(0.1)
    finally:
        async_memory_next_time.clear_cache()
