"""Tests for async/coroutine support in Cachier."""

import asyncio
import queue
import threading
from datetime import datetime, timedelta
from random import random
from time import sleep, time

import pytest

from cachier import cachier


# Test basic async caching with memory backend
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_basic_memory():
    """Test basic async caching with memory backend."""
    @cachier(backend="memory")
    async def async_func(x):
        await asyncio.sleep(0.1)
        return x * 2
    
    async_func.clear_cache()
    
    # First call should execute
    result1 = await async_func(5)
    assert result1 == 10
    
    # Second call should use cache
    start = time()
    result2 = await async_func(5)
    end = time()
    assert result2 == 10
    assert end - start < 0.05  # Should be much faster than 0.1s
    
    async_func.clear_cache()


# Test async caching with pickle backend
@pytest.mark.pickle
@pytest.mark.asyncio
async def test_async_basic_pickle():
    """Test basic async caching with pickle backend."""
    @cachier(backend="pickle")
    async def async_func(x):
        await asyncio.sleep(0.1)
        return x * 3
    
    async_func.clear_cache()
    
    # First call should execute
    result1 = await async_func(7)
    assert result1 == 21
    
    # Second call should use cache
    start = time()
    result2 = await async_func(7)
    end = time()
    assert result2 == 21
    assert end - start < 0.05  # Should be much faster than 0.1s
    
    async_func.clear_cache()


# Test async with stale_after
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_stale_after():
    """Test async caching with stale_after."""
    call_count = 0
    
    @cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=False)
    async def async_func(x):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.1)
        return x * 2
    
    async_func.clear_cache()
    call_count = 0
    
    # First call
    result1 = await async_func(5)
    assert result1 == 10
    assert call_count == 1
    
    # Second call - should use cache
    result2 = await async_func(5)
    assert result2 == 10
    assert call_count == 1
    
    # Wait for cache to become stale
    await asyncio.sleep(1.5)
    
    # Third call - should recalculate
    result3 = await async_func(5)
    assert result3 == 10
    assert call_count == 2
    
    async_func.clear_cache()


# Test async with next_time=True
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_next_time():
    """Test async caching with next_time=True."""
    call_count = 0
    
    @cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=True)
    async def async_func(x):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.1)
        return call_count * 10
    
    async_func.clear_cache()
    call_count = 0
    
    # First call
    result1 = await async_func(5)
    assert result1 == 10
    assert call_count == 1
    
    # Second call - should use cache
    result2 = await async_func(5)
    assert result2 == 10
    assert call_count == 1
    
    # Wait for cache to become stale
    await asyncio.sleep(1.5)
    
    # Third call - should return stale value and trigger background update
    result3 = await async_func(5)
    assert result3 == 10  # Still returns old value
    
    # Wait for background calculation to complete
    await asyncio.sleep(0.5)
    
    # Fourth call - should return new value
    result4 = await async_func(5)
    assert result4 == 20  # New value from background calculation
    
    async_func.clear_cache()


# Test async with ignore_cache
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_ignore_cache():
    """Test async caching with ignore_cache."""
    call_count = 0
    
    @cachier(backend="memory")
    async def async_func(x):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.1)
        return call_count * 10
    
    async_func.clear_cache()
    call_count = 0
    
    # First call
    result1 = await async_func(5)
    assert result1 == 10
    
    # Second call with ignore_cache
    result2 = await async_func(5, cachier__skip_cache=True)
    assert result2 == 20
    
    # Third call - should use cache from first call
    result3 = await async_func(5)
    assert result3 == 10
    
    async_func.clear_cache()


# Test async with overwrite_cache
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_overwrite_cache():
    """Test async caching with overwrite_cache."""
    call_count = 0
    
    @cachier(backend="memory")
    async def async_func(x):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.1)
        return call_count * 10
    
    async_func.clear_cache()
    call_count = 0
    
    # First call
    result1 = await async_func(5)
    assert result1 == 10
    
    # Second call with overwrite_cache
    result2 = await async_func(5, cachier__overwrite_cache=True)
    assert result2 == 20
    
    # Third call - should use new cached value
    result3 = await async_func(5)
    assert result3 == 20
    
    async_func.clear_cache()


# Test async method
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_method():
    """Test async caching on class methods."""
    class MyClass:
        def __init__(self, value):
            self.value = value
        
        @cachier(backend="memory")
        async def async_method(self, x):
            await asyncio.sleep(0.1)
            return x * self.value
    
    obj1 = MyClass(2)
    obj2 = MyClass(3)
    
    obj1.async_method.clear_cache()
    
    # First call on obj1
    result1 = await obj1.async_method(5)
    assert result1 == 10
    
    # Second call on obj1 - should use cache
    start = time()
    result2 = await obj1.async_method(5)
    end = time()
    assert result2 == 10
    assert end - start < 0.05
    
    # Call on obj2 with same argument - should also use cache
    # (because cache is based on method arguments, not instance)
    result3 = await obj2.async_method(5)
    assert result3 == 10  # Returns cached value from obj1
    
    obj1.async_method.clear_cache()


# Test that sync functions still work
@pytest.mark.memory
def test_sync_still_works():
    """Ensure sync functions still work after adding async support."""
    @cachier(backend="memory")
    def sync_func(x):
        sleep(0.1)
        return x * 2
    
    sync_func.clear_cache()
    
    # First call
    result1 = sync_func(5)
    assert result1 == 10
    
    # Second call should use cache
    start = time()
    result2 = sync_func(5)
    end = time()
    assert result2 == 10
    assert end - start < 0.05
    
    sync_func.clear_cache()


# Test async with different argument types
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_different_args():
    """Test async caching with different argument types."""
    @cachier(backend="memory")
    async def async_func(x, y, z=10):
        await asyncio.sleep(0.1)
        return x + y + z
    
    async_func.clear_cache()
    
    # Test positional args
    result1 = await async_func(1, 2)
    assert result1 == 13
    
    # Test keyword args
    result2 = await async_func(1, y=2)
    assert result2 == 13
    
    # Test with different z
    result3 = await async_func(1, 2, z=5)
    assert result3 == 8
    
    async_func.clear_cache()


# Test async with max_age parameter
@pytest.mark.memory
@pytest.mark.asyncio
@pytest.mark.maxage
async def test_async_max_age():
    """Test async caching with max_age parameter."""
    call_count = 0
    
    @cachier(backend="memory", stale_after=timedelta(days=1))
    async def async_func(x):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.1)
        return x * 2
    
    async_func.clear_cache()
    call_count = 0
    
    # First call
    result1 = await async_func(5)
    assert result1 == 10
    assert call_count == 1
    
    # Second call - should use cache
    result2 = await async_func(5)
    assert result2 == 10
    assert call_count == 1
    
    # Wait a bit
    await asyncio.sleep(0.5)
    
    # Third call with max_age - should recalculate because cache is older than max_age
    result3 = await async_func(5, max_age=timedelta(milliseconds=100))
    assert result3 == 10
    assert call_count == 2
    
    async_func.clear_cache()


# Test concurrent async calls
@pytest.mark.memory
@pytest.mark.asyncio
async def test_async_concurrent():
    """Test concurrent async calls with caching.
    
    Note: For async functions, concurrent calls with the same arguments
    will all execute in parallel (no waiting/blocking). However, once
    any of them completes and caches the result, subsequent calls will
    use the cached value.
    """
    call_count = 0
    
    @cachier(backend="memory")
    async def async_func(x):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.2)
        return x * 2
    
    async_func.clear_cache()
    call_count = 0
    
    # First concurrent calls - all will execute in parallel
    results1 = await asyncio.gather(
        async_func(5),
        async_func(5),
        async_func(5),
    )
    assert all(r == 10 for r in results1)
    # All three calls executed
    assert call_count == 3
    
    # Subsequent calls should use cache
    call_count = 0
    results2 = await asyncio.gather(
        async_func(5),
        async_func(5),
        async_func(5),
    )
    assert all(r == 10 for r in results2)
    assert call_count == 0  # No new calls, all from cache
    
    async_func.clear_cache()
