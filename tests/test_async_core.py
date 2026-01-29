"""Tests for async/coroutine support in Cachier."""

import asyncio
from datetime import timedelta
from time import sleep, time

import pytest

from cachier import cachier

# =============================================================================
# Basic Async Caching Tests
# =============================================================================


class TestBasicAsyncCaching:
    """Tests for basic async caching functionality."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_memory(self):
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

    @pytest.mark.pickle
    @pytest.mark.asyncio
    async def test_pickle(self):
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


# =============================================================================
# Stale Cache Tests
# =============================================================================


class TestStaleCache:
    """Tests for stale_after and next_time functionality."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_recalculates_after_expiry(self):
        """Test that stale_after causes recalculation after expiry."""
        call_count = 0

        @cachier(
            backend="memory",
            stale_after=timedelta(seconds=0.5),
            next_time=False,
        )
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

        # Wait for cache to become stale
        await asyncio.sleep(0.6)

        # Second call - should recalculate
        result2 = await async_func(5)
        assert result2 == 10
        assert call_count == 2

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_uses_cache_before_expiry(self):
        """Test that cache is used before stale_after expiry."""
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

        # Second call - should use cache (no additional call)
        previous_call_count = call_count
        result2 = await async_func(5)
        assert result2 == 10
        assert call_count == previous_call_count  # Verify cache was used

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_next_time_returns_stale_and_updates_background(self):
        """Test next_time=True returns stale value and updates in bg."""
        call_count = 0

        @cachier(
            backend="memory",
            stale_after=timedelta(seconds=0.5),
            next_time=True,
        )
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

        # Wait for cache to become stale
        await asyncio.sleep(0.6)

        # Second call - should return stale value and trigger background update
        result2 = await async_func(5)
        assert result2 == 10  # Still returns old value

        # Wait for background calculation to complete
        await asyncio.sleep(0.5)

        # Third call - should return new value
        result3 = await async_func(5)
        assert result3 == 20  # New value from background calculation

        async_func.clear_cache()


# =============================================================================
# Cache Control Tests
# =============================================================================


class TestCacheControl:
    """Tests for cache control parameters - skip_cache & overwrite_cache."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_skip_cache(self):
        """Test async caching with cachier__skip_cache parameter."""
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

        # Second call with skip_cache
        result2 = await async_func(5, cachier__skip_cache=True)
        assert result2 == 20

        # Third call - should use cache from first call
        result3 = await async_func(5)
        assert result3 == 10

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_overwrite_cache(self):
        """Test async caching with cachier__overwrite_cache parameter."""
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


# =============================================================================
# Class Method Tests
# =============================================================================


class TestAsyncMethod:
    """Tests for async caching on class methods."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_caches_result(self):
        """Test async caching on class methods returns cached result."""

        class MyClass:
            def __init__(self, value):
                self.value = value

            @cachier(backend="memory")
            async def async_method(self, x):
                await asyncio.sleep(0.1)
                return x * self.value

        obj1 = MyClass(2)

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

        obj1.async_method.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_shares_cache_across_instances(self):
        """Test that async method cache is shared across instances."""

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

        # Call on obj2 with same argument - should also use cache
        # (because cache is based on method arguments, not instance)
        result2 = await obj2.async_method(5)
        assert result2 == 10  # Returns cached value from obj1

        obj1.async_method.clear_cache()


# =============================================================================
# Sync Function Compatibility Tests
# =============================================================================


class TestSyncCompatibility:
    """Tests to ensure sync functions still work."""

    @pytest.mark.memory
    def test_still_works(self):
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


# =============================================================================
# Argument Handling Tests
# =============================================================================


class TestArgumentHandling:
    """Tests for different argument types and patterns."""

    @pytest.mark.parametrize(
        ("args", "kwargs", "expected"),
        [
            ((1, 2), {}, 13),  # positional args
            ((1,), {"y": 2}, 13),  # keyword args
            ((1, 2), {"z": 5}, 8),  # different default override
        ],
    )
    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_different_types(self, args, kwargs, expected):
        """Test async caching with different argument types."""

        @cachier(backend="memory")
        async def async_func(x, y, z=10):
            await asyncio.sleep(0.1)
            return x + y + z

        async_func.clear_cache()

        result = await async_func(*args, **kwargs)
        assert result == expected

        async_func.clear_cache()


# =============================================================================
# Max Age Tests
# =============================================================================


class TestMaxAge:
    """Tests for max_age parameter functionality."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    @pytest.mark.maxage
    async def test_recalculates_when_expired(self):
        """Test that max_age causes recalculation when cache is too old."""
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

        # Wait a bit
        await asyncio.sleep(0.5)

        # Second call with max_age - should recalculate because cache is older
        # than max_age
        result2 = await async_func(5, max_age=timedelta(milliseconds=100))
        assert result2 == 10
        assert call_count == 2

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    @pytest.mark.maxage
    async def test_uses_cache_when_fresh(self):
        """Test that cache is used when within max_age."""
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

        # Second call with max_age - should use cache
        previous_call_count = call_count
        result2 = await async_func(5, max_age=timedelta(seconds=10))
        assert result2 == 10
        assert call_count == previous_call_count  # No additional call

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    @pytest.mark.maxage
    async def test_negative_max_age_forces_recalculation(self):
        """Test that negative max_age forces recalculation."""
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

        # Second call with negative max_age - should recalculate
        result2 = await async_func(5, max_age=timedelta(seconds=-1))
        assert result2 == 10
        assert call_count == 2

        async_func.clear_cache()


# =============================================================================
# Concurrent Access Tests
# =============================================================================


class TestConcurrentAccess:
    """Tests for concurrent async call behavior."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_calls_execute_in_parallel(self):
        """Test that concurrent async calls execute in parallel."""
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

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_consequent_calls_use_cache(self):
        """Test that calls after caching use cached value."""
        call_count = 0

        @cachier(backend="memory")
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.2)
            return x * 2

        async_func.clear_cache()
        call_count = 0

        # First call to populate cache
        await async_func(5)
        assert call_count == 1

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

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_stale_entry_being_processed_with_next_time(self):
        """Test stale entry being processed returns stale value with next_time=True."""
        call_count = 0

        @cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=True)
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.5)
            return call_count * 10

        async_func.clear_cache()
        call_count = 0

        # First call
        result1 = await async_func(5)
        assert result1 == 10
        assert call_count == 1

        # Wait for cache to become stale
        await asyncio.sleep(1.5)

        # Call returns stale value, triggers background update
        result2 = await async_func(5)
        assert result2 == 10  # Returns stale value

        # Wait for background task to complete
        await asyncio.sleep(1)

        # Next call gets the new value
        result3 = await async_func(5)
        assert result3 == 20

        async_func.clear_cache()


# =============================================================================
# None Value Handling Tests
# =============================================================================


class TestNoneHandling:
    """Tests for allow_none parameter behavior."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_not_cached_by_default(self):
        """Test that None values are not cached when allow_none=False."""
        call_count = 0

        @cachier(backend="memory")
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return None if x == 0 else x * 2

        async_func.clear_cache()
        call_count = 0

        # First call returning None - should not be cached
        result1 = await async_func(0)
        assert result1 is None
        assert call_count == 1

        # Second call with same args - should recalculate (None not cached)
        result2 = await async_func(0)
        assert result2 is None
        assert call_count == 2

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_cached_when_allowed(self):
        """Test that None values are cached when allow_none=True."""
        call_count = 0

        @cachier(backend="memory", allow_none=True)
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return None if x == 0 else x * 2

        async_func.clear_cache()
        call_count = 0

        # First call returning None - should be cached
        result1 = await async_func(0)
        assert result1 is None
        assert call_count == 1

        # Second call with same args - should use cached None
        previous_call_count = call_count
        result2 = await async_func(0)
        assert result2 is None
        assert call_count == previous_call_count  # No additional call

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_non_none_cached_with_allow_none_false(self):
        """Test that non-None values are cached even when allow_none=False."""
        call_count = 0

        @cachier(backend="memory")
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return None if x == 0 else x * 2

        async_func.clear_cache()
        call_count = 0

        # Call with non-None result - should be cached
        result1 = await async_func(5)
        assert result1 == 10
        assert call_count == 1

        # Call again - should use cache
        previous_call_count = call_count
        result2 = await async_func(5)
        assert result2 == 10
        assert call_count == previous_call_count  # No additional call

        async_func.clear_cache()


# =============================================================================
# Additional Coverage Tests
# =============================================================================


class TestAsyncVerboseMode:
    """Tests for verbose_cache parameter with async functions."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_verbose_cache_parameter(self, capsys):
        """Test verbose_cache parameter prints debug info."""
        import warnings

        call_count = 0

        @cachier(backend="memory")
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return x * 2

        async_func.clear_cache()
        call_count = 0

        # First call with verbose=True (deprecated but still works)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result1 = await async_func(5, verbose_cache=True)
        assert result1 == 10
        captured = capsys.readouterr()
        assert "No entry found" in captured.out or "Calling" in captured.out

        # Second call with verbose=True
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result2 = await async_func(5, verbose_cache=True)
        assert result2 == 10
        captured = capsys.readouterr()
        assert "Entry found" in captured.out or "Cached result" in captured.out

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_cachier_verbose_kwarg(self, capsys):
        """Test cachier__verbose keyword argument."""

        @cachier(backend="memory")
        async def async_func(x):
            await asyncio.sleep(0.1)
            return x * 3

        async_func.clear_cache()

        # Use cachier__verbose keyword
        result = await async_func(7, cachier__verbose=True)
        assert result == 21
        captured = capsys.readouterr()
        assert len(captured.out) > 0  # Should have printed something

        async_func.clear_cache()


class TestAsyncGlobalCachingControl:
    """Tests for global caching enable/disable with async functions."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_disable_caching_globally(self):
        """Test disabling caching globally affects async functions."""
        import cachier

        call_count = 0

        @cachier.cachier(backend="memory")
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return x * 2

        async_func.clear_cache()
        call_count = 0

        # Enable caching (default)
        cachier.enable_caching()

        # First call - should cache
        result1 = await async_func(5)
        assert result1 == 10
        assert call_count == 1

        # Second call - should use cache
        result2 = await async_func(5)
        assert result2 == 10
        assert call_count == 1

        # Disable caching
        cachier.disable_caching()

        # Third call - should not use cache
        result3 = await async_func(5)
        assert result3 == 10
        assert call_count == 2

        # Fourth call - still should not use cache
        result4 = await async_func(5)
        assert result4 == 10
        assert call_count == 3

        # Re-enable caching
        cachier.enable_caching()

        async_func.clear_cache()


class TestAsyncCleanupStale:
    """Tests for cleanup_stale functionality with async functions."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_cleanup_stale_entries(self):
        """Test that stale entries are cleaned up with cleanup_stale=True."""
        call_count = 0

        @cachier(
            backend="memory",
            stale_after=timedelta(seconds=1),
            cleanup_stale=True,
            cleanup_interval=timedelta(milliseconds=100),
        )
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

        # Wait for stale
        await asyncio.sleep(1.5)

        # Second call - triggers cleanup in background
        result2 = await async_func(5)
        assert result2 == 10
        assert call_count == 2

        # Give cleanup time to run
        await asyncio.sleep(0.5)

        async_func.clear_cache()


class TestAsyncProcessingEntry:
    """Tests for entry being processed scenarios with async functions."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_entry_processing_without_value(self):
        """Test async recalculation when entry is processing but has no value."""
        call_count = 0

        @cachier(backend="memory")
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.3)
            return x * 2

        async_func.clear_cache()
        call_count = 0

        # Launch concurrent calls - they should all execute
        results = await asyncio.gather(
            async_func(10),
            async_func(10),
            async_func(10),
        )

        assert all(r == 20 for r in results)
        # All three should have executed since async doesn't wait
        assert call_count == 3

        async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_stale_entry_processing_recalculates(self):
        """Test that stale entry being processed causes recalculation."""
        call_count = 0

        @cachier(backend="memory", stale_after=timedelta(seconds=1))
        async def async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.5)
            return call_count * 10

        async_func.clear_cache()
        call_count = 0

        # First call
        result1 = await async_func(5)
        assert result1 == 10
        assert call_count == 1

        # Wait for stale
        await asyncio.sleep(1.5)

        # Launch concurrent calls on stale entry
        # Both should recalculate (no waiting in async)
        await asyncio.gather(
            async_func(5),
            async_func(5),
        )

        # Both should have executed
        assert call_count >= 2

        async_func.clear_cache()


# =============================================================================
# Exception Handling and Edge Cases
# =============================================================================


class TestAsyncExceptionHandling:
    """Tests for exception handling in async background tasks."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_function_thread_async_exception_handling(self, capsys):
        """Test that exceptions in background async tasks are caught and printed."""
        exception_raised = False

        @cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=True)
        async def async_func_that_fails(x):
            nonlocal exception_raised
            await asyncio.sleep(0.1)
            if exception_raised:
                raise ValueError("Intentional test error in background")
            return x * 2

        async_func_that_fails.clear_cache()

        # First call with valid value
        result1 = await async_func_that_fails(5)
        assert result1 == 10

        # Wait for stale
        await asyncio.sleep(1.5)

        # Set flag to raise exception in next call
        exception_raised = True

        # Call again - should return stale value and update in background
        # Background task will fail and exception should be caught and printed
        result2 = await async_func_that_fails(5)
        assert result2 == 10  # Returns stale value

        # Wait for background task to complete and fail
        await asyncio.sleep(0.5)

        # Check that exception was caught and printed (line 65)
        captured = capsys.readouterr()
        assert "Function call failed with the following exception" in captured.out
        assert "Intentional test error in background" in captured.out

        async_func_that_fails.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_entry_size_limit_exceeded_async(self, capsys):
        """Test that exceeding entry_size_limit prints a message (line 86)."""

        @cachier(backend="memory", entry_size_limit=10)  # Very small limit
        async def async_func_large_result(x):
            await asyncio.sleep(0.1)
            # Return a large result that exceeds 10 bytes
            return "x" * 1000

        async_func_large_result.clear_cache()

        # Call function with cachier__verbose=True - result should exceed size limit
        result = await async_func_large_result(5, cachier__verbose=True)
        assert len(result) == 1000

        # Check that the size limit message was printed (line 86)
        captured = capsys.readouterr()
        assert "Result exceeds entry_size_limit; not cached" in captured.out

        async_func_large_result.clear_cache()


class TestAsyncStaleProcessing:
    """Tests for stale entry processing with next_time."""

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_stale_entry_being_processed_returns_stale(self):
        """Test lines 476-478: stale entry being processed with next_time returns stale value."""
        call_count = 0

        @cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=True)
        async def slow_async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(1.0)  # Long enough to overlap calls
            return call_count * 10

        slow_async_func.clear_cache()
        call_count = 0

        # First call - populate cache
        result1 = await slow_async_func(5)
        assert result1 == 10
        assert call_count == 1

        # Wait for stale
        await asyncio.sleep(1.5)

        # Start a slow recalculation that will take 1 second
        # Do NOT await it - let it run in background
        task1 = asyncio.create_task(slow_async_func(5))
        
        # Give it a tiny bit of time to mark entry as being processed
        await asyncio.sleep(0.1)
        
        # Now make another call while first one is still processing
        # This should hit lines 476-478 and return stale value
        result2 = await slow_async_func(5)
        assert result2 == 10  # Should return stale value (from first call)
        
        # Wait for background task to complete
        result3 = await task1
        # result3 might be 10 (stale) or 20 (new), depending on timing
        
        slow_async_func.clear_cache()

    @pytest.mark.memory
    @pytest.mark.asyncio
    async def test_stale_entry_processing_returns_stale_with_next_time(self):
        """Test that stale entry being processed returns stale value when next_time=True."""
        call_count = 0

        @cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=True)
        async def slow_async_func(x):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.8)  # Long enough to be "processing"
            return call_count * 10

        slow_async_func.clear_cache()
        call_count = 0

        # First call - populate cache
        result1 = await slow_async_func(5)
        assert result1 == 10
        assert call_count == 1

        # Wait for stale
        await asyncio.sleep(1.5)

        # Launch two concurrent calls when stale
        # First will trigger background update, both should return stale
        results = await asyncio.gather(
            slow_async_func(5),
            slow_async_func(5),
        )

        # Both should get the stale value (10)
        assert results[0] == 10
        assert results[1] == 10

        # Wait for background update to complete
        await asyncio.sleep(1.5)

        # Next call should get updated value
        result_new = await slow_async_func(5)
        assert result_new > 10  # Updated in background

        slow_async_func.clear_cache()
