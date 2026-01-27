"""Tests for cache metrics and observability framework."""

import time
from datetime import timedelta
from threading import Thread

import pytest

from cachier import cachier
from cachier.metrics import CacheMetrics, MetricSnapshot


@pytest.mark.memory
def test_metrics_enabled():
    """Test that metrics can be enabled for a cached function."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    # Check metrics object is attached
    assert hasattr(test_func, "metrics")
    assert isinstance(test_func.metrics, CacheMetrics)
    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_disabled_by_default():
    """Test that metrics are disabled by default."""

    @cachier(backend="memory")
    def test_func(x):
        return x * 2

    # Metrics should be None when disabled
    assert test_func.metrics is None
    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_hit_miss_tracking():
    """Test that cache hits and misses are correctly tracked."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    # First call should be a miss
    result1 = test_func(5)
    assert result1 == 10

    stats = test_func.metrics.get_stats()
    assert stats.hits == 0
    assert stats.misses == 1
    assert stats.total_calls == 1
    assert stats.hit_rate == 0.0

    # Second call should be a hit
    result2 = test_func(5)
    assert result2 == 10

    stats = test_func.metrics.get_stats()
    assert stats.hits == 1
    assert stats.misses == 1
    assert stats.total_calls == 2
    assert stats.hit_rate == 50.0

    # Third call with different arg should be a miss
    result3 = test_func(10)
    assert result3 == 20

    stats = test_func.metrics.get_stats()
    assert stats.hits == 1
    assert stats.misses == 2
    assert stats.total_calls == 3
    assert stats.hit_rate == pytest.approx(33.33, rel=0.1)

    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_stale_hit_tracking():
    """Test that stale cache hits are tracked."""

    @cachier(
        backend="memory",
        enable_metrics=True,
        stale_after=timedelta(milliseconds=100),
        next_time=False,
    )
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    # First call
    result1 = test_func(5)
    assert result1 == 10

    # Second call while fresh
    result2 = test_func(5)
    assert result2 == 10

    # Wait for cache to become stale
    time.sleep(0.15)

    # Third call when stale - should trigger recalculation
    result3 = test_func(5)
    assert result3 == 10

    stats = test_func.metrics.get_stats()
    assert stats.stale_hits >= 1
    assert stats.recalculations >= 2  # Initial + stale recalculation

    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_latency_tracking():
    """Test that operation latencies are tracked."""

    @cachier(backend="memory", enable_metrics=True)
    def slow_func(x):
        time.sleep(0.05)  # 50ms
        return x * 2

    slow_func.clear_cache()

    # First call (miss with computation)
    slow_func(5)

    stats = slow_func.metrics.get_stats()
    # Should have some latency recorded
    assert stats.avg_latency_ms > 0

    # Second call (hit, should be faster)
    slow_func(5)

    stats = slow_func.metrics.get_stats()
    # Average should still be positive
    assert stats.avg_latency_ms > 0

    slow_func.clear_cache()


@pytest.mark.memory
def test_metrics_recalculation_tracking():
    """Test that recalculations are tracked."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    # First call
    test_func(5)
    stats = test_func.metrics.get_stats()
    assert stats.recalculations == 1

    # Cached call
    test_func(5)
    stats = test_func.metrics.get_stats()
    assert stats.recalculations == 1  # No change

    # Force recalculation
    test_func(5, cachier__overwrite_cache=True)
    stats = test_func.metrics.get_stats()
    assert stats.recalculations == 2

    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_sampling_rate():
    """Test that sampling rate reduces metrics overhead."""

    # Full sampling
    @cachier(backend="memory", enable_metrics=True, metrics_sampling_rate=1.0)
    def func_full_sampling(x):
        return x * 2

    # Partial sampling
    @cachier(
        backend="memory", enable_metrics=True, metrics_sampling_rate=0.5
    )
    def func_partial_sampling(x):
        return x * 2

    func_full_sampling.clear_cache()
    func_partial_sampling.clear_cache()

    # Call many times
    for i in range(100):
        func_full_sampling(i % 10)
        func_partial_sampling(i % 10)

    stats_full = func_full_sampling.metrics.get_stats()
    stats_partial = func_partial_sampling.metrics.get_stats()

    # Full sampling should have all calls tracked
    assert stats_full.total_calls >= 90  # Allow some variance

    # Partial sampling should have roughly half
    assert stats_partial.total_calls < stats_full.total_calls

    func_full_sampling.clear_cache()
    func_partial_sampling.clear_cache()


@pytest.mark.memory
def test_metrics_thread_safety():
    """Test that metrics collection is thread-safe."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        time.sleep(0.001)  # Small delay
        return x * 2

    test_func.clear_cache()

    def worker():
        for i in range(10):
            test_func(i % 5)

    # Run multiple threads
    threads = [Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = test_func.metrics.get_stats()
    # Should have tracked calls from all threads
    assert stats.total_calls > 0
    assert stats.hits + stats.misses == stats.total_calls

    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_reset():
    """Test that metrics can be reset."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    # Generate some metrics
    test_func(5)
    test_func(5)

    stats_before = test_func.metrics.get_stats()
    assert stats_before.total_calls > 0

    # Reset metrics
    test_func.metrics.reset()

    stats_after = test_func.metrics.get_stats()
    assert stats_after.total_calls == 0
    assert stats_after.hits == 0
    assert stats_after.misses == 0

    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_get_stats_snapshot():
    """Test that get_stats returns a proper snapshot."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    test_func(5)
    test_func(5)

    stats = test_func.metrics.get_stats()

    # Check all expected fields are present
    assert isinstance(stats, MetricSnapshot)
    assert hasattr(stats, "hits")
    assert hasattr(stats, "misses")
    assert hasattr(stats, "hit_rate")
    assert hasattr(stats, "total_calls")
    assert hasattr(stats, "avg_latency_ms")
    assert hasattr(stats, "stale_hits")
    assert hasattr(stats, "recalculations")
    assert hasattr(stats, "wait_timeouts")
    assert hasattr(stats, "entry_count")
    assert hasattr(stats, "total_size_bytes")
    assert hasattr(stats, "size_limit_rejections")

    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_with_different_backends():
    """Test that metrics work with different cache backends."""

    @cachier(backend="memory", enable_metrics=True)
    def memory_func(x):
        return x * 2

    @cachier(backend="pickle", enable_metrics=True)
    def pickle_func(x):
        return x * 3

    memory_func.clear_cache()
    pickle_func.clear_cache()

    # Test both functions
    memory_func(5)
    memory_func(5)

    pickle_func(5)
    pickle_func(5)

    memory_stats = memory_func.metrics.get_stats()
    pickle_stats = pickle_func.metrics.get_stats()

    # Both should have tracked metrics independently
    assert memory_stats.total_calls == 2
    assert pickle_stats.total_calls == 2
    assert memory_stats.hits == 1
    assert pickle_stats.hits == 1

    memory_func.clear_cache()
    pickle_func.clear_cache()


def test_cache_metrics_invalid_sampling_rate():
    """Test that invalid sampling rates raise errors."""
    with pytest.raises(ValueError, match="sampling_rate must be between"):
        CacheMetrics(sampling_rate=1.5)

    with pytest.raises(ValueError, match="sampling_rate must be between"):
        CacheMetrics(sampling_rate=-0.1)


@pytest.mark.memory
def test_metrics_size_limit_rejection():
    """Test that size limit rejections are tracked."""

    @cachier(
        backend="memory", enable_metrics=True, entry_size_limit="1KB"
    )
    def test_func(n):
        # Return large data that exceeds 1KB
        return "x" * (n * 1000)

    test_func.clear_cache()

    # Call with large data that should be rejected
    result = test_func(10)
    assert len(result) == 10000

    stats = test_func.metrics.get_stats()
    # Should have recorded a size limit rejection
    assert stats.size_limit_rejections >= 1

    test_func.clear_cache()


@pytest.mark.memory
def test_metrics_with_max_age():
    """Test metrics tracking with per-call max_age parameter."""

    @cachier(backend="memory", enable_metrics=True)
    def test_func(x):
        return x * 2

    test_func.clear_cache()

    # First call
    test_func(5)

    # Second call with negative max_age (force stale)
    test_func(5, max_age=timedelta(seconds=-1))

    stats = test_func.metrics.get_stats()
    # Should have at least one stale hit and recalculation
    assert stats.stale_hits >= 1
    assert stats.recalculations >= 2

    test_func.clear_cache()
