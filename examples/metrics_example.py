"""Demonstration of cachier's metrics and observability features."""

import time
from datetime import timedelta

from cachier import cachier

# Example 1: Basic metrics tracking
print("=" * 60)
print("Example 1: Basic Metrics Tracking")
print("=" * 60)


@cachier(backend="memory", enable_metrics=True)
def expensive_operation(x):
    """Simulate an expensive computation."""
    time.sleep(0.1)  # Simulate work
    return x**2


# Clear any existing cache
expensive_operation.clear_cache()

# First call - cache miss
print("\nFirst call (cache miss):")
result1 = expensive_operation(5)
print(f"  Result: {result1}")

# Get metrics after first call
stats = expensive_operation.metrics.get_stats()
print(f"  Hits: {stats.hits}, Misses: {stats.misses}")
print(f"  Hit rate: {stats.hit_rate:.1f}%")
print(f"  Avg latency: {stats.avg_latency_ms:.2f}ms")

# Second call - cache hit
print("\nSecond call (cache hit):")
result2 = expensive_operation(5)
print(f"  Result: {result2}")

stats = expensive_operation.metrics.get_stats()
print(f"  Hits: {stats.hits}, Misses: {stats.misses}")
print(f"  Hit rate: {stats.hit_rate:.1f}%")
print(f"  Avg latency: {stats.avg_latency_ms:.2f}ms")

# Third call with different argument - cache miss
print("\nThird call with different argument (cache miss):")
result3 = expensive_operation(10)
print(f"  Result: {result3}")

stats = expensive_operation.metrics.get_stats()
print(f"  Hits: {stats.hits}, Misses: {stats.misses}")
print(f"  Hit rate: {stats.hit_rate:.1f}%")
print(f"  Avg latency: {stats.avg_latency_ms:.2f}ms")
print(f"  Total calls: {stats.total_calls}")

# Example 2: Stale cache tracking
print("\n" + "=" * 60)
print("Example 2: Stale Cache Tracking")
print("=" * 60)


@cachier(
    backend="memory",
    enable_metrics=True,
    stale_after=timedelta(seconds=1),
    next_time=False,
)
def time_sensitive_operation(x):
    """Operation with stale_after configured."""
    return x * 2


time_sensitive_operation.clear_cache()

# Initial call
print("\nInitial call:")
result = time_sensitive_operation(5)
print(f"  Result: {result}")

# Call while fresh
print("\nCall while fresh (within 1 second):")
result = time_sensitive_operation(5)
print(f"  Result: {result}")

# Wait for cache to become stale
print("\nWaiting for cache to become stale...")
time.sleep(1.5)

# Call after stale
print("Call after cache is stale:")
result = time_sensitive_operation(5)
print(f"  Result: {result}")

stats = time_sensitive_operation.metrics.get_stats()
print("\nMetrics after stale access:")
print(f"  Hits: {stats.hits}")
print(f"  Stale hits: {stats.stale_hits}")
print(f"  Recalculations: {stats.recalculations}")

# Example 3: Sampling rate to reduce overhead
print("\n" + "=" * 60)
print("Example 3: Metrics Sampling (50% sampling rate)")
print("=" * 60)


@cachier(
    backend="memory",
    enable_metrics=True,
    metrics_sampling_rate=0.5,  # Only sample 50% of calls
)
def sampled_operation(x):
    """Operation with reduced metrics sampling."""
    return x + 1


sampled_operation.clear_cache()

# Make many calls
print("\nMaking 100 calls with 10 unique arguments...")
for i in range(100):
    sampled_operation(i % 10)

stats = sampled_operation.metrics.get_stats()
print("\nMetrics (with 50% sampling):")
print(f"  Total calls recorded: {stats.total_calls}")
print(f"  Hits: {stats.hits}")
print(f"  Misses: {stats.misses}")
print(f"  Hit rate: {stats.hit_rate:.1f}%")
print("  Note: Total calls < 100 due to sampling; hit rate is approximately representative of overall behavior.")

# Example 4: Comprehensive metrics snapshot
print("\n" + "=" * 60)
print("Example 4: Comprehensive Metrics Snapshot")
print("=" * 60)


@cachier(backend="memory", enable_metrics=True, entry_size_limit="1KB")
def comprehensive_operation(x):
    """Operation to demonstrate all metrics."""
    if x > 1000:
        # Return large data to trigger size limit rejection
        return "x" * 2000
    return x * 2


comprehensive_operation.clear_cache()

# Generate various metric events
comprehensive_operation(5)  # Miss + recalculation
comprehensive_operation(5)  # Hit
comprehensive_operation(10)  # Miss + recalculation
comprehensive_operation(2000)  # Size limit rejection

stats = comprehensive_operation.metrics.get_stats()
print(
    f"\nComplete metrics snapshot:\n"
    f"  Hits: {stats.hits}\n"
    f"  Misses: {stats.misses}\n"
    f"  Hit rate: {stats.hit_rate:.1f}%\n"
    f"  Total calls: {stats.total_calls}\n"
    f"  Avg latency: {stats.avg_latency_ms:.2f}ms\n"
    f"  Stale hits: {stats.stale_hits}\n"
    f"  Recalculations: {stats.recalculations}\n"
    f"  Wait timeouts: {stats.wait_timeouts}\n"
    f"  Size limit rejections: {stats.size_limit_rejections}\n"
    f"  Entry count: {stats.entry_count}\n"
    f"  Total size (bytes): {stats.total_size_bytes}"
)

# Example 5: Programmatic access for monitoring
print("\n" + "=" * 60)
print("Example 5: Programmatic Monitoring")
print("=" * 60)


@cachier(backend="memory", enable_metrics=True)
def monitored_operation(x):
    """Operation being monitored."""
    return x**3


monitored_operation.clear_cache()


def check_cache_health(func, threshold=80.0):
    """Check if cache hit rate meets threshold."""
    stats = func.metrics.get_stats()
    if stats.total_calls == 0:
        return True, "No calls yet"

    if stats.hit_rate >= threshold:
        return True, f"Hit rate {stats.hit_rate:.1f}% meets threshold"
    else:
        return (
            False,
            f"Hit rate {stats.hit_rate:.1f}% below threshold {threshold}%",
        )


# Simulate some usage
print("\nSimulating cache usage...")
for i in range(20):
    monitored_operation(i % 5)

# Check health
is_healthy, message = check_cache_health(monitored_operation, threshold=70.0)
print("\nCache health check:")
print(f"  Status: {'✓ HEALTHY' if is_healthy else '✗ UNHEALTHY'}")
print(f"  {message}")

stats = monitored_operation.metrics.get_stats()
print(f"  Details: {stats.hits} hits, {stats.misses} misses")

print("\n" + "=" * 60)
print("Examples complete!")
print("=" * 60)
print("\nKey takeaways:")
print("  • Metrics are opt-in via enable_metrics=True")
print("  • Access metrics via function.metrics.get_stats()")
print("  • Sampling reduces overhead for high-traffic functions")
print("  • Metrics are thread-safe and backend-agnostic")
print("  • Use for production monitoring and optimization")
