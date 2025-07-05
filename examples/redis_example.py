#!/usr/bin/env python3
"""
Example demonstrating the Redis core for cachier.

This example shows how to use cachier with Redis as the backend for
high-performance caching.

Requirements:
    pip install redis cachier
"""

import time
from datetime import timedelta

try:
    import redis
    from cachier import cachier
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install redis cachier")
    exit(1)


def setup_redis_client():
    """Set up a Redis client for caching."""
    try:
        # Connect to Redis (adjust host/port as needed)
        client = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=False,  # Important: keep as bytes for pickle
        )
        # Test connection
        client.ping()
        print("✓ Connected to Redis successfully")
        return client
    except redis.ConnectionError:
        print("✗ Could not connect to Redis")
        print("Make sure Redis is running on localhost:6379")
        print("Or install and start Redis with: docker run -p 6379:6379 redis")
        return None


def expensive_calculation(n):
    """Simulate an expensive calculation."""
    print(f"  Computing expensive_calculation({n})...")
    time.sleep(2)  # Simulate work
    return n * n + 42


def demo_basic_caching():
    """Demonstrate basic Redis caching."""
    print("\n=== Basic Redis Caching ===")

    @cachier(backend="redis", redis_client=setup_redis_client())
    def cached_calculation(n):
        return expensive_calculation(n)

    # First call - should be slow
    start = time.time()
    result1 = cached_calculation(5)
    time1 = time.time() - start
    print(f"First call: {result1} (took {time1:.2f}s)")

    # Second call - should be fast (cached)
    start = time.time()
    result2 = cached_calculation(5)
    time2 = time.time() - start
    print(f"Second call: {result2} (took {time2:.2f}s)")

    assert result1 == result2
    assert time2 < time1
    print("✓ Caching working correctly!")


def demo_stale_after():
    """Demonstrate stale_after functionality with Redis."""
    print("\n=== Stale After Demo ===")

    @cachier(
        backend="redis",
        redis_client=setup_redis_client(),
        stale_after=timedelta(seconds=3),
    )
    def time_sensitive_calculation(n):
        return expensive_calculation(n)

    # First call
    result1 = time_sensitive_calculation(10)
    print(f"First call: {result1}")

    # Second call within 3 seconds - should use cache
    result2 = time_sensitive_calculation(10)
    print(f"Second call (within 3s): {result2}")
    assert result1 == result2

    # Wait for cache to become stale
    print("Waiting 4 seconds for cache to become stale...")
    time.sleep(4)

    # Third call after 4 seconds - should recalculate
    result3 = time_sensitive_calculation(10)
    print(f"Third call (after 4s): {result3}")
    assert result3 != result1
    print("✓ Stale after working correctly!")


def demo_callable_client():
    """Demonstrate using a callable Redis client."""
    print("\n=== Callable Client Demo ===")

    def get_redis_client():
        """Factory function for Redis client."""
        return redis.Redis(
            host="localhost", port=6379, db=0, decode_responses=False
        )

    @cachier(backend="redis", redis_client=get_redis_client)
    def cached_with_callable(n):
        return expensive_calculation(n)

    result1 = cached_with_callable(15)
    result2 = cached_with_callable(15)
    assert result1 == result2
    print(f"Callable client result: {result1}")
    print("✓ Callable client working correctly!")


def demo_cache_management():
    """Demonstrate cache management functions."""
    print("\n=== Cache Management Demo ===")

    @cachier(backend="redis", redis_client=setup_redis_client())
    def managed_calculation(n):
        return expensive_calculation(n)

    # Cache some values
    managed_calculation(20)
    managed_calculation(21)

    # Clear the cache
    managed_calculation.clear_cache()
    print("✓ Cache cleared successfully!")

    # Verify cache is empty
    start = time.time()
    result = managed_calculation(20)  # Should be slow again
    time_taken = time.time() - start
    print(f"After clearing cache: {result} (took {time_taken:.2f}s)")


def main():
    """Run all Redis core demonstrations."""
    print("Cachier Redis Core Demo")
    print("=" * 50)

    # Check if Redis is available
    client = setup_redis_client()
    if client is None:
        return

    try:
        demo_basic_caching()
        demo_stale_after()
        demo_callable_client()
        demo_cache_management()

        print("\n" + "=" * 50)
        print("✓ All Redis core demonstrations completed successfully!")
        print("\nKey benefits of Redis core:")
        print("- High-performance in-memory caching")
        print("- Cross-process and cross-machine caching")
        print("- Optional persistence with Redis configuration")
        print("- Built-in expiration and eviction policies")

    except Exception as e:
        print(f"\n✗ Demo failed with error: {e}")
    finally:
        # Clean up
        if client:
            client.close()


if __name__ == "__main__":
    main()
