"""Example demonstrating async/coroutine support in Cachier.

This example shows how to use the @cachier decorator with async functions to
cache the results of HTTP requests or other async operations.

"""

import asyncio
import time
from datetime import timedelta

from cachier import cachier


# Example 1: Basic async function caching
@cachier(backend="pickle", stale_after=timedelta(hours=1))
async def fetch_user_data(user_id: int) -> dict:
    """Simulate fetching user data from an API."""
    print(f"  Fetching user {user_id} from API...")
    await asyncio.sleep(1)  # Simulate network delay
    return {
        "id": user_id,
        "name": f"User{user_id}",
        "email": f"user{user_id}@example.com",
    }


# Example 2: Async function with memory backend (faster, but not persistent)
@cachier(backend="memory")
async def calculate_complex_result(x: int, y: int) -> int:
    """Simulate a complex calculation."""
    print(f"  Computing {x} ** {y}...")
    await asyncio.sleep(0.5)  # Simulate computation time
    return x**y


# Example 3: Async function with stale_after (without next_time for simplicity)
@cachier(backend="memory", stale_after=timedelta(seconds=3), next_time=False)
async def get_weather_data(city: str) -> dict:
    """Simulate fetching weather data with automatic refresh when stale."""
    print(f"  Fetching weather for {city}...")
    await asyncio.sleep(0.5)
    return {
        "city": city,
        "temp": 72,
        "condition": "sunny",
        "timestamp": time.time(),
    }


# Example 4: Real-world HTTP request caching (requires httpx)
async def demo_http_caching():
    """Demonstrate caching actual HTTP requests."""
    print("\n=== HTTP Request Caching Example ===")
    try:
        import httpx

        @cachier(backend="pickle", stale_after=timedelta(minutes=5))
        async def fetch_github_user(username: str) -> dict:
            """Fetch GitHub user data with caching."""
            print(f"  Making API request for {username}...")
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"https://api.github.com/users/{username}"
                )
                return response.json()

        # First call - makes actual HTTP request
        start = time.time()
        user1 = await fetch_github_user("torvalds")
        duration1 = time.time() - start
        print(f"  First call took {duration1:.2f}s")
        user_name = user1.get("name", "N/A")
        user_repos = user1.get("public_repos", "N/A")
        print(f"  User: {user_name}, Repos: {user_repos}")

        # Second call - uses cache (much faster)
        start = time.time()
        await fetch_github_user("torvalds")
        duration2 = time.time() - start
        print(f"  Second call took {duration2:.2f}s (from cache)")
        print(f"  Cache speedup: {duration1 / duration2:.1f}x")

    except ImportError:
        msg = "  (Skipping - httpx not installed. "
        msg += "Install with: pip install httpx)"
        print(msg)


async def main():
    """Run all async caching examples."""
    print("=" * 60)
    print("Cachier Async/Coroutine Support Examples")
    print("=" * 60)

    # Example 1: Basic async caching
    print("\n=== Example 1: Basic Async Caching ===")
    start = time.time()
    user = await fetch_user_data(42)
    duration1 = time.time() - start
    print(f"First call: {user} (took {duration1:.2f}s)")

    start = time.time()
    user = await fetch_user_data(42)
    duration2 = time.time() - start
    print(f"Second call: {user} (took {duration2:.2f}s)")
    print(f"Speedup: {duration1 / duration2:.1f}x faster!")

    # Example 2: Memory backend
    print("\n=== Example 2: Memory Backend (Fast, Non-Persistent) ===")
    start = time.time()
    result = await calculate_complex_result(2, 20)
    duration1 = time.time() - start
    print(f"First call: 2^20 = {result} (took {duration1:.2f}s)")

    start = time.time()
    result = await calculate_complex_result(2, 20)
    duration2 = time.time() - start
    print(f"Second call: 2^20 = {result} (took {duration2:.2f}s)")

    # Example 3: Stale-after
    print("\n=== Example 3: Stale-After ===")
    weather = await get_weather_data("San Francisco")
    print(f"First call: {weather}")

    weather = await get_weather_data("San Francisco")
    print(f"Second call (cached): {weather}")

    print("Waiting 4 seconds for cache to become stale...")
    await asyncio.sleep(4)

    weather = await get_weather_data("San Francisco")
    print(f"Third call (recalculates because stale): {weather}")

    # Example 4: Concurrent requests
    print("\n=== Example 4: Concurrent Async Requests ===")
    print("Making 5 concurrent requests...")
    print("(First 3 are unique and will execute, last 2 are duplicates)")
    start = time.time()
    await asyncio.gather(
        fetch_user_data(1),
        fetch_user_data(2),
        fetch_user_data(3),
        fetch_user_data(1),  # Duplicate - will execute in parallel with first
        fetch_user_data(2),  # Duplicate - will execute in parallel with second
    )
    duration = time.time() - start
    print(f"All requests completed in {duration:.2f}s")

    # Now test that subsequent calls use cache
    print("\nMaking the same requests again (should use cache):")
    start = time.time()
    await asyncio.gather(
        fetch_user_data(1),
        fetch_user_data(2),
        fetch_user_data(3),
    )
    duration2 = time.time() - start
    print(f"Completed in {duration2:.2f}s - much faster!")

    # Example 5: HTTP caching (if httpx is available)
    await demo_http_caching()

    # Clean up
    print("\n=== Cleanup ===")
    fetch_user_data.clear_cache()
    calculate_complex_result.clear_cache()
    get_weather_data.clear_cache()
    print("All caches cleared!")

    print("\n" + "=" * 60)
    print("Key Features Demonstrated:")
    print("  - Async function caching with @cachier decorator")
    print("  - Multiple backends (pickle, memory)")
    print("  - Automatic cache invalidation (stale_after)")
    print("  - Concurrent request handling")
    print("  - Significant performance improvements")
    print("\nNote: For async functions, concurrent calls with the same")
    print("arguments will execute in parallel initially. Subsequent calls")
    print("will use the cached result for significant speedup.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
