#!/usr/bin/env python3
"""Demonstration of the new return_stale_on_timeout feature."""

import time
import threading
from datetime import timedelta

import cachier


def demo_return_stale_on_timeout():
    """Demonstrate the return_stale_on_timeout feature."""
    
    print("🎯 Cachier return_stale_on_timeout Feature Demo")
    print("=" * 50)
    
    @cachier.cachier(
        backend="memory",
        stale_after=timedelta(seconds=2),     # Fresh for 2 seconds
        wait_for_calc_timeout=3,              # Wait up to 3 seconds for calculation
        return_stale_on_timeout=True,         # Return stale value if timeout
        next_time=False,                      # Don't return stale immediately
    )
    def expensive_api_call(query):
        """Simulate an expensive API call that takes 5 seconds."""
        print(f"  🔄 Making expensive API call for '{query}'...")
        time.sleep(5)  # Simulates network request
        return f"Result for {query}: {len(query)} chars"
    
    expensive_api_call.clear_cache()
    
    # 1. First call - will cache the result
    print("\n1️⃣ First call (cold cache):")
    result1 = expensive_api_call("hello world")
    print(f"   ✅ Got: {result1}")
    
    # 2. Second call while fresh - returns cached result immediately  
    print("\n2️⃣ Second call (fresh cache):")
    start_time = time.time()
    result2 = expensive_api_call("hello world")
    elapsed = time.time() - start_time
    print(f"   ✅ Got: {result2} (took {elapsed:.2f}s)")
    
    # 3. Wait for cache to become stale
    print("\n⏰ Waiting for cache to become stale (2+ seconds)...")
    time.sleep(2.5)
    
    # 4. Start a background calculation
    print("\n3️⃣ Starting background calculation...")
    def background_refresh():
        expensive_api_call("hello world")
    
    thread = threading.Thread(target=background_refresh)
    thread.start()
    time.sleep(0.5)  # Let background thread start
    
    # 5. This call will wait up to 3 seconds, then return stale value
    print("\n4️⃣ Main call (should return stale value after 3s timeout):")
    start_time = time.time()
    result3 = expensive_api_call("hello world") 
    elapsed = time.time() - start_time
    print(f"   ✅ Got: {result3} (took {elapsed:.2f}s)")
    
    if elapsed < 4:
        print("   🎉 SUCCESS! Returned stale value instead of waiting 5 seconds!")
    else:
        print("   ❌ Something went wrong - took too long")
    
    # Wait for background thread to complete
    thread.join()
    
    print("\n📋 Summary:")
    print("   • Fresh values returned immediately")  
    print("   • Stale values trigger background refresh")
    print("   • If refresh takes too long, return stale value")
    print("   • This keeps your application responsive!")


def demo_comparison():
    """Compare with and without return_stale_on_timeout."""
    
    print("\n\n🔄 Comparison Demo")
    print("=" * 50)
    
    # Without return_stale_on_timeout (default behavior)
    @cachier.cachier(
        backend="memory",
        stale_after=timedelta(seconds=1),
        wait_for_calc_timeout=2,
        return_stale_on_timeout=False,  # Default
    )
    def slow_func_old(x):
        time.sleep(3)
        return x * 2
    
    # With return_stale_on_timeout
    @cachier.cachier(
        backend="memory", 
        stale_after=timedelta(seconds=1),
        wait_for_calc_timeout=2,
        return_stale_on_timeout=True,   # New feature
    )
    def slow_func_new(x):
        time.sleep(3)
        return x * 2
    
    slow_func_old.clear_cache()
    slow_func_new.clear_cache()
    
    # Cache initial values
    print("Caching initial values...")
    slow_func_old(10)
    slow_func_new(10)
    
    # Wait for stale
    time.sleep(1.5)
    
    # Start background calculations
    def bg_old():
        slow_func_old(10)
    def bg_new():
        slow_func_new(10)
    
    threading.Thread(target=bg_old).start()
    threading.Thread(target=bg_new).start()
    time.sleep(0.5)
    
    print("\nTesting behavior when calculation times out:")
    
    # Test old behavior
    print("📊 OLD behavior (return_stale_on_timeout=False):")
    start = time.time()
    result_old = slow_func_old(10)  # Will wait, then start new calculation
    elapsed_old = time.time() - start
    print(f"   Result: {result_old}, Time: {elapsed_old:.2f}s")
    
    time.sleep(0.5)  # Brief pause
    
    # Test new behavior  
    print("🆕 NEW behavior (return_stale_on_timeout=True):")
    start = time.time()
    result_new = slow_func_new(10)  # Will return stale value after timeout
    elapsed_new = time.time() - start
    print(f"   Result: {result_new}, Time: {elapsed_new:.2f}s")
    
    print(f"\n🏆 Time saved: {elapsed_old - elapsed_new:.2f} seconds!")


if __name__ == "__main__":
    demo_return_stale_on_timeout()
    demo_comparison() 