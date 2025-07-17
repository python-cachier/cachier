"""Test return_stale_on_timeout functionality."""

import time
import threading
import queue
from datetime import timedelta

import pytest

import cachier


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_return_stale_on_timeout_true(backend):
    """Test that stale values are returned when timeout expires and return_stale_on_timeout=True."""
    
    @cachier.cachier(
        backend=backend,
        stale_after=timedelta(seconds=1),
        wait_for_calc_timeout=2,
        return_stale_on_timeout=True,
        next_time=False,
    )
    def slow_function(x):
        time.sleep(3)  # Longer than wait_for_calc_timeout
        return x * 2
    
    slow_function.clear_cache()
    
    # First call - will be cached
    result1 = slow_function(5)
    assert result1 == 10
    
    # Wait for value to become stale
    time.sleep(1.5)
    
    # Start a background thread that will trigger recalculation
    def background_call(result_queue):
        result = slow_function(5)
        result_queue.put(result)
    
    result_queue = queue.Queue()
    thread1 = threading.Thread(target=background_call, args=(result_queue,))
    thread1.start()
    
    # Give thread1 time to start the calculation
    time.sleep(0.5)
    
    # This call should timeout waiting for the calculation and return stale value
    start_time = time.time()
    result2 = slow_function(5)
    elapsed_time = time.time() - start_time
    
    # Should return quickly with stale value, not wait for full calculation
    assert elapsed_time < 2.5  # Less than wait_for_calc_timeout + some buffer
    assert result2 == 10  # Should return the stale value
    
    # Clean up
    thread1.join(timeout=5)
    if not result_queue.empty():
        result_queue.get()


@pytest.mark.parametrize("backend", ["memory", "pickle"]) 
def test_return_stale_on_timeout_false(backend):
    """Test that new calculation is triggered when timeout expires and return_stale_on_timeout=False."""
    
    @cachier.cachier(
        backend=backend,
        stale_after=timedelta(seconds=1),
        wait_for_calc_timeout=2,
        return_stale_on_timeout=False,  # Default behavior
        next_time=False,
    )
    def slow_function(x):
        time.sleep(3)  # Longer than wait_for_calc_timeout
        return x * 3  # Different multiplier to distinguish results
    
    slow_function.clear_cache()
    
    # First call - will be cached
    result1 = slow_function(5)
    assert result1 == 15
    
    # Wait for value to become stale
    time.sleep(1.5)
    
    # Start a background thread that will trigger recalculation
    def background_call(result_queue):
        result = slow_function(5)
        result_queue.put(result)
    
    result_queue = queue.Queue()
    thread1 = threading.Thread(target=background_call, args=(result_queue,))
    thread1.start()
    
    # Give thread1 time to start the calculation
    time.sleep(0.5)
    
    # This call should timeout waiting for the calculation and trigger a new calculation
    start_time = time.time()
    result2 = slow_function(5)
    elapsed_time = time.time() - start_time
    
    # Should take about as long as the function execution time
    assert elapsed_time >= 2.5  # At least close to the function execution time
    assert result2 == 15  # Should return the newly calculated value
    
    # Clean up
    thread1.join(timeout=8)
    if not result_queue.empty():
        result_queue.get()


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_return_stale_on_timeout_no_stale_value(backend):
    """Test that new calculation is triggered when no stale value exists, regardless of return_stale_on_timeout."""
    
    @cachier.cachier(
        backend=backend,
        wait_for_calc_timeout=2,
        return_stale_on_timeout=True,
        next_time=False,
    )
    def slow_function(x):
        time.sleep(3)  # Longer than wait_for_calc_timeout
        return x * 4
    
    slow_function.clear_cache()
    
    # Start two threads simultaneously - no cached value exists
    def background_call(result_queue, thread_id):
        result = slow_function(5)
        result_queue.put((thread_id, result))
    
    result_queue = queue.Queue()
    thread1 = threading.Thread(target=background_call, args=(result_queue, 1))
    thread2 = threading.Thread(target=background_call, args=(result_queue, 2))
    
    thread1.start()
    time.sleep(0.1)  # Small delay to ensure thread1 starts first
    thread2.start()
    
    # Wait for both threads to complete
    thread1.join(timeout=8)
    thread2.join(timeout=8)
    
    # Should get results from both threads
    assert result_queue.qsize() == 2
    results = []
    while not result_queue.empty():
        thread_id, result = result_queue.get()
        results.append(result)
    
    # Both should have calculated the value (one calculated, one waited or recalculated)
    assert all(result == 20 for result in results)


def test_return_stale_on_timeout_global_config():
    """Test that return_stale_on_timeout can be set globally."""
    
    # Set global configuration
    cachier.set_global_params(
        wait_for_calc_timeout=2,
        return_stale_on_timeout=True
    )
    
    @cachier.cachier(
        backend="memory",
        stale_after=timedelta(seconds=1),
        next_time=False,
    )
    def slow_function(x):
        time.sleep(3)
        return x * 5
    
    slow_function.clear_cache()
    
    # First call - will be cached
    result1 = slow_function(3)
    assert result1 == 15
    
    # Wait for value to become stale
    time.sleep(1.5)
    
    # Start background calculation
    def background_call():
        slow_function(3)
    
    thread1 = threading.Thread(target=background_call)
    thread1.start()
    time.sleep(0.5)  # Let background calculation start
    
    # This should return stale value due to global configuration
    start_time = time.time()
    result2 = slow_function(3)
    elapsed_time = time.time() - start_time
    
    assert elapsed_time < 2.5  # Should return quickly
    assert result2 == 15  # Should return stale value
    
    # Clean up
    thread1.join(timeout=5)
    
    # Reset global configuration
    cachier.set_global_params(
        wait_for_calc_timeout=0,
        return_stale_on_timeout=False
    ) 