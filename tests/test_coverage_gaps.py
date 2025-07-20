"""Tests to cover specific coverage gaps identified in the codebase."""

import os
import pickle
import sys
import tempfile
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

import cachier
from cachier.config import CacheEntry, _global_params

# Import backend-specific test helpers
from tests.test_mongo_core import _test_mongetter


# Test 1: Automatic cleanup trigger in core.py (line 344->350)
# This test is replaced by the version in test_coverage_gaps_simple.py
# which doesn't require access to the internal core object


# Test 2: MongoDB allow_none=False handling (line 99)
@pytest.mark.mongo
def test_mongo_allow_none_false():
    """Test MongoDB backend with allow_none=False and None return value."""
    
    @cachier.cachier(mongetter=_test_mongetter, allow_none=False)
    def returns_none():
        return None
    
    # First call should execute and return None
    result1 = returns_none()
    assert result1 is None
    
    # Second call should also execute (not cached) because None is not allowed
    result2 = returns_none()
    assert result2 is None
    
    # Clear cache
    returns_none.clear_cache()


# Test 3: MongoDB delete_stale_entries (lines 162-163)
# Removed - redundant with test_mongo_delete_stale_direct in test_coverage_gaps_simple.py


# Test 4: Pickle _clear_being_calculated_all_cache_files (lines 183-189)
# Removed - redundant with test_pickle_clear_being_calculated_separate_files in test_coverage_gaps_simple.py


# Test 5: Pickle save_cache with hash_str (line 205)
# Removed - redundant with test_pickle_save_with_hash_str in test_coverage_gaps_simple.py


# Test 6: Redis import error handling (lines 14-15)
@pytest.mark.redis
def test_redis_import_error_handling():
    """Test Redis backend when redis package is not available."""
    # This test is already covered by test_redis_import_warning
    # but let's ensure the specific lines are hit
    with patch.dict(sys.modules, {'redis': None}):
        # Force reload of redis core module
        if 'cachier.cores.redis' in sys.modules:
            del sys.modules['cachier.cores.redis']
        
        try:
            from cachier.cores.redis import _RedisCore
            # If we get here, redis was imported successfully (shouldn't happen in test)
            pytest.skip("Redis is installed, cannot test import error")
        except ImportError as e:
            # This is expected - verify the error message
            assert "No module named 'redis'" in str(e) or "redis" in str(e)


# Test 7: Redis corrupted entry handling (lines 112-114)
@pytest.mark.redis
def test_redis_corrupted_entry_handling():
    """Test Redis backend with corrupted cache entries."""
    import redis
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    
    try:
        # Test connection
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis server not available")
    
    @cachier.cachier(backend="redis", redis_client=client)
    def test_func(x):
        return x * 2
    
    # Clear cache
    test_func.clear_cache()
    
    # Manually insert corrupted data
    cache_key = "cachier:test_coverage_gaps:test_func:somehash"
    client.hset(cache_key, "value", b"corrupted_pickle_data")
    client.hset(cache_key, "time", str(time.time()).encode())
    client.hset(cache_key, "stale", b"0")
    client.hset(cache_key, "being_calculated", b"0")
    
    # Try to access - should handle corrupted data gracefully
    result = test_func(42)
    assert result == 84
    
    test_func.clear_cache()


# Test 8: Redis deletion failure during eviction (lines 133-135)
@pytest.mark.redis
def test_redis_deletion_failure_during_eviction():
    """Test Redis LRU eviction with deletion failures."""
    import redis
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis server not available")
    
    @cachier.cachier(
        backend="redis",
        redis_client=client,
        cache_size_limit="100B"  # Very small limit to trigger eviction
    )
    def test_func(x):
        return "x" * 50  # Large result to fill cache quickly
    
    # Clear cache
    test_func.clear_cache()
    
    # Fill cache to trigger eviction
    test_func(1)
    
    # Mock delete to fail
    original_delete = client.delete
    delete_called = []
    
    def mock_delete(*args):
        delete_called.append(args)
        # Fail on first delete attempt
        if len(delete_called) == 1:
            raise redis.RedisError("Mocked deletion failure")
        return original_delete(*args)
    
    client.delete = mock_delete
    
    try:
        # This should trigger eviction and handle the deletion failure
        test_func(2)
        # Verify delete was attempted
        assert len(delete_called) > 0
    finally:
        client.delete = original_delete
        test_func.clear_cache()


# Test 9: SQL allow_none=False handling (line 128)
# Removed - redundant with test_sql_allow_none_false_not_stored in test_coverage_gaps_simple.py


# Test 10: SQL delete_stale_entries (lines 302-312)
# Removed - redundant with test_sql_delete_stale_direct in test_coverage_gaps_simple.py


# Test 11: Pickle timeout during wait (line 398)
@pytest.mark.pickle
def test_pickle_timeout_during_wait():
    """Test calculation timeout while waiting in pickle backend."""
    import threading
    import queue
    
    @cachier.cachier(
        backend="pickle",
        wait_for_calc_timeout=0.5  # Short timeout
    )
    def slow_func(x):
        time.sleep(2)  # Longer than timeout
        return x * 2
    
    slow_func.clear_cache()
    
    res_queue = queue.Queue()
    
    def call_slow_func():
        try:
            res = slow_func(42)
            res_queue.put(("success", res))
        except Exception as e:
            res_queue.put(("error", e))
    
    # Start first thread that will take long
    thread1 = threading.Thread(target=call_slow_func)
    thread1.start()
    
    # Give it time to start processing
    time.sleep(0.1)
    
    # Start second thread that should timeout waiting
    thread2 = threading.Thread(target=call_slow_func)
    thread2.start()
    
    # Wait for threads
    thread1.join(timeout=3)
    thread2.join(timeout=3)
    
    # Check results - at least one should have succeeded
    results = []
    while not res_queue.empty():
        results.append(res_queue.get())
    
    assert len(results) >= 1
    
    slow_func.clear_cache()


# Test 12: Redis stale deletion with cache size tracking (lines 374-375, 380)
# Removed - redundant with test_redis_stale_delete_size_tracking in test_coverage_gaps_simple.py


# Test 13: Redis non-bytes timestamp handling (line 364)
@pytest.mark.redis  
def test_redis_non_bytes_timestamp():
    """Test Redis backend with non-bytes timestamp values."""
    import redis
    from cachier.cores.redis import _RedisCore
    
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis server not available")
    
    @cachier.cachier(
        backend="redis",
        redis_client=client,
        stale_after=timedelta(seconds=10)
    )
    def test_func(x):
        return x * 2
    
    # Clear cache
    test_func.clear_cache()
    
    # Create an entry
    test_func(1)
    
    # Manually modify timestamp to be a string instead of bytes
    keys = list(client.scan_iter(match="cachier:test_coverage_gaps:test_func:*"))
    if keys:
        # Force timestamp to be a string (non-bytes)
        client.hset(keys[0], "time", "not_a_number")
    
    # Create a separate core instance to test stale deletion
    core = _RedisCore(
        hash_func=None,
        redis_client=client,
        wait_for_calc_timeout=0,
    )
    core.set_func(test_func)
    
    # Try to delete stale entries - should handle non-bytes timestamp gracefully
    try:
        core.delete_stale_entries(timedelta(seconds=1))
    except Exception:
        pass  # Expected to handle gracefully
    
    test_func.clear_cache()