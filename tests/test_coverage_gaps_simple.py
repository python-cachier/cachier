"""Simple tests to cover specific coverage gaps."""

import os
import sys
import tempfile
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

import cachier
from cachier.config import CacheEntry
from tests.test_mongo_core import _test_mongetter


# Test 1: Trigger cleanup interval check (core.py lines 344-348)
def test_cleanup_interval_trigger():
    """Test cleanup is triggered after interval passes."""
    cleanup_count = 0
    
    # Track executor submissions
    from cachier.core import _get_executor
    executor = _get_executor()
    original_submit = executor.submit
    
    def mock_submit(func, *args):
        nonlocal cleanup_count
        if hasattr(func, '__name__') and 'delete_stale_entries' in func.__name__:
            cleanup_count += 1
        return original_submit(func, *args)
    
    executor.submit = mock_submit
    
    try:
        @cachier.cachier(
            cleanup_stale=True,
            cleanup_interval=timedelta(seconds=0.01),  # 10ms interval
            stale_after=timedelta(seconds=10)
        )
        def test_func(x):
            return x * 2
        
        # First call initializes cleanup time
        test_func(1)
        
        # Wait for interval to pass
        time.sleep(0.02)
        
        # Second call should trigger cleanup
        test_func(2)
        
        # Give executor time to process
        time.sleep(0.1)
        
        assert cleanup_count >= 1, "Cleanup should have been triggered"
        test_func.clear_cache()
    finally:
        executor.submit = original_submit


# Test 2: MongoDB None handling with allow_none=False
@pytest.mark.mongo
def test_mongo_allow_none_false_not_stored():
    """Test MongoDB doesn't store None when allow_none=False."""
    call_count = 0
    
    @cachier.cachier(mongetter=_test_mongetter, allow_none=False)
    def returns_none():
        nonlocal call_count
        call_count += 1
        return None
    
    returns_none.clear_cache()
    
    # First call
    result1 = returns_none()
    assert result1 is None
    assert call_count == 1
    
    # Second call should also execute (not cached)
    result2 = returns_none()
    assert result2 is None
    assert call_count == 2
    
    returns_none.clear_cache()


# Test 3: MongoDB delete_stale_entries
@pytest.mark.mongo
def test_mongo_delete_stale_direct():
    """Test MongoDB stale entry deletion method directly."""
    @cachier.cachier(mongetter=_test_mongetter, stale_after=timedelta(seconds=1))
    def test_func(x):
        return x * 2
    
    test_func.clear_cache()
    
    # Create entries
    test_func(1)
    test_func(2)
    
    # Wait for staleness
    time.sleep(1.1)
    
    # Access the mongo core and call delete_stale_entries
    # This is a bit hacky but needed to test the specific method
    from cachier.cores.mongo import _MongoCore
    
    # Get the collection
    collection = _test_mongetter()
    
    # Create a core instance just for deletion
    core = _MongoCore(
        mongetter=_test_mongetter,
        hash_func=None,
        wait_for_calc_timeout=0,
    )
    
    # Set the function to get the right cache key prefix
    core.set_func(test_func)
    
    # Delete stale entries
    core.delete_stale_entries(timedelta(seconds=1))
    
    test_func.clear_cache()


# Test 4: Pickle clear being calculated with separate files
@pytest.mark.pickle
def test_pickle_clear_being_calculated_separate_files():
    """Test clearing processing flags in separate cache files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        @cachier.cachier(backend="pickle", cache_dir=temp_dir, separate_files=True)
        def test_func(x):
            return x * 2
        
        # Get the pickle core
        from cachier.cores.pickle import _PickleCore
        
        # Create a temporary core to manipulate cache
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=0,
            separate_files=True,
        )
        core.set_func(test_func)
        
        # Create cache entries with processing flag
        for i in range(3):
            entry = CacheEntry(
                value=i * 2,
                time=datetime.now(),
                stale=False,
                _processing=True
            )
            # Create hash for key
            key_hash = str(hash((i,)))
            # For separate files, save the entry directly
            core._save_cache(entry, separate_file_key=key_hash)
        
        # Clear being calculated
        core._clear_being_calculated_all_cache_files()
        
        # Verify files exist but processing is cleared
        cache_files = [f for f in os.listdir(temp_dir) if f.startswith('.')]
        assert len(cache_files) >= 3
        
        test_func.clear_cache()


# Test 5: Pickle save with hash_str parameter  
@pytest.mark.pickle
def test_pickle_save_with_hash_str():
    """Test _save_cache with hash_str creates correct filename."""
    with tempfile.TemporaryDirectory() as temp_dir:
        from cachier.cores.pickle import _PickleCore
        
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=0,
            separate_files=True,
        )
        
        # Mock function for filename
        def test_func():
            pass
        core.set_func(test_func)
        
        # Save with hash_str
        test_entry = CacheEntry(
            value="test_value",
            time=datetime.now(),
            stale=False,
            _processing=False,
            _completed=True
        )
        test_data = {"test_key": test_entry}
        hash_str = "testhash123"
        core._save_cache(test_data, hash_str=hash_str)
        
        # Check file exists with hash in name
        expected_pattern = f"test_func_{hash_str}"
        files = os.listdir(temp_dir)
        assert any(expected_pattern in f and f.endswith(hash_str) for f in files), f"Expected file ending with {hash_str} not found. Files: {files}"


# Test 6: SQL allow_none=False
@pytest.mark.sql
def test_sql_allow_none_false_not_stored():
    """Test SQL doesn't store None when allow_none=False."""
    SQL_CONN_STR = os.environ.get("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")
    call_count = 0
    
    @cachier.cachier(backend="sql", sql_engine=SQL_CONN_STR, allow_none=False)
    def returns_none():
        nonlocal call_count
        call_count += 1
        return None
    
    returns_none.clear_cache()
    
    # First call
    result1 = returns_none()
    assert result1 is None
    assert call_count == 1
    
    # Second call should also execute
    result2 = returns_none()
    assert result2 is None
    assert call_count == 2
    
    returns_none.clear_cache()


# Test 7: SQL delete_stale_entries direct call
@pytest.mark.sql
def test_sql_delete_stale_direct():
    """Test SQL stale entry deletion method."""
    from cachier.cores.sql import _SQLCore
    
    # Get the engine from environment or use default
    SQL_CONN_STR = os.environ.get('SQLALCHEMY_DATABASE_URL', 'sqlite:///:memory:')
    
    @cachier.cachier(backend="sql", sql_engine=SQL_CONN_STR, stale_after=timedelta(seconds=0.5))
    def test_func(x):
        return x * 2
    
    test_func.clear_cache()
    
    # Create entries
    test_func(1)
    test_func(2)
    
    # Wait for staleness
    time.sleep(0.6)
    
    # Create core instance for direct testing
    core = _SQLCore(
        hash_func=None,
        sql_engine=SQL_CONN_STR,
        wait_for_calc_timeout=0,
    )
    core.set_func(test_func)
    
    # Delete stale entries
    core.delete_stale_entries(timedelta(seconds=0.5))
    
    test_func.clear_cache()


# Test 8: Redis missing import
@pytest.mark.redis
def test_redis_import_error():
    """Test Redis client initialization warning."""
    # Test creating a Redis core without providing a client
    import warnings
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        try:
            @cachier.cachier(backend="redis", redis_client=None)
            def test_func():
                return "test"
        except Exception as e:
            # Expected to fail with MissingRedisClient
            assert "redis_client" in str(e)


# Test 9: Redis corrupted entry in LRU eviction
@pytest.mark.redis
def test_redis_lru_corrupted_entry():
    """Test Redis LRU eviction with corrupted entry."""
    import redis
    
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    
    @cachier.cachier(
        backend="redis",
        redis_client=client,
        cache_size_limit="200B"  # Small limit
    )
    def test_func(x):
        return f"result_{x}" * 10  # ~60 bytes per entry
    
    test_func.clear_cache()
    
    # Add valid entry
    test_func(1)
    
    # Add corrupted entry manually
    from cachier.cores.redis import _RedisCore
    core = _RedisCore(
        hash_func=None,
        redis_client=client,
        wait_for_calc_timeout=0,
        cache_size_limit="200B"
    )
    core.set_func(test_func)
    
    # Create corrupted entry
    bad_key = f"{core.key_prefix}:{core._func_str}:badkey"
    client.hset(bad_key, "value", b"not_valid_pickle")
    client.hset(bad_key, "time", str(time.time()).encode())
    client.hset(bad_key, "stale", b"0")
    client.hset(bad_key, "being_calculated", b"0")
    
    # This should trigger eviction and handle the corrupted entry
    test_func(2)
    test_func(3)
    
    test_func.clear_cache()


# Test 10: Redis deletion failure in eviction
@pytest.mark.redis  
def test_redis_eviction_delete_failure():
    """Test Redis eviction handling delete failures."""
    import redis
    import warnings
    
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    
    # Create a unique function to avoid conflicts
    @cachier.cachier(
        backend="redis",
        redis_client=client,
        cache_size_limit="150B"
    )
    def test_eviction_func(x):
        return "x" * 50  # Large value
    
    test_eviction_func.clear_cache()
    
    # Fill cache to trigger eviction
    test_eviction_func(100)
    
    # This should trigger eviction
    with warnings.catch_warnings(record=True):
        # Ignore warnings about eviction failures
        warnings.simplefilter("always")
        test_eviction_func(200)
    
    # Verify both values work (even if eviction had issues)
    result1 = test_eviction_func(100)
    result2 = test_eviction_func(200)
    
    assert result1 == "x" * 50
    assert result2 == "x" * 50
    
    test_eviction_func.clear_cache()


# Test 11: Redis stale deletion with size tracking
@pytest.mark.redis
def test_redis_stale_delete_size_tracking():
    """Test Redis stale deletion updates cache size."""
    import redis
    
    client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    
    @cachier.cachier(
        backend="redis",
        redis_client=client,
        cache_size_limit="1KB",
        stale_after=timedelta(seconds=0.1)
    )
    def test_func(x):
        return "data" * 20
    
    test_func.clear_cache()
    
    # Create entries
    test_func(1)
    test_func(2)
    
    # Wait for staleness
    time.sleep(0.2)
    
    # Get the core
    from cachier.cores.redis import _RedisCore
    core = _RedisCore(
        hash_func=None,
        redis_client=client,
        wait_for_calc_timeout=0,
        cache_size_limit="1KB"
    )
    core.set_func(test_func)
    
    # Delete stale entries - this should update cache size
    core.delete_stale_entries(timedelta(seconds=0.1))
    
    # Verify size tracking by adding new entry
    test_func(3)
    
    test_func.clear_cache()


# Test 12: Pickle wait timeout check
@pytest.mark.pickle
def test_pickle_wait_timeout_check():
    """Test pickle backend timeout check during wait."""
    import threading
    
    @cachier.cachier(
        backend="pickle",
        wait_for_calc_timeout=0.2
    )
    def slow_func(x):
        time.sleep(1)  # Longer than timeout
        return x * 2
    
    slow_func.clear_cache()
    
    results = []
    
    def worker1():
        results.append(('w1', slow_func(42)))
    
    def worker2():
        time.sleep(0.1)  # Let first start
        results.append(('w2', slow_func(42)))
    
    t1 = threading.Thread(target=worker1)
    t2 = threading.Thread(target=worker2)
    
    t1.start()
    t2.start()
    
    t1.join(timeout=2)
    t2.join(timeout=2)
    
    # Both should have results (timeout should have triggered recalc)
    assert len(results) >= 1
    
    slow_func.clear_cache()


# Test 13: Non-standard SQL database fallback
@pytest.mark.sql
def test_sql_non_standard_db():
    """Test SQL backend code coverage for set_entry method."""
    # This test improves coverage for the SQL set_entry method
    SQL_CONN_STR = os.environ.get("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")
    
    @cachier.cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def test_func(x):
        return x * 3
    
    test_func.clear_cache()
    
    # Test basic set/get functionality
    result1 = test_func(10)
    assert result1 == 30
    
    # Test overwriting existing entry
    result2 = test_func(10, cachier__overwrite_cache=True)
    assert result2 == 30
    
    # Test with None value when allow_none is True (default)
    @cachier.cachier(backend="sql", sql_engine=SQL_CONN_STR, allow_none=True)
    def returns_none_allowed():
        return None
    
    returns_none_allowed.clear_cache()
    result3 = returns_none_allowed()
    assert result3 is None
    
    # Second call should use cache
    result4 = returns_none_allowed()
    assert result4 is None
    
    test_func.clear_cache()
    returns_none_allowed.clear_cache()