"""Testing the Redis core of cachier."""

import hashlib
import queue
import threading
import warnings
from datetime import datetime, timedelta
from random import random
from time import sleep
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from birch import Birch  # type: ignore[import-not-found]

from cachier import cachier
from cachier.cores.redis import MissingRedisClient, _RedisCore

# === Enables testing vs a real Redis instance ===

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class CfgKey:
    HOST = "TEST_REDIS_HOST"
    PORT = "TEST_REDIS_PORT"
    DB = "TEST_REDIS_DB"
    TEST_VS_DOCKERIZED_REDIS = "TEST_VS_DOCKERIZED_REDIS"


CFG = Birch(
    namespace="cachier",
    defaults={CfgKey.TEST_VS_DOCKERIZED_REDIS: False},
)


def _get_test_redis_client():
    """Get a Redis client for testing."""
    if not REDIS_AVAILABLE:
        pytest.skip("Redis not available")

    if str(CFG.mget(CfgKey.TEST_VS_DOCKERIZED_REDIS)).lower() == "true":
        print("Using live Redis instance for testing.")
        host = CFG.get(CfgKey.HOST, "localhost")
        port = int(CFG.get(CfgKey.PORT, 6379))
        db = int(CFG.get(CfgKey.DB, 0))
        try:
            client = redis.Redis(host=host, port=port, db=db, decode_responses=False)
            # Test connection
            client.ping()
            return client
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            pytest.skip("Redis not available")
    else:
        print("Using mock Redis for testing.")
        # For testing without Redis, we'll use a mock
        return None


def _test_redis_getter():
    """Get Redis client for testing."""
    client = _get_test_redis_client()
    if client is None:
        # Create a mock Redis client for testing
        # Use a singleton pattern to ensure the same instance is returned
        if not hasattr(_test_redis_getter, "_mock_client"):

            class MockRedis:
                def __init__(self):
                    self.data = {}
                    print("DEBUG: MockRedis initialized")

                def hgetall(self, key):
                    result = self.data.get(key, {})
                    # Convert string values to bytes to match Redis behavior
                    bytes_result = {}
                    for k, v in result.items():
                        if isinstance(v, str):
                            bytes_result[k.encode("utf-8")] = v.encode("utf-8")
                        else:
                            bytes_result[k.encode("utf-8")] = v
                    print(f"DEBUG: hgetall({key}) = {result} -> {bytes_result}")
                    return bytes_result

                def hset(self, key, field=None, value=None, mapping=None, **kwargs):
                    if key not in self.data:
                        self.data[key] = {}

                    # Handle different calling patterns
                    if mapping is not None:
                        # Called with mapping dict
                        self.data[key].update(mapping)
                    elif field is not None and value is not None:
                        # Called with field, value arguments
                        self.data[key][field] = value
                    elif kwargs:
                        # Called with keyword arguments
                        self.data[key].update(kwargs)

                    print(
                        f"DEBUG: hset({key}, field={field}, value={value}, "
                        f"mapping={mapping}, kwargs={kwargs}) -> "
                        f"{self.data[key]}"
                    )

                def keys(self, pattern):
                    import re

                    pattern = pattern.replace("*", ".*")
                    # Fix: keys are strings, not bytes, so no need to decode
                    result = [k for k in self.data if re.match(pattern, k)]
                    print(f"DEBUG: keys({pattern}) = {result}")
                    return result

                def delete(self, *keys):
                    for key in keys:
                        self.data.pop(key, None)
                    print(f"DEBUG: delete({keys})")

                def pipeline(self):
                    return MockPipeline(self)

                def ping(self):
                    return True

                def set(self, key, value):
                    self.data[key] = value
                    print(f"DEBUG: set({key}, {value})")

                def get(self, key):
                    result = self.data.get(key)
                    if isinstance(result, str):
                        result = result.encode("utf-8")
                    print(f"DEBUG: get({key}) = {result}")
                    return result

            class MockPipeline:
                def __init__(self, redis_client):
                    self.redis_client = redis_client
                    self.commands = []

                def hset(self, key, field, value):
                    self.commands.append(("hset", key, field, value))
                    return self

                def execute(self):
                    for cmd, key, field, value in self.commands:
                        if cmd == "hset":
                            self.redis_client.hset(key, field=field, value=value)

            _test_redis_getter._mock_client = MockRedis()

        return _test_redis_getter._mock_client
    return client


# === Redis core tests ===


@pytest.mark.redis
def test_information():
    if REDIS_AVAILABLE:
        print(f"\nredis version: {redis.__version__}")
    else:
        print("\nredis not available")


@pytest.mark.redis
def test_redis_connection():
    """Test Redis connection with environment variables."""
    client = _get_test_redis_client()
    if client is None:
        pytest.skip("Redis not available")

    try:
        # Test basic Redis operations
        client.set("test_key", "test_value")
        value = client.get("test_key")
        assert value == b"test_value"
        client.delete("test_key")
        print("✓ Redis connection and basic operations working")
    except Exception as e:
        pytest.fail(f"Redis connection test failed: {e}")


@pytest.mark.redis
def test_redis_core():
    """Basic Redis core functionality."""

    @cachier(backend="redis", redis_client=_test_redis_getter)
    def _test_redis_caching(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    _test_redis_caching.clear_cache()
    val1 = _test_redis_caching(1, 2)
    val2 = _test_redis_caching(1, 2)
    assert val1 == val2
    val3 = _test_redis_caching(1, 2, cachier__skip_cache=True)
    assert val3 != val1
    val4 = _test_redis_caching(1, 2)
    assert val4 == val1
    val5 = _test_redis_caching(1, 2, cachier__overwrite_cache=True)
    assert val5 != val1
    val6 = _test_redis_caching(1, 2)
    assert val6 == val5


@pytest.mark.redis
def test_redis_core_keywords():
    """Basic Redis core functionality with keyword arguments."""

    @cachier(backend="redis", redis_client=_test_redis_getter)
    def _test_redis_caching(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    _test_redis_caching.clear_cache()
    val1 = _test_redis_caching(1, arg_2=2)
    val2 = _test_redis_caching(1, arg_2=2)
    assert val1 == val2
    val3 = _test_redis_caching(1, arg_2=2, cachier__skip_cache=True)
    assert val3 != val1
    val4 = _test_redis_caching(1, arg_2=2)
    assert val4 == val1
    val5 = _test_redis_caching(1, arg_2=2, cachier__overwrite_cache=True)
    assert val5 != val1
    val6 = _test_redis_caching(1, arg_2=2)
    assert val6 == val5


@pytest.mark.redis
def test_redis_stale_after():
    """Testing Redis core stale_after functionality."""

    @cachier(
        backend="redis",
        redis_client=_test_redis_getter,
        stale_after=timedelta(seconds=3),
        next_time=False,
    )
    def _stale_after_redis(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    _stale_after_redis.clear_cache()
    val1 = _stale_after_redis(1, 2)
    val2 = _stale_after_redis(1, 2)
    assert val1 == val2
    sleep(3)
    val3 = _stale_after_redis(1, 2)
    assert val3 != val1


def _calls_takes_time_redis(res_queue):
    print("DEBUG: _calls_takes_time_redis started")

    @cachier(backend="redis", redis_client=_test_redis_getter)
    def _takes_time(arg_1, arg_2):
        """Some function."""
        print(f"DEBUG: _calls_takes_time_redis._takes_time({arg_1}, {arg_2}) called")
        sleep(3)
        result = random() + arg_1 + arg_2
        print(f"DEBUG: _calls_takes_time_redis._takes_time({arg_1}, {arg_2}) returning {result}")
        return result

    print("DEBUG: _calls_takes_time_redis calling _takes_time(34, 82.3)")
    res = _takes_time(34, 82.3)
    print(f"DEBUG: _calls_takes_time_redis got result {res}, putting in queue")
    res_queue.put(res)
    print("DEBUG: _calls_takes_time_redis completed")


@pytest.mark.redis
def test_redis_being_calculated():
    """Testing Redis core handling of being calculated scenarios."""
    print("DEBUG: test_redis_being_calculated started")

    @cachier(backend="redis", redis_client=_test_redis_getter)
    def _takes_time(arg_1, arg_2):
        """Some function."""
        print(f"DEBUG: _takes_time({arg_1}, {arg_2}) called")
        sleep(3)
        result = random() + arg_1 + arg_2
        print(f"DEBUG: _takes_time({arg_1}, {arg_2}) returning {result}")
        return result

    print("DEBUG: Clearing cache")
    _takes_time.clear_cache()
    res_queue = queue.Queue()
    print("DEBUG: Starting thread1")
    thread1 = threading.Thread(
        target=_calls_takes_time_redis,
        kwargs={"res_queue": res_queue},
        daemon=True,
    )
    print("DEBUG: Starting thread2")
    thread2 = threading.Thread(
        target=_calls_takes_time_redis,
        kwargs={"res_queue": res_queue},
        daemon=True,
    )
    print("DEBUG: Starting thread1")
    thread1.start()
    print("DEBUG: Sleeping 1 second")
    sleep(1)
    print("DEBUG: Starting thread2")
    thread2.start()
    print("DEBUG: Waiting for thread1 to join")
    thread1.join()
    print("DEBUG: Waiting for thread2 to join")
    thread2.join()
    print("DEBUG: Getting results from queue")
    res1 = res_queue.get()
    res2 = res_queue.get()
    print(f"DEBUG: Results: res1={res1}, res2={res2}")
    assert res1 == res2
    print("DEBUG: test_redis_being_calculated completed successfully")


@pytest.mark.redis
def test_redis_callable_hash_param():
    """Testing Redis core with callable hash function."""

    def _hash_func(args, kwargs):
        def _hash(obj):
            if isinstance(obj, pd.DataFrame):
                return hashlib.sha256(obj.to_string().encode("utf-8")).hexdigest()
            return str(obj)

        key_parts = []
        for arg in args:
            key_parts.append(_hash(arg))
        for key, value in sorted(kwargs.items()):
            key_parts.append(f"{key}:{_hash(value)}")
        return hashlib.sha256(":".join(key_parts).encode("utf-8")).hexdigest()

    @cachier(backend="redis", redis_client=_test_redis_getter, hash_func=_hash_func)
    def _params_with_dataframe(*args, **kwargs):
        """Function that can handle DataFrames."""
        return sum(len(str(arg)) for arg in args) + sum(len(str(val)) for val in kwargs.values())

    df1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df2 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df3 = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})

    _params_with_dataframe.clear_cache()
    val1 = _params_with_dataframe(df1, x=1)
    val2 = _params_with_dataframe(df2, x=1)
    assert val1 == val2
    val3 = _params_with_dataframe(df3, x=1)
    assert val3 != val1


@pytest.mark.redis
def test_redis_missing_client():
    """Test that MissingRedisClient is raised when no client is provided."""
    with pytest.raises(MissingRedisClient):

        @cachier(backend="redis")
        def _test_func():
            return "test"


@pytest.mark.redis
def test_redis_core_direct():
    """Test Redis core directly."""
    redis_client = _test_redis_getter()
    core = _RedisCore(
        hash_func=None,
        redis_client=redis_client,
        wait_for_calc_timeout=None,
    )

    def test_func(x, y):
        return x + y

    core.set_func(test_func)

    # Test setting and getting entries
    core.set_entry("test_key", "test_value")
    key, entry = core.get_entry_by_key("test_key")
    assert entry is not None
    assert entry.value == "test_value"

    # Test marking as being calculated
    core.mark_entry_being_calculated("calc_key")
    key, entry = core.get_entry_by_key("calc_key")
    assert entry is not None
    assert entry._processing is True

    # Test marking as not being calculated
    core.mark_entry_not_calculated("calc_key")
    key, entry = core.get_entry_by_key("calc_key")
    assert entry is not None
    assert entry._processing is False

    # Test clearing cache
    core.clear_cache()
    key, entry = core.get_entry_by_key("test_key")
    assert entry is None


@pytest.mark.redis
def test_redis_callable_client():
    """Test Redis core with callable client."""

    def get_redis_client():
        return _test_redis_getter()

    @cachier(backend="redis", redis_client=get_redis_client)
    def _test_callable_client(arg_1, arg_2):
        """Test function with callable Redis client."""
        return random() + arg_1 + arg_2

    _test_callable_client.clear_cache()
    val1 = _test_callable_client(1, 2)
    val2 = _test_callable_client(1, 2)
    assert val1 == val2


def test_redis_import_warning():
    """Test that import warning is raised when redis is not available."""
    ptc = patch("cachier.cores.redis.REDIS_AVAILABLE", False)
    with ptc, pytest.warns(ImportWarning, match="`redis` was not found"):
        _RedisCore(hash_func=None, redis_client=Mock(), wait_for_calc_timeout=None)


@pytest.mark.redis
def test_missing_redis_client():
    """Test MissingRedisClient exception when redis_client is None."""
    with pytest.raises(MissingRedisClient, match="must specify ``redis_client``"):
        _RedisCore(hash_func=None, redis_client=None, wait_for_calc_timeout=None)


@pytest.mark.redis
def test_redis_core_exceptions():
    """Test exception handling in Redis core methods."""
    # Create a mock Redis client that raises exceptions
    mock_client = MagicMock()

    # Configure all methods to raise exceptions
    mock_client.hgetall = MagicMock(side_effect=Exception("Redis connection error"))
    mock_client.hset = MagicMock(side_effect=Exception("Redis write error"))
    mock_client.keys = MagicMock(side_effect=Exception("Redis keys error"))
    mock_client.delete = MagicMock(side_effect=Exception("Redis delete error"))

    core = _RedisCore(hash_func=None, redis_client=mock_client, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    # Test get_entry_by_key exception handling
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        key, entry = core.get_entry_by_key("test_key")
        assert key == "test_key"
        assert entry is None
        assert len(w) == 1
        assert "Redis get_entry_by_key failed" in str(w[0].message)

    # Test set_entry exception handling
    # Mock the client to ensure it's not callable
    test_mock_client = MagicMock()
    test_mock_client.hset = MagicMock(side_effect=Exception("Redis write error"))

    # Create a new core with this specific mock
    test_core = _RedisCore(hash_func=None, redis_client=test_mock_client, wait_for_calc_timeout=10)
    test_core.set_func(mock_func)

    # Override _should_store to return True
    test_core._should_store = lambda x: True

    # Also need to mock _resolve_redis_client and _get_redis_key
    test_core._resolve_redis_client = lambda: test_mock_client
    test_core._get_redis_key = lambda key: f"test:{key}"

    with warnings.catch_warnings(record=True) as w2:
        warnings.simplefilter("always")
        result = test_core.set_entry("test_key", "test_value")
        assert result is False
        assert len(w2) == 1
        assert "Redis set_entry failed" in str(w2[0].message)

    # Mock _resolve_redis_client and _get_redis_key for the core
    core._resolve_redis_client = lambda: mock_client
    core._get_redis_key = lambda key: f"test:{key}"

    # Test mark_entry_being_calculated exception handling
    with warnings.catch_warnings(record=True) as w3:
        warnings.simplefilter("always")
        core.mark_entry_being_calculated("test_key")
        assert len(w3) == 1
        assert "Redis mark_entry_being_calculated failed" in str(w3[0].message)

    # Test mark_entry_not_calculated exception handling
    with warnings.catch_warnings(record=True) as w4:
        warnings.simplefilter("always")
        core.mark_entry_not_calculated("test_key")
        assert len(w4) == 1
        assert "Redis mark_entry_not_calculated failed" in str(w4[0].message)

    # Test clear_cache exception handling
    with warnings.catch_warnings(record=True) as w5:
        warnings.simplefilter("always")
        core.clear_cache()
        assert len(w5) == 1
        assert "Redis clear_cache failed" in str(w5[0].message)

    # Test clear_being_calculated exception handling
    with warnings.catch_warnings(record=True) as w6:
        warnings.simplefilter("always")
        core.clear_being_calculated()
        assert len(w6) == 1
        assert "Redis clear_being_calculated failed" in str(w6[0].message)


@pytest.mark.redis
def test_redis_delete_stale_entries():
    """Test delete_stale_entries method with various scenarios."""
    mock_client = MagicMock()

    core = _RedisCore(hash_func=None, redis_client=mock_client, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    # Test normal operation
    # Create a new mock client for this test
    delete_mock_client = MagicMock()

    # Set up keys method
    delete_mock_client.keys = MagicMock(return_value=[b"key1", b"key2", b"key3"])

    now = datetime.now()
    old_timestamp = (now - timedelta(hours=2)).isoformat()
    recent_timestamp = (now - timedelta(minutes=30)).isoformat()

    # Set up hget responses
    delete_mock_client.hget = MagicMock(
        side_effect=[
            old_timestamp.encode("utf-8"),  # key1 - stale
            recent_timestamp.encode("utf-8"),  # key2 - not stale
            None,  # key3 - no timestamp
        ]
    )

    # Set up delete mock
    delete_mock_client.delete = MagicMock()

    # Create a new core for this test
    delete_core = _RedisCore(hash_func=None, redis_client=delete_mock_client, wait_for_calc_timeout=10)
    delete_core.set_func(mock_func)

    # Need to mock _resolve_redis_client to return our mock
    delete_core._resolve_redis_client = lambda: delete_mock_client

    delete_core.delete_stale_entries(timedelta(hours=1))

    # Should only delete key1
    assert delete_mock_client.delete.call_count == 1
    delete_mock_client.delete.assert_called_with(b"key1")

    # Test exception during timestamp parsing
    mock_client.reset_mock()
    mock_client.keys.return_value = [b"key4"]
    mock_client.hget.return_value = b"invalid-timestamp"

    # Need to mock _resolve_redis_client for the original core as well
    core._resolve_redis_client = lambda: mock_client

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        core.delete_stale_entries(timedelta(hours=1))
        assert len(w) == 1
        assert "Redis timestamp parse failed" in str(w[0].message)

    # Test exception during keys operation
    mock_client.reset_mock()
    mock_client.keys.side_effect = Exception("Redis keys error")

    with warnings.catch_warnings(record=True) as w2:
        warnings.simplefilter("always")
        core.delete_stale_entries(timedelta(hours=1))
        assert len(w2) == 1
        assert "Redis delete_stale_entries failed" in str(w2[0].message)


@pytest.mark.redis
def test_redis_wait_on_entry_calc_no_entry():
    """Test wait_on_entry_calc when entry is None."""
    from cachier.cores.base import RecalculationNeeded

    # Create a mock client
    mock_client = MagicMock()

    # Mock get_entry_by_key to always return None entry
    # This avoids the pickle.loads issue
    _ = _RedisCore.get_entry_by_key

    def mock_get_entry_by_key(self, key):
        return key, None

    core = _RedisCore(hash_func=None, redis_client=mock_client, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    # Patch the method
    core.get_entry_by_key = lambda key: mock_get_entry_by_key(core, key)

    # The test expects RecalculationNeeded to be raised when no entry exists
    with pytest.raises(RecalculationNeeded):
        core.wait_on_entry_calc("test_key")


@pytest.mark.redis
def test_redis_set_entry_should_not_store():
    """Test set_entry when value should not be stored (None not allowed)."""
    mock_client = MagicMock()

    core = _RedisCore(hash_func=None, redis_client=mock_client, wait_for_calc_timeout=10)

    # Mock _should_store to return False
    core._should_store = Mock(return_value=False)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    result = core.set_entry("test_key", None)
    assert result is False
    mock_client.hset.assert_not_called()


@pytest.mark.redis
def test_redis_clear_being_calculated_with_pipeline():
    """Test clear_being_calculated with multiple keys."""
    # Create fresh mocks for this test
    pipeline_mock_client = MagicMock()
    pipeline_mock = MagicMock()

    # Set up keys to return 3 keys
    pipeline_mock_client.keys = MagicMock(return_value=[b"key1", b"key2", b"key3"])

    # Set up pipeline
    pipeline_mock_client.pipeline = MagicMock(return_value=pipeline_mock)
    pipeline_mock.hset = MagicMock()
    pipeline_mock.execute = MagicMock()

    core = _RedisCore(hash_func=None, redis_client=pipeline_mock_client, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    # Need to mock _resolve_redis_client to return our mock
    core._resolve_redis_client = lambda: pipeline_mock_client

    core.clear_being_calculated()

    # Verify pipeline was used
    assert pipeline_mock.hset.call_count == 3
    # Verify hset was called with correct parameters for each key
    pipeline_mock.hset.assert_any_call(b"key1", "processing", "false")
    pipeline_mock.hset.assert_any_call(b"key2", "processing", "false")
    pipeline_mock.hset.assert_any_call(b"key3", "processing", "false")
    pipeline_mock.execute.assert_called_once()
