"""Testing the Redis core of cachier."""

import datetime
import hashlib
import queue
import threading
from random import random
from time import sleep

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
            client = redis.Redis(
                host=host, port=port, db=db, decode_responses=False
            )
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
                    print(
                        f"DEBUG: hgetall({key}) = {result} -> {bytes_result}"
                    )
                    return bytes_result

                def hset(
                    self, key, field=None, value=None, mapping=None, **kwargs
                ):
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
                        "mapping={mapping}, kwargs={kwargs}) -> "
                        "{self.data[key]}"
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
                            self.redis_client.hset(
                                key, field=field, value=value
                            )

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
        stale_after=datetime.timedelta(seconds=3),
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
        print(
            f"DEBUG: _calls_takes_time_redis._takes_time({arg_1}, {arg_2})"
            " called"
        )
        sleep(3)
        result = random() + arg_1 + arg_2
        print(
            f"DEBUG: _calls_takes_time_redis._takes_time({arg_1}, {arg_2}) "
            f"returning {result}"
        )
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
                return hashlib.sha256(
                    obj.to_string().encode("utf-8")
                ).hexdigest()
            return str(obj)

        key_parts = []
        for arg in args:
            key_parts.append(_hash(arg))
        for key, value in sorted(kwargs.items()):
            key_parts.append(f"{key}:{_hash(value)}")
        return hashlib.sha256(":".join(key_parts).encode("utf-8")).hexdigest()

    @cachier(
        backend="redis", redis_client=_test_redis_getter, hash_func=_hash_func
    )
    def _params_with_dataframe(*args, **kwargs):
        """Function that can handle DataFrames."""
        return sum(len(str(arg)) for arg in args) + sum(
            len(str(val)) for val in kwargs.values()
        )

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
