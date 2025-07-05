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
        class MockRedis:
            def __init__(self):
                self.data = {}

            def hgetall(self, key):
                return self.data.get(key, {})

            def hset(self, key, mapping=None, **kwargs):
                if key not in self.data:
                    self.data[key] = {}
                if mapping:
                    self.data[key].update(mapping)
                if kwargs:
                    self.data[key].update(kwargs)

            def keys(self, pattern):
                import re

                pattern = pattern.replace("*", ".*")
                return [k for k in self.data if re.match(pattern, k.decode())]

            def delete(self, *keys):
                for key in keys:
                    self.data.pop(key, None)

            def pipeline(self):
                return MockPipeline(self)

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
                        self.redis_client.hset(key, field, value)

        return MockRedis()
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
    @cachier(backend="redis", redis_client=_test_redis_getter)
    def _takes_time(arg_1, arg_2):
        """Some function."""
        sleep(3)
        return random() + arg_1 + arg_2

    res = _takes_time(34, 82.3)
    res_queue.put(res)


@pytest.mark.redis
def test_redis_being_calculated():
    """Testing Redis core handling of being calculated scenarios."""

    @cachier(backend="redis", redis_client=_test_redis_getter)
    def _takes_time(arg_1, arg_2):
        """Some function."""
        sleep(3)
        return random() + arg_1 + arg_2

    _takes_time.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_takes_time_redis,
        kwargs={"res_queue": res_queue},
        daemon=True,
    )
    thread2 = threading.Thread(
        target=_calls_takes_time_redis,
        kwargs={"res_queue": res_queue},
        daemon=True,
    )
    thread1.start()
    sleep(1)
    thread2.start()
    thread1.join()
    thread2.join()
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


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
