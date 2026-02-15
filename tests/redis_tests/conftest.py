"""Shared Redis test configuration and helpers."""

import pytest
from birch import Birch  # type: ignore[import-not-found]

from .clients import _MockRedis

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    redis = None  # type: ignore[assignment]
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
            client.ping()
            return client
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            pytest.skip("Redis not available")
    else:
        print("Using mock Redis for testing.")
        return None


def _test_redis_getter():
    """Get Redis client for testing."""
    client = _get_test_redis_client()
    if client is None:
        if not hasattr(_test_redis_getter, "_mock_client"):
            _test_redis_getter._mock_client = _MockRedis()
        return _test_redis_getter._mock_client
    return client
