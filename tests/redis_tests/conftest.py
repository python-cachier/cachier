"""Shared Redis test fixtures."""

from contextlib import suppress

import pytest

from .clients import _AsyncInMemoryRedis, _MockRedis
from .helpers import _get_test_redis_client


@pytest.fixture
def sync_redis_client_fixture():
    """Yield a fresh sync Redis client for each test, cleared after use."""
    live_client = _get_test_redis_client()
    client = _MockRedis() if live_client is None else live_client
    yield client
    with suppress(Exception):
        if hasattr(client, "data"):
            client.data.clear()
        else:
            client.flushdb()


@pytest.fixture
def async_redis_client_fixture():
    """Yield a fresh async in-memory Redis client for each test, cleared after use."""
    client = _AsyncInMemoryRedis()
    yield client
    client._data.clear()
