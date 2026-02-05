"""Tests for async client factories (redis_client / mongetter) with async functions."""

import asyncio
from contextlib import suppress
from random import random

import pytest

from cachier import cachier


class _AsyncInMemoryRedis:
    """Minimal async Redis-like client implementing required hash operations."""

    def __init__(self):
        self._data: dict[str, dict[str, object]] = {}

    async def hgetall(self, key: str) -> dict[bytes, object]:
        raw = self._data.get(key, {})
        res: dict[bytes, object] = {}
        for k, v in raw.items():
            res[k.encode("utf-8")] = v.encode("utf-8") if isinstance(v, str) else v
        return res

    async def hset(self, key: str, field=None, value=None, mapping=None, **kwargs):
        if key not in self._data:
            self._data[key] = {}

        if mapping is not None:
            self._data[key].update(mapping)
            return
        if field is not None and value is not None:
            self._data[key][field] = value
            return
        if kwargs:
            self._data[key].update(kwargs)


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_client_factory():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()

    async def get_redis_client():
        return client

    @cachier(backend="redis", redis_client=get_redis_client)
    async def async_cached_redis(x: int) -> float:
        await asyncio.sleep(0.01)
        return random() + x

    val1 = await async_cached_redis(3)
    val2 = await async_cached_redis(3)
    assert val1 == val2


@pytest.mark.redis
@pytest.mark.asyncio
async def test_async_redis_client_factory_method_args_and_kwargs():
    pytest.importorskip("redis")

    client = _AsyncInMemoryRedis()

    async def get_redis_client():
        return client

    call_count = 0

    class _RedisMethods:
        @cachier(backend="redis", redis_client=get_redis_client)
        async def async_cached_redis_method_args_kwargs(self, x: int, y: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.01)
            return call_count

    obj = _RedisMethods()
    val1 = await obj.async_cached_redis_method_args_kwargs(1, 2)
    val2 = await obj.async_cached_redis_method_args_kwargs(y=2, x=1)
    assert val1 == val2 == 1
    assert call_count == 1


@pytest.mark.mongo
@pytest.mark.filterwarnings("ignore:Python 3\\.14 will, by default, filter extracted tar archives.*:DeprecationWarning")
@pytest.mark.asyncio
async def test_async_mongo_mongetter():
    pymongo_inmemory = pytest.importorskip("pymongo_inmemory")

    from cachier.cores.mongo import _MongoCore

    client = pymongo_inmemory.MongoClient()
    try:
        collection = client["cachier_test"]["cachier_async_mongetter"]
        collection.delete_many({})

        async def async_mongetter():
            return collection

        @cachier(mongetter=async_mongetter)
        async def async_cached_mongo(x: int) -> float:
            await asyncio.sleep(0.01)
            return random() + x

        val1 = await async_cached_mongo(7)
        val2 = await async_cached_mongo(7)
        assert val1 == val2

        assert _MongoCore._INDEX_NAME in collection.index_information()
    finally:
        # pymongo_inmemory doesn't close the internal health-check client.
        # Close it to avoid ResourceWarning being treated as an error.
        with suppress(Exception):
            client._mongod._client.close()  # type: ignore[attr-defined]
        client.close()


@pytest.mark.mongo
@pytest.mark.filterwarnings("ignore:Python 3\\.14 will, by default, filter extracted tar archives.*:DeprecationWarning")
@pytest.mark.asyncio
async def test_async_mongo_mongetter_method_args_and_kwargs():
    pymongo_inmemory = pytest.importorskip("pymongo_inmemory")

    from cachier.cores.mongo import _MongoCore

    client = pymongo_inmemory.MongoClient()
    try:
        collection = client["cachier_test"]["cachier_async_mongetter_methods"]
        collection.delete_many({})

        async def async_mongetter():
            return collection

        call_count = 0

        class _MongoMethods:
            @cachier(mongetter=async_mongetter)
            async def async_cached_mongo_method_args_kwargs(self, x: int, y: int) -> int:
                nonlocal call_count
                call_count += 1
                await asyncio.sleep(0.01)
                return call_count

        obj = _MongoMethods()
        val1 = await obj.async_cached_mongo_method_args_kwargs(4, 5)
        val2 = await obj.async_cached_mongo_method_args_kwargs(y=5, x=4)
        assert val1 == val2 == 1
        assert call_count == 1

        assert _MongoCore._INDEX_NAME in collection.index_information()
    finally:
        with suppress(Exception):
            client._mongod._client.close()  # type: ignore[attr-defined]
        client.close()
