"""Tests for async Mongo client with async functions."""

import asyncio
from random import random

import pytest

from cachier import cachier
from tests.mongo_tests.test_mongo_core import _test_mongetter


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_mongetter():
    from cachier.cores.mongo import _MongoCore

    base_collection = _test_mongetter()
    collection = base_collection.database["cachier_async_mongetter"]
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


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_mongetter_method_args_and_kwargs():
    from cachier.cores.mongo import _MongoCore

    base_collection = _test_mongetter()
    collection = base_collection.database["cachier_async_mongetter_methods"]
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
