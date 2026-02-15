"""Tests for async Mongo client with async functions."""

import asyncio
from datetime import datetime, timedelta
from random import random

import pytest

from cachier import cachier
from cachier.cores.mongo import _MongoCore


class _AsyncInMemoryMongoCollection:
    """Minimal in-memory Mongo-like collection with async methods only."""

    def __init__(self):
        self._docs: dict[tuple[str, str], dict[str, object]] = {}
        self._indexes: dict[str, dict[str, object]] = {}

    async def index_information(self):
        return dict(self._indexes)

    async def create_indexes(self, indexes):
        for index in indexes:
            document = getattr(index, "document", {})
            name = document.get("name", "index") if isinstance(document, dict) else "index"
            self._indexes[name] = {"name": name}
        return list(self._indexes)

    async def find_one(self, query=None, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        doc = self._docs.get((query.get("func"), query.get("key")))
        return None if doc is None else dict(doc)

    async def update_one(self, query=None, update=None, upsert=False, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        if update is None:
            update = kwargs.get("update", {})
        key = (query.get("func"), query.get("key"))
        doc = self._docs.get(key)
        if doc is None:
            if not upsert:
                return {"matched_count": 0}
            doc = {"func": query.get("func"), "key": query.get("key")}
        doc.update(update.get("$set", {}))
        self._docs[key] = doc
        return {"matched_count": 1}

    async def update_many(self, query=None, update=None, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        if update is None:
            update = kwargs.get("update", {})
        changed = 0
        for doc in self._docs.values():
            if "func" in query and doc.get("func") != query["func"]:
                continue
            if "processing" in query and doc.get("processing") != query["processing"]:
                continue
            doc.update(update.get("$set", {}))
            changed += 1
        return {"matched_count": changed}

    async def delete_many(self, query=None, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        deleted = 0
        time_filter = query.get("time")
        for key, doc in list(self._docs.items()):
            if "func" in query and doc.get("func") != query["func"]:
                continue
            if isinstance(time_filter, dict) and "$lt" in time_filter:
                doc_time = doc.get("time")
                if doc_time is None or doc_time >= time_filter["$lt"]:
                    continue
            del self._docs[key]
            deleted += 1
        return {"deleted_count": deleted}


def _build_async_mongo_core(collection: _AsyncInMemoryMongoCollection) -> _MongoCore:
    """Build a Mongo core configured for async core tests."""

    async def mongetter():
        return collection

    core = _MongoCore(hash_func=None, mongetter=mongetter, wait_for_calc_timeout=10)

    def _func(x: int) -> int:
        return x

    core.set_func(_func)
    return core


@pytest.mark.mongo
def test_async_client_over_sync_async_functions():
    async def async_mongetter():
        return _AsyncInMemoryMongoCollection()

    @cachier(mongetter=async_mongetter)
    async def async_cached_mongo_with_async_client(_: int) -> int:
        return 1

    assert callable(async_cached_mongo_with_async_client)

    with pytest.raises(TypeError, match="Async mongetter requires an async cached function."):

        @cachier(mongetter=async_mongetter)
        def sync_cached_mongo_requires_sync_mongetter(_: int) -> int:
            return 1


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_mongetter():
    collection = _AsyncInMemoryMongoCollection()

    async def async_mongetter():
        return collection

    @cachier(mongetter=async_mongetter)
    async def async_cached_mongo(x: int) -> float:
        await asyncio.sleep(0.01)
        return random() + x

    val1 = await async_cached_mongo(7)
    val2 = await async_cached_mongo(7)
    assert val1 == val2

    index_info = await collection.index_information()
    assert _MongoCore._INDEX_NAME in index_info


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_mongetter_method_args_and_kwargs():
    collection = _AsyncInMemoryMongoCollection()

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

    index_info = await collection.index_information()
    assert _MongoCore._INDEX_NAME in index_info


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_core_collection_resolution_and_index_branches():
    collection = _AsyncInMemoryMongoCollection()
    core = _build_async_mongo_core(collection)

    assert await core._ensure_collection_async() is collection
    assert _MongoCore._INDEX_NAME in collection._indexes

    core.mongo_collection = None
    core._index_verified = True
    assert await core._ensure_collection_async() is collection


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_core_entry_read_write_paths():
    collection = _AsyncInMemoryMongoCollection()
    core = _build_async_mongo_core(collection)

    assert await core.aset_entry("k1", {"value": 1}) is True
    _, entry = await core.aget_entry_by_key("k1")
    assert entry is not None
    assert entry.value == {"value": 1}

    collection._docs[(core._func_str, "no-value")] = {
        "func": core._func_str,
        "key": "no-value",
        "time": datetime.now(),
        "processing": False,
        "completed": True,
    }
    _, no_value_entry = await core.aget_entry_by_key("no-value")
    assert no_value_entry is not None
    assert no_value_entry.value is None

    core._should_store = lambda _value: False
    assert await core.aset_entry("ignored", None) is False


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_core_mark_clear_and_stale_paths():
    collection = _AsyncInMemoryMongoCollection()
    core = _build_async_mongo_core(collection)

    await core.aset_entry("stale", 1)
    await core.aset_entry("fresh", 2)
    collection._docs[(core._func_str, "stale")]["time"] = datetime.now() - timedelta(hours=2)
    collection._docs[(core._func_str, "fresh")]["time"] = datetime.now()

    await core.amark_entry_being_calculated("fresh")
    await core.amark_entry_not_calculated("fresh")
    await core.aclear_being_calculated()
    await core.adelete_stale_entries(timedelta(hours=1))

    assert (core._func_str, "stale") not in collection._docs
    assert (core._func_str, "fresh") in collection._docs

    await core.aclear_cache()
    assert (core._func_str, "fresh") not in collection._docs
