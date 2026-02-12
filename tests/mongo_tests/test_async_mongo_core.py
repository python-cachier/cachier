"""Tests for async Mongo client with async functions."""

import asyncio
from datetime import datetime, timedelta
from random import random

import pytest

from cachier import cachier
from cachier.cores.mongo import _MongoCore
from tests.mongo_tests.test_mongo_core import _test_mongetter


class _AsyncInMemoryMongoCollection:
    """Minimal in-memory Mongo-like collection for async and sync path tests."""

    def __init__(self):
        self._docs: dict[tuple[str, str], dict[str, object]] = {}
        self._indexes: dict[str, dict[str, object]] = {}
        self.await_index_information = False
        self.await_create_indexes = False
        self.await_find_one = False
        self.await_update_one = False
        self.await_update_many = False
        self.await_delete_many = False

    @staticmethod
    async def _awaitable(value):
        return value

    def index_information(self):
        result = dict(self._indexes)
        if self.await_index_information:
            return _AsyncInMemoryMongoCollection._awaitable(result)
        return result

    def create_indexes(self, indexes):
        for index in indexes:
            document = getattr(index, "document", {})
            name = document.get("name", "index") if isinstance(document, dict) else "index"
            self._indexes[name] = {"name": name}
        result = list(self._indexes)
        if self.await_create_indexes:
            return _AsyncInMemoryMongoCollection._awaitable(result)
        return result

    def find_one(self, query=None, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        doc = self._docs.get((query.get("func"), query.get("key")))
        result = None if doc is None else dict(doc)
        if self.await_find_one:
            return _AsyncInMemoryMongoCollection._awaitable(result)
        return result

    def update_one(self, query=None, update=None, upsert=False, **kwargs):
        if query is None:
            query = kwargs.get("filter", {})
        if update is None:
            update = kwargs.get("update", {})
        key = (query.get("func"), query.get("key"))
        doc = self._docs.get(key)
        if doc is None:
            if not upsert:
                result = {"matched_count": 0}
                if self.await_update_one:
                    return _AsyncInMemoryMongoCollection._awaitable(result)
                return result
            doc = {"func": query.get("func"), "key": query.get("key")}
        doc.update(update.get("$set", {}))
        self._docs[key] = doc
        result = {"matched_count": 1}
        if self.await_update_one:
            return _AsyncInMemoryMongoCollection._awaitable(result)
        return result

    def update_many(self, query=None, update=None, **kwargs):
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
        result = {"matched_count": changed}
        if self.await_update_many:
            return _AsyncInMemoryMongoCollection._awaitable(result)
        return result

    def delete_many(self, query=None, **kwargs):
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
        result = {"deleted_count": deleted}
        if self.await_delete_many:
            return _AsyncInMemoryMongoCollection._awaitable(result)
        return result


def _build_mongo_core(mongetter):
    """Build a Mongo core configured for direct core tests."""
    core = _MongoCore(hash_func=None, mongetter=mongetter, wait_for_calc_timeout=10)

    def _func(x: int) -> int:
        return x

    core.set_func(_func)
    return core


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_mongetter():
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


@pytest.mark.mongo
def test_sync_mongo_core_ensure_collection_state_branches():
    collection = _AsyncInMemoryMongoCollection()
    mongetter_calls = 0

    def mongetter():
        nonlocal mongetter_calls
        mongetter_calls += 1
        return collection

    core = _build_mongo_core(mongetter)
    assert core._ensure_collection() is collection
    assert mongetter_calls == 1
    assert _MongoCore._INDEX_NAME in collection.index_information()

    core._index_verified = False
    core.mongo_collection = collection
    assert core._ensure_collection() is collection
    assert mongetter_calls == 1

    core._index_verified = True
    core.mongo_collection = None
    assert core._ensure_collection() is collection
    assert mongetter_calls == 2


@pytest.mark.mongo
def test_sync_mongo_core_rejects_async_mongetter():
    async def async_mongetter():
        return _AsyncInMemoryMongoCollection()

    core = _build_mongo_core(async_mongetter)
    with pytest.raises(TypeError, match="async mongetter is only supported for async cached functions"):
        core._ensure_collection()


@pytest.mark.mongo
def test_sync_mongo_core_rejects_awaitable_without_close():
    class _AwaitableNoClose:
        def __await__(self):
            async def _resolve():
                return _AsyncInMemoryMongoCollection()

            return _resolve().__await__()

    def mongetter():
        return _AwaitableNoClose()

    core = _build_mongo_core(mongetter)
    with pytest.raises(TypeError, match="async mongetter is only supported for async cached functions"):
        core._ensure_collection()


@pytest.mark.mongo
def test_mongo_core_set_entry_should_not_store():
    core = _build_mongo_core(lambda: _AsyncInMemoryMongoCollection())
    core._should_store = lambda _value: False
    assert core.set_entry("ignored", None) is False


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_core_collection_resolution_and_index_branches():
    sync_collection = _AsyncInMemoryMongoCollection()
    sync_collection._indexes[_MongoCore._INDEX_NAME] = {"name": _MongoCore._INDEX_NAME}
    sync_core = _build_mongo_core(lambda: sync_collection)

    assert await sync_core._ensure_collection_async() is sync_collection
    sync_core.mongo_collection = None
    sync_core._index_verified = True
    assert await sync_core._ensure_collection_async() is sync_collection

    async_collection = _AsyncInMemoryMongoCollection()
    async_collection.await_index_information = True
    async_collection.await_create_indexes = True

    async def async_mongetter():
        return async_collection

    async_core = _build_mongo_core(async_mongetter)
    assert await async_core._ensure_collection_async() is async_collection
    assert _MongoCore._INDEX_NAME in async_collection._indexes


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_core_entry_read_write_paths():
    collection = _AsyncInMemoryMongoCollection()
    collection.await_find_one = True
    collection.await_update_one = True
    core = _build_mongo_core(lambda: collection)

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
    collection.await_update_one = True
    collection.await_update_many = True
    collection.await_delete_many = True
    core = _build_mongo_core(lambda: collection)

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


@pytest.mark.mongo
@pytest.mark.asyncio
async def test_async_mongo_core_mark_clear_and_stale_paths_non_awaitable_results():
    collection = _AsyncInMemoryMongoCollection()
    core = _build_mongo_core(lambda: collection)

    await core.aset_entry("old", 1)
    await core.aset_entry("new", 2)
    collection._docs[(core._func_str, "old")]["time"] = datetime.now() - timedelta(hours=2)
    collection._docs[(core._func_str, "new")]["time"] = datetime.now()

    await core.amark_entry_being_calculated("new")
    await core.amark_entry_not_calculated("new")
    await core.aclear_being_calculated()
    await core.adelete_stale_entries(timedelta(hours=1))
    await core.aclear_cache()

    assert collection._docs == {}


@pytest.mark.mongo
def test_mongo_core_delete_stale_entries_sync_path():
    collection = _AsyncInMemoryMongoCollection()
    core = _build_mongo_core(lambda: collection)

    assert core.set_entry("stale", 1) is True
    assert core.set_entry("fresh", 2) is True
    collection._docs[(core._func_str, "stale")]["time"] = datetime.now() - timedelta(hours=2)
    collection._docs[(core._func_str, "fresh")]["time"] = datetime.now()

    core.delete_stale_entries(timedelta(hours=1))

    assert (core._func_str, "stale") not in collection._docs
    assert (core._func_str, "fresh") in collection._docs
