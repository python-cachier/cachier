"""A MongoDB-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import pickle  # for serialization of python objects
import sys  # to make sure that pymongo was imported
import time  # to sleep when waiting on Mongo cache\
import warnings  # to warn if pymongo is missing
from contextlib import suppress
from datetime import datetime, timedelta
from inspect import isawaitable
from typing import Any, Optional, Tuple

from .._types import HashFunc, Mongetter
from ..config import CacheEntry

with suppress(ImportError):
    from bson.binary import Binary  # to save binary data to mongodb
    from pymongo import ASCENDING, IndexModel
    from pymongo.errors import OperationFailure

from .base import RecalculationNeeded, _BaseCore, _get_func_str

MONGO_SLEEP_DURATION_IN_SEC = 1


class MissingMongetter(ValueError):
    """Thrown when the mongetter keyword argument is missing."""


class _MongoCore(_BaseCore):
    _INDEX_NAME = "func_1_key_1"

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        mongetter: Optional[Mongetter],
        wait_for_calc_timeout: Optional[int],
        entry_size_limit: Optional[int] = None,
    ):
        if "pymongo" not in sys.modules:
            warnings.warn(
                "`pymongo` was not found. MongoDB cores will not function.",
                ImportWarning,
                stacklevel=2,
            )  # pragma: no cover

        super().__init__(
            hash_func=hash_func,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=entry_size_limit,
        )
        if mongetter is None:
            raise MissingMongetter("must specify ``mongetter`` when using the mongo core")
        self.mongetter = mongetter
        self.mongo_collection: Any = None
        self._index_verified = False

    def _ensure_collection(self) -> Any:
        """Ensure we have a resolved Mongo collection for sync operations."""
        if self.mongo_collection is not None and self._index_verified:
            return self.mongo_collection

        with self.lock:
            if self.mongo_collection is None:
                coll = self.mongetter()
                if isawaitable(coll):
                    # Avoid "coroutine was never awaited" warnings.
                    close = getattr(coll, "close", None)
                    if callable(close):
                        with suppress(Exception):
                            close()
                    msg = "async mongetter is only supported for async cached functions"
                    raise TypeError(msg)
                self.mongo_collection = coll

            if not self._index_verified:
                index_inf = self.mongo_collection.index_information()
                if _MongoCore._INDEX_NAME not in index_inf:
                    func1key1 = IndexModel(
                        keys=[("func", ASCENDING), ("key", ASCENDING)],
                        name=_MongoCore._INDEX_NAME,
                    )
                    self.mongo_collection.create_indexes([func1key1])
                self._index_verified = True

        return self.mongo_collection

    async def _ensure_collection_async(self) -> Any:
        """Ensure we have a resolved Mongo collection for async operations."""
        if self.mongo_collection is not None and self._index_verified:
            return self.mongo_collection

        coll = self.mongetter()
        if isawaitable(coll):
            coll = await coll
        self.mongo_collection = coll

        if not self._index_verified:
            index_inf = self.mongo_collection.index_information()
            if isawaitable(index_inf):
                index_inf = await index_inf
            if _MongoCore._INDEX_NAME not in index_inf:
                func1key1 = IndexModel(
                    keys=[("func", ASCENDING), ("key", ASCENDING)],
                    name=_MongoCore._INDEX_NAME,
                )
                res = self.mongo_collection.create_indexes([func1key1])
                if isawaitable(res):
                    await res
            self._index_verified = True

        return self.mongo_collection

    @property
    def _func_str(self) -> str:
        return _get_func_str(self.func)

    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        mongo_collection = self._ensure_collection()
        res = mongo_collection.find_one({"func": self._func_str, "key": key})
        if not res:
            return key, None
        val = None
        if "value" in res:
            val = pickle.loads(res["value"])
        entry = CacheEntry(
            value=val,
            time=res.get("time", None),
            stale=res.get("stale", False),
            _processing=res.get("processing", False),
            _completed=res.get("completed", False),
        )
        return key, entry

    async def aget_entry(self, args, kwds) -> Tuple[str, Optional[CacheEntry]]:
        key = self.get_key(args, kwds)
        return await self.aget_entry_by_key(key)

    async def aget_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        mongo_collection = await self._ensure_collection_async()
        res = mongo_collection.find_one({"func": self._func_str, "key": key})
        if isawaitable(res):
            res = await res
        if not res:
            return key, None
        val = None
        if "value" in res:
            val = pickle.loads(res["value"])
        entry = CacheEntry(
            value=val,
            time=res.get("time", None),
            stale=res.get("stale", False),
            _processing=res.get("processing", False),
            _completed=res.get("completed", False),
        )
        return key, entry

    def set_entry(self, key: str, func_res: Any) -> bool:
        if not self._should_store(func_res):
            return False
        mongo_collection = self._ensure_collection()
        thebytes = pickle.dumps(func_res)
        mongo_collection.update_one(
            filter={"func": self._func_str, "key": key},
            update={
                "$set": {
                    "func": self._func_str,
                    "key": key,
                    "value": Binary(thebytes),
                    "time": datetime.now(),
                    "stale": False,
                    "processing": False,
                    "completed": True,
                }
            },
            upsert=True,
        )
        return True

    async def aset_entry(self, key: str, func_res: Any) -> bool:
        if not self._should_store(func_res):
            return False
        mongo_collection = await self._ensure_collection_async()
        thebytes = pickle.dumps(func_res)
        res = mongo_collection.update_one(
            filter={"func": self._func_str, "key": key},
            update={
                "$set": {
                    "func": self._func_str,
                    "key": key,
                    "value": Binary(thebytes),
                    "time": datetime.now(),
                    "stale": False,
                    "processing": False,
                    "completed": True,
                }
            },
            upsert=True,
        )
        if isawaitable(res):
            await res
        return True

    def mark_entry_being_calculated(self, key: str) -> None:
        mongo_collection = self._ensure_collection()
        mongo_collection.update_one(
            filter={"func": self._func_str, "key": key},
            update={"$set": {"processing": True}},
            upsert=True,
        )

    async def amark_entry_being_calculated(self, key: str) -> None:
        mongo_collection = await self._ensure_collection_async()
        res = mongo_collection.update_one(
            filter={"func": self._func_str, "key": key},
            update={"$set": {"processing": True}},
            upsert=True,
        )
        if isawaitable(res):
            await res

    def mark_entry_not_calculated(self, key: str) -> None:
        mongo_collection = self._ensure_collection()
        with suppress(OperationFailure):  # don't care in this case
            mongo_collection.update_one(
                filter={
                    "func": self._func_str,
                    "key": key,
                },
                update={"$set": {"processing": False}},
                upsert=False,  # should not insert in this case
            )

    async def amark_entry_not_calculated(self, key: str) -> None:
        mongo_collection = await self._ensure_collection_async()
        with suppress(OperationFailure):
            res = mongo_collection.update_one(
                filter={"func": self._func_str, "key": key},
                update={"$set": {"processing": False}},
                upsert=False,
            )
            if isawaitable(res):
                await res

    def wait_on_entry_calc(self, key: str) -> Any:
        time_spent = 0
        while True:
            time.sleep(MONGO_SLEEP_DURATION_IN_SEC)
            time_spent += MONGO_SLEEP_DURATION_IN_SEC
            key, entry = self.get_entry_by_key(key)
            if entry is None:
                raise RecalculationNeeded()
            if not entry._processing:
                return entry.value
            self.check_calc_timeout(time_spent)

    def clear_cache(self) -> None:
        mongo_collection = self._ensure_collection()
        mongo_collection.delete_many(filter={"func": self._func_str})

    async def aclear_cache(self) -> None:
        mongo_collection = await self._ensure_collection_async()
        res = mongo_collection.delete_many(filter={"func": self._func_str})
        if isawaitable(res):
            await res

    def clear_being_calculated(self) -> None:
        mongo_collection = self._ensure_collection()
        mongo_collection.update_many(
            filter={"func": self._func_str, "processing": True},
            update={"$set": {"processing": False}},
        )

    async def aclear_being_calculated(self) -> None:
        mongo_collection = await self._ensure_collection_async()
        res = mongo_collection.update_many(
            filter={"func": self._func_str, "processing": True},
            update={"$set": {"processing": False}},
        )
        if isawaitable(res):
            await res

    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Delete stale entries from the MongoDB cache."""
        mongo_collection = self._ensure_collection()
        threshold = datetime.now() - stale_after
        mongo_collection.delete_many(filter={"func": self._func_str, "time": {"$lt": threshold}})

    async def adelete_stale_entries(self, stale_after: timedelta) -> None:
        """Delete stale entries from the MongoDB cache."""
        mongo_collection = await self._ensure_collection_async()
        threshold = datetime.now() - stale_after
        res = mongo_collection.delete_many(filter={"func": self._func_str, "time": {"$lt": threshold}})
        if isawaitable(res):
            await res
