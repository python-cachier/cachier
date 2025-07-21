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
        cache_size_limit: Optional[int] = None,
        replacement_policy: str = "lru",
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
            cache_size_limit=cache_size_limit,
            replacement_policy=replacement_policy,
        )
        if mongetter is None:
            raise MissingMongetter(
                "must specify ``mongetter`` when using the mongo core"
            )
        self.mongetter = mongetter
        self.mongo_collection = self.mongetter()
        index_inf = self.mongo_collection.index_information()
        if _MongoCore._INDEX_NAME not in index_inf:
            func1key1 = IndexModel(
                keys=[("func", ASCENDING), ("key", ASCENDING)],
                name=_MongoCore._INDEX_NAME,
            )
            self.mongo_collection.create_indexes([func1key1])

    @property
    def _func_str(self) -> str:
        return _get_func_str(self.func)

    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        res = self.mongo_collection.find_one(
            {"func": self._func_str, "key": key}
        )
        if not res:
            return key, None
        val = None
        if "value" in res:
            val = pickle.loads(res["value"])

        # Update last_access for LRU tracking
        if (
            self.cache_size_limit is not None
            and self.replacement_policy == "lru"
        ):
            self.mongo_collection.update_one(
                {"func": self._func_str, "key": key},
                {"$set": {"last_access": datetime.now()}},
            )

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
        thebytes = pickle.dumps(func_res)
        entry_size = self._estimate_size(func_res)
        now = datetime.now()

        self.mongo_collection.update_one(
            filter={"func": self._func_str, "key": key},
            update={
                "$set": {
                    "func": self._func_str,
                    "key": key,
                    "value": Binary(thebytes),
                    "time": now,
                    "last_access": now,
                    "size": entry_size,
                    "stale": False,
                    "processing": False,
                    "completed": True,
                }
            },
            upsert=True,
        )

        # Check if we need to evict entries
        if self.cache_size_limit is not None:
            self._evict_if_needed()

        return True

    def mark_entry_being_calculated(self, key: str) -> None:
        self.mongo_collection.update_one(
            filter={"func": self._func_str, "key": key},
            update={"$set": {"processing": True}},
            upsert=True,
        )

    def mark_entry_not_calculated(self, key: str) -> None:
        with suppress(OperationFailure):  # don't care in this case
            self.mongo_collection.update_one(
                filter={
                    "func": self._func_str,
                    "key": key,
                },
                update={"$set": {"processing": False}},
                upsert=False,  # should not insert in this case
            )

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
        self.mongo_collection.delete_many(filter={"func": self._func_str})

    def clear_being_calculated(self) -> None:
        self.mongo_collection.update_many(
            filter={
                "func": self._func_str,
                "processing": True,
            },
            update={"$set": {"processing": False}},
        )

    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Delete stale entries from the MongoDB cache."""
        threshold = datetime.now() - stale_after
        self.mongo_collection.delete_many(
            filter={"func": self._func_str, "time": {"$lt": threshold}}
        )

    def _get_total_cache_size(self) -> int:
        """Calculate the total size of all cache entries for this function."""
        pipeline = [
            {"$match": {"func": self._func_str, "size": {"$exists": True}}},
            {"$group": {"_id": None, "total": {"$sum": "$size"}}},
        ]
        result = list(self.mongo_collection.aggregate(pipeline))
        return result[0]["total"] if result else 0

    def _evict_if_needed(self) -> None:
        """Evict entries if cache size exceeds the limit."""
        if self.cache_size_limit is None:
            return

        total_size = self._get_total_cache_size()

        if total_size <= self.cache_size_limit:
            return

        if self.replacement_policy == "lru":
            self._evict_lru_entries(total_size)
        else:
            raise ValueError(
                f"Unsupported replacement policy: {self.replacement_policy}"
            )

    def _evict_lru_entries(self, current_size: int) -> None:
        """Evict least recently used entries to stay within cache_size_limit.

        Removes entries in order of least recently accessed until the total
        cache size is within the configured limit.

        """
        # Find all entries with their last access time and size
        # For entries without last_access, use the time field as fallback
        pipeline = [
            {"$match": {"func": self._func_str, "size": {"$exists": True}}},
            {"$addFields": {
                "sort_time": {
                    "$ifNull": ["$last_access", "$time"]
                }
            }},
            {"$sort": {"sort_time": 1}},  # Sort by sort_time ascending (oldest first)
            {"$project": {"key": 1, "size": 1}}
        ]
        entries = self.mongo_collection.aggregate(pipeline)

        total_evicted = 0
        keys_to_evict = []

        # cache_size_limit is guaranteed to be not None by the caller
        cache_limit = self.cache_size_limit
        if cache_limit is None:  # pragma: no cover
            return

        for entry in entries:
            if current_size - total_evicted <= cache_limit:
                break

            keys_to_evict.append(entry["key"])
            total_evicted += entry.get("size", 0)

        # Delete the entries
        if keys_to_evict:
            self.mongo_collection.delete_many(
                {"func": self._func_str, "key": {"$in": keys_to_evict}}
            )
