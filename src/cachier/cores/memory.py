"""A memory-based caching core for cachier."""

import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any, Optional, Tuple

from .._types import HashFunc
from ..config import CacheEntry
from .base import _BaseCore, _get_func_str


class _MemoryCore(_BaseCore):
    """The memory core class for cachier."""

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        wait_for_calc_timeout: Optional[int],
        entry_size_limit: Optional[int] = None,
        cache_size_limit: Optional[int] = None,
        replacement_policy: str = "lru",
    ):
        super().__init__(
            hash_func,
            wait_for_calc_timeout,
            entry_size_limit,
            cache_size_limit,
            replacement_policy,
        )
        self.cache: "OrderedDict[str, CacheEntry]" = OrderedDict()
        self._cache_size = 0

    def _hash_func_key(self, key: str) -> str:
        return f"{_get_func_str(self.func)}:{key}"

    def get_entry_by_key(
        self, key: str, reload=False
    ) -> Tuple[str, Optional[CacheEntry]]:
        with self.lock:
            hkey = self._hash_func_key(key)
            entry = self.cache.get(hkey, None)
            if entry is not None:
                self.cache.move_to_end(hkey)
            return key, entry

    def set_entry(self, key: str, func_res: Any) -> bool:
        if not self._should_store(func_res):
            return False
        hash_key = self._hash_func_key(key)
        size = self._estimate_size(func_res)
        with self.lock:
            try:
                cond = self.cache[hash_key]._condition
                old_size = self._estimate_size(self.cache[hash_key].value)
                self._cache_size -= old_size
            except KeyError:  # pragma: no cover
                cond = None
            self.cache[hash_key] = CacheEntry(
                value=func_res,
                time=datetime.now(),
                stale=False,
                _processing=False,
                _condition=cond,
                _completed=True,
            )
            self.cache.move_to_end(hash_key)
            self._cache_size += size
            if self.cache_size_limit is not None:
                while self._cache_size > self.cache_size_limit and self.cache:
                    old_key, old_entry = self.cache.popitem(last=False)
                    self._cache_size -= self._estimate_size(old_entry.value)
        return True

    def mark_entry_being_calculated(self, key: str) -> None:
        with self.lock:
            condition = threading.Condition()
            hash_key = self._hash_func_key(key)
            if hash_key in self.cache:
                self.cache[hash_key]._processing = True
                self.cache[hash_key]._condition = condition
            # condition.acquire()
            else:
                self.cache[hash_key] = CacheEntry(
                    value=None,
                    time=datetime.now(),
                    stale=False,
                    _processing=True,
                    _condition=condition,
                )

    def mark_entry_not_calculated(self, key: str) -> None:
        hash_key = self._hash_func_key(key)
        with self.lock:
            if hash_key not in self.cache:
                return  # that's ok, we don't need an entry in that case
            entry = self.cache[hash_key]
            entry._processing = False
            cond = entry._condition
            if cond:
                cond.acquire()
                cond.notify_all()
                cond.release()
                entry._condition = None

    def wait_on_entry_calc(self, key: str) -> Any:
        hash_key = self._hash_func_key(key)
        with self.lock:  # pragma: no cover
            entry = self.cache[hash_key]
            if entry is None:
                return None
            if not entry._processing:
                return entry.value
        if entry._condition is None:
            raise RuntimeError("No condition set for entry")
        entry._condition.acquire()
        entry._condition.wait()
        entry._condition.release()
        return self.cache[hash_key].value

    def clear_cache(self) -> None:
        with self.lock:
            self.cache.clear()
            self._cache_size = 0

    def clear_being_calculated(self) -> None:
        with self.lock:
            for entry in self.cache.values():
                entry._processing = False
                entry._condition = None

    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Remove stale entries from the in-memory cache."""
        now = datetime.now()
        with self.lock:
            keys_to_delete = [
                k for k, v in self.cache.items() if now - v.time > stale_after
            ]
            for key in keys_to_delete:
                entry = self.cache.pop(key)
                self._cache_size -= self._estimate_size(entry.value)
