"""A memory-based caching core for cachier."""

import threading
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from .._types import HashFunc
from ..config import CacheEntry
from .base import _BaseCore, _get_func_str


class _MemoryCore(_BaseCore):
    """The memory core class for cachier."""

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        wait_for_calc_timeout: Optional[int],
    ):
        super().__init__(hash_func, wait_for_calc_timeout)
        self.cache: Dict[str, CacheEntry] = {}

    def _hash_func_key(self, key: str) -> str:
        return f"{_get_func_str(self.func)}:{key}"

    def get_entry_by_key(
        self, key: str, reload=False
    ) -> Tuple[str, Optional[CacheEntry]]:
        with self.lock:
            return key, self.cache.get(self._hash_func_key(key), None)

    def set_entry(self, key: str, func_res: Any) -> None:
        hash_key = self._hash_func_key(key)
        with self.lock:
            try:
                # we need to retain the existing condition so that
                # mark_entry_not_calculated can notify all possibly-waiting
                # threads about it
                cond = self.cache[hash_key]._condition
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

    def clear_being_calculated(self) -> None:
        with self.lock:
            for entry in self.cache.values():
                entry._processing = False
                entry._condition = None
