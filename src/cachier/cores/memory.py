"""A memory-based caching core for cachier."""

import threading
import time
from datetime import datetime, timedelta
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
        entry_size_limit: Optional[int] = None,
        return_stale_on_timeout: Optional[bool] = None,
    ):
        super().__init__(hash_func, wait_for_calc_timeout, entry_size_limit, return_stale_on_timeout)
        self.cache: Dict[str, CacheEntry] = {}

    def _hash_func_key(self, key: str) -> str:
        return f"{_get_func_str(self.func)}:{key}"

    def get_entry_by_key(
        self, key: str, reload=False
    ) -> Tuple[str, Optional[CacheEntry]]:
        with self.lock:
            return key, self.cache.get(self._hash_func_key(key), None)

    def set_entry(self, key: str, func_res: Any) -> bool:
        if not self._should_store(func_res):
            return False
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

        # Wait with timeout checking similar to other cores
        time_spent = 0
        while True:
            entry._condition.acquire()
            # Wait for 1 second at a time to allow timeout checking
            signaled = entry._condition.wait(timeout=1.0)
            entry._condition.release()

            # Check if the calculation completed
            with self.lock:
                if hash_key in self.cache and not self.cache[hash_key]._processing:
                    return self.cache[hash_key].value

            # If we weren't signaled and the entry is still processing, check timeout
            if not signaled:
                time_spent += 1
                self.check_calc_timeout(time_spent)

    def clear_cache(self) -> None:
        with self.lock:
            self.cache.clear()

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
                del self.cache[key]
