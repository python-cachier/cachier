"""A memory-based caching core for cachier."""

import threading
from datetime import datetime

from .base import _BaseCore


class _MemoryCore(_BaseCore):
    """The memory core class for cachier."""

    def __init__(self, hash_func, default_params):
        super().__init__(hash_func, default_params)
        self.cache = {}
        self.lock = threading.RLock()

    def get_entry_by_key(self, key, reload=False):
        with self.lock:
            return key, self.cache.get(key, None)

    def set_entry(self, key, func_res):
        with self.lock:
            try:
                # we need to retain the existing condition so that
                # mark_entry_not_calculated can notify all possibly-waiting
                # threads about it
                cond = self.cache[key]["condition"]
            except KeyError:  # pragma: no cover
                cond = None
            self.cache[key] = {
                "value": func_res,
                "time": datetime.now(),
                "stale": False,
                "being_calculated": False,
                "condition": cond,
            }

    def mark_entry_being_calculated(self, key):
        with self.lock:
            condition = threading.Condition()
            # condition.acquire()
            try:
                self.cache[key]["being_calculated"] = True
                self.cache[key]["condition"] = condition
            except KeyError:
                self.cache[key] = {
                    "value": None,
                    "time": datetime.now(),
                    "stale": False,
                    "being_calculated": True,
                    "condition": condition,
                }

    def mark_entry_not_calculated(self, key):
        with self.lock:
            try:
                entry = self.cache[key]
            except KeyError:  # pragma: no cover
                return  # that's ok, we don't need an entry in that case
            entry["being_calculated"] = False
            cond = entry["condition"]
            if cond:
                cond.acquire()
                cond.notify_all()
                cond.release()
                entry["condition"] = None

    def wait_on_entry_calc(self, key):
        with self.lock:  # pragma: no cover
            entry = self.cache[key]
            if not entry["being_calculated"]:
                return entry["value"]
        entry["condition"].acquire()
        entry["condition"].wait()
        entry["condition"].release()
        return self.cache[key]["value"]

    def clear_cache(self):
        with self.lock:
            self.cache.clear()

    def clear_being_calculated(self):
        with self.lock:
            for entry in self.cache.values():
                entry["being_calculated"] = False
                entry["condition"] = None
