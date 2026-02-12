"""Defines the interface of a cachier caching core."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import abc  # for the _BaseCore abstract base class
import asyncio
import inspect
import sys
import threading
from datetime import timedelta
from typing import Any, Callable, Optional, Tuple

from pympler import asizeof  # type: ignore

from .._types import HashFunc
from ..config import CacheEntry, _update_with_defaults


class RecalculationNeeded(Exception):
    """Exception raised when a recalculation is needed."""

    pass


def _get_func_str(func: Callable) -> str:
    """Return a string identifier for the function (module + name).

    We accept Any here because static analysis can't always prove that the runtime object will have __module__ and
    __name__, but at runtime the decorated functions always do.

    """
    return f".{func.__module__}.{func.__name__}"


class _BaseCore(metaclass=abc.ABCMeta):
    def __init__(
        self,
        hash_func: Optional[HashFunc],
        wait_for_calc_timeout: Optional[int],
        entry_size_limit: Optional[int] = None,
    ):
        self.hash_func = _update_with_defaults(hash_func, "hash_func")
        self.wait_for_calc_timeout = wait_for_calc_timeout
        self.lock = threading.RLock()
        self.entry_size_limit = entry_size_limit

    def set_func(self, func):
        """Set the function this core will use.

        This has to be set before any method is called. Also determine if the function is an object method.

        """
        # unwrap if the function is functools.partial
        if hasattr(func, "func"):
            func = func.func
        func_params = list(inspect.signature(func).parameters)
        self.func_is_method = func_params and func_params[0] == "self"
        self.func = func

    def get_key(self, args, kwds):
        """Return a unique key based on the arguments provided."""
        return self.hash_func(args, kwds)

    def get_entry(self, args, kwds) -> Tuple[str, Optional[CacheEntry]]:
        """Get entry based on given arguments.

        Return the result mapped to the given arguments in this core's cache, if such a mapping exists.

        """
        key = self.get_key(args, kwds)
        return self.get_entry_by_key(key)

    async def aget_entry(self, args, kwds) -> Tuple[str, Optional[CacheEntry]]:
        """Async-compatible variant of :meth:`get_entry`.

        Subclasses may override this to support async backends (e.g. async client factories).

        """
        return self.get_entry(args, kwds)

    def precache_value(self, args, kwds, value_to_cache):
        """Write a precomputed value into the cache."""
        key = self.get_key(args, kwds)
        self.set_entry(key, value_to_cache)
        return value_to_cache

    async def aprecache_value(self, args, kwds, value_to_cache):
        """Async-compatible variant of :meth:`precache_value`."""
        key = self.get_key(args, kwds)
        await self.aset_entry(key, value_to_cache)
        return value_to_cache

    def check_calc_timeout(self, time_spent):
        """Raise an exception if a recalculation is needed."""
        calc_timeout = _update_with_defaults(self.wait_for_calc_timeout, "wait_for_calc_timeout")
        if calc_timeout > 0 and (time_spent >= calc_timeout):
            raise RecalculationNeeded()

    @abc.abstractmethod
    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        """Get entry based on given key.

        Return the key and the :class:`~cachier.config.CacheEntry` mapped
        to the given key in this core's cache, if such a mapping exists.

        """

    async def aget_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        """Async-compatible variant of :meth:`get_entry_by_key`."""
        return self.get_entry_by_key(key)

    def _estimate_size(self, value: Any) -> int:
        try:
            return asizeof.asizeof(value)
        except Exception:
            return sys.getsizeof(value)

    def _should_store(self, value: Any) -> bool:
        if self.entry_size_limit is None:
            return True
        try:
            return self._estimate_size(value) <= self.entry_size_limit
        except Exception:
            return True

    @abc.abstractmethod
    def set_entry(self, key: str, func_res: Any) -> bool:
        """Map the given result to the given key in this core's cache."""

    async def aset_entry(self, key: str, func_res: Any) -> bool:
        """Async-compatible variant of :meth:`set_entry`."""
        return self.set_entry(key, func_res)

    @abc.abstractmethod
    def mark_entry_being_calculated(self, key: str) -> None:
        """Mark the entry mapped by the given key as being calculated."""

    async def amark_entry_being_calculated(self, key: str) -> None:
        """Async-compatible variant of :meth:`mark_entry_being_calculated`."""
        self.mark_entry_being_calculated(key)

    @abc.abstractmethod
    def mark_entry_not_calculated(self, key: str) -> None:
        """Mark the entry mapped by the given key as not being calculated."""

    async def amark_entry_not_calculated(self, key: str) -> None:
        """Async-compatible variant of :meth:`mark_entry_not_calculated`."""
        self.mark_entry_not_calculated(key)

    @abc.abstractmethod
    def wait_on_entry_calc(self, key: str) -> None:
        """Wait on the entry with keys being calculated and returns result."""

    async def await_on_entry_calc(self, key: str) -> Any:
        """Async-compatible variant of :meth:`wait_on_entry_calc`.

        By default this runs in a thread to avoid blocking the event loop.

        """
        return await asyncio.to_thread(self.wait_on_entry_calc, key)

    @abc.abstractmethod
    def clear_cache(self) -> None:
        """Clear the cache of this core."""

    async def aclear_cache(self) -> None:
        """Async-compatible variant of :meth:`clear_cache`.

        By default this runs in a thread to avoid blocking the event loop.

        """
        await asyncio.to_thread(self.clear_cache)

    @abc.abstractmethod
    def clear_being_calculated(self) -> None:
        """Mark all entries in this cache as not being calculated."""

    async def aclear_being_calculated(self) -> None:
        """Async-compatible variant of :meth:`clear_being_calculated`.

        By default this runs in a thread to avoid blocking the event loop.

        """
        await asyncio.to_thread(self.clear_being_calculated)

    @abc.abstractmethod
    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Delete cache entries older than ``stale_after``."""

    async def adelete_stale_entries(self, stale_after: timedelta) -> None:
        """Async-compatible variant of :meth:`delete_stale_entries`.

        By default this runs in a thread to avoid blocking the event loop.

        """
        await asyncio.to_thread(self.delete_stale_entries, stale_after)
