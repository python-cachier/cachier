"""Defines the interface of a cachier caching core."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import abc  # for the _BaseCore abstract base class
import inspect
import threading
from typing import Callable, Optional, Tuple

from .._types import HashFunc
from ..config import CacheEntry, _update_with_defaults


class RecalculationNeeded(Exception):
    """Exception raised when a recalculation is needed."""

    pass


def _get_func_str(func: Callable) -> str:
    return f".{func.__module__}.{func.__name__}"


class _BaseCore:
    __metaclass__ = abc.ABCMeta

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        wait_for_calc_timeout: Optional[int],
    ):
        self.hash_func = _update_with_defaults(hash_func, "hash_func")
        self.wait_for_calc_timeout = wait_for_calc_timeout
        self.lock = threading.RLock()

    def set_func(self, func):
        """Set the function this core will use.

        This has to be set before any method is called. Also determine if the
        function is an object method.

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

        Return the result mapped to the given arguments in this core's cache,
        if such a mapping exists.

        """
        key = self.get_key(args, kwds)
        return self.get_entry_by_key(key)

    def precache_value(self, args, kwds, value_to_cache):
        """Write a precomputed value into the cache."""
        key = self.get_key(args, kwds)
        self.set_entry(key, value_to_cache)
        return value_to_cache

    def check_calc_timeout(self, time_spent):
        """Raise an exception if a recalculation is needed."""
        calc_timeout = _update_with_defaults(
            self.wait_for_calc_timeout, "wait_for_calc_timeout"
        )
        if calc_timeout > 0 and (time_spent >= calc_timeout):
            raise RecalculationNeeded()

    @abc.abstractmethod
    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        """Get entry based on given key.

        Return the result mapped to the given key in this core's cache, if such
        a mapping exists.

        """

    @abc.abstractmethod
    def set_entry(self, key: str, func_res):
        """Map the given result to the given key in this core's cache."""

    @abc.abstractmethod
    def mark_entry_being_calculated(self, key: str) -> None:
        """Mark the entry mapped by the given key as being calculated."""

    @abc.abstractmethod
    def mark_entry_not_calculated(self, key: str) -> None:
        """Mark the entry mapped by the given key as not being calculated."""

    @abc.abstractmethod
    def wait_on_entry_calc(self, key: str) -> None:
        """Wait on the entry with keys being calculated and returns result."""

    @abc.abstractmethod
    def clear_cache(self) -> None:
        """Clear the cache of this core."""

    @abc.abstractmethod
    def clear_being_calculated(self) -> None:
        """Mark all entries in this cache as not being calculated."""
