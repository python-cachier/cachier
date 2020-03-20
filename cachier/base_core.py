"""Defines the interface of a cachier caching core."""
# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import abc  # for the _BaseCore abstract base class


class _BaseCore():
    __metaclass__ = abc.ABCMeta

    def __init__(self, stale_after, next_time):
        self.stale_after = stale_after
        self.next_time = next_time
        self.func = None

    def set_func(self, func):
        """Sets the function this core will use. This has to be set before
        any method is called"""
        self.func = func

    @abc.abstractmethod
    def get_entry_by_key(self, key):
        """Returns the result mapped to the given key in this core's cache,
        if such a mapping exists."""

    @abc.abstractmethod
    def get_entry(self, args, kwds, hash_params):
        """Returns the result mapped to the given arguments in this core's
        cache, if such a mapping exists."""

    @abc.abstractmethod
    def set_entry(self, key, func_res):
        """Maps the given result to the given key in this core's cache."""

    @abc.abstractmethod
    def mark_entry_being_calculated(self, key):
        """Marks the entry mapped by the given key as being calculated."""

    @abc.abstractmethod
    def mark_entry_not_calculated(self, key):
        """Marks the entry mapped by the given key as not being calculated."""

    @abc.abstractmethod
    def wait_on_entry_calc(self, key):
        """Waits on the entry mapped by key being calculated and returns the
        result."""

    @abc.abstractmethod
    def clear_cache(self):
        """Clears the cache of this core."""

    @abc.abstractmethod
    def clear_being_calculated(self):
        """Marks all entries in this cache as not being calculated."""
