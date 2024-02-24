"""A pickle-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>
import os
import pickle  # for local caching
from contextlib import suppress
from datetime import datetime

import portalocker  # to lock on pickle cache IO
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from .._types import HashFunc
from ..config import _update_with_defaults

# Alternative:  https://github.com/WoLpH/portalocker
from .base import _BaseCore


class _PickleCore(_BaseCore):
    """The pickle core class for cachier."""

    class CacheChangeHandler(PatternMatchingEventHandler):
        """Handles cache-file modification events."""

        def __init__(self, filename, core, key):
            PatternMatchingEventHandler.__init__(
                self,
                patterns=["*" + filename],
                ignore_patterns=None,
                ignore_directories=True,
                case_sensitive=False,
            )
            self.core = core
            self.key = key
            self.observer = None
            self.value = None

        def inject_observer(self, observer):
            """Inject the observer running this handler."""
            self.observer = observer

        def _check_calculation(self):
            # print('checking calc')
            entry = self.core.get_entry_by_key(self.key, True)[1]
            # print(self.key)
            # print(entry)
            try:
                if not entry["being_calculated"]:
                    # print('stopping observer!')
                    self.value = entry["value"]
                    self.observer.stop()
                # else:
                # print('NOT stopping observer... :(')
            except TypeError:
                self.value = None
                self.observer.stop()

        def on_created(self, event):
            """A Watchdog Event Handler method."""
            self._check_calculation()  # pragma: no cover

        def on_modified(self, event):
            """A Watchdog Event Handler method."""
            self._check_calculation()

    def __init__(
        self,
        hash_func: HashFunc,
        pickle_reload: bool,
        cache_dir: str,
        separate_files: bool,
        wait_for_calc_timeout: int,
    ):
        super().__init__(hash_func, wait_for_calc_timeout)
        self.cache = None
        self.reload = _update_with_defaults(pickle_reload, "pickle_reload")
        self.cache_dir = os.path.expanduser(
            _update_with_defaults(cache_dir, "cache_dir")
        )
        self.separate_files = _update_with_defaults(
            separate_files, "separate_files"
        )

    @property
    def cache_fname(self) -> str:
        fname = f".{self.func.__module__}.{self.func.__qualname__}"
        return fname.replace("<", "_").replace(">", "_")

    @property
    def cache_fpath(self) -> str:
        os.makedirs(self.cache_dir, exist_ok=True)
        return os.path.abspath(
            os.path.join(os.path.realpath(self.cache_dir), self.cache_fname)
        )

    def _reload_cache(self):
        with self.lock:
            try:
                with portalocker.Lock(
                    self.cache_fpath, mode="rb"
                ) as cache_file:
                    self.cache = pickle.load(cache_file)  # noqa: S301
            except (FileNotFoundError, EOFError):
                self.cache = {}

    def _get_cache(self):
        with self.lock:
            if not self.cache:
                self._reload_cache()
            return self.cache

    def _get_cache_by_key(self, key=None, hash=None):
        fpath = self.cache_fpath
        fpath += f"_{key}" if hash is None else f"_{hash}"
        try:
            with portalocker.Lock(fpath, mode="rb") as cache_file:
                return pickle.load(cache_file)  # noqa: S301
        except (FileNotFoundError, EOFError):
            return None

    def _clear_all_cache_files(self):
        path, name = os.path.split(self.cache_fpath)
        for subpath in os.listdir(path):
            if subpath.startswith(f"{name}_"):
                os.remove(os.path.join(path, subpath))

    def _clear_being_calculated_all_cache_files(self):
        path, name = os.path.split(self.cache_fpath)
        for subpath in os.listdir(path):
            if subpath.startswith(name):
                entry = self._get_cache_by_key(hash=subpath.split("_")[-1])
                if entry is not None:
                    entry["being_calculated"] = False
                    self._save_cache(entry, hash=subpath.split("_")[-1])

    def _save_cache(self, cache, key=None, hash=None):
        fpath = self.cache_fpath
        if key is not None:
            fpath += f"_{key}"
        elif hash is not None:
            fpath += f"_{hash}"
        with self.lock:
            self.cache = cache
            with portalocker.Lock(fpath, mode="wb") as cache_file:
                pickle.dump(cache, cache_file, protocol=4)
            if key is None:
                self._reload_cache()

    def get_entry_by_key(self, key, reload=False):
        with self.lock:
            if self.separate_files:
                return key, self._get_cache_by_key(key)
            if self.reload or reload:
                self._reload_cache()
            return key, self._get_cache().get(key, None)

    def set_entry(self, key, func_res):
        key_data = {
            "value": func_res,
            "time": datetime.now(),
            "stale": False,
            "being_calculated": False,
        }
        if self.separate_files:
            self._save_cache(key_data, key)
            return  # pragma: no cover

        with self.lock:
            cache = self._get_cache()
            cache[key] = key_data
            self._save_cache(cache)

    def mark_entry_being_calculated_separate_files(self, key):
        self._save_cache(
            {
                "value": None,
                "time": datetime.now(),
                "stale": False,
                "being_calculated": True,
            },
            key=key,
        )

    def mark_entry_not_calculated_separate_files(self, key):
        _, entry = self.get_entry_by_key(key)
        entry["being_calculated"] = False
        self._save_cache(entry, key=key)

    def mark_entry_being_calculated(self, key):
        if self.separate_files:
            self.mark_entry_being_calculated_separate_files(key)
            return  # pragma: no cover

        with self.lock:
            cache = self._get_cache()
            try:
                cache[key]["being_calculated"] = True
            except KeyError:
                cache[key] = {
                    "value": None,
                    "time": datetime.now(),
                    "stale": False,
                    "being_calculated": True,
                }
            self._save_cache(cache)

    def mark_entry_not_calculated(self, key):
        if self.separate_files:
            self.mark_entry_not_calculated_separate_files(key)
        with self.lock:
            cache = self._get_cache()
            # that's ok, we don't need an entry in that case
            with suppress(KeyError):
                cache[key]["being_calculated"] = False
                self._save_cache(cache)

    def wait_on_entry_calc(self, key):
        if self.separate_files:
            entry = self._get_cache_by_key(key)
            filename = f"{self.cache_fname}_{key}"
        else:
            with self.lock:
                self._reload_cache()
                entry = self._get_cache()[key]
            filename = self.cache_fname
        if not entry["being_calculated"]:
            return entry["value"]
        event_handler = _PickleCore.CacheChangeHandler(
            filename=filename, core=self, key=key
        )
        observer = Observer()
        event_handler.inject_observer(observer)
        observer.schedule(event_handler, path=self.cache_dir, recursive=True)
        observer.start()
        time_spent = 0
        while observer.is_alive():
            observer.join(timeout=1.0)
            time_spent += 1
            self.check_calc_timeout(time_spent)
        return event_handler.value

    def clear_cache(self):
        if self.separate_files:
            self._clear_all_cache_files()
        else:
            self._save_cache({})

    def clear_being_calculated(self):
        if self.separate_files:
            self._clear_being_calculated_all_cache_files()
            return  # pragma: no cover

        with self.lock:
            cache = self._get_cache()
            for key in cache:
                cache[key]["being_calculated"] = False
            self._save_cache(cache)
