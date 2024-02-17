"""A pickle-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>
import os
import pickle  # for local caching
import threading
from contextlib import suppress
from datetime import datetime

import portalocker  # to lock on pickle cache IO
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

# Alternative:  https://github.com/WoLpH/portalocker
from .base import _BaseCore


class _PickleCore(_BaseCore):
    """The pickle core class for cachier.

    Parameters
    ----------
    pickle_reload : bool, optional
        See core.cachier() documentation.
    cache_dir : str, optional.
        See core.cachier() documentation.

    """

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
        hash_func,
        pickle_reload,
        cache_dir,
        separate_files,
        wait_for_calc_timeout,
        default_params,
    ):
        super().__init__(hash_func, default_params)
        self.cache = None
        if pickle_reload is not None:
            self.reload = pickle_reload
        else:
            self.reload = self.default_params["pickle_reload"]
        if cache_dir is not None:
            self.cache_dir = os.path.expanduser(cache_dir)
        else:
            self.cache_dir = os.path.expanduser(
                self.default_params["cache_dir"]
            )
        if separate_files is not None:
            self.separate_files = separate_files
        else:
            self.separate_files = self.default_params["separate_files"]
        self.wait_for_calc_timeout = wait_for_calc_timeout
        self.cache_fname = None
        self.cache_fpath = None
        self.lock = threading.RLock()

    def _cache_fname(self):
        if self.cache_fname is None:
            self.cache_fname = (
                f".{self.func.__module__}.{self.func.__qualname__}"
            )
            self.cache_fname = self.cache_fname.replace("<", "_").replace(
                ">", "_"
            )
        return self.cache_fname

    def _cache_fpath(self):
        if self.cache_fpath is None:
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)
            self.cache_fpath = os.path.abspath(
                os.path.join(
                    os.path.realpath(self.cache_dir),
                    self._cache_fname(),
                )
            )
        return self.cache_fpath

    def _reload_cache(self):
        with self.lock:
            fpath = self._cache_fpath()
            try:
                with portalocker.Lock(fpath, mode="rb") as cache_file:
                    try:
                        self.cache = pickle.load(cache_file)  # noqa: S301
                    except EOFError:
                        self.cache = {}
            except FileNotFoundError:
                self.cache = {}

    def _get_cache(self):
        with self.lock:
            if not self.cache:
                self._reload_cache()
            return self.cache

    def _get_cache_by_key(self, key=None, hash=None):
        fpath = self._cache_fpath()
        fpath += f"_{key}" if hash is None else f"_{hash}"
        try:
            with portalocker.Lock(fpath, mode="rb") as cache_file:
                return pickle.load(cache_file)  # noqa: S301
        except (FileNotFoundError, EOFError):
            return None

    def _clear_all_cache_files(self):
        fpath = self._cache_fpath()
        path, name = os.path.split(fpath)
        for subpath in os.listdir(path):
            if subpath.startswith(f"{name}_"):
                os.remove(os.path.join(path, subpath))

    def _clear_being_calculated_all_cache_files(self):
        fpath = self._cache_fpath()
        path, name = os.path.split(fpath)
        for subpath in os.listdir(path):
            if subpath.startswith(name):
                entry = self._get_cache_by_key(hash=subpath.split("_")[-1])
                if entry is not None:
                    entry["being_calculated"] = False
                    self._save_cache(entry, hash=subpath.split("_")[-1])

    def _save_cache(self, cache, key=None, hash=None):
        with self.lock:
            self.cache = cache
            fpath = self._cache_fpath()
            if key is not None:
                fpath += f"_{key}"
            elif hash is not None:
                fpath += f"_{hash}"
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
        else:
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
        else:
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
            filename = f"{self._cache_fname()}_{key}"
        else:
            with self.lock:
                self._reload_cache()
                entry = self._get_cache()[key]
            filename = self._cache_fname()
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
        else:
            with self.lock:
                cache = self._get_cache()
                for key in cache:
                    cache[key]["being_calculated"] = False
                self._save_cache(cache)
