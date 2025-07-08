"""A pickle-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>
import logging
import os
import pickle  # for local caching
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union

import portalocker  # to lock on pickle cache IO
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from .._types import HashFunc
from ..config import CacheEntry, _update_with_defaults

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

        def inject_observer(self, observer) -> None:
            """Inject the observer running this handler."""
            self.observer = observer

        def _check_calculation(self) -> None:
            entry = self.core.get_entry_by_key(self.key, True)[1]
            try:
                if not entry._processing:
                    # print('stopping observer!')
                    self.value = entry.value
                    if self.observer is not None:
                        self.observer.stop()
                # else:
                #     print('NOT stopping observer... :(')
            except AttributeError:  # catching entry being None
                self.value = None
                if self.observer is not None:
                    self.observer.stop()

        def on_created(self, event) -> None:
            """A Watchdog Event Handler method."""  # noqa: D401
            self._check_calculation()  # pragma: no cover

        def on_modified(self, event) -> None:
            """A Watchdog Event Handler method."""  # noqa: D401
            self._check_calculation()

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        pickle_reload: Optional[bool],
        cache_dir: Optional[Union[str, os.PathLike]],
        separate_files: Optional[bool],
        wait_for_calc_timeout: Optional[int],
    ):
        super().__init__(hash_func, wait_for_calc_timeout)
        self._cache_dict: Dict[str, CacheEntry] = {}
        self.reload = _update_with_defaults(pickle_reload, "pickle_reload")
        self.cache_dir = os.path.expanduser(
            _update_with_defaults(cache_dir, "cache_dir")
        )
        self.separate_files = _update_with_defaults(
            separate_files, "separate_files"
        )
        self._cache_used_fpath = ""

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

    @staticmethod
    def _convert_legacy_cache_entry(
        entry: Union[dict, CacheEntry],
    ) -> CacheEntry:
        if isinstance(entry, CacheEntry):
            return entry
        return CacheEntry(
            value=entry["value"],
            time=entry["time"],
            stale=entry["stale"],
            _processing=entry["being_calculated"],
            _condition=entry.get("condition", None),
        )

    def _load_cache_dict(self) -> Dict[str, CacheEntry]:
        try:
            with portalocker.Lock(self.cache_fpath, mode="rb") as cf:
                cache = pickle.load(cf)
            self._cache_used_fpath = str(self.cache_fpath)
        except (FileNotFoundError, EOFError):
            cache = {}
        return {
            k: _PickleCore._convert_legacy_cache_entry(v)
            for k, v in cache.items()
        }

    def get_cache_dict(self, reload: bool = False) -> Dict[str, CacheEntry]:
        if self._cache_used_fpath != self.cache_fpath:
            # force reload if the cache file has changed
            # this change is dies to using different wrapped function
            reload = True
        if self._cache_dict and not (self.reload or reload):
            return self._cache_dict
        with self.lock:
            self._cache_dict = self._load_cache_dict()
        return self._cache_dict

    def _load_cache_by_key(
        self, key=None, hash_str=None
    ) -> Optional[CacheEntry]:
        fpath = self.cache_fpath
        fpath += f"_{hash_str or key}"
        try:
            with portalocker.Lock(fpath, mode="rb") as cache_file:
                entry = pickle.load(cache_file)
            return _PickleCore._convert_legacy_cache_entry(entry)
        except (FileNotFoundError, EOFError):
            return None

    def _clear_all_cache_files(self) -> None:
        path, name = os.path.split(self.cache_fpath)
        for subpath in os.listdir(path):
            if subpath.startswith(f"{name}_"):
                os.remove(os.path.join(path, subpath))

    def _clear_being_calculated_all_cache_files(self) -> None:
        path, name = os.path.split(self.cache_fpath)
        for subpath in os.listdir(path):
            if subpath.startswith(name):
                entry = self._load_cache_by_key(
                    hash_str=subpath.split("_")[-1]
                )
                if entry is not None:
                    entry._processing = False
                    self._save_cache(entry, hash_str=subpath.split("_")[-1])

    def _save_cache(
        self,
        cache: Union[Dict[str, CacheEntry], CacheEntry],
        separate_file_key: Optional[str] = None,
        hash_str: Optional[str] = None,
    ) -> None:
        if separate_file_key and not isinstance(cache, CacheEntry):
            raise ValueError(
                "`separate_file_key` should only be used with a CacheEntry"
            )
        fpath = self.cache_fpath
        if separate_file_key is not None:
            fpath += f"_{separate_file_key}"
        elif hash_str is not None:
            fpath += f"_{hash_str}"
        with self.lock:
            with portalocker.Lock(fpath, mode="wb") as cf:
                pickle.dump(cache, cf, protocol=4)
            # the same as check for separate_file, but changed for typing
            if isinstance(cache, dict):
                self._cache_dict = cache
                self._cache_used_fpath = str(self.cache_fpath)

    def get_entry_by_key(
        self, key: str, reload: bool = False
    ) -> Tuple[str, Optional[CacheEntry]]:
        if self.separate_files:
            return key, self._load_cache_by_key(key)
        return key, self.get_cache_dict(reload).get(key)

    def set_entry(self, key: str, func_res: Any) -> None:
        key_data = CacheEntry(
            value=func_res,
            time=datetime.now(),
            stale=False,
            _processing=False,
            _completed=True,
        )
        if self.separate_files:
            self._save_cache(key_data, key)
            return  # pragma: no cover

        with self.lock:
            cache = self.get_cache_dict()
            cache[key] = key_data
            self._save_cache(cache)

    def mark_entry_being_calculated_separate_files(self, key: str) -> None:
        self._save_cache(
            CacheEntry(
                value=None,
                time=datetime.now(),
                stale=False,
                _processing=True,
            ),
            separate_file_key=key,
        )

    def _mark_entry_not_calculated_separate_files(self, key: str) -> None:
        _, entry = self.get_entry_by_key(key)
        if entry is None:
            return  # that's ok, we don't need an entry in that case
        entry._processing = False
        self._save_cache(entry, separate_file_key=key)

    def mark_entry_being_calculated(self, key: str) -> None:
        if self.separate_files:
            self.mark_entry_being_calculated_separate_files(key)
            return  # pragma: no cover

        with self.lock:
            cache = self.get_cache_dict()
            if key in cache:
                cache[key]._processing = True
            else:
                cache[key] = CacheEntry(
                    value=None,
                    time=datetime.now(),
                    stale=False,
                    _processing=True,
                )
            self._save_cache(cache)

    def mark_entry_not_calculated(self, key: str) -> None:
        if self.separate_files:
            self._mark_entry_not_calculated_separate_files(key)
        with self.lock:
            cache = self.get_cache_dict()
            # that's ok, we don't need an entry in that case
            if isinstance(cache, dict) and key in cache:
                cache[key]._processing = False
                self._save_cache(cache)

    def _create_observer(self) -> Observer:
        """Create a new observer instance."""
        return Observer()

    def _cleanup_observer(self, observer: Observer) -> None:
        """Clean up observer properly."""
        try:
            if observer.is_alive():
                observer.stop()
                observer.join(timeout=1.0)
        except Exception as e:
            logging.debug("Observer cleanup failed: %s", e)

    def wait_on_entry_calc(self, key: str) -> Any:
        """Wait for entry calculation to complete with inotify protection."""
        if self.separate_files:
            entry = self._load_cache_by_key(key)
            filename = f"{self.cache_fname}_{key}"
        else:
            with self.lock:
                entry = self.get_cache_dict().get(key)
            filename = self.cache_fname

        if entry and not entry._processing:
            return entry.value

        # Try to use inotify-based waiting
        try:
            return self._wait_with_inotify(key, filename)
        except OSError as e:
            if "inotify instance limit reached" in str(e):
                # Fall back to polling if inotify limit is reached
                return self._wait_with_polling(key)
            else:
                raise

    def _wait_with_inotify(self, key: str, filename: str) -> Any:
        """Wait for calculation using inotify with proper cleanup."""
        event_handler = _PickleCore.CacheChangeHandler(
            filename=filename, core=self, key=key
        )

        observer = self._create_observer()
        event_handler.inject_observer(observer)

        try:
            observer.schedule(
                event_handler, path=self.cache_dir, recursive=True
            )
            observer.start()

            time_spent = 0
            while observer.is_alive():
                observer.join(timeout=1.0)
                time_spent += 1
                self.check_calc_timeout(time_spent)

                # Check if calculation is complete
                if event_handler.value is not None:
                    break

            return event_handler.value
        finally:
            # Always cleanup the observer
            self._cleanup_observer(observer)

    def _wait_with_polling(self, key: str) -> Any:
        """Fallback method using polling instead of inotify."""
        time_spent = 0
        while True:
            time.sleep(1)  # Poll every 1 second (matching other cores)
            time_spent += 1

            try:
                if self.separate_files:
                    entry = self._load_cache_by_key(key)
                else:
                    with self.lock:
                        entry = self.get_cache_dict().get(key)

                if entry and not entry._processing:
                    return entry.value

                self.check_calc_timeout(time_spent)
            except (FileNotFoundError, EOFError):
                # Continue polling even if there are file errors
                pass

    def clear_cache(self) -> None:
        if self.separate_files:
            self._clear_all_cache_files()
        else:
            self._save_cache({})

    def clear_being_calculated(self) -> None:
        if self.separate_files:
            self._clear_being_calculated_all_cache_files()
            return  # pragma: no cover

        with self.lock:
            cache = self.get_cache_dict()
            for key in cache:
                cache[key]._processing = False
            self._save_cache(cache)
