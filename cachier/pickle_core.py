"""A pickle-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import os
import pickle  # for local caching
from datetime import datetime
import threading

import portalocker  # to lock on pickle cache IO
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
#Altenative:  https://github.com/WoLpH/portalocker

from .base_core import _BaseCore

try:
    FileNotFoundError
except NameError:  # we're on python 2
    FileNotFoundError = IOError


CACHIER_DIR = '~/.cachier/'
EXPANDED_CACHIER_DIR = os.path.expanduser(CACHIER_DIR)


class _PickleCore(_BaseCore):

    class CacheChangeHandler(PatternMatchingEventHandler):
        """Handles cache-file modification events."""

        def __init__(self, filename, core, key):
            PatternMatchingEventHandler.__init__(
                self,
                patterns=["*" + filename],
                ignore_patterns=None,
                ignore_directories=True,
                case_sensitive=False
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
                if not entry['being_calculated']:
                    # print('stoping observer!')
                    self.value = entry['value']
                    self.observer.stop()
                # else:
                    # print('NOT stoping observer... :(')
            except TypeError:
                self.value = None
                self.observer.stop()

        def on_created(self, event):
            self._check_calculation() # pragma: no cover

        def on_modified(self, event):
            self._check_calculation()

    def __init__(self, stale_after, next_time, reload):
        _BaseCore.__init__(self, stale_after, next_time)
        self.cache = None
        self.reload = reload
        self.lock = threading.RLock()

    def _cache_fname(self):
        if not hasattr(self, 'cache_fname'):
            self.cache_fname = '.{}.{}'.format(
                self.func.__module__, self.func.__name__)
        return self.cache_fname

    def _cache_fpath(self):
        if not hasattr(self, 'cache_fpath'):
            # print(EXPANDED_CACHIER_DIR)
            if not os.path.exists(EXPANDED_CACHIER_DIR):
                os.makedirs(EXPANDED_CACHIER_DIR)
            self.cache_fpath = os.path.abspath(os.path.join(
                os.path.realpath(EXPANDED_CACHIER_DIR),
                self._cache_fname()
            ))
        return self.cache_fpath

    def _reload_cache(self):
        with self.lock:
            fpath = self._cache_fpath()
            try:
                with portalocker.Lock(fpath, mode='rb') as cache_file:
                    try:
                        self.cache = pickle.load(cache_file)
                    except EOFError:
                        self.cache = {}
            except FileNotFoundError:
                self.cache = {}

    def _get_cache(self):
        with self.lock:
            if not self.cache:
                self._reload_cache()
            return self.cache

    def _save_cache(self, cache):
        with self.lock:
            self.cache = cache
            fpath = self._cache_fpath()
            with portalocker.Lock(fpath, mode='wb') as cache_file:
                pickle.dump(cache, cache_file)
            self._reload_cache()

    def get_entry_by_key(self, key, reload=False):  # pylint: disable=W0221
        with self.lock:
            # print('{}, {}'.format(self.reload, reload))
            if self.reload or reload:
                self._reload_cache()
            return key, self._get_cache().get(key, None)

    def get_entry(self, args, kwds):
        key = args + tuple(sorted(kwds.items()))
        # print('key type={}, key={}'.format(type(key), key))
        return self.get_entry_by_key(key)

    def set_entry(self, key, func_res):
        with self.lock:
            cache = self._get_cache()
            cache[key] = {
                'value': func_res,
                'time': datetime.now(),
                'stale': False,
                'being_calculated': False
            }
            self._save_cache(cache)

    def mark_entry_being_calculated(self, key):
        with self.lock:
            cache = self._get_cache()
            try:
                cache[key]['being_calculated'] = True
            except KeyError:
                cache[key] = {
                    'value': None,
                    'time': datetime.now(),
                    'stale': False,
                    'being_calculated': True
                }
            self._save_cache(cache)

    def mark_entry_not_calculated(self, key):
        with self.lock:
            cache = self._get_cache()
            try:
                cache[key]['being_calculated'] = False
                self._save_cache(cache)
            except KeyError:
                pass  # that's ok, we don't need an entry in that case

    def wait_on_entry_calc(self, key):
        with self.lock:
            self._reload_cache()
            entry = self._get_cache()[key]
            if not entry['being_calculated']:
                return entry['value']
        event_handler = _PickleCore.CacheChangeHandler(
            filename=self._cache_fname(),
            core=self,
            key=key
        )
        observer = Observer()
        event_handler.inject_observer(observer)
        observer.schedule(
            event_handler,
            path=EXPANDED_CACHIER_DIR,
            recursive=True
        )
        observer.start()
        observer.join(timeout=1.0)
        if observer.isAlive():
            # print('Timedout waiting. Starting again...')
            return self.wait_on_entry_calc(key)
        # print("Returned value: {}".format(event_handler.value))
        return event_handler.value

    def clear_cache(self):
        self._save_cache({})

    def clear_being_calculated(self):
        with self.lock:
            cache = self._get_cache()
            for key in cache:
                cache[key]['being_calculated'] = False
            self._save_cache(cache)
