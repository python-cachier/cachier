"""Persistent, stale-free memoization decorators for Python."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import os
from functools import wraps
import pickle  # for local caching
import datetime
import abc  # for the _BaseCore abstract base class
import concurrent.futures  # for asynchronous file uploads
import time   # to sleep when waiting on Mongo cache
import fcntl  # to lock on pickle cache IO

import pymongo
from bson.binary import Binary  # to save binary data to mongodb
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

CACHIER_DIR = '~/.cachier/'
EXPANDED_CACHIER_DIR = os.path.expanduser(CACHIER_DIR)
DEFAULT_MAX_WORKERS = 8
MONGO_SLEEP_DURATION_IN_SEC = 6


# === Cores definitions ===

class _BaseCore(object):
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
    def get_entry(self, args, kwds):
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


class _MongoCore(_BaseCore):

    def __init__(self, mongetter, stale_after, next_time):
        super().__init__(stale_after, next_time)
        self.mongetter = mongetter
        self.mongo_collection = None

    @staticmethod
    def _get_func_str(func):
        return '.{}.{}'.format(func.__module__, func.__name__)

    def _get_mongo_collection(self):
        if not self.mongo_collection:
            self.mongo_collection = self.mongetter()
        return self.mongo_collection

    def get_entry_by_key(self, key):
        res = self._get_mongo_collection().find_one({
            'func': _MongoCore._get_func_str(self.func),
            'key': key
        })
        if res:
            try:
                entry = {
                    'value': pickle.loads(res['value']),
                    'time': res.get('time', None),
                    'stale': res.get('stale', False),
                    'being_calculated': res.get('being_calculated', False)
                }
            except KeyError:
                entry = {
                    'value': None,
                    'time': res.get('time', None),
                    'stale': res.get('stale', False),
                    'being_calculated': res.get('being_calculated', False)
                }
            return key, entry
        return key, None

    def get_entry(self, args, kwds):
        key = pickle.dumps(args + tuple(sorted(kwds.items())))
        # print('key type={}, key={}'.format(
        #     type(key), key))
        return self.get_entry_by_key(key)

    def set_entry(self, key, func_res):
        thebytes = pickle.dumps(func_res)
        self._get_mongo_collection().update_one(
            {
                'func': _MongoCore._get_func_str(self.func),
                'key': key
            },
            {
                '$set': {
                    'func': _MongoCore._get_func_str(self.func),
                    'key': key,
                    'value': Binary(thebytes),
                    'time': datetime.datetime.now(),
                    'stale': False,
                    'being_calculated': False
                }
            },
            upsert=True
        )

    def mark_entry_being_calculated(self, key):
        self._get_mongo_collection().update_one(
            {
                'func': _MongoCore._get_func_str(self.func),
                'key': key
            },
            {
                '$set': {'being_calculated': True}
            },
            upsert=True
        )

    def mark_entry_not_calculated(self, key):
        try:
            self._get_mongo_collection().update_one(
                {
                    'func': _MongoCore._get_func_str(self.func),
                    'key': key
                },
                {
                    '$set': {'being_calculated': False}
                },
                upsert=False  # should not insert in this case
            )
        except pymongo.errors.OperationFailure:
            pass  # don't care in this case

    def wait_on_entry_calc(self, key):
        while True:
            time.sleep(MONGO_SLEEP_DURATION_IN_SEC)
            key, entry = self.get_entry_by_key(key)
            if entry is not None and not entry['being_calculated']:
                return entry['value']
        # key, entry = self.get_entry_by_key(key)
        # if entry is not None:
        #     return entry['value']
        # return None

    def clear_cache(self):
        self._get_mongo_collection().delete_many(
            {'func': _MongoCore._get_func_str(self.func)})

    def clear_being_calculated(self):
        self._get_mongo_collection().update_many(
            {
                'func': _MongoCore._get_func_str(self.func),
                'being_calculated': True
            },
            {
                '$set': {'being_calculated': False}
            }
        )

class _PickleCore(_BaseCore):

    class CacheChangeHandler(PatternMatchingEventHandler):
        """Handles cache-file modification events."""

        def __init__(self, filename, core, key):
            super(_PickleCore.CacheChangeHandler, self).__init__(
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
            if not entry['being_calculated']:
                # print('stoping observer!')
                self.value = entry['value']
                self.observer.stop()
            # print('NOT stoping observer... :(')

        def on_created(self, event):
            self._check_calculation()

        def on_modified(self, event):
            self._check_calculation()

    def __init__(self, stale_after, next_time, reload):
        super().__init__(stale_after, next_time)
        self.cache = None
        self.reload = reload

    def _get_cache_file_name(self):
        return '.{}.{}'.format(
            self.func.__module__, self.func.__name__)  # pylint: disable=W0212

    def _get_cache_path(self):
        # print(EXPANDED_CACHIER_DIR)
        if not os.path.exists(EXPANDED_CACHIER_DIR):
            os.makedirs(EXPANDED_CACHIER_DIR)
        fpath = os.path.abspath(os.path.join(
            os.path.realpath(EXPANDED_CACHIER_DIR),
            self._get_cache_file_name()
        ))
        # print(fpath)
        return fpath

    def _reload_cache(self):
        fpath = self._get_cache_path()
        try:
            with open(fpath, 'rb') as cache_file:
                fcntl.flock(cache_file, fcntl.LOCK_SH)
                try:
                    self.cache = pickle.load(cache_file)
                except EOFError:
                    self.cache = {}
                fcntl.flock(cache_file, fcntl.LOCK_UN)
        except FileNotFoundError:
            self.cache = {}

    def _get_cache(self):
        if not self.cache:
            self._reload_cache()
        return self.cache

    def _save_cache(self, cache):
        self.cache = cache
        fpath = self._get_cache_path()
        with open(fpath, 'wb') as cache_file:
            fcntl.flock(cache_file, fcntl.LOCK_EX)
            pickle.dump(cache, cache_file)
            fcntl.flock(cache_file, fcntl.LOCK_UN)
        self._reload_cache()

    def get_entry_by_key(self, key, reload=False):  # pylint: disable=W0221
        # print('{}, {}'.format(self.reload, reload))
        if self.reload or reload:
            self._reload_cache()
        return key, self._get_cache().get(key, None)

    def get_entry(self, args, kwds):
        key = args + tuple(sorted(kwds.items()))
        # print('key type={}, key={}'.format(type(key), key))
        return self.get_entry_by_key(key)

    def set_entry(self, key, func_res):
        cache = self._get_cache()
        cache[key] = {
            'value': func_res,
            'time': datetime.datetime.now(),
            'stale': False,
            'being_calculated': False
        }
        self._save_cache(cache)

    def mark_entry_being_calculated(self, key):
        cache = self._get_cache()
        try:
            cache[key]['being_calculated'] = True
        except KeyError:
            cache[key] = {
                'value': None,
                'time': datetime.datetime.now(),
                'stale': False,
                'being_calculated': True
            }
        self._save_cache(cache)

    def mark_entry_not_calculated(self, key):
        cache = self._get_cache()
        try:
            cache[key]['being_calculated'] = False
            self._save_cache(cache)
        except KeyError:
            pass  # that's ok, we don't need an entry in that case


    def wait_on_entry_calc(self, key):
        entry = self._get_cache()[key]
        if not entry['being_calculated']:
            return entry['value']
        event_handler = _PickleCore.CacheChangeHandler(
            filename=self._get_cache_file_name(),
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
        observer.join(timeout=2.0)
        if observer.isAlive():
            # print('Timedout waiting. Starting again...')
            return self.wait_on_entry_calc(key)
        # print("Returned value: {}".format(event_handler.value))
        return event_handler.value

    def clear_cache(self):
        self._save_cache({})

    def clear_being_calculated(self):
        cache = self._get_cache()
        for key in cache:
            cache[key]['being_calculated'] = False
        self._save_cache(cache)


# === Main functionality ===

def _max_workers():
    try:
        return int(os.environ['CACHIER_MAX_WORKERS'])
    except KeyError:
        os.environ['CACHIER_MAX_WORKERS'] = str(DEFAULT_MAX_WORKERS)
        return DEFAULT_MAX_WORKERS


def _set_max_workets(max_workers):
    os.environ['CACHIER_MAX_WORKERS'] = str(max_workers)
    _get_executor(True)


def _get_executor(reset=False):
    if reset:
        _get_executor.executor = concurrent.futures.ThreadPoolExecutor(
            _max_workers())
    try:
        return _get_executor.executor
    except AttributeError:
        _get_executor.executor = concurrent.futures.ThreadPoolExecutor(
            _max_workers())
        return _get_executor.executor


def _function_thread(core, key, func, args, kwds):
    try:
        func_res = func(*args, **kwds)
        core.set_entry(key, func_res)
    except BaseException as exc:  # pylint: disable=W0703
        print(
            'Function call failed with the following exception:\n{}'.format(
                exc),
            flush=True
        )


def _calc_entry(core, key, func, args, kwds):
    try:
        core.mark_entry_being_calculated(key)
        # _get_executor().submit(core.mark_entry_being_calculated, key)
        func_res = func(*args, **kwds)
        core.set_entry(key, func_res)
        # _get_executor().submit(core.set_entry, key, func_res)
        return func_res
    finally:
        core.mark_entry_not_calculated(key)



def cachier(stale_after=None, next_time=False, pickle_reload=True,
            mongetter=None):
    """A persistent, stale-free memoization decorator.

    The positional and keyword arguments to the wrapped function must be
    hashable (i.e. Python's immutable built-in objects, not mutable
    containers). Also, notice that since objects which are instances of
    user-defined classes are hashable but all compare unequal (their hash
    value is their id), equal objects across different sessions will not yield
    identical keys.

    Arguments
    ---------
    stale_after (optional) : datetime.timedelta
        The time delta afterwhich a cached result is considered stale. Calls
        made after the result goes stale will trigger a recalculation of the
        result, but whether a stale or fresh result will be returned is
        determined by the optional next_time argument.
    next_time (optional) : bool
        If set to True, a stale result will be returned when finding one, not
        waiting for the calculation of the fresh result to return. Defaults to
        False.
    pickle_reload (optional) : bool
        If set to True, in-memory cache will be reloaded on each cache read,
        enabling different threads to share cache. Should be set to False for
        faster reads in single-read programs. Defaults to True.
    mongetter (optional) : callable
        A callable that takes no arguments and returns a pymongo.Collection
        object with writing permissions. If unset a local pickle cache is used
        instead.
    """
    # print('Inside the wrapper maker')
    # print('mongetter={}'.format(mongetter))
    # print('stale_after={}'.format(stale_after))
    # print('next_time={}'.format(next_time))

    if mongetter:
        core = _MongoCore(mongetter, stale_after, next_time)
    else:
        core = _PickleCore(  # pylint: disable=R0204
            stale_after, next_time, pickle_reload)

    def _cachier_decorator(func):
        core.set_func(func)

        @wraps(func)
        def func_wrapper(
                *args,
                overwrite_cache=False,
                ignore_cache=False,
                verbose_cache=False,
                **kwds):  # pylint: disable=C0111,R0911
            # print('Inside general wrapper for {}.'.format(func.__name__))
            if ignore_cache:
                return func(*args, **kwds)
            key, entry = core.get_entry(args, kwds)
            if overwrite_cache:
                return _calc_entry(core, key, func, args, kwds)
            if entry is not None:  # pylint: disable=R0101
                if verbose_cache:
                    print('Entry found.')
                if entry.get('value', None) is not None:
                    if verbose_cache:
                        print('Cached result found.')
                    if stale_after:
                        now = datetime.datetime.now()
                        if now - entry['time'] > stale_after:
                            if verbose_cache:
                                print('But it is stale... :(')
                            if entry['being_calculated']:
                                if next_time:
                                    if verbose_cache:
                                        print('Returning stale.')
                                    return entry['value']  # return stale val
                                if verbose_cache:
                                    print('Already calc. Waiting on change.')
                                return core.wait_on_entry_calc(key)
                            if next_time:
                                if verbose_cache:
                                    print('Async calc and return stale')
                                try:
                                    core.mark_entry_being_calculated(key)
                                    _get_executor().submit(
                                        _function_thread, core, key, func,
                                        args, kwds)
                                finally:
                                    core.mark_entry_not_calculated(key)
                                return entry['value']
                            if verbose_cache:
                                print('Calling decorated function and waiting')
                            return _calc_entry(core, key, func, args, kwds)
                    if verbose_cache:
                        print('And it is fresh!')
                    return entry['value']
                if entry['being_calculated']:
                    if verbose_cache:
                        print('No value but being calculated. Waiting.')
                    return core.wait_on_entry_calc(key)
            if verbose_cache:
                print('No entry found. No current calc. Calling like a boss.')
            return _calc_entry(core, key, func, args, kwds)

        def clear_cache():
            """Clear the cache."""
            core.clear_cache()

        def clear_being_calculated():
            """Marks all entries in this cache as not being calculated."""
            core.clear_being_calculated()

        func_wrapper.clear_cache = clear_cache
        func_wrapper.clear_being_calculated = clear_being_calculated
        return func_wrapper

    return _cachier_decorator
