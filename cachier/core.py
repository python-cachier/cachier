"""Persistent, stale-free memoization decorators for Python."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

# Used a little code from Andrew Barnert's <abarnert at yahoo.com>
# persistent-lru-cache, which can be found at
# https://github.com/abarnert/persistent-lru-cache

import os
from functools import wraps
import pickle  # for local caching
import datetime
import abc  # for the _BaseCore abstract base class
import concurrent.futures  # for asynchronous file uploads
from bson.binary import Binary  # to save binary data to mongodb


CACHIER_DIR = '~/.cachier/'
EXPANDED_CACHIER_DIR = os.path.expanduser(CACHIER_DIR)
DEFAULT_MAX_WORKERS = 5


# === Cores definitions ===

class _BaseCore(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, mongetter, stale_after, next_time):
        self.mongetter = mongetter
        self.stale_after = stale_after
        self.next_time = next_time
        self.func = None

    def set_func(self, func):
        """Sets the function this core will use. This has to be set before
        any method is called"""
        self.func = func

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


class _MongoCore(_BaseCore):

    def __init__(self, mongetter, stale_after, next_time):
        super().__init__(mongetter, stale_after, next_time)
        self.mongo_collection = None

    @staticmethod
    def _get_func_str(func):
        return '.{}.{}'.format(func.__module__, func.__name__)

    def _get_mongo_collection(self):
        if not self.mongo_collection:
            self.mongo_collection = self.mongetter()
        return self.mongo_collection

    def get_entry(self, args, kwds):
        key = pickle.dumps(args + tuple(sorted(kwds.items())))
        print('key type={}, key={}'.format(
            type(key), key))
        res = self._get_mongo_collection().find_one({
            'func': _MongoCore._get_func_str(self.func),
            'key': key
        })
        if res:
            entry = {
                'value': pickle.loads(res['value']),
                'time': res['time'],
                'stale': res['stale'],
                'being_calculated': res['being_calculated']
            }
            return key, entry
        return key, None

    def set_entry(self, key, func_res):
        thebytes = pickle.dumps(func_res)
        self._get_mongo_collection().insert_one({
            'func': _MongoCore._get_func_str(self.func),
            'key': key,
            'value': Binary(thebytes),
            'time': datetime.datetime.now(),
            'stale': False,
            'being_calculated': False
        })

    def mark_entry_being_calculated(self, key):
        self._get_mongo_collection().update(
            {
                'func': _MongoCore._get_func_str(self.func),
                'key': key
            },
            {
                'being_calculated': False
            }
        )


class _PickleCore(_BaseCore):

    def __init__(self, mongetter, stale_after, next_time):
        super().__init__(mongetter, stale_after, next_time)
        self.cache = None

    def _get_cache_path(self):
        if not os.path.exists(EXPANDED_CACHIER_DIR):
            os.makedirs(EXPANDED_CACHIER_DIR)
        fname = '.{}.{}'.format(
            self.func.__module__, self.func.__name__)  # pylint: disable=W0212
        fpath = os.path.abspath(os.path.join(
            os.path.realpath(EXPANDED_CACHIER_DIR), fname))
        return fpath

    def _get_cache(self):
        if not self.cache:
            fpath = self._get_cache_path()
            try:
                self.cache = pickle.load(open(fpath, 'rb'))
            except FileNotFoundError:
                self.cache = {}
        return self.cache

    def _save_cache(self, cache):
        self.cache = cache
        fpath = self._get_cache_path()
        pickle.dump(cache, open(fpath, 'wb'))

    def get_entry(self, args, kwds):
        key = args + tuple(sorted(kwds.items()))
        print('key type={}, key={}'.format(type(key), key))
        cache = self._get_cache()
        return key, cache.get(key, None)

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
        cache[key]['being_calculated'] = True
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
            'Function call failed with following exception:\n{}'.format(exc),
            flush=True
        )


def cachier(mongetter=None, stale_after=None, next_time=True):
    """A persistent, stale-free memoization decorator.

    When using a MongoDB-backed caching, the positional and keyword arguments
    to the wrapped function must be hashable (i.e. Python's immutable built-in
    objects, not mutable containers). Also, notice that since objects which
    are instances of user-defined classes are hashable but all compare unequal
    (their hash value is their id), equal objects across different sessions
    will not yield identical keys.

    Arguments
    ---------
    mongetter (optional) : callable
        A callable that takes no arguments and returns a pymongo.Collection
        object with writing permissions. If unset a local pickle cache is used
        instead.
    stale_after (optional) : datetime.timedelta
        The time delta afterwhich a cached result is considered stale. Calls
        made after the result goes stale will trigger a recalculation of the
        result, but whether a stale or fresh result will be returned is
        determined by the optional next_time argument.
    next_time (optional) : bool
        If set to True, a stale result will be returned when finding one, not
        waiting for the calculation of the fresh result to return. Defaults to
        True.
    """
    print('Inside the wrapper maker')
    print('mongetter={}'.format(mongetter))
    print('stale_after={}'.format(stale_after))
    print('next_time={}'.format(next_time))

    if mongetter:
        core = _MongoCore(mongetter, stale_after, next_time)
    else:
        core = _PickleCore(  # pylint: disable=R0204
            mongetter, stale_after, next_time)

    def _cachier_decorator(func):
        core.set_func(func)

        @wraps(func)
        def func_wrapper(*args, **kwds):  # pylint: disable=C0111
            print('Inside general wrapper for {}.'.format(func.__name__))
            key, entry = core.get_entry(args, kwds)
            if entry:
                print('Cached result found.')
                if stale_after:
                    now = datetime.datetime.now()
                    if now - entry['time'] > stale_after:
                        if next_time:
                            if entry['being_calculated']:
                                return entry['value']
                            # trigger async calculation and return stale
                            core.mark_entry_being_calculated(key)
                            _get_executor().submit(
                                _function_thread, core, key, func, args, kwds)
                            return entry['value']
                        print('Calling decorated function and waiting')
                        func_res = func(*args, **kwds)
                        core.set_entry(key, func_res)
                        return func_res
                return entry['value']
            func_res = func(*args, **kwds)
            core.set_entry(key, func_res)
            return func_res
        return func_wrapper

    return _cachier_decorator
