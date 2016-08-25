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
import pickle
import datetime
import abc

from bson.binary import Binary


CACHIER_DIR = '~/.cachier/'
EXPANDED_CACHIER_DIR = os.path.expanduser(CACHIER_DIR)


class _BaseCore(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, mongetter, stale_after, next_time):
        self.mongetter = mongetter
        self.stale_after = stale_after
        self.next_time = next_time
        self.func = None

    def set_func(self, func):
        """Sets the function this core will use."""
        self.func = func

    @abc.abstractmethod
    def get_entry(self, args, kwds):
        """Returns the result mapped to the given arguments in this core's
        cache, if such a mapping exists."""

    @abc.abstractmethod
    def set_entry(self, key, func_res):
        """Maps the given result to the given key in this core's cache."""


class _MongoCore(_BaseCore):

    def __init__(self, mongetter, stale_after, next_time):
        super().__init__(self, mongetter, stale_after, next_time)
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
                'stale': res['stale']
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
            'stale': False
        })


class _PickleCore(_BaseCore):

    def __init__(self, mongetter, stale_after, next_time):
        super().__init__(self, mongetter, stale_after, next_time)
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
            'stale': False
        }
        self._save_cache(cache)


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
                            # trigger calculation in another thread and
                            # return stale result
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

    # else:

    #     def _cachier_pickle_decorator(func):
    #         @wraps(func)
    #         def func_wrapper(*args, **kwds):  # pylint: disable=C0111
    #             print('Inside pickle wrapper for {}.'.format(func.__name__))
    #             key = args + tuple(sorted(kwds.items()))
    #             print('key type={}, key={}'.format(type(key), key))

    #             cache = _get_cache(func)
    #             if key in cache:
    #                 print('Cached result found.')
    #                 return cache[key]['value']

    #             print('Calling decorated function')
    #             func_res = func(*args, **kwds)
    #             cache[key] = {
    #                 'value': func_res,
    #                 'time': datetime.datetime.now(),
    #                 'stale': False
    #             }
    #             _save_cache(cache, func)
    #             return func_res

    #         return func_wrapper

    # return _cachier_pickle_decorator
