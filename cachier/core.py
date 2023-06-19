"""Persistent, stale-free memoization decorators for Python."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

# python 2 compatibility
from __future__ import absolute_import, division, print_function

import datetime
import functools
import hashlib
import os
import pickle
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Callable, Literal, Optional, TypedDict, Union
from warnings import warn

from pymongo.collection import Collection

from .base_core import RecalculationNeeded, _BaseCore
from .memory_core import _MemoryCore
from .mongo_core import _MongoCore
from .pickle_core import _PickleCore

MAX_WORKERS_ENVAR_NAME = 'CACHIER_MAX_WORKERS'
DEFAULT_MAX_WORKERS = 8


def _max_workers():
    try:
        return int(os.environ[MAX_WORKERS_ENVAR_NAME])
    except KeyError:
        os.environ[MAX_WORKERS_ENVAR_NAME] = str(DEFAULT_MAX_WORKERS)
        return DEFAULT_MAX_WORKERS


def _set_max_workers(max_workers):
    os.environ[MAX_WORKERS_ENVAR_NAME] = str(max_workers)
    _get_executor(True)


def _get_executor(reset=False):
    if reset:
        _get_executor.executor = ThreadPoolExecutor(_max_workers())
    try:
        return _get_executor.executor
    except AttributeError:
        _get_executor.executor = ThreadPoolExecutor(_max_workers())
        return _get_executor.executor


def _function_thread(core, key, func, args, kwds):
    try:
        func_res = func(*args, **kwds)
        core.set_entry(key, func_res)
    except BaseException as exc:  # pylint: disable=W0703
        print(
            'Function call failed with the following exception:\n{}'.format(
                exc
            )
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


def _default_hash_func(args, kwds):
    # pylint: disable-next=protected-access
    key = functools._make_key(args, kwds, typed=True)
    hash = hashlib.sha256()
    for item in key:
        hash.update(pickle.dumps(item))
    return hash.hexdigest()


class MissingMongetter(ValueError):
    """Thrown when the mongetter keyword argument is missing."""


HashFunc = Callable[..., str]
Mongetter = Callable[[], Collection]
Backend = Literal["pickle", "mongo", "memory"]


class Params(TypedDict):
    caching_enabled: bool
    hash_func: HashFunc
    backend: Backend
    mongetter: Optional[Mongetter]
    stale_after: datetime.timedelta
    next_time: bool
    cache_dir: Union[str, os.PathLike]
    pickle_reload: bool
    separate_files: bool
    wait_for_calc_timeout: int
    allow_none: bool


_default_params: Params = {
    'caching_enabled': True,
    'hash_func': _default_hash_func,
    'backend': 'pickle',
    'mongetter': None,
    'stale_after': datetime.timedelta.max,
    'next_time': False,
    'cache_dir': '~/.cachier/',
    'pickle_reload': True,
    'separate_files': False,
    'wait_for_calc_timeout': 0,
    'allow_none': False,
}


def cachier(
    hash_func: Optional[HashFunc] = None,
    hash_params: Optional[HashFunc] = None,
    backend: Optional[Backend] = None,
    mongetter: Optional[Mongetter] = None,
    stale_after: Optional[datetime.timedelta] = None,
    next_time: Optional[bool] = None,
    cache_dir: Optional[Union[str, os.PathLike]] = None,
    pickle_reload: Optional[bool] = None,
    separate_files: Optional[bool] = None,
    wait_for_calc_timeout: Optional[int] = None,
    allow_none: Optional[bool] = None,
):
    """A persistent, stale-free memoization decorator.

    The positional and keyword arguments to the wrapped function must be
    hashable (i.e. Python's immutable built-in objects, not mutable
    containers). Also, notice that since objects which are instances of
    user-defined classes are hashable but all compare unequal (their hash
    value is their id), equal objects across different sessions will not yield
    identical keys.

    Arguments
    ---------
    hash_func : callable, optional
        A callable that gets the args and kwargs from the decorated function
        and returns a hash key for them. This parameter can be used to enable
        the use of cachier with functions that get arguments that are not
        automatically hashable by Python.
    backend : str, optional
        The name of the backend to use. Valid options currently include
        'pickle', 'mongo' and 'memory'. If not provided, defaults to
        'pickle' unless the 'mongetter' argument is passed, in which
        case the mongo backend is automatically selected.
    mongetter : callable, optional
        A callable that takes no arguments and returns a pymongo.Collection
        object with writing permissions. If unset a local pickle cache is used
        instead.
    stale_after : datetime.timedelta, optional
        The time delta afterwhich a cached result is considered stale. Calls
        made after the result goes stale will trigger a recalculation of the
        result, but whether a stale or fresh result will be returned is
        determined by the optional next_time argument.
    next_time : bool, optional
        If set to True, a stale result will be returned when finding one, not
        waiting for the calculation of the fresh result to return. Defaults to
        False.
    cache_dir : str, optional
        A fully qualified path to a file directory to be used for cache files.
        The running process must have running permissions to this folder. If
        not provided, a default directory at `~/.cachier/` is used.
    pickle_reload : bool, optional
        If set to True, in-memory cache will be reloaded on each cache read,
        enabling different threads to share cache. Should be set to False for
        faster reads in single-thread programs. Defaults to True.
    separate_files: bool, default False, for Pickle cores only
        Instead of a single cache file per-function, each function's cache is
        split between several files, one for each argument set. This can help
        if you per-function cache files become too large.
    wait_for_calc_timeout: int, optional, for MongoDB only
        The maximum time to wait for an ongoing calculation. When a
        process started to calculate the value setting being_calculated to
        True, any process trying to read the same entry will wait a maximum of
        seconds specified in this parameter. 0 means wait forever.
        Once the timeout expires the calculation will be triggered.
    allow_none: bool, optional
        Allows storing None values in the cache. If False, functions returning
        None will not be cached and are recalculated every call.
    """
    # Check for deprecated parameters
    if hash_params is not None:
        message = 'hash_params will be removed in a future release, ' \
                  'please use hash_func instead'
        warn(message, DeprecationWarning, stacklevel=2)
        hash_func = hash_params
    # Override the backend parameter if a mongetter is provided.
    if mongetter is None:
        mongetter = _default_params['mongetter']
    if callable(mongetter):
        backend = 'mongo'
    if backend is None:
        backend = _default_params['backend']
    core: _BaseCore
    if backend == 'pickle':
        core = _PickleCore(  # pylint: disable=R0204
            hash_func=hash_func,
            pickle_reload=pickle_reload,
            cache_dir=cache_dir,
            separate_files=separate_files,
            wait_for_calc_timeout=wait_for_calc_timeout,
            default_params=_default_params,
        )
    elif backend == 'mongo':
        if mongetter is None:
            raise MissingMongetter(
                'must specify ``mongetter`` when using the mongo core')
        core = _MongoCore(
            mongetter=mongetter,
            hash_func=hash_func,
            wait_for_calc_timeout=wait_for_calc_timeout,
            default_params=_default_params,
        )
    elif backend == 'memory':
        core = _MemoryCore(
            hash_func=hash_func,
            default_params=_default_params,
        )
    else:
        raise ValueError('specified an invalid core: {}'.format(backend))

    def _cachier_decorator(func):
        core.set_func(func)

        @wraps(func)
        def func_wrapper(*args, **kwds):  # pylint: disable=C0111,R0911
            nonlocal allow_none
            # print('Inside general wrapper for {}.'.format(func.__name__))
            ignore_cache = kwds.pop('ignore_cache', False)
            overwrite_cache = kwds.pop('overwrite_cache', False)
            verbose_cache = kwds.pop('verbose_cache', False)
            _print = lambda x: None  # skipcq: FLK-E731  # noqa: E731
            if verbose_cache:
                _print = print
            if ignore_cache or not _default_params['caching_enabled']:
                return func(*args, **kwds)
            if core.func_is_method:
                key, entry = core.get_entry(args[1:], kwds)
            else:
                key, entry = core.get_entry(args, kwds)
            if overwrite_cache:
                return _calc_entry(core, key, func, args, kwds)
            if entry is not None:  # pylint: disable=R0101
                _print('Entry found.')
                if (
                    (allow_none if allow_none is not None else _default_params['allow_none'])
                    or entry.get('value', None) is not None
                ):
                    _print('Cached result found.')
                    local_stale_after = stale_after if stale_after is not None else _default_params['stale_after']  # noqa: E501
                    local_next_time = next_time if next_time is not None else _default_params['next_time']  # noqa: E501
                    now = datetime.datetime.now()
                    if now - entry['time'] > local_stale_after:
                        _print('But it is stale... :(')
                        if entry['being_calculated']:
                            if local_next_time:
                                _print('Returning stale.')
                                return entry['value']  # return stale val
                            _print('Already calc. Waiting on change.')
                            try:
                                return core.wait_on_entry_calc(key)
                            except RecalculationNeeded:
                                return _calc_entry(
                                    core, key, func, args, kwds
                                )
                        if local_next_time:
                            _print('Async calc and return stale')
                            try:
                                core.mark_entry_being_calculated(key)
                                _get_executor().submit(
                                    _function_thread,
                                    core,
                                    key,
                                    func,
                                    args,
                                    kwds,
                                )
                            finally:
                                core.mark_entry_not_calculated(key)
                            return entry['value']
                        _print('Calling decorated function and waiting')
                        return _calc_entry(core, key, func, args, kwds)
                    _print('And it is fresh!')
                    return entry['value']
                if entry['being_calculated']:
                    _print('No value but being calculated. Waiting.')
                    try:
                        return core.wait_on_entry_calc(key)
                    except RecalculationNeeded:
                        return _calc_entry(core, key, func, args, kwds)
            _print('No entry found. No current calc. Calling like a boss.')
            return _calc_entry(core, key, func, args, kwds)

        def clear_cache():
            """Clear the cache."""
            core.clear_cache()

        def clear_being_calculated():
            """Marks all entries in this cache as not being calculated."""
            core.clear_being_calculated()

        def cache_dpath():
            """Returns the path to the cache dir, if exists; None if not."""
            try:
                return core.cache_dir
            except AttributeError:
                return None

        def precache_value(*args, value_to_cache, **kwds):
            """Add an initial value to the cache.

            Arguments
            ---------
            value : any
                entry to be written into the cache
            """
            return core.precache_value(args, kwds, value_to_cache)

        func_wrapper.clear_cache = clear_cache
        func_wrapper.clear_being_calculated = clear_being_calculated
        func_wrapper.cache_dpath = cache_dpath
        func_wrapper.precache_value = precache_value
        return func_wrapper

    return _cachier_decorator


def set_default_params(**params):
    """Configure global parameters applicable to all memoized functions.

    This function takes the same keyword parameters as the ones defined
    in the decorator, which can be passed all at once or with multiple
    calls. Parameters given directly to a decorator take precedence over
    any values set by this function.

    Only 'stale_after', 'next_time', and 'wait_for_calc_timeout' can be
    changed after the memoization decorator has been applied. Other parameters
    will only have an effect on decorators applied after this function is run.
    """
    valid_params = (p for p in params.items() if p[0] in _default_params)
    _default_params.update(valid_params)


def get_default_params():
    """Get current set of default parameters."""
    return _default_params


def enable_caching():
    """Enable caching globally."""
    _default_params['caching_enabled'] = True


def disable_caching():
    """Disable caching globally."""
    _default_params['caching_enabled'] = False
