"""Persistent, stale-free memoization decorators for Python."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import inspect
import os
import warnings
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Optional, Union
from warnings import warn

from ._types import RedisClient
from .config import (
    Backend,
    HashFunc,
    Mongetter,
    _update_with_defaults,
)
from .cores.base import RecalculationNeeded, _BaseCore
from .cores.memory import _MemoryCore
from .cores.mongo import _MongoCore
from .cores.pickle import _PickleCore
from .cores.redis import _RedisCore
from .cores.sql import _SQLCore

MAX_WORKERS_ENVAR_NAME = "CACHIER_MAX_WORKERS"
DEFAULT_MAX_WORKERS = 8
ZERO_TIMEDELTA = timedelta(seconds=0)


def _max_workers():
    return int(os.environ.get(MAX_WORKERS_ENVAR_NAME, DEFAULT_MAX_WORKERS))


def _set_max_workers(max_workers):
    os.environ[MAX_WORKERS_ENVAR_NAME] = str(max_workers)
    _get_executor(True)


def _get_executor(reset=False):
    if reset or not hasattr(_get_executor, "executor"):
        _get_executor.executor = ThreadPoolExecutor(_max_workers())
    return _get_executor.executor


def _function_thread(core, key, func, args, kwds):
    try:
        func_res = func(*args, **kwds)
        core.set_entry(key, func_res)
    except BaseException as exc:
        print(f"Function call failed with the following exception:\n{exc}")


def _calc_entry(core, key, func, args, kwds) -> Optional[Any]:
    core.mark_entry_being_calculated(key)
    try:
        func_res = func(*args, **kwds)
        core.set_entry(key, func_res)
        return func_res
    finally:
        core.mark_entry_not_calculated(key)


def _convert_args_kwargs(
    func, _is_method: bool, args: tuple, kwds: dict
) -> dict:
    """Convert mix of positional and keyword arguments to aggregated kwargs."""
    # unwrap if the function is functools.partial
    if hasattr(func, "func"):
        args = func.args + args
        kwds.update({k: v for k, v in func.keywords.items() if k not in kwds})
        func = func.func
    func_params = list(inspect.signature(func).parameters)
    args_as_kw = dict(
        zip(func_params[1:], args[1:])
        if _is_method
        else zip(func_params, args)
    )
    # init with default values
    kwargs = {
        k: v.default
        for k, v in inspect.signature(func).parameters.items()
        if v.default is not inspect.Parameter.empty
    }
    # merge args expanded as kwargs and the original kwds
    kwargs.update(dict(**args_as_kw, **kwds))
    return OrderedDict(sorted(kwargs.items()))


def _pop_kwds_with_deprecation(kwds, name: str, default_value: bool):
    if name in kwds:
        warnings.warn(
            f"`{name}` is deprecated and will be removed in a future release,"
            " use `cachier__` alternative instead.",
            DeprecationWarning,
            stacklevel=2,
        )
    return kwds.pop(name, default_value)


def cachier(
    hash_func: Optional[HashFunc] = None,
    hash_params: Optional[HashFunc] = None,
    backend: Optional[Backend] = None,
    mongetter: Optional[Mongetter] = None,
    sql_engine: Optional[Union[str, Any, Callable[[], Any]]] = None,
    redis_client: Optional["RedisClient"] = None,
    stale_after: Optional[timedelta] = None,
    next_time: Optional[bool] = None,
    cache_dir: Optional[Union[str, os.PathLike]] = None,
    pickle_reload: Optional[bool] = None,
    separate_files: Optional[bool] = None,
    wait_for_calc_timeout: Optional[int] = None,
    allow_none: Optional[bool] = None,
):
    """Wrap as a persistent, stale-free memoization decorator.

    The positional and keyword arguments to the wrapped function must be
    hashable (i.e. Python's immutable built-in objects, not mutable
    containers). Also, notice that since objects which are instances of
    user-defined classes are hashable but all compare unequal (their hash
    value is their id), equal objects across different sessions will not yield
    identical keys.

    Arguments:
    ---------
    hash_func : callable, optional
        A callable that gets the args and kwargs from the decorated function
        and returns a hash key for them. This parameter can be used to enable
        the use of cachier with functions that get arguments that are not
        automatically hashable by Python.
    hash_params : callable, optional
    backend : str, optional
        The name of the backend to use. Valid options currently include
        'pickle', 'mongo', 'memory', 'sql', and 'redis'. If not provided,
        defaults to 'pickle', unless a core-associated parameter is provided

    mongetter : callable, optional
        A callable that takes no arguments and returns a pymongo.Collection
        object with writing permissions. If unset a local pickle cache is used
        instead.
    sql_engine : str, Engine, or callable, optional
        SQLAlchemy connection string, Engine, or callable returning an Engine.
        Used for the SQL backend.
    redis_client : redis.Redis or callable, optional
        Redis client instance or callable returning a Redis client.
        Used for the Redis backend.
    stale_after : datetime.timedelta, optional
        The time delta after which a cached result is considered stale. Calls
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
        message = (
            "hash_params will be removed in a future release, "
            "please use hash_func instead"
        )
        warn(message, DeprecationWarning, stacklevel=2)
        hash_func = hash_params
    # Update parameters with defaults if input is None
    backend = _update_with_defaults(backend, "backend")
    mongetter = _update_with_defaults(mongetter, "mongetter")
    # Override the backend parameter if a mongetter is provided.
    if callable(mongetter):
        backend = "mongo"
    core: _BaseCore
    if backend == "pickle":
        core = _PickleCore(
            hash_func=hash_func,
            pickle_reload=pickle_reload,
            cache_dir=cache_dir,
            separate_files=separate_files,
            wait_for_calc_timeout=wait_for_calc_timeout,
        )
    elif backend == "mongo":
        core = _MongoCore(
            hash_func=hash_func,
            mongetter=mongetter,
            wait_for_calc_timeout=wait_for_calc_timeout,
        )
    elif backend == "memory":
        core = _MemoryCore(
            hash_func=hash_func, wait_for_calc_timeout=wait_for_calc_timeout
        )
    elif backend == "sql":
        core = _SQLCore(
            hash_func=hash_func,
            sql_engine=sql_engine,
            wait_for_calc_timeout=wait_for_calc_timeout,
        )
    elif backend == "redis":
        core = _RedisCore(
            hash_func=hash_func,
            redis_client=redis_client,
            wait_for_calc_timeout=wait_for_calc_timeout,
        )
    else:
        raise ValueError("specified an invalid core: %s" % backend)

    def _cachier_decorator(func):
        core.set_func(func)

        # ---
        # MAINTAINER NOTE: max_age parameter
        #
        # The _call function below supports a per-call 'max_age' parameter,
        # allowing users to specify a maximum allowed age for a cached value.
        # If the cached value is older than 'max_age',
        # a recalculation is triggered. This is in addition to the
        # per-decorator 'stale_after' parameter.
        #
        # The effective staleness threshold is the minimum of 'stale_after'
        # and 'max_age' (if provided).
        # This ensures that the strictest max age requirement is enforced.
        #
        # The main function wrapper is a standard function that passes
        # *args and **kwargs to _call. By default, max_age is None,
        # so only 'stale_after' is considered unless overridden.
        #
        # The user-facing API exposes:
        #   - Per-call: myfunc(..., max_age=timedelta(...))
        #
        # This design allows both one-off (per-call) and default
        # (per-decorator) max age constraints.
        # ---

        def _call(*args, max_age: Optional[timedelta] = None, **kwds):
            nonlocal allow_none
            _allow_none = _update_with_defaults(allow_none, "allow_none", kwds)
            # print('Inside general wrapper for {}.'.format(func.__name__))
            ignore_cache = _pop_kwds_with_deprecation(
                kwds, "ignore_cache", False
            )
            overwrite_cache = _pop_kwds_with_deprecation(
                kwds, "overwrite_cache", False
            )
            verbose = _pop_kwds_with_deprecation(kwds, "verbose_cache", False)
            ignore_cache = kwds.pop("cachier__skip_cache", ignore_cache)
            overwrite_cache = kwds.pop(
                "cachier__overwrite_cache", overwrite_cache
            )
            verbose = kwds.pop("cachier__verbose", verbose)
            _stale_after = _update_with_defaults(
                stale_after, "stale_after", kwds
            )
            _next_time = _update_with_defaults(next_time, "next_time", kwds)
            # merge args expanded as kwargs and the original kwds
            kwargs = _convert_args_kwargs(
                func, _is_method=core.func_is_method, args=args, kwds=kwds
            )

            _print = print if verbose else lambda x: None

            # Check current global caching state dynamically
            from .config import _global_params

            if ignore_cache or not _global_params.caching_enabled:
                return (
                    func(args[0], **kwargs)
                    if core.func_is_method
                    else func(**kwargs)
                )
            key, entry = core.get_entry((), kwargs)
            if overwrite_cache:
                return _calc_entry(core, key, func, args, kwds)
            if entry is None or (
                not entry._completed and not entry._processing
            ):
                _print("No entry found. No current calc. Calling like a boss.")
                return _calc_entry(core, key, func, args, kwds)
            _print("Entry found.")
            if _allow_none or entry.value is not None:
                _print("Cached result found.")
                now = datetime.now()
                max_allowed_age = _stale_after
                nonneg_max_age = True
                if max_age is not None:
                    if max_age < ZERO_TIMEDELTA:
                        _print(
                            "max_age is negative. "
                            "Cached result considered stale."
                        )
                        nonneg_max_age = False
                    else:
                        max_allowed_age = (
                            min(_stale_after, max_age)
                            if max_age is not None
                            else _stale_after
                        )
                # note: if max_age < 0, we always consider a value stale
                if nonneg_max_age and (now - entry.time <= max_allowed_age):
                    _print("And it is fresh!")
                    return entry.value
                _print("But it is stale... :(")
                if entry._processing:
                    if _next_time:
                        _print("Returning stale.")
                        return entry.value  # return stale val
                    _print("Already calc. Waiting on change.")
                    try:
                        return core.wait_on_entry_calc(key)
                    except RecalculationNeeded:
                        return _calc_entry(core, key, func, args, kwds)
                if _next_time:
                    _print("Async calc and return stale")
                    core.mark_entry_being_calculated(key)
                    try:
                        _get_executor().submit(
                            _function_thread, core, key, func, args, kwds
                        )
                    finally:
                        core.mark_entry_not_calculated(key)
                    return entry.value
                _print("Calling decorated function and waiting")
                return _calc_entry(core, key, func, args, kwds)
            if entry._processing:
                _print("No value but being calculated. Waiting.")
                try:
                    return core.wait_on_entry_calc(key)
                except RecalculationNeeded:
                    return _calc_entry(core, key, func, args, kwds)
            _print("No entry found. No current calc. Calling like a boss.")
            return _calc_entry(core, key, func, args, kwds)

        # MAINTAINER NOTE: The main function wrapper is now a standard function
        # that passes *args and **kwargs to _call. This ensures that user
        # arguments are not shifted, and max_age is only settable via keyword
        # argument.
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            return _call(*args, **kwargs)

        def _clear_cache():
            """Clear the cache."""
            core.clear_cache()

        def _clear_being_calculated():
            """Mark all entries in this cache as not being calculated."""
            core.clear_being_calculated()

        def _cache_dpath():
            """Return the path to the cache dir, if exists; None if not."""
            return getattr(core, "cache_dir", None)

        def _precache_value(*args, value_to_cache, **kwds):  # noqa: D417
            """Add an initial value to the cache.

            Arguments:
            ---------
            value_to_cache : any
                entry to be written into the cache

            """
            # merge args expanded as kwargs and the original kwds
            kwargs = _convert_args_kwargs(
                func, _is_method=core.func_is_method, args=args, kwds=kwds
            )
            return core.precache_value((), kwargs, value_to_cache)

        func_wrapper.clear_cache = _clear_cache
        func_wrapper.clear_being_calculated = _clear_being_calculated
        func_wrapper.cache_dpath = _cache_dpath
        func_wrapper.precache_value = _precache_value
        return func_wrapper

    return _cachier_decorator
