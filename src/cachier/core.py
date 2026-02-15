"""Persistent, stale-free memoization decorators for Python."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import asyncio
import inspect
import os
import threading
import warnings
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Optional, Union
from warnings import warn

from ._types import RedisClient
from .config import Backend, HashFunc, Mongetter, _update_with_defaults
from .cores.base import RecalculationNeeded, _BaseCore
from .cores.memory import _MemoryCore
from .cores.mongo import _MongoCore
from .cores.pickle import _PickleCore
from .cores.redis import _RedisCore
from .cores.sql import _SQLCore
from .util import parse_bytes

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


def _function_thread(core: _BaseCore, key, func, args, kwds):
    try:
        func_res = func(*args, **kwds)
        core.set_entry(key, func_res)
    except BaseException as exc:
        print(f"Function call failed with the following exception:\n{exc}")


async def _function_thread_async(core: _BaseCore, key, func, args, kwds):
    try:
        func_res = await func(*args, **kwds)
        await core.aset_entry(key, func_res)
    except BaseException as exc:
        print(f"Function call failed with the following exception:\n{exc}")


def _calc_entry(core: _BaseCore, key, func, args, kwds, printer=lambda *_: None) -> Optional[Any]:
    core.mark_entry_being_calculated(key)
    try:
        func_res = func(*args, **kwds)
        stored = core.set_entry(key, func_res)
        if not stored:
            printer("Result exceeds entry_size_limit; not cached")
        return func_res
    finally:
        core.mark_entry_not_calculated(key)


async def _calc_entry_async(core: _BaseCore, key, func, args, kwds, printer=lambda *_: None) -> Optional[Any]:
    await core.amark_entry_being_calculated(key)
    try:
        func_res = await func(*args, **kwds)
        stored = await core.aset_entry(key, func_res)
        if not stored:
            printer("Result exceeds entry_size_limit; not cached")
        return func_res
    finally:
        await core.amark_entry_not_calculated(key)


def _convert_args_kwargs(func, _is_method: bool, args: tuple, kwds: dict) -> dict:
    """Convert mix of positional and keyword arguments to aggregated kwargs."""
    # unwrap if the function is functools.partial
    if hasattr(func, "func"):
        args = func.args + args
        kwds.update({k: v for k, v in func.keywords.items() if k not in kwds})
        func = func.func

    sig = inspect.signature(func)
    func_params = list(sig.parameters)

    # Separate regular parameters from VAR_POSITIONAL
    regular_params = []
    var_positional_name = None

    for param_name in func_params:
        param = sig.parameters[param_name]
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            var_positional_name = param_name
        elif param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            regular_params.append(param_name)

    # Map positional arguments to regular parameters
    if _is_method:
        # Skip 'self' for methods
        args_to_map = args[1:]
        params_to_use = regular_params[1:]
    else:
        args_to_map = args
        params_to_use = regular_params

    # Map as many args as possible to regular parameters
    num_regular = len(params_to_use)
    args_as_kw = dict(zip(params_to_use, args_to_map[:num_regular]))

    # Handle variadic positional arguments
    # Store them with indexed keys like __varargs_0__, __varargs_1__, etc.
    if var_positional_name and len(args_to_map) > num_regular:
        var_args = args_to_map[num_regular:]
        for i, arg in enumerate(var_args):
            args_as_kw[f"__varargs_{i}__"] = arg

    # Init with default values
    kwargs = {k: v.default for k, v in sig.parameters.items() if v.default is not inspect.Parameter.empty}

    # Merge args expanded as kwargs and the original kwds
    kwargs.update(args_as_kw)

    # Handle keyword arguments (including variadic keyword arguments)
    kwargs.update(kwds)

    return OrderedDict(sorted(kwargs.items()))


def _pop_kwds_with_deprecation(kwds, name: str, default_value: bool):
    if name in kwds:
        warnings.warn(
            f"`{name}` is deprecated and will be removed in a future release, use `cachier__` alternative instead.",
            DeprecationWarning,
            stacklevel=2,
        )
    return kwds.pop(name, default_value)


def _is_async_redis_client(client: Any) -> bool:
    if client is None:
        return False
    method_names = ("hgetall", "hset", "keys", "delete", "hget")
    return all(inspect.iscoroutinefunction(getattr(client, name, None)) for name in method_names)


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
    cleanup_stale: Optional[bool] = None,
    cleanup_interval: Optional[timedelta] = None,
    entry_size_limit: Optional[Union[int, str]] = None,
):
    """Wrap as a persistent, stale-free memoization decorator.

    The positional and keyword arguments to the wrapped function must be
    hashable (i.e. Python's immutable built-in objects, not mutable
    containers). Also, notice that since objects which are instances of
    user-defined classes are hashable but all compare unequal (their hash
    value is their id), equal objects across different sessions will not yield
    identical keys.

    Parameters
    ----------
    hash_func : callable, optional
        A callable that gets the args and kwargs from the decorated function
        and returns a hash key for them. This parameter can be used to enable
        the use of cachier with functions that get arguments that are not
        automatically hashable by Python.
    hash_params : callable, optional
        Deprecated, use :func:`~cachier.core.cachier.hash_func` instead.
    backend : str, optional
        The name of the backend to use. Valid options currently include
        'pickle', 'mongo', 'memory', 'sql', and 'redis'. If not provided,
        defaults to 'pickle', unless a core-associated parameter is provided

    mongetter : callable, optional
        A callable that takes no arguments and returns a pymongo.Collection
        object with writing permissions. If provided, the backend is set to
        'mongo'.
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
        if your per-function cache files become too large.
    wait_for_calc_timeout: int, optional
        The maximum time to wait for an ongoing calculation. When a
        process started to calculate the value setting being_calculated to
        True, any process trying to read the same entry will wait a maximum of
        seconds specified in this parameter. 0 means wait forever.
        Once the timeout expires the calculation will be triggered.
    allow_none: bool, optional
        Allows storing None values in the cache. If False, functions returning
        None will not be cached and are recalculated every call.
    cleanup_stale: bool, optional
        If True, stale cache entries are periodically deleted in a background
        thread. Defaults to False.
    cleanup_interval: datetime.timedelta, optional
        Minimum time between automatic cleanup runs. Defaults to one day.
    entry_size_limit: int or str, optional
        Maximum serialized size of a cached value. Values exceeding the limit
        are returned but not cached. Human readable strings like ``"10MB"`` are
        allowed.

    """
    # Check for deprecated parameters
    if hash_params is not None:
        message = "hash_params will be removed in a future release, please use hash_func instead"
        warn(message, DeprecationWarning, stacklevel=2)
        hash_func = hash_params
    # Update parameters with defaults if input is None
    backend = _update_with_defaults(backend, "backend")
    mongetter = _update_with_defaults(mongetter, "mongetter")
    size_limit_bytes = parse_bytes(_update_with_defaults(entry_size_limit, "entry_size_limit"))
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
            entry_size_limit=size_limit_bytes,
        )
    elif backend == "mongo":
        core = _MongoCore(
            hash_func=hash_func,
            mongetter=mongetter,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=size_limit_bytes,
        )
    elif backend == "memory":
        core = _MemoryCore(
            hash_func=hash_func, wait_for_calc_timeout=wait_for_calc_timeout, entry_size_limit=size_limit_bytes
        )
    elif backend == "sql":
        core = _SQLCore(
            hash_func=hash_func,
            sql_engine=sql_engine,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=size_limit_bytes,
        )
    elif backend == "redis":
        core = _RedisCore(
            hash_func=hash_func,
            redis_client=redis_client,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=size_limit_bytes,
        )
    else:
        raise ValueError("specified an invalid core: %s" % backend)

    def _cachier_decorator(func):
        core.set_func(func)
        is_coroutine = inspect.iscoroutinefunction(func)

        if backend == "mongo":
            if is_coroutine and not inspect.iscoroutinefunction(mongetter):
                msg = "Async cached functions with Mongo backend require an async mongetter."
                raise TypeError(msg)
            if (not is_coroutine) and inspect.iscoroutinefunction(mongetter):
                msg = "Async mongetter requires an async cached function."
                raise TypeError(msg)

        if backend == "redis":
            if is_coroutine:
                if callable(redis_client):
                    if not inspect.iscoroutinefunction(redis_client):
                        msg = "Async cached functions with Redis backend require an async redis_client callable."
                        raise TypeError(msg)
                elif not _is_async_redis_client(redis_client):
                    msg = "Async cached functions with Redis backend require an async Redis client."
                    raise TypeError(msg)
            else:
                if callable(redis_client) and inspect.iscoroutinefunction(redis_client):
                    msg = "Async redis_client callable requires an async cached function."
                    raise TypeError(msg)
                if _is_async_redis_client(redis_client):
                    msg = "Async Redis client requires an async cached function."
                    raise TypeError(msg)

        if backend == "sql":
            sql_core = core
            assert isinstance(sql_core, _SQLCore)  # noqa: S101
            if is_coroutine and not sql_core.has_async_engine():
                msg = "Async cached functions with SQL backend require an AsyncEngine sql_engine."
                raise TypeError(msg)
            if (not is_coroutine) and sql_core.has_async_engine():
                msg = "Async SQL engines require an async cached function."
                raise TypeError(msg)

        last_cleanup = datetime.min
        cleanup_lock = threading.Lock()

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
            nonlocal allow_none, last_cleanup
            _allow_none = _update_with_defaults(allow_none, "allow_none", kwds)
            # print('Inside general wrapper for {}.'.format(func.__name__))
            ignore_cache = _pop_kwds_with_deprecation(kwds, "ignore_cache", False)
            overwrite_cache = _pop_kwds_with_deprecation(kwds, "overwrite_cache", False)
            verbose = _pop_kwds_with_deprecation(kwds, "verbose_cache", False)
            ignore_cache = kwds.pop("cachier__skip_cache", ignore_cache)
            overwrite_cache = kwds.pop("cachier__overwrite_cache", overwrite_cache)
            verbose = kwds.pop("cachier__verbose", verbose)
            _stale_after = _update_with_defaults(stale_after, "stale_after", kwds)
            _next_time = _update_with_defaults(next_time, "next_time", kwds)
            _cleanup_flag = _update_with_defaults(cleanup_stale, "cleanup_stale", kwds)
            _cleanup_interval_val = _update_with_defaults(cleanup_interval, "cleanup_interval", kwds)
            # merge args expanded as kwargs and the original kwds
            kwargs = _convert_args_kwargs(func, _is_method=core.func_is_method, args=args, kwds=kwds)

            if _cleanup_flag:
                now = datetime.now()
                with cleanup_lock:
                    if now - last_cleanup >= _cleanup_interval_val:
                        last_cleanup = now
                        _get_executor().submit(core.delete_stale_entries, _stale_after)

            _print = print if verbose else lambda x: None

            # Check current global caching state dynamically
            from .config import _global_params

            if ignore_cache or not _global_params.caching_enabled:
                return func(args[0], **kwargs) if core.func_is_method else func(**kwargs)
            key, entry = core.get_entry((), kwargs)
            if overwrite_cache:
                return _calc_entry(core, key, func, args, kwds, _print)
            if entry is None or (not entry._completed and not entry._processing):
                _print("No entry found. No current calc. Calling like a boss.")
                return _calc_entry(core, key, func, args, kwds, _print)
            _print("Entry found.")
            if _allow_none or entry.value is not None:
                _print("Cached result found.")
                now = datetime.now()
                max_allowed_age = _stale_after
                nonneg_max_age = True
                if max_age is not None:
                    if max_age < ZERO_TIMEDELTA:
                        _print("max_age is negative. Cached result considered stale.")
                        nonneg_max_age = False
                    else:
                        assert max_age is not None  # noqa: S101
                        max_allowed_age = min(_stale_after, max_age)
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
                        return _calc_entry(core, key, func, args, kwds, _print)
                if _next_time:
                    _print("Async calc and return stale")
                    core.mark_entry_being_calculated(key)
                    try:
                        _get_executor().submit(_function_thread, core, key, func, args, kwds)
                    finally:
                        core.mark_entry_not_calculated(key)
                    return entry.value
                _print("Calling decorated function and waiting")
                return _calc_entry(core, key, func, args, kwds, _print)
            if entry._processing:
                _print("No value but being calculated. Waiting.")
                try:
                    return core.wait_on_entry_calc(key)
                except RecalculationNeeded:
                    return _calc_entry(core, key, func, args, kwds, _print)
            _print("No entry found. No current calc. Calling like a boss.")
            return _calc_entry(core, key, func, args, kwds, _print)

        async def _call_async(*args, max_age: Optional[timedelta] = None, **kwds):
            # NOTE: For async functions, wait_for_calc_timeout is not honored.
            # Instead of blocking the event loop waiting for concurrent
            # calculations, async functions will recalculate in parallel.
            # This avoids deadlocks and maintains async efficiency.
            nonlocal allow_none, last_cleanup
            _allow_none = _update_with_defaults(allow_none, "allow_none", kwds)
            # print('Inside async wrapper for {}.'.format(func.__name__))
            ignore_cache = _pop_kwds_with_deprecation(kwds, "ignore_cache", False)
            overwrite_cache = _pop_kwds_with_deprecation(kwds, "overwrite_cache", False)
            verbose = _pop_kwds_with_deprecation(kwds, "verbose_cache", False)
            ignore_cache = kwds.pop("cachier__skip_cache", ignore_cache)
            overwrite_cache = kwds.pop("cachier__overwrite_cache", overwrite_cache)
            verbose = kwds.pop("cachier__verbose", verbose)
            _stale_after = _update_with_defaults(stale_after, "stale_after", kwds)
            _next_time = _update_with_defaults(next_time, "next_time", kwds)
            _cleanup_flag = _update_with_defaults(cleanup_stale, "cleanup_stale", kwds)
            _cleanup_interval_val = _update_with_defaults(cleanup_interval, "cleanup_interval", kwds)
            # merge args expanded as kwargs and the original kwds
            kwargs = _convert_args_kwargs(func, _is_method=core.func_is_method, args=args, kwds=kwds)

            if _cleanup_flag:
                now = datetime.now()
                with cleanup_lock:
                    if now - last_cleanup >= _cleanup_interval_val:
                        last_cleanup = now
                        _get_executor().submit(core.delete_stale_entries, _stale_after)

            _print = print if verbose else lambda x: None

            # Check current global caching state dynamically
            from .config import _global_params

            if ignore_cache or not _global_params.caching_enabled:
                return await func(args[0], **kwargs) if core.func_is_method else await func(**kwargs)
            key, entry = await core.aget_entry((), kwargs)
            if overwrite_cache:
                result = await _calc_entry_async(core, key, func, args, kwds, _print)
                return result
            if entry is None or (not entry._completed and not entry._processing):
                _print("No entry found. No current calc. Calling like a boss.")
                result = await _calc_entry_async(core, key, func, args, kwds, _print)
                return result
            _print("Entry found.")
            if _allow_none or entry.value is not None:
                _print("Cached result found.")
                now = datetime.now()
                max_allowed_age = _stale_after
                nonneg_max_age = True
                if max_age is not None:
                    if max_age < ZERO_TIMEDELTA:
                        _print("max_age is negative. Cached result considered stale.")
                        nonneg_max_age = False
                    else:
                        assert max_age is not None  # noqa: S101
                        max_allowed_age = min(_stale_after, max_age)
                # note: if max_age < 0, we always consider a value stale
                if nonneg_max_age and (now - entry.time <= max_allowed_age):
                    _print("And it is fresh!")
                    return entry.value
                _print("But it is stale... :(")
                if _next_time:
                    _print("Async calc and return stale")
                    # Mark entry as being calculated then immediately unmark
                    # This matches sync behavior and ensures entry exists
                    # Background task will update cache when complete
                    await core.amark_entry_being_calculated(key)
                    # Use asyncio.create_task for background execution
                    asyncio.create_task(_function_thread_async(core, key, func, args, kwds))
                    await core.amark_entry_not_calculated(key)
                    return entry.value
                _print("Calling decorated function and waiting")
                result = await _calc_entry_async(core, key, func, args, kwds, _print)
                return result
            if entry._processing:
                msg = "No value but being calculated. Recalculating"
                _print(f"{msg} (async - no wait).")
                # For async, don't wait - just recalculate
                # This avoids blocking the event loop
                result = await _calc_entry_async(core, key, func, args, kwds, _print)
                return result
            _print("No entry found. No current calc. Calling like a boss.")
            return await _calc_entry_async(core, key, func, args, kwds, _print)

        # MAINTAINER NOTE: The main function wrapper is now a standard function
        # that passes *args and **kwargs to _call. This ensures that user
        # arguments are not shifted, and max_age is only settable via keyword
        # argument.
        # For async functions, we create an async wrapper that calls
        # _call_async.
        if is_coroutine:

            @wraps(func)
            async def func_wrapper(*args, **kwargs):
                return await _call_async(*args, **kwargs)
        else:

            @wraps(func)
            def func_wrapper(*args, **kwargs):
                return _call(*args, **kwargs)

        def _clear_cache():
            """Clear the cache."""
            core.clear_cache()

        def _clear_being_calculated():
            """Mark all entries in this cache as not being calculated."""
            core.clear_being_calculated()

        async def _aclear_cache():
            """Clear the cache asynchronously."""
            await core.aclear_cache()

        async def _aclear_being_calculated():
            """Mark all entries in this cache as not being calculated asynchronously."""
            await core.aclear_being_calculated()

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
            kwargs = _convert_args_kwargs(func, _is_method=core.func_is_method, args=args, kwds=kwds)
            return core.precache_value((), kwargs, value_to_cache)

        func_wrapper.clear_cache = _clear_cache
        func_wrapper.clear_being_calculated = _clear_being_calculated
        func_wrapper.aclear_cache = _aclear_cache
        func_wrapper.aclear_being_calculated = _aclear_being_calculated
        func_wrapper.cache_dpath = _cache_dpath
        func_wrapper.precache_value = _precache_value
        return func_wrapper

    return _cachier_decorator
