"""Test for the Cachier python package."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

# from os.path import (
#     realpath,
#     dirname
# )
import os
import pickle
import threading
from datetime import timedelta
from random import random
from time import sleep, time

import pytest

try:
    import queue
except ImportError:  # python 2
    import Queue as queue  # type: ignore

import hashlib
import sys

import pandas as pd

from cachier import cachier
from cachier.config import _global_params


def _get_decorated_func(func, **kwargs):
    cachier_decorator = cachier(**kwargs)
    decorated_func = cachier_decorator(func)
    return decorated_func


# Pickle core tests


def _takes_2_seconds(arg_1, arg_2):
    """Some function."""
    sleep(2)
    return f"arg_1:{arg_1}, arg_2:{arg_2}"


@pytest.mark.pickle
@pytest.mark.parametrize("reload", [True, False])
@pytest.mark.parametrize("separate_files", [True, False])
def test_pickle_core(reload, separate_files):
    """Basic Pickle core functionality."""
    _takes_2_seconds_decorated = _get_decorated_func(
        _takes_2_seconds,
        next_time=False,
        pickle_reload=reload,
        separate_files=separate_files,
    )
    _takes_2_seconds_decorated.clear_cache()
    _takes_2_seconds_decorated("a", "b")
    start = time()
    _takes_2_seconds_decorated("a", "b", cachier__verbose=True)
    end = time()
    assert end - start < 1
    _takes_2_seconds_decorated.clear_cache()


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_pickle_core_keywords(separate_files):
    """Basic Pickle core functionality with keyword arguments."""
    _takes_2_seconds_decorated = _get_decorated_func(
        _takes_2_seconds, next_time=False, separate_files=separate_files
    )
    _takes_2_seconds_decorated.clear_cache()
    _takes_2_seconds_decorated("a", arg_2="b")
    start = time()
    _takes_2_seconds_decorated("a", arg_2="b", cachier__verbose=True)
    end = time()
    assert end - start < 1
    _takes_2_seconds_decorated.clear_cache()


SECONDS_IN_DELTA = 3
DELTA = timedelta(seconds=SECONDS_IN_DELTA)


def _stale_after_seconds(arg_1, arg_2):
    """Some function."""
    return random()


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_stale_after(separate_files):
    """Testing the stale_after functionality."""
    _stale_after_seconds_decorated = _get_decorated_func(
        _stale_after_seconds,
        stale_after=DELTA,
        next_time=False,
        separate_files=separate_files,
    )
    _stale_after_seconds_decorated.clear_cache()
    val1 = _stale_after_seconds_decorated(1, 2)
    val2 = _stale_after_seconds_decorated(1, 2)
    val3 = _stale_after_seconds_decorated(1, 3)
    assert val1 == val2
    assert val1 != val3
    sleep(3)
    val4 = _stale_after_seconds_decorated(1, 2)
    assert val4 != val1
    _stale_after_seconds_decorated.clear_cache()


def _stale_after_next_time(arg_1, arg_2):
    """Some function."""
    return random()


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_stale_after_next_time(separate_files):
    """Testing the stale_after with next_time functionality."""
    _stale_after_next_time_decorated = _get_decorated_func(
        _stale_after_next_time,
        stale_after=DELTA,
        next_time=True,
        separate_files=separate_files,
    )
    _stale_after_next_time_decorated.clear_cache()
    val1 = _stale_after_next_time_decorated(1, 2)
    val2 = _stale_after_next_time_decorated(1, 2)
    val3 = _stale_after_next_time_decorated(1, 3)
    assert val1 == val2
    assert val1 != val3
    sleep(SECONDS_IN_DELTA + 1)
    val4 = _stale_after_next_time_decorated(1, 2)
    assert val4 == val1
    sleep(0.5)
    val5 = _stale_after_next_time_decorated(1, 2)
    assert val5 != val1
    _stale_after_next_time_decorated.clear_cache()


def _random_num():
    return random()


def _random_num_with_arg(a):
    # print(a)
    return random()


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_overwrite_cache(separate_files):
    """Tests that the overwrite feature works correctly."""
    _random_num_decorated = _get_decorated_func(
        _random_num, separate_files=separate_files
    )
    _random_num_with_arg_decorated = _get_decorated_func(
        _random_num_with_arg, separate_files=separate_files
    )
    _random_num_decorated.clear_cache()
    int1 = _random_num_decorated()
    int2 = _random_num_decorated()
    assert int2 == int1
    int3 = _random_num_decorated(cachier__overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_decorated()
    assert int4 == int3
    _random_num_decorated.clear_cache()

    _random_num_with_arg_decorated.clear_cache()
    int1 = _random_num_with_arg_decorated("a")
    int2 = _random_num_with_arg_decorated("a")
    assert int2 == int1
    int3 = _random_num_with_arg_decorated("a", cachier__overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg_decorated("a")
    assert int4 == int3
    _random_num_with_arg_decorated.clear_cache()


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_ignore_cache(separate_files):
    """Tests that the ignore_cache feature works correctly."""
    _random_num_decorated = _get_decorated_func(
        _random_num, separate_files=separate_files
    )
    _random_num_with_arg_decorated = _get_decorated_func(
        _random_num_with_arg, separate_files=separate_files
    )
    _random_num_decorated.clear_cache()
    int1 = _random_num_decorated()
    int2 = _random_num_decorated()
    assert int2 == int1
    int3 = _random_num_decorated(cachier__skip_cache=True)
    assert int3 != int1
    int4 = _random_num_decorated()
    assert int4 != int3
    assert int4 == int1
    _random_num_decorated.clear_cache()

    _random_num_with_arg_decorated.clear_cache()
    int1 = _random_num_with_arg_decorated("a")
    int2 = _random_num_with_arg_decorated("a")
    assert int2 == int1
    int3 = _random_num_with_arg_decorated("a", cachier__skip_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg_decorated("a")
    assert int4 != int3
    assert int4 == int1
    _random_num_with_arg_decorated.clear_cache()


def _takes_time(arg_1, arg_2):
    """Some function."""
    sleep(2)  # this has to be enough time for check_calculation to run twice
    return random() + arg_1 + arg_2


def _calls_takes_time(takes_time_func, res_queue):
    res = takes_time_func(0.13, 0.02)
    res_queue.put(res)


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_pickle_being_calculated(separate_files):
    """Testing pickle core handling of being calculated scenarios."""
    _takes_time_decorated = _get_decorated_func(
        _takes_time, separate_files=separate_files
    )
    _takes_time_decorated.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_takes_time,
        kwargs={
            "takes_time_func": _takes_time_decorated,
            "res_queue": res_queue,
        },
        daemon=True,
    )
    thread2 = threading.Thread(
        target=_calls_takes_time,
        kwargs={
            "takes_time_func": _takes_time_decorated,
            "res_queue": res_queue,
        },
        daemon=True,
    )
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join(timeout=4)
    thread2.join(timeout=4)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


def _being_calc_next_time(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


def _calls_being_calc_next_time(being_calc_func, res_queue):
    res = being_calc_func(0.13, 0.02)
    res_queue.put(res)


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_being_calc_next_time(separate_files):
    """Testing pickle core handling of being calculated scenarios."""
    _being_calc_next_time_decorated = _get_decorated_func(
        _being_calc_next_time,
        stale_after=timedelta(seconds=1),
        next_time=True,
        separate_files=separate_files,
    )
    _being_calc_next_time_decorated.clear_cache()
    _being_calc_next_time(0.13, 0.02)
    sleep(1.1)
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_being_calc_next_time,
        kwargs={
            "being_calc_func": _being_calc_next_time_decorated,
            "res_queue": res_queue,
        },
        daemon=True,
    )
    thread2 = threading.Thread(
        target=_calls_being_calc_next_time,
        kwargs={
            "being_calc_func": _being_calc_next_time_decorated,
            "res_queue": res_queue,
        },
        daemon=True,
    )
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join(timeout=2)
    thread2.join(timeout=2)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


def _bad_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


# _BAD_CACHE_FNAME = '.__main__._bad_cache'
_BAD_CACHE_FNAME = ".tests.test_pickle_core._bad_cache"
_BAD_CACHE_FNAME_SEPARATE_FILES = (
    ".tests.test_pickle_core._bad_cache_"
    f"{hashlib.sha256(pickle.dumps((0.13, 0.02))).hexdigest()}"
)
EXPANDED_CACHIER_DIR = os.path.expanduser(_global_params.cache_dir)
_BAD_CACHE_FPATH = os.path.join(EXPANDED_CACHIER_DIR, _BAD_CACHE_FNAME)
_BAD_CACHE_FPATH_SEPARATE_FILES = os.path.join(
    EXPANDED_CACHIER_DIR, _BAD_CACHE_FNAME_SEPARATE_FILES
)
_BAD_CACHE_FPATHS = {
    True: _BAD_CACHE_FPATH_SEPARATE_FILES,
    False: _BAD_CACHE_FPATH,
}


def _calls_bad_cache(bad_cache_func, res_queue, trash_cache, separate_files):
    try:
        res = bad_cache_func(0.13, 0.02, cachier__verbose=True)
        if trash_cache:
            with open(_BAD_CACHE_FPATHS[separate_files], "w") as cache_file:
                cache_file.seek(0)
                cache_file.truncate()
        res_queue.put(res)
    except Exception as exc:
        res_queue.put(exc)


def _helper_bad_cache_file(sleep_time: float, separate_files: bool):
    """Test pickle core handling of bad cache files."""
    _bad_cache_decorated = _get_decorated_func(
        _bad_cache, separate_files=separate_files
    )
    _bad_cache_decorated.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_bad_cache,
        kwargs={
            "bad_cache_func": _bad_cache_decorated,
            "res_queue": res_queue,
            "trash_cache": True,
            "separate_files": separate_files,
        },
        daemon=True,
    )
    thread2 = threading.Thread(
        target=_calls_bad_cache,
        kwargs={
            "bad_cache_func": _bad_cache_decorated,
            "res_queue": res_queue,
            "trash_cache": False,
            "separate_files": separate_files,
        },
        daemon=True,
    )
    thread1.start()
    sleep(sleep_time)
    thread2.start()
    thread1.join(timeout=2)
    thread2.join(timeout=2)
    if res_queue.qsize() != 2:
        return False
    res1 = res_queue.get()
    if not isinstance(res1, float):
        return False
    res2 = res_queue.get()
    return res2 is None


# we want this to succeed at least once
@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_bad_cache_file(separate_files):
    """Test pickle core handling of bad cache files."""
    sleep_times = [0.1, 0.2, 0.3, 0.5, 0.6, 0.7, 0.8, 1, 1.5, 2]
    bad_file = False
    for sleep_time in sleep_times * 2:
        if _helper_bad_cache_file(sleep_time, separate_files):
            bad_file = True
            break
    # it is expected that for separate_files=True files will not be bad
    assert bad_file is not separate_files


def _delete_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


# _DEL_CACHE_FNAME = '.__main__._delete_cache'
_DEL_CACHE_FNAME = ".tests.test_pickle_core._delete_cache"
_DEL_CACHE_FNAME_SEPARATE_FILES = (
    ".tests.test_pickle_core._delete_cache_"
    f"{hashlib.sha256(pickle.dumps((0.13, 0.02))).hexdigest()}"
)
_DEL_CACHE_FPATH = os.path.join(EXPANDED_CACHIER_DIR, _DEL_CACHE_FNAME)
_DEL_CACHE_FPATH_SEPARATE_FILES = os.path.join(
    EXPANDED_CACHIER_DIR, _DEL_CACHE_FNAME_SEPARATE_FILES
)
_DEL_CACHE_FPATHS = {
    True: _DEL_CACHE_FPATH_SEPARATE_FILES,
    False: _DEL_CACHE_FPATH,
}


def _calls_delete_cache(
    del_cache_func, res_queue, del_cache: bool, separate_files: bool
):
    try:
        # print('in')
        res = del_cache_func(0.13, 0.02)
        # print('out with {}'.format(res))
        if del_cache:
            os.remove(_DEL_CACHE_FPATHS[separate_files])
            # print(os.path.isfile(_DEL_CACHE_FPATH))
        res_queue.put(res)
    except Exception as exc:
        # print('found')
        res_queue.put(exc)


def _helper_delete_cache_file(sleep_time: float, separate_files: bool):
    """Test pickle core handling of missing cache files."""
    _delete_cache_decorated = _get_decorated_func(
        _delete_cache, separate_files=separate_files
    )
    _delete_cache_decorated.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_delete_cache,
        kwargs={
            "del_cache_func": _delete_cache_decorated,
            "res_queue": res_queue,
            "del_cache": True,
            "separate_files": separate_files,
        },
        daemon=True,
    )
    thread2 = threading.Thread(
        target=_calls_delete_cache,
        kwargs={
            "del_cache_func": _delete_cache_decorated,
            "res_queue": res_queue,
            "del_cache": False,
            "separate_files": separate_files,
        },
        daemon=True,
    )
    thread1.start()
    sleep(sleep_time)
    thread2.start()
    thread1.join(timeout=2)
    thread2.join(timeout=2)
    if res_queue.qsize() != 2:
        return False
    res1 = res_queue.get()
    # print(res1)
    if not isinstance(res1, float):
        return False
    res2 = res_queue.get()
    return isinstance(res2, KeyError) or (res2 is None)


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [False, True])
def test_delete_cache_file(separate_files):
    """Test pickle core handling of missing cache files."""
    sleep_times = [0.1, 0.2, 0.3, 0.5, 0.7, 1]
    deleted = False
    for sleep_time in sleep_times * 4:
        if _helper_delete_cache_file(sleep_time, separate_files):
            deleted = True
            break
    # it is expected that for separate_files=True files will not be deleted
    assert deleted is not separate_files


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [False, True])
def test_clear_being_calculated(separate_files):
    """Test pickle core clear `being calculated` functionality."""
    _takes_time_decorated = _get_decorated_func(
        _takes_time, separate_files=separate_files
    )
    _takes_time_decorated.clear_being_calculated()


def _error_throwing_func(arg1):
    if not hasattr(_error_throwing_func, "count"):
        _error_throwing_func.count = 0
    _error_throwing_func.count += 1
    if _error_throwing_func.count > 1:
        raise ValueError("Tiny Rick!")
    return 7


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_error_throwing_func(separate_files):
    # with
    _error_throwing_func.count = 0
    _error_throwing_func_decorated = _get_decorated_func(
        _error_throwing_func,
        stale_after=timedelta(seconds=1),
        next_time=True,
        separate_files=separate_files,
    )
    _error_throwing_func_decorated.clear_cache()
    res1 = _error_throwing_func_decorated(4)
    sleep(1.5)
    res2 = _error_throwing_func_decorated(4)
    assert res1 == res2


# test custom cache dir for pickle core

CUSTOM_DIR = "~/.exparrot"
EXPANDED_CUSTOM_DIR = os.path.expanduser(CUSTOM_DIR)


def _takes_2_seconds_custom_dir(arg_1, arg_2):
    """Some function."""
    sleep(2)
    return f"arg_1:{arg_1}, arg_2:{arg_2}"


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_pickle_core_custom_cache_dir(separate_files):
    """Basic Pickle core functionality."""
    _takes_2_seconds_custom_dir_decorated = _get_decorated_func(
        _takes_2_seconds_custom_dir,
        next_time=False,
        cache_dir=CUSTOM_DIR,
        separate_files=separate_files,
    )
    _takes_2_seconds_custom_dir_decorated.clear_cache()
    _takes_2_seconds_custom_dir_decorated("a", "b")
    start = time()
    _takes_2_seconds_custom_dir_decorated("a", "b", cachier__verbose=True)
    end = time()
    assert end - start < 1
    _takes_2_seconds_custom_dir_decorated.clear_cache()
    path2test = _takes_2_seconds_custom_dir_decorated.cache_dpath()
    assert path2test == EXPANDED_CUSTOM_DIR


@pytest.mark.pickle
@pytest.mark.parametrize("separate_files", [True, False])
def test_callable_hash_param(separate_files):
    def _hash_func(args, kwargs):
        def _hash(obj):
            if isinstance(obj, pd.core.frame.DataFrame):
                return hashlib.sha256(
                    pd.util.hash_pandas_object(obj).values.tobytes()
                ).hexdigest()
            return obj

        k_args = tuple(map(_hash, args))
        k_kwargs = tuple(
            sorted({k: _hash(v) for k, v in kwargs.items()}.items())
        )
        return k_args + k_kwargs

    @cachier(hash_func=_hash_func, separate_files=separate_files)
    def _params_with_dataframe(*args, **kwargs):
        """Some function."""
        return random()

    _params_with_dataframe.clear_cache()

    df_a = pd.DataFrame.from_dict({"a": [0], "b": [2], "c": [3]})
    df_b = pd.DataFrame.from_dict({"a": [0], "b": [2], "c": [3]})
    value_a = _params_with_dataframe(df_a, 1)
    value_b = _params_with_dataframe(df_b, 1)

    assert value_a == value_b  # same content --> same key

    value_a = _params_with_dataframe(1, df=df_a)
    value_b = _params_with_dataframe(1, df=df_b)

    assert value_a == value_b  # same content --> same key


@pytest.mark.pickle
@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="inotify instance limit is only relevant on Linux",
)
def test_inotify_instance_limit_reached():
    """Reproduces the inotify instance exhaustion issue (see Issue #24).

    Rapidly creates many cache waits to exhaust inotify instances.
    Reference: https://github.com/python-cachier/cachier/issues/24

    """
    import queue
    import time
    import subprocess

    # Try to get the current inotify limit
    try:
        result = subprocess.run(
            ["cat", "/proc/sys/fs/inotify/max_user_instances"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            current_limit = int(result.stdout.strip())
            print(f"Current inotify max_user_instances limit: {current_limit}")
        else:
            current_limit = None
            print("Could not determine inotify limit")
    except Exception as e:
        current_limit = None
        print(f"Error getting inotify limit: {e}")

    @cachier(backend="pickle", wait_for_calc_timeout=0.1)
    def slow_func(x):
        time.sleep(0.5)  # Make it slower to increase chance of hitting limit
        return x

    # Start many threads to trigger wait_on_entry_calc
    threads = []
    errors = []
    results = queue.Queue()
    
    # Be more aggressive - try to exhaust the limit
    if current_limit is not None:
        N = min(current_limit * 4, 4096)  # Try to exceed the limit more aggressively
    else:
        N = 4096  # Default aggressive value
    
    print(f"Starting {N} threads to test inotify exhaustion")

    def call():
        try:
            results.put(slow_func(1))
        except OSError as e:
            errors.append(e)
        except Exception as e:
            # Capture any other exceptions for debugging
            errors.append(e)

    for i in range(N):
        t = threading.Thread(target=call)
        threads.append(t)
        t.start()
        if i % 100 == 0:
            print(f"Started {i} threads...")

    print("Waiting for all threads to complete...")
    for t in threads:
        t.join()

    print(f"Test completed. Got {len(errors)} errors, {results.qsize()} results")

    # If any OSError with "inotify instance limit reached" is raised,
    # the test passes
    if any("inotify instance limit reached" in str(e) for e in errors):
        print("SUCCESS: Hit inotify instance limit as expected")
        return  # Test passes
    
    # If no error, print a warning (system limit may be high in CI)
    if not errors:
        print("WARNING: No inotify errors occurred. System limit may be too high.")
        print("Forcing test to fail to debug the issue...")
        # Force the test to fail instead of skipping to see what's happening
        raise AssertionError(
            "Did not hit inotify instance limit. This test should fail to "
            "reproduce the issue. Check if the limit is set correctly in CI."
        )
    else:
        # If other OSErrors, fail
        print(f"Unexpected errors occurred: {errors}")
        raise AssertionError(f"Unexpected OSErrors: {errors}")
