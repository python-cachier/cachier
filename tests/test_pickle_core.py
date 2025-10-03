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
import hashlib
import os
import pickle
import sys
import tempfile
import threading
from datetime import datetime, timedelta
from random import random
from time import sleep, time
from unittest.mock import Mock, patch

import pytest

try:
    import queue
except ImportError:  # python 2
    import Queue as queue  # type: ignore


import pandas as pd

from cachier import cachier
from cachier.config import CacheEntry, _global_params
from cachier.cores.pickle import _PickleCore


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
    import subprocess
    import time

    # Try to get the current inotify limit
    try:
        result = subprocess.run(
            ["/bin/cat", "/proc/sys/fs/inotify/max_user_instances"],
            capture_output=True,
            text=True,
            timeout=5,
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
    N = (
        min(current_limit * 4, 4096) if current_limit is not None else 4096
    )  # Try to exceed the limit more aggressively
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

    print(
        f"Test completed. Got {len(errors)} errors, {results.qsize()} results"
    )

    # If any OSError with "inotify instance limit reached" is raised,
    # the test FAILS (expected failure due to the bug)
    if any("inotify instance limit reached" in str(e) for e in errors):
        print(
            "FAILURE: Hit inotify instance limit - this indicates the bug "
            "still exists"
        )
        raise AssertionError(
            "inotify instance limit reached error occurred. "
            f"Got {len(errors)} errors with inotify limit issues."
        )

    # If no inotify errors but other errors, fail
    if errors:
        print(f"Unexpected errors occurred: {errors}")
        raise AssertionError(f"Unexpected OSErrors: {errors}")

    # If no errors at all, the test PASSES (issue is fixed!)
    print(
        "SUCCESS: No inotify instance limit errors occurred - the issue "
        "appears to be fixed!"
    )
    # No need to return - test passes naturally


@pytest.mark.pickle
def test_convert_legacy_cache_entry_dict():
    """Test _convert_legacy_cache_entry with dict input."""
    # Test line 112-118: converting legacy dict format
    legacy_entry = {
        "value": "test_value",
        "time": datetime.now(),
        "stale": False,
        "being_calculated": True,
        "condition": None,
    }

    result = _PickleCore._convert_legacy_cache_entry(legacy_entry)

    assert isinstance(result, CacheEntry)
    assert result.value == "test_value"
    assert result.stale is False
    assert result._processing is True


@pytest.mark.pickle
def test_save_cache_with_invalid_separate_file_key():
    """Test _save_cache raises error with invalid separate_file_key."""
    # Test line 179-181: ValueError when separate_file_key used with dict
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=False,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Should raise ValueError when using separate_file_key with a dict
        with pytest.raises(
            ValueError,
            match="`separate_file_key` should only be used with a CacheEntry",
        ):
            core._save_cache({"key": "value"}, separate_file_key="test_key")


@pytest.mark.pickle
def test_set_entry_should_not_store():
    """Test set_entry when value should not be stored."""
    # Test line 204: early return when _should_store returns False
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=False,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Mock _should_store to return False
        core._should_store = Mock(return_value=False)

        result = core.set_entry("test_key", None)
        assert result is False


@pytest.mark.pickle
def test_mark_entry_not_calculated_separate_files_no_entry():
    """Test _mark_entry_not_calculated_separate_files with no entry."""
    # Test line 236: early return when entry is None
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=True,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Mock get_entry_by_key to return None
        core.get_entry_by_key = Mock(return_value=("test_key", None))

        # Should return without error
        core._mark_entry_not_calculated_separate_files("test_key")


@pytest.mark.pickle
def test_cleanup_observer_exception():
    """Test _cleanup_observer with exception during cleanup."""
    # Test lines 278-279: exception handling in observer cleanup
    core = _PickleCore(
        hash_func=None,
        cache_dir=".",
        pickle_reload=False,
        wait_for_calc_timeout=10,
        separate_files=False,
    )

    # Set a mock function
    mock_func = Mock()
    mock_func.__name__ = "test_func"
    mock_func.__module__ = "test_module"
    mock_func.__qualname__ = "test_func"
    core.set_func(mock_func)

    # Mock observer that raises exception
    mock_observer = Mock()
    mock_observer.is_alive.return_value = True
    mock_observer.stop.side_effect = Exception("Observer error")

    # Should not raise exception
    core._cleanup_observer(mock_observer)


@pytest.mark.pickle
def test_wait_on_entry_calc_inotify_limit():
    """Test wait_on_entry_calc fallback when inotify limit is reached."""
    # Test lines 298-302: OSError handling for inotify limit
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=False,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Create a cache entry that's being calculated
        cache_entry = CacheEntry(
            value="test_value",
            time=datetime.now(),
            stale=False,
            _processing=True,  # Should be processing
        )
        core._save_cache({"test_key": cache_entry})

        # Mock _wait_with_inotify to raise OSError with inotify message
        def mock_wait_inotify(key, filename):
            raise OSError("inotify instance limit reached")

        core._wait_with_inotify = mock_wait_inotify

        # Mock _wait_with_polling to return a value
        core._wait_with_polling = Mock(return_value="polling_result")

        result = core.wait_on_entry_calc("test_key")
        assert result == "polling_result"
        core._wait_with_polling.assert_called_once_with("test_key")


@pytest.mark.pickle
def test_wait_on_entry_calc_other_os_error():
    """Test wait_on_entry_calc re-raises non-inotify OSErrors."""
    # Test line 302: re-raise other OSErrors
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=False,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Mock _wait_with_inotify to raise different OSError
        def mock_wait_inotify(key, filename):
            raise OSError("Different error")

        core._wait_with_inotify = mock_wait_inotify

        with pytest.raises(OSError, match="Different error"):
            core.wait_on_entry_calc("test_key")


@pytest.mark.pickle
def test_wait_with_polling_file_errors():
    """Test _wait_with_polling handles file errors gracefully."""
    # Test lines 352-354: FileNotFoundError/EOFError handling
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=2,  # Short timeout
            separate_files=False,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Mock methods to simulate file errors then success
        call_count = 0

        def mock_get_cache_dict():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise FileNotFoundError("Cache file not found")
            elif call_count == 2:
                raise EOFError("Cache file corrupted")
            else:
                return {
                    "test_key": CacheEntry(
                        value="result",
                        time=datetime.now(),
                        stale=False,
                        _processing=False,
                    )
                }

        core.get_cache_dict = mock_get_cache_dict
        core.separate_files = False

        with patch("time.sleep", return_value=None):  # Speed up test
            result = core._wait_with_polling("test_key")
            assert result == "result"


@pytest.mark.pickle
def test_wait_with_polling_separate_files():
    """Test _wait_with_polling with separate files mode."""
    # Test lines 342-343: separate files branch
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=True,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Mock _load_cache_by_key
        entry = CacheEntry(
            value="test_value",
            time=datetime.now(),
            stale=False,
            _processing=False,
        )
        core._load_cache_by_key = Mock(return_value=entry)

        with patch("time.sleep", return_value=None):
            result = core._wait_with_polling("test_key")
            assert result == "test_value"


@pytest.mark.pickle
def test_delete_stale_entries_separate_files():
    """Test delete_stale_entries with separate files mode."""
    # Test lines 377-387: separate files deletion logic
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=True,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Create some cache files
        base_path = core.cache_fpath

        # Create stale entry file
        stale_entry = CacheEntry(
            value="stale_value",
            time=datetime.now() - timedelta(hours=2),
            stale=False,
            _processing=False,
        )
        stale_file = f"{base_path}_stalekey"
        with open(stale_file, "wb") as f:
            pickle.dump(stale_entry, f)

        # Create fresh entry file
        fresh_entry = CacheEntry(
            value="fresh_value",
            time=datetime.now(),
            stale=False,
            _processing=False,
        )
        fresh_file = f"{base_path}_freshkey"
        with open(fresh_file, "wb") as f:
            pickle.dump(fresh_entry, f)

        # Create non-matching file (should be ignored)
        other_file = os.path.join(temp_dir, "other_file.txt")
        with open(other_file, "w") as f:
            f.write("other content")

        # Before running delete, check that files exist
        assert os.path.exists(stale_file)
        assert os.path.exists(fresh_file)

        # Run delete_stale_entries
        core.delete_stale_entries(timedelta(hours=1))

        # Check that only stale file was deleted
        assert not os.path.exists(stale_file)
        assert os.path.exists(fresh_file)
        assert os.path.exists(other_file)


@pytest.mark.pickle
def test_delete_stale_entries_file_not_found():
    """Test delete_stale_entries handles FileNotFoundError."""
    # Test lines 385-386: FileNotFoundError suppression
    with tempfile.TemporaryDirectory() as temp_dir:
        core = _PickleCore(
            hash_func=None,
            cache_dir=temp_dir,
            pickle_reload=False,
            wait_for_calc_timeout=10,
            separate_files=True,
        )

        # Set a mock function
        def mock_func():
            pass

        core.set_func(mock_func)

        # Mock _load_cache_by_key to return a stale entry
        stale_entry = CacheEntry(
            value="stale",
            time=datetime.now() - timedelta(hours=2),
            stale=False,
            _processing=False,
        )
        core._load_cache_by_key = Mock(return_value=stale_entry)

        # Mock os.remove to raise FileNotFoundError
        with patch("os.remove", side_effect=FileNotFoundError):
            # Should not raise exception
            core.delete_stale_entries(timedelta(hours=1))


@pytest.mark.pickle
def test_loading_pickle(temp_dir):
    """Cover the internal _loading_pickle behavior for valid, corrupted, and
    missing files.
    """
    import importlib

    pickle_module = importlib.import_module("cachier.cores.pickle")
    loading = getattr(pickle_module, "_loading_pickle", None)
    if loading is None:
        # fallback to class method if implemented there
        loading = getattr(_PickleCore, "_loading_pickle", None)

    assert callable(loading)

    # Valid pickle file should return the unpickled object
    valid_obj = {"ok": True, "n": 1}
    valid_file = os.path.join(temp_dir, "valid.pkl")
    with open(valid_file, "wb") as f:
        pickle.dump(valid_obj, f)

    assert loading(valid_file) == valid_obj

    # Corrupted / truncated pickle should be handled gracefully (return None)
    corrupt_file = os.path.join(temp_dir, "corrupt.pkl")
    with open(corrupt_file, "wb") as f:
        f.write(b"\x80\x04\x95")  # truncated/invalid pickle bytes

    assert loading(corrupt_file) is None

    # Missing file should be handled gracefully (return None)
    assert loading(os.path.join(temp_dir, "does_not_exist.pkl")) is None


# Redis core static method tests
@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not available")
def test_redis_loading_pickle():
    """Test _RedisCore._loading_pickle with various inputs and exceptions."""
    # Valid bytes
    valid_obj = {"test": 123}
    valid_bytes = pickle.dumps(valid_obj)
    assert _RedisCore._loading_pickle(valid_bytes) == valid_obj

    # Valid string (UTF-8 encoded)
    valid_str = valid_bytes.decode("utf-8")
    assert _RedisCore._loading_pickle(valid_str) == valid_obj

    # Invalid string that needs latin-1 fallback
    with patch("pickle.loads") as mock_loads:
        mock_loads.side_effect = [Exception("UTF-8 failed"), valid_obj]
        result = _RedisCore._loading_pickle("invalid_utf8")
        assert result == valid_obj
        assert mock_loads.call_count == 2

    # Corrupted bytes
    assert _RedisCore._loading_pickle(b"\x80\x04\x95") is None

    # Unexpected type with direct pickle.loads attempt
    with patch("pickle.loads", side_effect=Exception("Failed")):
        assert _RedisCore._loading_pickle(123) is None

    # Exception during deserialization should warn and return None
    with patch("warnings.warn") as mock_warn:
        result = _RedisCore._loading_pickle(b"corrupted")
        assert result is None
        mock_warn.assert_called_once()


@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not available")
def test_redis_get_raw_field():
    """Test _RedisCore._get_raw_field with bytes and string keys."""
    # Test with bytes key
    cached_data = {b"field": b"value", "other": "data"}
    assert _RedisCore._get_raw_field(cached_data, "field") == b"value"

    # Test with string key fallback
    cached_data = {"field": "value", b"other": b"data"}
    assert _RedisCore._get_raw_field(cached_data, "field") == "value"

    # Test with missing field
    cached_data = {"other": "value"}
    assert _RedisCore._get_raw_field(cached_data, "field") is None


@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not available")
def test_redis_get_bool_field():
    """Test _RedisCore._get_bool_field with various inputs and exceptions."""
    # Test with bytes "true"
    cached_data = {b"flag": b"true"}
    assert _RedisCore._get_bool_field(cached_data, "flag") is True

    # Test with bytes "false"
    cached_data = {b"flag": b"false"}
    assert _RedisCore._get_bool_field(cached_data, "flag") is False

    # Test with string "TRUE" (case insensitive)
    cached_data = {"flag": "TRUE"}
    assert _RedisCore._get_bool_field(cached_data, "flag") is True

    # Test with missing field (defaults to false)
    cached_data = {}
    assert _RedisCore._get_bool_field(cached_data, "flag") is False

    # Test with bytes that can't decode UTF-8 (fallback to latin-1)
    with patch.object(_RedisCore, "_get_raw_field", return_value=b"\xff\xfe"):
        result = _RedisCore._get_bool_field({}, "flag")
        assert result is False  # Should decode with latin-1 and be "false"

    # Test with non-string/bytes value
    cached_data = {b"flag": 123}
    assert _RedisCore._get_bool_field(cached_data, "flag") is False
