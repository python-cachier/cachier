"""Test for the Cachier python package."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

# from os.path import (
#     realpath,
#     dirname
# )
import os
import pickle
from time import (
    time,
    sleep
)
from datetime import timedelta
from random import random
import threading

import pytest

try:
    import queue
except ImportError:  # python 2
    import Queue as queue

import hashlib
import pandas as pd

from cachier import cachier
from cachier.pickle_core import DEF_CACHIER_DIR


def _get_decorated_func(func, **kwargs):
    cachier_decorator = cachier(**kwargs)
    decorated_func = cachier_decorator(func)
    return decorated_func


# Pickle core tests

def _takes_5_seconds(arg_1, arg_2):
    """Some function."""
    sleep(5)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


@pytest.mark.parametrize('separate_files', [True, False])
def test_pickle_core(separate_files):
    """Basic Pickle core functionality."""
    _takes_5_seconds_decorated = _get_decorated_func(
        _takes_5_seconds, next_time=False, separate_files=separate_files)
    _takes_5_seconds_decorated.clear_cache()
    _takes_5_seconds_decorated('a', 'b')
    start = time()
    _takes_5_seconds_decorated('a', 'b', verbose_cache=True)
    end = time()
    assert end - start < 1
    _takes_5_seconds_decorated.clear_cache()


SECONDS_IN_DELTA = 3
DELTA = timedelta(seconds=SECONDS_IN_DELTA)


def _stale_after_seconds(arg_1, arg_2):
    """Some function."""
    return random()


@pytest.mark.parametrize('separate_files', [True, False])
def test_stale_after(separate_files):
    """Testing the stale_after functionality."""
    _stale_after_seconds_decorated = _get_decorated_func(
        _stale_after_seconds, stale_after=DELTA, next_time=False,
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


@pytest.mark.parametrize('separate_files', [True, False])
def test_stale_after_next_time(separate_files):
    """Testing the stale_after with next_time functionality."""
    _stale_after_next_time_decorated = _get_decorated_func(
        _stale_after_next_time, stale_after=DELTA, next_time=True,
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


@pytest.mark.parametrize('separate_files', [True, False])
def test_overwrite_cache(separate_files):
    """Tests that the overwrite feature works correctly."""
    _random_num_decorated = _get_decorated_func(
        _random_num, separate_files=separate_files)
    _random_num_with_arg_decorated = _get_decorated_func(
        _random_num_with_arg, separate_files=separate_files)
    _random_num_decorated.clear_cache()
    int1 = _random_num_decorated()
    int2 = _random_num_decorated()
    assert int2 == int1
    int3 = _random_num_decorated(overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_decorated()
    assert int4 == int3
    _random_num_decorated.clear_cache()

    _random_num_with_arg_decorated.clear_cache()
    int1 = _random_num_with_arg_decorated('a')
    int2 = _random_num_with_arg_decorated('a')
    assert int2 == int1
    int3 = _random_num_with_arg_decorated('a', overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg_decorated('a')
    assert int4 == int3
    _random_num_with_arg_decorated.clear_cache()


@pytest.mark.parametrize('separate_files', [True, False])
def test_ignore_cache(separate_files):
    """Tests that the ignore_cache feature works correctly."""
    _random_num_decorated = _get_decorated_func(
        _random_num, separate_files=separate_files)
    _random_num_with_arg_decorated = _get_decorated_func(
        _random_num_with_arg, separate_files=separate_files)
    _random_num_decorated.clear_cache()
    int1 = _random_num_decorated()
    int2 = _random_num_decorated()
    assert int2 == int1
    int3 = _random_num_decorated(ignore_cache=True)
    assert int3 != int1
    int4 = _random_num_decorated()
    assert int4 != int3
    assert int4 == int1
    _random_num_decorated.clear_cache()

    _random_num_with_arg_decorated.clear_cache()
    int1 = _random_num_with_arg_decorated('a')
    int2 = _random_num_with_arg_decorated('a')
    assert int2 == int1
    int3 = _random_num_with_arg_decorated('a', ignore_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg_decorated('a')
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


@pytest.mark.parametrize('separate_files', [True, False])
def test_pickle_being_calculated(separate_files):
    """Testing pickle core handling of being calculated scenarios."""
    _takes_time_decorated = _get_decorated_func(
        _takes_time, separate_files=separate_files)
    _takes_time_decorated.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_takes_time,
        kwargs={
            'takes_time_func': _takes_time_decorated,
            'res_queue': res_queue,
        }
    )
    thread2 = threading.Thread(
        target=_calls_takes_time,
        kwargs={
            'takes_time_func': _takes_time_decorated,
            'res_queue': res_queue,
        }
    )
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join()
    thread2.join()
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


@pytest.mark.parametrize('separate_files', [True, False])
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
            'being_calc_func': _being_calc_next_time_decorated,
            'res_queue': res_queue,
        }
    )
    thread2 = threading.Thread(
        target=_calls_being_calc_next_time,
        kwargs={
            'being_calc_func': _being_calc_next_time_decorated,
            'res_queue': res_queue,
        }
    )
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join()
    thread2.join()
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


def _bad_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


# _BAD_CACHE_FNAME = '.__main__._bad_cache'
_BAD_CACHE_FNAME = '.tests.test_pickle_core._bad_cache'
_BAD_CACHE_FNAME_SEPARATE_FILES = (
    '.tests.test_pickle_core._bad_cache_'
    f'{hashlib.sha256(pickle.dumps((0.13, 0.02))).hexdigest()}'
)
EXPANDED_CACHIER_DIR = os.path.expanduser(DEF_CACHIER_DIR)
_BAD_CACHE_FPATH = os.path.join(EXPANDED_CACHIER_DIR, _BAD_CACHE_FNAME)
_BAD_CACHE_FPATH_SEPARATE_FILES = os.path.join(
    EXPANDED_CACHIER_DIR, _BAD_CACHE_FNAME_SEPARATE_FILES)
_BAD_CACHE_FPATHS = {
    True: _BAD_CACHE_FPATH_SEPARATE_FILES,
    False: _BAD_CACHE_FPATH,
}


def _calls_bad_cache(bad_cache_func, res_queue, trash_cache, separate_files):
    try:
        res = bad_cache_func(0.13, 0.02, verbose_cache=True)
        if trash_cache:
            with open(_BAD_CACHE_FPATHS[separate_files], 'w') as cache_file:
                cache_file.seek(0)
                cache_file.truncate()
        res_queue.put(res)
    except Exception as exc:  # skipcq: PYL-W0703
        res_queue.put(exc)


def _helper_bad_cache_file(sleeptime, separate_files):
    """Test pickle core handling of bad cache files."""
    _bad_cache_decorated = _get_decorated_func(
        _bad_cache, separate_files=separate_files)
    _bad_cache_decorated.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_bad_cache,
        kwargs={
            'bad_cache_func': _bad_cache_decorated,
            'res_queue': res_queue,
            'trash_cache': True,
            'separate_files': separate_files,
        },
    )
    thread2 = threading.Thread(
        target=_calls_bad_cache,
        kwargs={
            'bad_cache_func': _bad_cache_decorated,
            'res_queue': res_queue,
            'trash_cache': False,
            'separate_files': separate_files,
        },
    )
    thread1.start()
    sleep(sleeptime)
    thread2.start()
    thread1.join()
    thread2.join()
    if not res_queue.qsize() == 2:
        return False
    res1 = res_queue.get()
    if not isinstance(res1, float):
        return False
    res2 = res_queue.get()
    if not (res2 is None) or isinstance(res2, KeyError):
        return False
    return True


# we want this to succeed at leat once
@pytest.mark.xfail
@pytest.mark.parametrize('separate_files', [True, False])
def test_bad_cache_file(separate_files):
    """Test pickle core handling of bad cache files."""
    sleeptimes = [0.1, 0.2, 0.3, 0.5, 0.6, 0.7, 0.8, 1, 1.5, 2]
    sleeptimes = sleeptimes + sleeptimes
    for sleeptime in sleeptimes:
        if _helper_bad_cache_file(sleeptime, separate_files):
            return
    assert False


def _delete_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


# _DEL_CACHE_FNAME = '.__main__._delete_cache'
_DEL_CACHE_FNAME = '.tests.test_pickle_core._delete_cache'
_DEL_CACHE_FNAME_SEPARATE_FILES = (
    '.tests.test_pickle_core._delete_cache_'
    f'{hashlib.sha256(pickle.dumps((0.13, 0.02))).hexdigest()}'
)
_DEL_CACHE_FPATH = os.path.join(EXPANDED_CACHIER_DIR, _DEL_CACHE_FNAME)
_DEL_CACHE_FPATH_SEPARATE_FILES = os.path.join(
    EXPANDED_CACHIER_DIR, _DEL_CACHE_FNAME_SEPARATE_FILES)
_DEL_CACHE_FPATHS = {
    True: _DEL_CACHE_FPATH_SEPARATE_FILES,
    False: _DEL_CACHE_FPATH,
}


def _calls_delete_cache(del_cache_func, res_queue, del_cache, separate_files):
    try:
        # print('in')
        res = del_cache_func(0.13, 0.02)
        # print('out with {}'.format(res))
        if del_cache:
            os.remove(_DEL_CACHE_FPATHS[separate_files])
            # print(os.path.isfile(_DEL_CACHE_FPATH))
        res_queue.put(res)
    except Exception as exc:  # skipcq: PYL-W0703
        # print('found')
        res_queue.put(exc)


def _helper_delete_cache_file(sleeptime, separate_files):
    """Test pickle core handling of missing cache files."""
    _delete_cache_decorated = _get_decorated_func(
        _delete_cache, separate_files=separate_files)
    _delete_cache_decorated.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_delete_cache,
        kwargs={
            'del_cache_func': _delete_cache_decorated,
            'res_queue': res_queue,
            'del_cache': True,
            'separate_files': separate_files,
        },
    )
    thread2 = threading.Thread(
        target=_calls_delete_cache,
        kwargs={
            'del_cache_func': _delete_cache_decorated,
            'res_queue': res_queue,
            'del_cache': False,
            'separate_files': separate_files,
        },
    )
    thread1.start()
    sleep(sleeptime)
    thread2.start()
    thread1.join()
    thread2.join()
    if not res_queue.qsize() == 2:
        return False
    res1 = res_queue.get()
    # print(res1)
    if not isinstance(res1, float):
        return False
    res2 = res_queue.get()
    if not ((isinstance(res2, KeyError)) or ((res2 is None))):
        return False
    return True
    # print(res2)
    # print(type(res2))


@pytest.mark.xfail
@pytest.mark.parametrize('separate_files', [False, True])
def test_delete_cache_file(separate_files):
    """Test pickle core handling of missing cache files."""
    sleeptimes = [0.1, 0.2, 0.3, 0.5, 0.7, 1]
    sleeptimes = sleeptimes * 4
    for sleeptime in sleeptimes:
        if _helper_delete_cache_file(sleeptime, separate_files):
            return
    assert False


@pytest.mark.parametrize('separate_files', [False, True])
def test_clear_being_calculated(separate_files):
    """Test pickle core clear `being calculated` functionality."""
    _takes_time_decorated = _get_decorated_func(
        _takes_time, separate_files=separate_files)
    _takes_time_decorated.clear_being_calculated()


def _error_throwing_func(arg1):
    if not hasattr(_error_throwing_func, 'count'):
        _error_throwing_func.count = 0
    _error_throwing_func.count += 1
    if _error_throwing_func.count > 1:
        raise ValueError("Tiny Rick!")
    return 7


@pytest.mark.parametrize('separate_files', [True, False])
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

CUSTOM_DIR = '~/.exparrot'
EXPANDED_CUSTOM_DIR = os.path.expanduser(CUSTOM_DIR)


def _takes_5_seconds_custom_dir(arg_1, arg_2):
    """Some function."""
    sleep(5)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


@pytest.mark.parametrize('separate_files', [True, False])
def test_pickle_core_custom_cache_dir(separate_files):
    """Basic Pickle core functionality."""
    _takes_5_seconds_custom_dir_decorated = _get_decorated_func(
        _takes_5_seconds_custom_dir, next_time=False,
        cache_dir=CUSTOM_DIR, separate_files=separate_files,
    )
    _takes_5_seconds_custom_dir_decorated.clear_cache()
    _takes_5_seconds_custom_dir_decorated('a', 'b')
    start = time()
    _takes_5_seconds_custom_dir_decorated('a', 'b', verbose_cache=True)
    end = time()
    assert end - start < 1
    _takes_5_seconds_custom_dir_decorated.clear_cache()
    path2test = _takes_5_seconds_custom_dir_decorated.cache_dpath()
    assert path2test == EXPANDED_CUSTOM_DIR


@pytest.mark.parametrize('separate_files', [True, False])
def test_callable_hash_param(separate_files):
    def _hash_params(args, kwargs):
        def _hash(obj):
            if isinstance(obj, pd.core.frame.DataFrame):
                return hashlib.sha256(
                    pd.util.hash_pandas_object(obj).values.tobytes()
                ).hexdigest()
            return obj

        k_args = tuple(map(_hash, args))
        k_kwargs = tuple(
            sorted(
                {k: _hash(v) for k, v in kwargs.items()}.items()
            )
        )
        return k_args + k_kwargs

    @cachier(hash_params=_hash_params, separate_files=separate_files)
    def _params_with_dataframe(*args, **kwargs):
        """Some function."""
        return random()

    _params_with_dataframe.clear_cache()

    df_a = pd.DataFrame.from_dict(dict(a=[0], b=[2], c=[3]))
    df_b = pd.DataFrame.from_dict(dict(a=[0], b=[2], c=[3]))
    value_a = _params_with_dataframe(df_a, 1)
    value_b = _params_with_dataframe(df_b, 1)

    assert value_a == value_b  # same content --> same key

    value_a = _params_with_dataframe(1, df=df_a)
    value_b = _params_with_dataframe(1, df=df_b)

    assert value_a == value_b  # same content --> same key
