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
from time import (
    time,
    sleep
)
from datetime import timedelta
from random import random
import threading
try:
    import queue
except ImportError:  # python 2
    import Queue as queue

from cachier import cachier
from cachier.pickle_core import DEF_CACHIER_DIR


# Pickle core tests

@cachier(next_time=False)
def _takes_5_seconds(arg_1, arg_2):
    """Some function."""
    sleep(5)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


def test_pickle_core():
    """Basic Pickle core functionality."""
    _takes_5_seconds.clear_cache()
    _takes_5_seconds('a', 'b')
    start = time()
    _takes_5_seconds('a', 'b', verbose_cache=True)
    end = time()
    assert end - start < 1
    _takes_5_seconds.clear_cache()


SECONDS_IN_DELTA = 3
DELTA = timedelta(seconds=SECONDS_IN_DELTA)


@cachier(stale_after=DELTA, next_time=False)
def _stale_after_seconds(arg_1, arg_2):
    """Some function."""
    return random()


def test_stale_after():
    """Testing the stale_after functionality."""
    _stale_after_seconds.clear_cache()
    val1 = _stale_after_seconds(1, 2)
    val2 = _stale_after_seconds(1, 2)
    val3 = _stale_after_seconds(1, 3)
    assert val1 == val2
    assert val1 != val3
    sleep(3)
    val4 = _stale_after_seconds(1, 2)
    assert val4 != val1
    _stale_after_seconds.clear_cache()


@cachier(stale_after=DELTA, next_time=True)
def _stale_after_next_time(arg_1, arg_2):
    """Some function."""
    return random()


def test_stale_after_next_time():
    """Testing the stale_after with next_time functionality."""
    _stale_after_next_time.clear_cache()
    val1 = _stale_after_next_time(1, 2)
    val2 = _stale_after_next_time(1, 2)
    val3 = _stale_after_next_time(1, 3)
    assert val1 == val2
    assert val1 != val3
    sleep(SECONDS_IN_DELTA + 1)
    val4 = _stale_after_next_time(1, 2)
    assert val4 == val1
    sleep(0.5)
    val5 = _stale_after_next_time(1, 2)
    assert val5 != val1
    _stale_after_next_time.clear_cache()


@cachier()
def _random_num():
    return random()


@cachier()
def _random_num_with_arg(a):
    # print(a)
    return random()


def test_overwrite_cache():
    """Tests that the overwrite feature works correctly."""
    _random_num.clear_cache()
    int1 = _random_num()
    int2 = _random_num()
    assert int2 == int1
    int3 = _random_num(overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 == int3
    _random_num.clear_cache()

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg('a')
    int2 = _random_num_with_arg('a')
    assert int2 == int1
    int3 = _random_num_with_arg('a', overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg('a')
    assert int4 == int3
    _random_num_with_arg.clear_cache()


def test_ignore_cache():
    """Tests that the ignore_cache feature works correctly."""
    _random_num.clear_cache()
    int1 = _random_num()
    int2 = _random_num()
    assert int2 == int1
    int3 = _random_num(ignore_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 != int3
    assert int4 == int1
    _random_num.clear_cache()

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg('a')
    int2 = _random_num_with_arg('a')
    assert int2 == int1
    int3 = _random_num_with_arg('a', ignore_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg('a')
    assert int4 != int3
    assert int4 == int1
    _random_num_with_arg.clear_cache()


@cachier()
def _takes_time(arg_1, arg_2):
    """Some function."""
    sleep(2)  # this has to be enough time for check_calculation to run twice
    return random() + arg_1 + arg_2


def _calls_takes_time(res_queue):
    res = _takes_time(0.13, 0.02)
    res_queue.put(res)


def test_pickle_being_calculated():
    """Testing pickle core handling of being calculated scenarios."""
    _takes_time.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread2 = threading.Thread(
        target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join()
    thread2.join()
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


@cachier(stale_after=timedelta(seconds=1), next_time=True)
def _being_calc_next_time(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


def _calls_being_calc_next_time(res_queue):
    res = _being_calc_next_time(0.13, 0.02)
    res_queue.put(res)


def test_being_calc_next_time():
    """Testing pickle core handling of being calculated scenarios."""
    _takes_time.clear_cache()
    _being_calc_next_time(0.13, 0.02)
    sleep(1.1)
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_being_calc_next_time, kwargs={'res_queue': res_queue})
    thread2 = threading.Thread(
        target=_calls_being_calc_next_time, kwargs={'res_queue': res_queue})
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join()
    thread2.join()
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


@cachier()
def _bad_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


# _BAD_CACHE_FNAME = '.__main__._bad_cache'
_BAD_CACHE_FNAME = '.tests.test_pickle_core._bad_cache'
EXPANDED_CACHIER_DIR = os.path.expanduser(DEF_CACHIER_DIR)
_BAD_CACHE_FPATH = os.path.join(EXPANDED_CACHIER_DIR, _BAD_CACHE_FNAME)


def _calls_bad_cache(res_queue, trash_cache):
    try:
        res = _bad_cache(0.13, 0.02)
        if trash_cache:
            with open(_BAD_CACHE_FPATH, 'w') as cache_file:
                cache_file.seek(0)
                cache_file.truncate()
        res_queue.put(res)
    except Exception as exc:  # skipcq: PYL-W0703
        res_queue.put(exc)


def _helper_bad_cache_file(sleeptime):
    """Test pickle core handling of bad cache files."""
    _bad_cache.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_bad_cache,
        kwargs={'res_queue': res_queue, 'trash_cache': True})
    thread2 = threading.Thread(
        target=_calls_bad_cache,
        kwargs={'res_queue': res_queue, 'trash_cache': False})
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
def test_bad_cache_file():
    """Test pickle core handling of bad cache files."""
    sleeptimes = [0.5, 0.1, 0.2, 0.3, 0.8, 1, 2]
    sleeptimes = sleeptimes + sleeptimes
    for sleeptime in sleeptimes:
        if _helper_bad_cache_file(sleeptime):
            return
    assert False


@cachier()
def _delete_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


# _DEL_CACHE_FNAME = '.__main__._delete_cache'
_DEL_CACHE_FNAME = '.tests.test_pickle_core._delete_cache'
_DEL_CACHE_FPATH = os.path.join(EXPANDED_CACHIER_DIR, _DEL_CACHE_FNAME)


def _calls_delete_cache(res_queue, del_cache):
    try:
        # print('in')
        res = _delete_cache(0.13, 0.02)
        # print('out with {}'.format(res))
        if del_cache:
            # print('deleteing!')
            os.remove(_DEL_CACHE_FPATH)
            # print(os.path.isfile(_DEL_CACHE_FPATH))
        res_queue.put(res)
    except Exception as exc:  # skipcq: PYL-W0703
        # print('found')
        res_queue.put(exc)


def _helper_delete_cache_file(sleeptime):
    """Test pickle core handling of missing cache files."""
    _delete_cache.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_delete_cache,
        kwargs={'res_queue': res_queue, 'del_cache': True})
    thread2 = threading.Thread(
        target=_calls_delete_cache,
        kwargs={'res_queue': res_queue, 'del_cache': False})
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


def test_delete_cache_file():
    """Test pickle core handling of missing cache files."""
    sleeptimes = [0.5, 0.3, 0.1, 0.2, 0.8, 1, 2]
    sleeptimes = sleeptimes + sleeptimes
    for sleeptime in sleeptimes:
        if _helper_delete_cache_file(sleeptime):
            return
    assert False


def test_clear_being_calculated():
    """Test pickle core clear `being calculated` functionality."""
    _takes_time.clear_being_calculated()


@cachier(stale_after=timedelta(seconds=1), next_time=True)
def _error_throwing_func(arg1):
    if not hasattr(_error_throwing_func, 'count'):
        _error_throwing_func.count = 0
    _error_throwing_func.count += 1
    if _error_throwing_func.count > 1:
        raise ValueError("Tiny Rick!")
    return 7


def test_error_throwing_func():
    # with
    res1 = _error_throwing_func(4)
    sleep(1.5)
    res2 = _error_throwing_func(4)
    assert res1 == res2


# test custom cache dir for pickle core

CUSTOM_DIR = '~/.exparrot'
EXPANDED_CUSTOM_DIR = os.path.expanduser(CUSTOM_DIR)


@cachier(next_time=False, cache_dir=CUSTOM_DIR)
def _takes_5_seconds_custom_dir(arg_1, arg_2):
    """Some function."""
    sleep(5)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


def test_pickle_core_custom_cache_dir():
    """Basic Pickle core functionality."""
    _takes_5_seconds_custom_dir.clear_cache()
    _takes_5_seconds_custom_dir('a', 'b')
    start = time()
    _takes_5_seconds_custom_dir('a', 'b', verbose_cache=True)
    end = time()
    assert end - start < 1
    _takes_5_seconds_custom_dir.clear_cache()
    assert _takes_5_seconds_custom_dir.cache_dpath() == EXPANDED_CUSTOM_DIR
