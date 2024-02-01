import datetime
import os
import queue
import random
import tempfile
import threading
import time

import pytest

import cachier
from tests.test_mongo_core import MONGO_DELTA, _test_mongetter

_default_params = cachier.get_default_params().copy()


def setup_function():
    cachier.set_default_params(**_default_params)


def teardown_function():
    cachier.set_default_params(**_default_params)


def test_hash_func_default_param():

    def slow_hash_func(args, kwds):
        time.sleep(2)
        return 'hash'

    def fast_hash_func(args, kwds):
        return 'hash'

    cachier.set_default_params(hash_func=slow_hash_func)

    @cachier.cachier()
    def global_test_1():
        return None

    @cachier.cachier(hash_func=fast_hash_func)
    def global_test_2():
        return None

    start = time.time()
    global_test_1()
    end = time.time()
    assert end - start > 1
    start = time.time()
    global_test_2()
    end = time.time()
    assert end - start < 1


def test_backend_default_param():

    cachier.set_default_params(backend='memory')

    @cachier.cachier()
    def global_test_1():
        return None

    @cachier.cachier(backend='pickle')
    def global_test_2():
        return None

    assert global_test_1.cache_dpath() is None
    assert global_test_2.cache_dpath() is not None


def test_mongetter_default_param():

    cachier.set_default_params(mongetter=_test_mongetter)

    @cachier.cachier()
    def global_test_1():
        return None

    @cachier.cachier(mongetter=False)
    def global_test_2():
        return None

    assert global_test_1.cache_dpath() is None
    assert global_test_2.cache_dpath() is not None


def test_cache_dir_default_param():

    cachier.set_default_params(cache_dir='/path_1')

    @cachier.cachier()
    def global_test_1():
        return None

    @cachier.cachier(cache_dir='/path_2')
    def global_test_2():
        return None

    assert global_test_1.cache_dpath() == '/path_1'
    assert global_test_2.cache_dpath() == '/path_2'


def test_separate_files_default_param():

    cachier.set_default_params(separate_files=True)

    @cachier.cachier(cache_dir=tempfile.mkdtemp())
    def global_test_1(arg_1, arg_2):
        return arg_1 + arg_2

    @cachier.cachier(cache_dir=tempfile.mkdtemp(), separate_files=False)
    def global_test_2(arg_1, arg_2):
        return arg_1 + arg_2

    global_test_1.clear_cache()
    global_test_1(1, 2)
    global_test_1(3, 4)
    global_test_2.clear_cache()
    global_test_2(1, 2)
    global_test_2(3, 4)

    cache_dir_1 = global_test_1.cache_dpath()
    cache_dir_2 = global_test_2.cache_dpath()
    assert len(os.listdir(cache_dir_1)) == 2
    assert len(os.listdir(cache_dir_2)) == 1


def test_allow_none_default_param():
    cachier.set_default_params(
        allow_none=True,
        separate_files=True,
        verbose_cache=True,
    )
    allow_count = 0
    disallow_count = 0

    @cachier.cachier(cache_dir=tempfile.mkdtemp())
    def allow_none():
        nonlocal allow_count
        allow_count += 1
        return None

    @cachier.cachier(cache_dir=tempfile.mkdtemp(), allow_none=False)
    def disallow_none():
        nonlocal disallow_count
        disallow_count += 1
        return None

    allow_none.clear_cache()
    assert allow_count == 0
    allow_none()
    allow_none()
    assert allow_count == 1

    disallow_none.clear_cache()
    assert disallow_count == 0
    disallow_none()
    disallow_none()
    assert disallow_count == 2


parametrize_keys = 'backend,mongetter'
parametrize_values = [
    pytest.param('pickle', None, marks=pytest.mark.pickle),
    pytest.param('mongo', _test_mongetter, marks=pytest.mark.mongo),
]


@pytest.mark.parametrize(parametrize_keys, parametrize_values)
def test_stale_after_applies_dynamically(backend, mongetter):

    @cachier.cachier(backend=backend, mongetter=mongetter)
    def _stale_after_test(arg_1, arg_2):
        """Some function."""
        return random.random() + arg_1 + arg_2

    cachier.set_default_params(stale_after=MONGO_DELTA)

    _stale_after_test.clear_cache()
    val1 = _stale_after_test(1, 2)
    val2 = _stale_after_test(1, 2)
    assert val1 == val2
    time.sleep(3)
    val3 = _stale_after_test(1, 2)
    assert val3 != val1


@pytest.mark.parametrize(parametrize_keys, parametrize_values)
def test_next_time_applies_dynamically(backend, mongetter):

    NEXT_AFTER_DELTA = datetime.timedelta(seconds=3)

    @cachier.cachier(backend=backend, mongetter=mongetter)
    def _stale_after_next_time(arg_1, arg_2):
        """Some function."""
        return random.random()

    cachier.set_default_params(stale_after=NEXT_AFTER_DELTA, next_time=True)

    _stale_after_next_time.clear_cache()
    val1 = _stale_after_next_time(1, 2)
    val2 = _stale_after_next_time(1, 2)
    val3 = _stale_after_next_time(1, 3)
    assert val1 == val2
    assert val1 != val3
    time.sleep(NEXT_AFTER_DELTA.seconds + 1)
    val4 = _stale_after_next_time(1, 2)
    assert val4 == val1
    time.sleep(0.5)
    val5 = _stale_after_next_time(1, 2)
    assert val5 != val1
    _stale_after_next_time.clear_cache()


@pytest.mark.parametrize(parametrize_keys, parametrize_values)
def test_wait_for_calc_applies_dynamically(backend, mongetter):

    @cachier.cachier(backend=backend, mongetter=mongetter)
    def _wait_for_calc_timeout_slow(arg_1, arg_2):
        time.sleep(3)
        return random.random() + arg_1 + arg_2

    def _calls_wait_for_calc_timeout_slow(res_queue):
        res = _wait_for_calc_timeout_slow(1, 2)
        res_queue.put(res)

    cachier.set_default_params(wait_for_calc_timeout=2)

    """Testing for calls timing out to be performed twice when needed."""
    _wait_for_calc_timeout_slow.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_wait_for_calc_timeout_slow,
        kwargs={'res_queue': res_queue},
        daemon=True)
    thread2 = threading.Thread(
        target=_calls_wait_for_calc_timeout_slow,
        kwargs={'res_queue': res_queue},
        daemon=True)

    thread1.start()
    thread2.start()
    time.sleep(1)
    res3 = _wait_for_calc_timeout_slow(1, 2)
    time.sleep(5)
    thread1.join(timeout=5)
    thread2.join(timeout=5)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 != res2  # Timeout kicked in.  Two calls were done
    res4 = _wait_for_calc_timeout_slow(1, 2)
    # One of the cached values is returned
    assert res1 == res4 or res2 == res4 or res3 == res4
