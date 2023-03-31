"""Non-core-specific tests for cachier."""

from __future__ import print_function
import functools
import os
import queue
import subprocess  # nosec: B404
import threading
from random import random
from time import sleep, time
import pytest
import cachier as cachier_dir
from cachier import cachier
from cachier.core import (
    MAX_WORKERS_ENVAR_NAME,
    DEFAULT_MAX_WORKERS,
    _max_workers,
    _set_max_workers,
    _get_executor
)
from tests.test_mongo_core import (
    _test_mongetter,
    MONGO_DELTA_LONG,
)


def test_information():
    print("\ncachier version: ", end="")
    print(cachier_dir.__version__)


def test_max_workers():
    """Just call this function for coverage."""
    try:
        del os.environ[MAX_WORKERS_ENVAR_NAME]
    except KeyError:
        pass
    assert _max_workers() == DEFAULT_MAX_WORKERS


def test_get_executor():
    """Just call this function for coverage."""
    _get_executor()
    _get_executor(False)
    _get_executor(True)


def test_set_max_workers():
    """Just call this function for coverage."""
    _set_max_workers(9)


parametrize_keys = 'mongetter,stale_after,separate_files'
parametrize_values = [
    pytest.param(_test_mongetter, MONGO_DELTA_LONG, False,
                 marks=pytest.mark.mongo),
    (None, None, False),
    (None, None, True),
]


@pytest.mark.parametrize(parametrize_keys, parametrize_values)
def test_wait_for_calc_timeout_ok(mongetter, stale_after, separate_files):
    @cachier(
        mongetter=mongetter,
        stale_after=stale_after,
        separate_files=separate_files,
        next_time=False,
        wait_for_calc_timeout=2
    )
    def _wait_for_calc_timeout_fast(arg_1, arg_2):
        """Some function."""
        sleep(1)
        return random() + arg_1 + arg_2

    def _calls_wait_for_calc_timeout_fast(res_queue):
        res = _wait_for_calc_timeout_fast(1, 2)
        res_queue.put(res)

    """ Testing calls that avoid timeouts store the values in cache. """
    _wait_for_calc_timeout_fast.clear_cache()
    val1 = _wait_for_calc_timeout_fast(1, 2)
    val2 = _wait_for_calc_timeout_fast(1, 2)
    assert val1 == val2

    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_wait_for_calc_timeout_fast,
        kwargs={'res_queue': res_queue},
        daemon=True)
    thread2 = threading.Thread(
        target=_calls_wait_for_calc_timeout_fast,
        kwargs={'res_queue': res_queue},
        daemon=True)

    thread1.start()
    thread2.start()
    sleep(2)
    thread1.join(timeout=2)
    thread2.join(timeout=2)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2  # Timeout did not kick in, a single call was done


@pytest.mark.parametrize(parametrize_keys, parametrize_values)
def test_wait_for_calc_timeout_slow(mongetter, stale_after, separate_files):
    @cachier(
        mongetter=mongetter,
        stale_after=stale_after,
        separate_files=separate_files,
        next_time=False,
        wait_for_calc_timeout=2,
    )
    def _wait_for_calc_timeout_slow(arg_1, arg_2):
        sleep(3)
        return random() + arg_1 + arg_2

    def _calls_wait_for_calc_timeout_slow(res_queue):
        res = _wait_for_calc_timeout_slow(1, 2)
        res_queue.put(res)

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
    sleep(1)
    res3 = _wait_for_calc_timeout_slow(1, 2)
    sleep(4)
    thread1.join(timeout=4)
    thread2.join(timeout=4)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 != res2  # Timeout kicked in.  Two calls were done
    res4 = _wait_for_calc_timeout_slow(1, 2)
    # One of the cached values is returned
    assert res1 == res4 or res2 == res4 or res3 == res4


@pytest.mark.parametrize(
    'mongetter,backend',
    [
        (_test_mongetter, 'mongo'),
        (None, 'memory'),
        (None, 'pickle'),
    ]
)
def test_precache_value(mongetter, backend):

    @cachier(backend=backend, mongetter=mongetter)
    def func(arg_1, arg_2):
        """Some function."""
        return arg_1 + arg_2

    result = func.precache_value(2, 2, value_to_cache=5)
    assert result == 5
    result = func(2, 2)
    assert result == 5
    func.clear_cache()
    result = func(2, 2)
    assert result == 4
    result = func.precache_value(2, arg_2=2, value_to_cache=5)
    assert result == 5
    result = func(2, arg_2=2)
    assert result == 5


@pytest.mark.parametrize(
    'mongetter,backend',
    [
        (_test_mongetter, 'mongo'),
        (None, 'memory'),
        (None, 'pickle'),
    ]
)
def test_ignore_self_in_methods(mongetter, backend):

    class TestClass():
        @cachier(backend=backend, mongetter=mongetter)
        def takes_2_seconds(self, arg_1, arg_2):
            """Some function."""
            sleep(2)
            return arg_1 + arg_2

    test_object_1 = TestClass()
    test_object_2 = TestClass()
    test_object_1.takes_2_seconds.clear_cache()
    test_object_2.takes_2_seconds.clear_cache()
    result_1 = test_object_1.takes_2_seconds(1, 2)
    assert result_1 == 3
    start = time()
    result_2 = test_object_2.takes_2_seconds(1, 2)
    end = time()
    assert result_2 == 3
    assert end - start < 1


def test_hash_params_deprecation():
    with pytest.deprecated_call(match='hash_params will be removed'):
        @cachier(hash_params=lambda a, k: 'key')
        def test():
            return 'value'
    assert test() == 'value'


def test_separate_processes():
    test_args = ('python', 'tests/standalone_script.py')
    run_params = {'args': test_args, 'capture_output': True, 'text': True}
    run_process = functools.partial(subprocess.run, **run_params)
    result = run_process()
    assert result.stdout.strip() == 'two 2'
    start = time()
    result = run_process()
    end = time()
    assert result.stdout.strip() == 'two 2'
    assert end - start < 3
