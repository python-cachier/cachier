"""Non-core-specific tests for cachier."""

from __future__ import print_function
import os
import queue
import threading
from random import random
from time import sleep
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
    MONGO_DELTA,
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


@pytest.mark.parametrize(
    "mongetter,stale_after,separate_files",
    [
        (_test_mongetter, MONGO_DELTA, False),
        (None, None, False),
        (None, None, True),
    ]
)
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
        kwargs={'res_queue': res_queue})
    thread2 = threading.Thread(
        target=_calls_wait_for_calc_timeout_fast,
        kwargs={'res_queue': res_queue})

    thread1.start()
    thread2.start()
    sleep(2)
    thread1.join()
    thread2.join()
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2  # Timeout did not kick in, a single call was done


@pytest.mark.parametrize(
    "mongetter,stale_after,separate_files",
    [
        (_test_mongetter, MONGO_DELTA_LONG, False),
        (None, None, False),
        (None, None, True),
    ]
)
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
        kwargs={'res_queue': res_queue})
    thread2 = threading.Thread(
        target=_calls_wait_for_calc_timeout_slow,
        kwargs={'res_queue': res_queue})

    thread1.start()
    thread2.start()
    sleep(1)
    res3 = _wait_for_calc_timeout_slow(1, 2)
    sleep(4)
    thread1.join()
    thread2.join()
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 != res2  # Timeout kicked in.  Two calls were done
    res4 = _wait_for_calc_timeout_slow(1, 2)
    # One of the cached values is returned
    assert res1 == res4 or res2 == res4 or res3 == res4
