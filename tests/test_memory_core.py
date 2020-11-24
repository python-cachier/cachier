"""Test for the in-memory implementation of the  Cachier python package."""

import hashlib
import queue
import threading
from datetime import timedelta
from random import random
from time import sleep, time

import pytest
import pandas as pd

from cachier import cachier


@cachier(backend='memory', next_time=False)
def _takes_2_seconds(arg_1, arg_2):
    """Some function."""
    sleep(2)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


@pytest.mark.memory
def test_memory_core():
    """Basic memory core functionality."""
    _takes_2_seconds.clear_cache()
    _takes_2_seconds('a', 'b')
    start = time()
    _takes_2_seconds('a', 'b', verbose_cache=True)
    end = time()
    assert end - start < 1
    _takes_2_seconds.clear_cache()


SECONDS_IN_DELTA = 3
DELTA = timedelta(seconds=SECONDS_IN_DELTA)


@cachier(backend='memory', stale_after=DELTA, next_time=False)
def _stale_after_seconds(arg_1, arg_2):
    """Some function."""
    return random()


@pytest.mark.memory
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


@cachier(backend='memory', stale_after=DELTA, next_time=True)
def _stale_after_next_time(arg_1, arg_2):
    """Some function."""
    return random()


@pytest.mark.memory
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


@cachier(backend='memory')
def _random_num():
    return random()


@cachier(backend='memory')
def _random_num_with_arg(a):
    # print(a)
    return random()


@pytest.mark.memory
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


@pytest.mark.memory
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


@cachier(backend='memory')
def _takes_time(arg_1, arg_2):
    """Some function."""
    sleep(2)  # this has to be enough time for check_calculation to run twice
    return random() + arg_1 + arg_2


def _calls_takes_time(res_queue):
    res = _takes_time(0.13, 0.02)
    res_queue.put(res)


@pytest.mark.memory
def test_memory_being_calculated():
    """Testing memory core handling of being calculated scenarios."""
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


@cachier(backend='memory', stale_after=timedelta(seconds=1), next_time=True)
def _being_calc_next_time(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


def _calls_being_calc_next_time(res_queue):
    res = _being_calc_next_time(0.13, 0.02)
    res_queue.put(res)


@pytest.mark.memory
def test_being_calc_next_time():
    """Testing memory core handling of being calculated scenarios."""
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


@cachier(backend='memory')
def _bad_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


@cachier(backend='memory')
def _delete_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


@pytest.mark.memory
def test_clear_being_calculated():
    """Test memory core clear `being calculated` functionality."""
    _takes_time.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
            target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread2 = threading.Thread(
            target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread1.start()
    _takes_time.clear_being_calculated()
    sleep(0.5)
    thread2.start()
    thread1.join()
    thread2.join()
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 != res2


@pytest.mark.memory
def test_clear_being_calculated_with_empty_cache():
    """Test memory core clear `being calculated` functionality."""
    _takes_time.clear_cache()
    _takes_time.clear_being_calculated()


@cachier(backend='memory', stale_after=timedelta(seconds=1), next_time=True)
def _error_throwing_func(arg1):
    if not hasattr(_error_throwing_func, 'count'):
        _error_throwing_func.count = 0
    _error_throwing_func.count += 1
    if _error_throwing_func.count > 1:
        raise ValueError("Tiny Rick!")
    return 7


@pytest.mark.memory
def test_error_throwing_func():
    # with
    res1 = _error_throwing_func(4)
    sleep(1.5)
    res2 = _error_throwing_func(4)
    assert res1 == res2


@pytest.mark.memory
def test_callable_hash_param():
    def _hash_params(args, kwargs):
        def _hash(obj):
            if isinstance(obj, pd.core.frame.DataFrame):
                return hashlib.sha256(
                        pd.util.hash_pandas_object(obj).values.tobytes()
                ).hexdigest()
            return obj
        k_args = tuple(map(_hash, args))
        k_kwargs = tuple(sorted(
            {k: _hash(v) for k, v in kwargs.items()}.items()))
        return k_args + k_kwargs

    @cachier(backend='memory', hash_params=_hash_params)
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


if __name__ == '__main__':
    test_memory_being_calculated()
