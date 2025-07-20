"""Non-core-specific tests for cachier."""

import functools
import os
import queue
import subprocess  # nosec: B404
import threading
from contextlib import suppress
from random import random
from time import sleep, time

import pytest

import cachier
from cachier.core import (
    DEFAULT_MAX_WORKERS,
    MAX_WORKERS_ENVAR_NAME,
    _get_executor,
    _max_workers,
    _set_max_workers,
)


def test_information():
    print("\ncachier version: ", end="")
    print(cachier.__version__)


def test_max_workers():
    """Just call this function for coverage."""
    with suppress(KeyError):
        del os.environ[MAX_WORKERS_ENVAR_NAME]
    assert _max_workers() == DEFAULT_MAX_WORKERS


def test_get_executor():
    """Just call this function for coverage."""
    _get_executor()
    _get_executor(False)
    _get_executor(True)


def test_set_max_workers():
    """Just call this function for coverage."""
    _set_max_workers(9)


@pytest.mark.seriallocal
@pytest.mark.parametrize("separate_files", [True, False])
def test_wait_for_calc_timeout_ok(separate_files):
    @cachier.cachier(
        separate_files=separate_files,
        next_time=False,
        wait_for_calc_timeout=2,
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
        kwargs={"res_queue": res_queue},
        daemon=True,
    )
    thread2 = threading.Thread(
        target=_calls_wait_for_calc_timeout_fast,
        kwargs={"res_queue": res_queue},
        daemon=True,
    )

    thread1.start()
    thread2.start()
    sleep(2)
    thread1.join(timeout=2)
    thread2.join(timeout=2)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2  # Timeout did not kick in, a single call was done


# @pytest.mark.flaky(reruns=5, reruns_delay=0.5)
@pytest.mark.seriallocal
@pytest.mark.parametrize("separate_files", [True, False])
def test_wait_for_calc_timeout_slow(separate_files):
    # Use unique test parameters to avoid cache conflicts in parallel execution
    import os
    import uuid

    test_id = os.getpid() + int(
        uuid.uuid4().int >> 96
    )  # Unique but deterministic within test
    arg1, arg2 = test_id, test_id + 1

    # In parallel tests, add random delay to reduce thread contention
    if os.environ.get("PYTEST_XDIST_WORKER"):
        import time

        time.sleep(random() * 0.5)  # 0-500ms random delay

    @cachier.cachier(
        separate_files=separate_files,
        next_time=False,
        wait_for_calc_timeout=2,
    )
    def _wait_for_calc_timeout_slow(arg_1, arg_2):
        sleep(2)
        return random() + arg_1 + arg_2

    def _calls_wait_for_calc_timeout_slow(res_queue):
        res = _wait_for_calc_timeout_slow(arg1, arg2)
        res_queue.put(res)

    """Testing for calls timing out to be performed twice when needed."""
    _wait_for_calc_timeout_slow.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_wait_for_calc_timeout_slow,
        kwargs={"res_queue": res_queue},
        daemon=True,
    )
    thread2 = threading.Thread(
        target=_calls_wait_for_calc_timeout_slow,
        kwargs={"res_queue": res_queue},
        daemon=True,
    )

    thread1.start()
    thread2.start()
    sleep(1)
    res3 = _wait_for_calc_timeout_slow(arg1, arg2)
    sleep(3)  # Increased from 4 to give more time for threads to complete
    thread1.join(timeout=10)  # Increased timeout for thread joins
    thread2.join(timeout=10)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 != res2  # Timeout kicked in.  Two calls were done
    res4 = _wait_for_calc_timeout_slow(arg1, arg2)
    # One of the cached values is returned
    assert res1 == res4 or res2 == res4 or res3 == res4


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_precache_value(backend):
    @cachier.cachier(backend=backend)
    def dummy_func(arg_1, arg_2):
        """Some function."""
        return arg_1 + arg_2

    assert dummy_func.precache_value(2, 2, value_to_cache=5) == 5
    assert dummy_func(2, 2) == 5
    dummy_func.clear_cache()
    assert dummy_func(2, 2) == 4
    assert dummy_func.precache_value(2, arg_2=2, value_to_cache=5) == 5
    assert dummy_func(2, arg_2=2) == 5


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_ignore_self_in_methods(backend):
    class DummyClass:
        @cachier.cachier(backend=backend)
        def takes_2_seconds(self, arg_1, arg_2):
            """Some function."""
            sleep(2)
            return arg_1 + arg_2

    test_object_1 = DummyClass()
    test_object_2 = DummyClass()
    test_object_1.takes_2_seconds.clear_cache()
    test_object_2.takes_2_seconds.clear_cache()
    assert test_object_1.takes_2_seconds(1, 2) == 3
    start = time()
    assert test_object_2.takes_2_seconds(1, 2) == 3
    end = time()
    assert end - start < 1


def test_hash_params_deprecation():
    with pytest.deprecated_call(match="hash_params will be removed"):

        @cachier.cachier(hash_params=lambda a, k: "key")
        def test():
            return "value"

    assert test() == "value"


def test_separate_processes():
    test_args = ("python", "tests/standalone_script.py")
    run_params = {"args": test_args, "capture_output": True, "text": True}
    run_process = functools.partial(subprocess.run, **run_params)
    result = run_process()
    assert result.stdout.strip() == "two 2"
    start = time()
    result = run_process()
    end = time()
    assert result.stdout.strip() == "two 2"
    assert end - start < 3


def test_global_disable():
    @cachier.cachier()
    def get_random() -> float:
        return random()

    get_random.clear_cache()
    result_1 = get_random()
    result_2 = get_random()
    cachier.disable_caching()
    assert cachier.config._global_params.caching_enabled is False
    result_3 = get_random()
    cachier.enable_caching()
    assert cachier.config._global_params.caching_enabled is True
    result_4 = get_random()
    assert result_1 == result_2 == result_4
    assert result_1 != result_3


def test_global_disable_function():
    @cachier.cachier()
    def test():
        return True

    cachier.disable_caching()
    try:
        assert test()
    finally:
        cachier.enable_caching()


def test_global_disable_method():
    class Test:
        @cachier.cachier()
        def test(self):
            return True

    cachier.disable_caching()
    try:
        assert Test().test()
    finally:
        cachier.enable_caching()


def test_global_disable_method_with_args():
    class Test:
        @cachier.cachier()
        def test(self, test):
            return test

    cachier.disable_caching()
    try:
        assert Test().test(1) == 1
    finally:
        cachier.enable_caching()


def test_global_disable_method_with_optional_parameters():
    class Test:
        def __init__(self, val):
            self.val = val

        @cachier.cachier()
        def test(self, test=0):
            return self.val + test

    cachier.disable_caching()
    try:
        assert Test(1).test(test=1) == 2
    finally:
        cachier.enable_caching()


def test_global_disable_method_with_args_and_optional_parameters():
    class Test:
        def __init__(self, val):
            self.val = val

        @cachier.cachier()
        def test(self, test1, test2=0):
            return self.val + test1 + test2

    cachier.disable_caching()
    try:
        assert Test(1).test(2, 3) == 6
    finally:
        cachier.enable_caching()


def test_none_not_cached_by_default():
    count = 0

    @cachier.cachier()
    def do_operation():
        nonlocal count
        count += 1
        return None

    do_operation.clear_cache()
    assert count == 0
    do_operation()
    do_operation()
    assert count == 2


def test_allow_caching_none():
    count = 0

    @cachier.cachier(allow_none=True)
    def do_operation():
        nonlocal count
        count += 1
        return None

    do_operation.clear_cache()
    assert count == 0
    do_operation()
    do_operation()
    assert count == 1


def test_identical_inputs():
    count = 0

    @cachier.cachier()
    def dummy_func(a: int, b: int = 2, c: int = 3):
        nonlocal count
        count += 1
        return a + b + c

    dummy_func.clear_cache()
    assert count == 0
    assert dummy_func(1, 2, 3) == 6
    assert dummy_func(1, 2, c=3) == 6
    assert dummy_func(1, b=2, c=3) == 6
    assert dummy_func(a=1, b=2, c=3) == 6
    assert count == 1


def test_list_inputs():
    count = 0

    @cachier.cachier()
    def dummy_func(a: list, b: list = [2]):  # noqa: B006
        nonlocal count
        count += 1
        return a + b

    dummy_func.clear_cache()
    assert count == 0
    assert dummy_func([1]) == [1, 2]
    assert dummy_func([1], [2]) == [1, 2]
    assert dummy_func([1], b=[2]) == [1, 2]
    assert dummy_func(a=[1], b=[2]) == [1, 2]
    assert count == 1


def test_order_independent_kwargs_handling():
    count = 0

    @cachier.cachier()
    def dummy_func(a, b):
        nonlocal count
        count += 1
        return a + b

    dummy_func.clear_cache()
    assert count == 0
    assert dummy_func(a=1, b=2) == 3
    assert dummy_func(a=1, b=2) == 3
    assert dummy_func(b=2, a=1) == 3
    assert count == 1


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_diff_functions_same_args(tmpdir, backend: str):
    count_p = count_m = 0

    @cachier.cachier(cache_dir=tmpdir, backend=backend)
    def fn_plus(a, b=2):
        nonlocal count_p
        count_p += 1
        return a + b

    @cachier.cachier(cache_dir=tmpdir, backend=backend)
    def fn_minus(a, b=2):
        nonlocal count_m
        count_m += 1
        return a - b

    assert count_p == count_m == 0

    for fn, expected in [(fn_plus, 3), (fn_minus, -1)]:
        assert fn(1) == expected
        assert fn(a=1, b=2) == expected
    assert count_p == 1
    assert count_m == 1


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_runtime_handling(tmpdir, backend):
    count_p = count_m = 0

    def fn_plus(a, b=2):
        nonlocal count_p
        count_p += 1
        return a + b

    def fn_minus(a, b=2):
        nonlocal count_m
        count_m += 1
        return a - b

    cachier_ = cachier.cachier(cache_dir=tmpdir, backend=backend)
    assert count_p == count_m == 0

    for fn, expected in [(fn_plus, 3), (fn_minus, -1)]:
        assert cachier_(fn)(1, 2) == expected, f"for {fn.__name__} inline"
        assert cachier_(fn)(a=1, b=2) == expected, f"for {fn.__name__} inline"
    assert count_p == 1
    assert count_m == 1

    for fn, expected in [(fn_plus, 5), (fn_minus, 1)]:
        assert cachier_(fn)(3, 2) == expected, f"for {fn.__name__} inline"
        assert cachier_(fn)(a=3, b=2) == expected, f"for {fn.__name__} inline"
    assert count_p == 2
    assert count_m == 2


def test_partial_handling(tmpdir):
    count_p = count_m = 0

    def fn_plus(a, b=2):
        nonlocal count_p
        count_p += 1
        return a + b

    def fn_minus(a, b=2):
        nonlocal count_m
        count_m += 1
        return a - b

    cachier_ = cachier.cachier(cache_dir=tmpdir)
    assert count_p == count_m == 0

    for fn, expected in [(fn_plus, 3), (fn_minus, -1)]:
        dummy_ = functools.partial(fn, 1)
        assert cachier_(dummy_)() == expected, f"for {fn.__name__} wrapped"

        dummy_ = functools.partial(fn, 1)
        assert cachier_(dummy_)(2) == expected, f"for {fn.__name__} wrapped"

        dummy_ = functools.partial(fn, a=1)
        assert cachier_(dummy_)() == expected, f"for {fn.__name__} wrapped"

        dummy_ = functools.partial(fn, b=2)
        assert cachier_(dummy_)(1) == expected, f"for {fn.__name__} wrapped"

        dummy_ = functools.partial(fn, b=2)
        expected_str = f"for {fn.__name__} wrapped"
        assert cachier_(dummy_)(1, b=2) == expected, expected_str

        assert cachier_(fn)(1, 2) == expected, f"for {fn.__name__} inline"
        assert cachier_(fn)(a=1, b=2) == expected, f"for {fn.__name__} inline"

    assert count_p == 1
    assert count_m == 1


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_raise_exception(tmpdir, backend: str):
    @cachier.cachier(cache_dir=tmpdir, backend=backend, allow_none=True)
    def tmp_test(_):
        raise RuntimeError("always raise")

    with pytest.raises(RuntimeError):
        tmp_test(123)
    with pytest.raises(RuntimeError):
        tmp_test(123)
