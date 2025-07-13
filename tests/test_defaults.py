import datetime
import os
import queue
import random
import threading
import time
from dataclasses import replace

import pytest

import cachier
from tests.test_mongo_core import _test_mongetter

MONGO_DELTA = datetime.timedelta(seconds=3)
_copied_defaults = replace(cachier.get_global_params())


def setup_function():
    cachier.set_global_params(**vars(_copied_defaults))


def teardown_function():
    cachier.set_global_params(**vars(_copied_defaults))


def test_hash_func_default_param():
    def slow_hash_func(args, kwds):
        time.sleep(2)
        return "hash"

    def fast_hash_func(args, kwds):
        return "hash"

    cachier.set_global_params(hash_func=slow_hash_func)

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
    cachier.set_global_params(backend="memory")

    @cachier.cachier()
    def global_test_1():
        return None

    @cachier.cachier(backend="pickle")
    def global_test_2():
        return None

    assert global_test_1.cache_dpath() is None
    assert global_test_2.cache_dpath() is not None


@pytest.mark.mongo
def test_mongetter_default_param():
    cachier.set_global_params(mongetter=_test_mongetter)

    @cachier.cachier()
    def global_test_1():
        return None

    @cachier.cachier(mongetter=False)
    def global_test_2():
        return None

    assert global_test_1.cache_dpath() is None
    assert global_test_2.cache_dpath() is not None


def test_cache_dir_default_param(tmpdir):
    cachier.set_global_params(cache_dir=tmpdir / "1")

    @cachier.cachier()
    def global_test_1():
        return None

    @cachier.cachier(cache_dir=tmpdir / "2")
    def global_test_2():
        return None

    assert global_test_1.cache_dpath() == str(tmpdir / "1")
    assert global_test_2.cache_dpath() == str(tmpdir / "2")


def test_cache_dir_respects_xdg(monkeypatch, tmpdir):
    xdg_path = str(tmpdir / "xdg_home")
    monkeypatch.setenv("XDG_CACHE_HOME", xdg_path)

    expected_path = os.path.join(xdg_path, "cachier")

    @cachier.cachier(backend="pickle")
    def my_func():
        return 123

    actual_path = my_func.cache_dpath()
    assert str(actual_path) == expected_path

    my_func()  # Create cache file
    assert os.path.exists(expected_path)
    files = os.listdir(expected_path) if os.path.exists(expected_path) else []
    assert any(os.path.isfile(os.path.join(expected_path, f)) for f in files)


def test_cache_dir_default_fallback(monkeypatch):
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)

    @cachier.cachier()
    def my_func():
        return 123

    expected_path = os.path.expanduser("~/.cachier/")
    assert my_func.cache_dpath().startswith(expected_path)


def test_lazy_cache_dir_eq_triggered():
    default_dir = cachier.get_global_params().cache_dir

    assert default_dir == str(default_dir)
    assert default_dir != "/some/random/path"

def test_separate_files_default_param(tmpdir):
    cachier.set_global_params(separate_files=True)

    @cachier.cachier(cache_dir=tmpdir / "1")
    def global_test_1(arg_1, arg_2):
        return arg_1 + arg_2

    @cachier.cachier(cache_dir=tmpdir / "2", separate_files=False)
    def global_test_2(arg_1, arg_2):
        return arg_1 + arg_2

    global_test_1(1, 2)
    global_test_1(3, 4)
    global_test_2(1, 2)
    global_test_2(3, 4)

    assert len(os.listdir(global_test_1.cache_dpath())) == 2
    assert len(os.listdir(global_test_2.cache_dpath())) == 1


def test_allow_none_default_param(tmpdir):
    cachier.set_global_params(
        allow_none=True,
        separate_files=True,
        verbose_cache=True,
    )
    allow_count = disallow_count = 0

    @cachier.cachier(cache_dir=tmpdir)
    def allow_none():
        nonlocal allow_count
        allow_count += 1
        return None

    @cachier.cachier(cache_dir=tmpdir, allow_none=False)
    def disallow_none():
        nonlocal disallow_count
        disallow_count += 1
        return None

    assert allow_count == 0
    allow_none()
    allow_none()
    assert allow_count == 1

    assert disallow_count == 0
    disallow_none()
    disallow_none()
    assert disallow_count == 2

    disallow_none(cachier__allow_none=True)
    disallow_none(cachier__allow_none=True)
    assert disallow_count == 2


PARAMETRIZE_TEST = (
    "backend,mongetter",
    [
        pytest.param("pickle", None, marks=pytest.mark.pickle),
        pytest.param("mongo", _test_mongetter, marks=pytest.mark.mongo),
    ],
)


@pytest.mark.parametrize(*PARAMETRIZE_TEST)
def test_stale_after_applies_dynamically(backend, mongetter):
    @cachier.cachier(backend=backend, mongetter=mongetter)
    def _stale_after_test(arg_1, arg_2):
        """Some function."""
        return random.random() + arg_1 + arg_2

    cachier.set_global_params(stale_after=MONGO_DELTA)

    _stale_after_test.clear_cache()
    val1 = _stale_after_test(1, 2)
    val2 = _stale_after_test(1, 2)
    assert val1 == val2
    time.sleep(3)
    val3 = _stale_after_test(1, 2)
    assert val3 != val1


@pytest.mark.parametrize(*PARAMETRIZE_TEST)
def test_next_time_applies_dynamically(backend, mongetter):
    NEXT_AFTER_DELTA = datetime.timedelta(seconds=3)

    @cachier.cachier(backend=backend, mongetter=mongetter)
    def _stale_after_next_time(arg_1, arg_2):
        """Some function."""
        return random.random()

    cachier.set_global_params(stale_after=NEXT_AFTER_DELTA, next_time=True)

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


@pytest.mark.parametrize(*PARAMETRIZE_TEST)
def test_wait_for_calc_applies_dynamically(backend, mongetter):
    """Testing for calls timing out to be performed twice when needed."""

    @cachier.cachier(backend=backend, mongetter=mongetter)
    def _wait_for_calc_timeout_slow(arg_1, arg_2):
        time.sleep(3)
        return random.random() + arg_1 + arg_2

    def _calls_wait_for_calc_timeout_slow(res_queue):
        res = _wait_for_calc_timeout_slow(1, 2)
        res_queue.put(res)

    cachier.set_global_params(wait_for_calc_timeout=2)
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


def test_default_kwargs_handling():
    count = 0

    @cachier.cachier()
    def dummy_func(a, b=2):
        nonlocal count
        count += 1
        return a + b

    dummy_func.clear_cache()
    assert count == 0
    assert dummy_func(1) == 3
    assert dummy_func(a=1) == 3
    assert dummy_func(a=1, b=2) == 3
    assert count == 1


def test_deprecated_func_kwargs():
    count = 0

    @cachier.cachier()
    def dummy_func(a, b=2):
        nonlocal count
        count += 1
        return a + b

    dummy_func.clear_cache()
    assert count == 0
    with pytest.deprecated_call(
        match="`verbose_cache` is deprecated and will be removed"
    ):
        assert dummy_func(1, verbose_cache=True) == 3
    assert count == 1
    with pytest.deprecated_call(
        match="`ignore_cache` is deprecated and will be removed"
    ):
        assert dummy_func(1, ignore_cache=True) == 3
    assert count == 2
    with pytest.deprecated_call(
        match="`overwrite_cache` is deprecated and will be removed"
    ):
        assert dummy_func(1, overwrite_cache=True) == 3
    assert count == 3
