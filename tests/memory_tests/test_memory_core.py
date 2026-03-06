"""Test for the in-memory implementation of the  Cachier python package."""

import hashlib
import queue
import threading
from datetime import datetime, timedelta
from random import random
from time import sleep, time

import pandas as pd
import pytest

from cachier import cachier
from cachier.config import CacheEntry
from cachier.cores.memory import _MemoryCore


@cachier(backend="memory", next_time=False)
def _takes_2_seconds(arg_1, arg_2):
    """Some function."""
    sleep(2)
    return f"arg_1:{arg_1}, arg_2:{arg_2}"


@pytest.mark.memory
def test_memory_core():
    """Basic memory core functionality."""
    _takes_2_seconds.clear_cache()
    _takes_2_seconds("a", "b")
    start = time()
    _takes_2_seconds("a", "b", cachier__verbose=True)
    end = time()
    assert end - start < 1
    _takes_2_seconds.clear_cache()


@pytest.mark.memory
def test_memory_core_keywords():
    """Basic memory core functionality with keyword arguments."""
    _takes_2_seconds.clear_cache()
    _takes_2_seconds("a", arg_2="b")
    start = time()
    _takes_2_seconds("a", arg_2="b", cachier__verbose=True)
    end = time()
    assert end - start < 1
    _takes_2_seconds.clear_cache()


@pytest.mark.memory
def test_sync_client_over_sync_async_functions():
    @cachier(backend="memory")
    def sync_memory_with_sync_client(_: int) -> int:
        return 1

    @cachier(backend="memory")
    async def async_memory_with_sync_client(_: int) -> int:
        return 1

    assert callable(sync_memory_with_sync_client)
    assert callable(async_memory_with_sync_client)


SECONDS_IN_DELTA = 3
DELTA = timedelta(seconds=SECONDS_IN_DELTA)


@cachier(backend="memory", stale_after=DELTA, next_time=False)
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


@cachier(backend="memory", stale_after=DELTA, next_time=True)
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


@cachier(backend="memory")
def _random_num():
    return random()


@cachier(backend="memory")
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
    int3 = _random_num(cachier__overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 == int3
    _random_num.clear_cache()

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg("a")
    int2 = _random_num_with_arg("a")
    assert int2 == int1
    int3 = _random_num_with_arg("a", cachier__overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg("a")
    assert int4 == int3
    _random_num_with_arg.clear_cache()


@pytest.mark.memory
def test_ignore_cache():
    """Tests that the ignore_cache feature works correctly."""
    _random_num.clear_cache()
    int1 = _random_num()
    int2 = _random_num()
    assert int2 == int1
    int3 = _random_num(cachier__skip_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 != int3
    assert int4 == int1
    _random_num.clear_cache()

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg("a")
    int2 = _random_num_with_arg("a")
    assert int2 == int1
    int3 = _random_num_with_arg("a", cachier__skip_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg("a")
    assert int4 != int3
    assert int4 == int1
    _random_num_with_arg.clear_cache()


@cachier(backend="memory")
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
    thread1 = threading.Thread(target=_calls_takes_time, kwargs={"res_queue": res_queue}, daemon=True)
    thread2 = threading.Thread(target=_calls_takes_time, kwargs={"res_queue": res_queue}, daemon=True)
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join(timeout=3)
    thread2.join(timeout=3)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


@cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=True)
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
    thread1 = threading.Thread(target=_calls_being_calc_next_time, kwargs={"res_queue": res_queue}, daemon=True)
    thread2 = threading.Thread(target=_calls_being_calc_next_time, kwargs={"res_queue": res_queue}, daemon=True)
    thread1.start()
    sleep(0.5)
    thread2.start()
    thread1.join(timeout=3)
    thread2.join(timeout=3)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


@cachier(backend="memory")
def _bad_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


@cachier(backend="memory")
def _delete_cache(arg_1, arg_2):
    """Some function."""
    sleep(1)
    return random() + arg_1 + arg_2


@pytest.mark.memory
def test_clear_being_calculated():
    """Test memory core clear `being calculated` functionality."""
    _takes_time.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(target=_calls_takes_time, kwargs={"res_queue": res_queue}, daemon=True)
    thread2 = threading.Thread(target=_calls_takes_time, kwargs={"res_queue": res_queue}, daemon=True)
    thread1.start()
    _takes_time.clear_being_calculated()
    sleep(0.5)
    thread2.start()
    thread1.join(timeout=3)
    thread2.join(timeout=3)
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 != res2


@pytest.mark.memory
def test_clear_being_calculated_with_empty_cache():
    """Test memory core clear `being calculated` functionality."""
    _takes_time.clear_cache()
    _takes_time.clear_being_calculated()


@cachier(backend="memory", stale_after=timedelta(seconds=1), next_time=True)
def _error_throwing_func(arg1):
    if not hasattr(_error_throwing_func, "count"):
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
    def _hash_func(args, kwargs):
        def _hash(obj):
            if isinstance(obj, pd.core.frame.DataFrame):
                return hashlib.sha256(pd.util.hash_pandas_object(obj).values.tobytes()).hexdigest()
            return obj

        k_args = tuple(map(_hash, args))
        k_kwargs = tuple(sorted({k: _hash(v) for k, v in kwargs.items()}.items()))
        return k_args + k_kwargs

    @cachier(backend="memory", hash_func=_hash_func)
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


@pytest.mark.memory
def test_mark_entry_not_calculated_no_entry():
    """Test mark_entry_not_calculated when entry doesn't exist."""
    # Test line 76: early return when entry not in cache
    core = _MemoryCore(hash_func=None, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    # Should return without error when key not in cache
    core.mark_entry_not_calculated("non_existent_key")


@pytest.mark.memory
def test_wait_on_entry_calc_no_condition():
    """Test wait_on_entry_calc raises error when no condition is set."""
    # Test line 95: RuntimeError when condition is None
    core = _MemoryCore(hash_func=None, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    # Create an entry that's being processed but has no condition
    entry = CacheEntry(
        value="test_value",
        time=datetime.now(),
        stale=False,
        _processing=True,
        _condition=None,  # No condition set
    )

    hash_key = core._hash_func_key("test_key")
    core.cache[hash_key] = entry

    with pytest.raises(RuntimeError, match="No condition set for entry"):
        core.wait_on_entry_calc("test_key")


@pytest.mark.memory
def test_delete_stale_entries():
    """Test delete_stale_entries removes old entries."""
    # Test lines 113-119: stale entry deletion
    core = _MemoryCore(hash_func=None, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    # Add some entries with different ages
    now = datetime.now()

    # Stale entry (2 hours old)
    stale_entry = CacheEntry(
        value="stale_value",
        time=now - timedelta(hours=2),
        stale=False,
        _processing=False,
    )
    core.cache[core._hash_func_key("stale_key")] = stale_entry

    # Fresh entry (30 minutes old)
    fresh_entry = CacheEntry(value="fresh_value", time=now - timedelta(minutes=30), stale=False, _processing=False)
    core.cache[core._hash_func_key("fresh_key")] = fresh_entry

    # Very fresh entry (just created)
    very_fresh_entry = CacheEntry(value="very_fresh_value", time=now, stale=False, _processing=False)
    core.cache[core._hash_func_key("very_fresh_key")] = very_fresh_entry

    # Delete entries older than 1 hour
    core.delete_stale_entries(timedelta(hours=1))

    # Check that only the stale entry was removed
    assert core._hash_func_key("stale_key") not in core.cache
    assert core._hash_func_key("fresh_key") in core.cache
    assert core._hash_func_key("very_fresh_key") in core.cache
    assert len(core.cache) == 2


@pytest.mark.memory
def test_delete_stale_entries_empty_cache():
    """Test delete_stale_entries with empty cache."""
    # Additional test for lines 113-119 with edge case
    core = _MemoryCore(hash_func=None, wait_for_calc_timeout=10)

    # Should not raise error on empty cache
    core.delete_stale_entries(timedelta(hours=1))
    assert len(core.cache) == 0


@pytest.mark.memory
def test_delete_stale_entries_all_stale():
    """Test delete_stale_entries when all entries are stale."""
    # Additional test for lines 113-119
    core = _MemoryCore(hash_func=None, wait_for_calc_timeout=10)

    # Set a mock function
    def mock_func():
        pass

    core.set_func(mock_func)

    now = datetime.now()
    old_time = now - timedelta(days=2)

    # Add only stale entries
    for i in range(5):
        entry = CacheEntry(value=f"value_{i}", time=old_time, stale=False, _processing=False)
        core.cache[core._hash_func_key(f"key_{i}")] = entry

    # Delete entries older than 1 day
    core.delete_stale_entries(timedelta(days=1))

    # All entries should be deleted
    assert len(core.cache) == 0


if __name__ == "__main__":
    test_memory_being_calculated()
