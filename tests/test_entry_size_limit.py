import pytest

import cachier


@pytest.mark.memory
def test_entry_size_limit_not_cached():
    call_count = 0

    @cachier.cachier(backend="memory", entry_size_limit="10B")
    def func(x):
        nonlocal call_count
        call_count += 1
        return "a" * 50

    func.clear_cache()
    val1 = func(1)
    val2 = func(1)
    assert val1 == val2
    assert call_count == 2


@pytest.mark.memory
def test_entry_size_limit_cached():
    call_count = 0

    @cachier.cachier(backend="memory", entry_size_limit="1KB")
    def func(x):
        nonlocal call_count
        call_count += 1
        return "small"

    func.clear_cache()
    val1 = func(1)
    val2 = func(1)
    assert val1 == val2
    assert call_count == 1
