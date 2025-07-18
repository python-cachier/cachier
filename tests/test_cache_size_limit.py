import pytest

import cachier


@pytest.mark.memory
def test_cache_size_limit_lru_eviction():
    call_count = 0

    @cachier.cachier(backend="memory", cache_size_limit="220B")
    def func(x):
        nonlocal call_count
        call_count += 1
        return "a" * 50

    func.clear_cache()
    func(1)
    func(2)
    assert call_count == 2
    func(1)  # access to update LRU order
    assert call_count == 2
    func(3)  # should evict key 2
    assert call_count == 3
    func(2)
    assert call_count == 4
