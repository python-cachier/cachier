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


@pytest.mark.pickle
def test_cache_size_limit_lru_eviction_pickle(tmp_path):
    call_count = 0

    @cachier.cachier(
        backend="pickle",
        cache_dir=tmp_path,
        cache_size_limit="220B",
    )
    def func(x):
        nonlocal call_count
        call_count += 1
        return "a" * 50

    func.clear_cache()
    func(1)
    func(2)
    assert call_count == 2
    func(1)
    assert call_count == 2
    func(3)
    assert call_count == 3
    func(2)
    assert call_count == 4


@pytest.mark.redis
def test_cache_size_limit_lru_eviction_redis():
    import redis

    redis_client = redis.Redis(
        host="localhost", port=6379, decode_responses=False
    )
    call_count = 0

    @cachier.cachier(
        backend="redis",
        redis_client=redis_client,
        cache_size_limit="220B",
    )
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
