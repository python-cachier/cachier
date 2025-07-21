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


@pytest.mark.mongo
def test_entry_size_limit_not_cached_mongo():
    import pymongo

    mongo_client = pymongo.MongoClient()
    try:
        mongo_db = mongo_client["cachier_test"]
        mongo_collection = mongo_db["test_entry_size_not_cached"]
        
        # Clear collection before test
        mongo_collection.delete_many({})
        
        call_count = 0

        @cachier.cachier(mongetter=lambda: mongo_collection, entry_size_limit="10B")
        def func(x):
            nonlocal call_count
            call_count += 1
            return "a" * 50  # This is larger than 10B

        func.clear_cache()
        val1 = func(1)
        val2 = func(1)
        assert val1 == val2
        assert call_count == 2  # Should be called twice since value is too large to cache
    finally:
        mongo_client.close()


@pytest.mark.mongo
def test_entry_size_limit_cached_mongo():
    import pymongo

    mongo_client = pymongo.MongoClient()
    try:
        mongo_db = mongo_client["cachier_test"]
        mongo_collection = mongo_db["test_entry_size_cached"]
        
        # Clear collection before test
        mongo_collection.delete_many({})
        
        call_count = 0

        @cachier.cachier(mongetter=lambda: mongo_collection, entry_size_limit="1KB")
        def func(x):
            nonlocal call_count
            call_count += 1
            return "small"  # This is smaller than 1KB

        func.clear_cache()
        val1 = func(1)
        val2 = func(1)
        assert val1 == val2
        assert call_count == 1  # Should be called once since value is cached
    finally:
        mongo_client.close()
