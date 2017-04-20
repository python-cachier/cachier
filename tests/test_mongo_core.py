"""Testing the MongoDB core of cachier."""

from random import random
from datetime import timedelta
from time import sleep

from cachier import cachier
from pymongo.mongo_client import MongoClient

_TEST_HOST = 'ds119508.mlab.com'
_TEST_PORT = 19508
_TEST_USERNAME = 'cachier_test'
_TEST_PWD = 'ZGhjO5CQESYJ69U4z65G79YG'


def _get_cachier_db_mongo_client():
    client = MongoClient(host=_TEST_HOST, port=_TEST_PORT)
    client.cachier_test.authenticate(
        name=_TEST_USERNAME,
        password=_TEST_PWD,
        mechanism='SCRAM-SHA-1'
    )
    return client


def _mongo_getter():
    if not hasattr(_mongo_getter, 'client'):
        _mongo_getter.client = _get_cachier_db_mongo_client()
    return _mongo_getter.client['cachier_test']['cachier_test']


# Mongo core tests

@cachier(mongetter=_mongo_getter)
def _test_mongo_caching(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2


def test_mongo_core():
    """Basic Mongo core functionality."""
    _test_mongo_caching.clear_cache()
    val1 = _test_mongo_caching(1, 2)
    val2 = _test_mongo_caching(1, 2)
    assert val1 == val2
    val3 = _test_mongo_caching(1, 2, ignore_cache=True)
    assert val3 != val1
    val4 = _test_mongo_caching(1, 2)
    assert val4 == val1
    val5 = _test_mongo_caching(1, 2, overwrite_cache=True)
    assert val5 != val1
    val6 = _test_mongo_caching(1, 2)
    assert val6 == val5


MONGO_DELTA = timedelta(seconds=3)

@cachier(mongetter=_mongo_getter, stale_after=MONGO_DELTA, next_time=False)
def _stale_after_mongo(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2


def test_mongo_stale_after():
    """Testing MongoDB core stale_after functionality.."""
    _stale_after_mongo.clear_cache()
    val1 = _stale_after_mongo(1, 2)
    val2 = _stale_after_mongo(1, 2)
    assert val1 == val2
    sleep(3)
    val3 = _stale_after_mongo(1, 2)
    assert val3 != val1
