"""Testing the MongoDB core of cachier."""

from random import random
from datetime import timedelta
from time import sleep
import threading
import queue

import pytest
from pymongo.mongo_client import MongoClient
from pymongo.errors import OperationFailure

from cachier import cachier
from cachier.mongo_core import _MongoCore


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


def _test_mongetter():
    if not hasattr(_test_mongetter, 'client'):
        _test_mongetter.client = _get_cachier_db_mongo_client()
    return _test_mongetter.client['cachier_test']['cachier_test']


# === Mongo core tests ===

@cachier(mongetter=_test_mongetter)
def _test_mongo_caching(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2

def test_mongo_index_creation():
    """Basic Mongo core functionality."""
    collection = _test_mongetter()
    _test_mongo_caching.clear_cache()
    val1 = _test_mongo_caching(1, 2)
    val2 = _test_mongo_caching(1, 2)
    assert val1 == val2
    assert _MongoCore._INDEX_NAME in collection.index_information()



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

@cachier(mongetter=_test_mongetter, stale_after=MONGO_DELTA, next_time=False)
def _stale_after_mongo(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2

def test_mongo_stale_after():
    """Testing MongoDB core stale_after functionality."""
    _stale_after_mongo.clear_cache()
    val1 = _stale_after_mongo(1, 2)
    val2 = _stale_after_mongo(1, 2)
    assert val1 == val2
    sleep(3)
    val3 = _stale_after_mongo(1, 2)
    assert val3 != val1


@cachier(mongetter=_test_mongetter)
def _takes_time(arg_1, arg_2):
    """Some function."""
    sleep(2)
    return random() + arg_1 + arg_2

def _calls_takes_time(res_queue):
    res = _takes_time(34, 82.3)
    res_queue.put(res)

def test_mongo_being_calculated():
    """Testing MongoDB core handling of being calculated scenarios."""
    _takes_time.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread2 = threading.Thread(
        target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    assert res_queue.qsize() == 2
    res1 = res_queue.get()
    res2 = res_queue.get()
    assert res1 == res2


class _BadMongoCollection:

    def __init__(self, mongetter):
        self.collection = mongetter()
        self.index_information = self.collection.index_information
        self.create_indexes = self.collection.create_indexes
        self.find_one = self.collection.find_one

    def delete_many(self, *args, **kwargs):
        pass

    def update_many(self, *args, **kwargs):
        pass

    def update_one(self, *args, **kwargs):
        raise OperationFailure(Exception())

def _bad_mongetter():
    return _BadMongoCollection(_test_mongetter)

@cachier(mongetter=_bad_mongetter)
def _func_w_bad_mongo(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2

def test_mongo_write_failure():
    """Testing MongoDB core handling of writing failure scenarios."""
    with pytest.raises(OperationFailure):
        val1 = _func_w_bad_mongo(1, 2)
        val2 = _func_w_bad_mongo(1, 2)
        assert val1 == val2


def test_mongo_clear_being_calculated():
    """Testing MongoDB core clear_being_calculated."""
    _func_w_bad_mongo.clear_being_calculated()
