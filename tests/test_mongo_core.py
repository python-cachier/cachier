"""Testing the MongoDB core of cachier."""

import sys
import platform
import datetime
from datetime import timedelta
from random import random
from time import sleep
import threading
import queue
from urllib.parse import quote_plus

from birch import Birch
import pytest
import pymongo
import hashlib
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.errors import OperationFailure

from cachier import cachier
from cachier.mongo_core import _MongoCore, RecalculationNeeded


CFG = Birch("cachier")


class CfgKey():
    HOST = "TEST_HOST"
    UNAME = "TEST_USERNAME"
    PWD = "TEST_PASSWORD"
    DB = "TEST_DB"


URI_TEMPLATE = (
    "mongodb+srv://{uname}:{pwd}@{host}/{db}?retrywrites=true&w=majority")


def _get_cachier_db_mongo_client():
    host = quote_plus(CFG[CfgKey.HOST])
    uname = quote_plus(CFG[CfgKey.UNAME])
    pwd = quote_plus(CFG[CfgKey.PWD])
    db = quote_plus(CFG[CfgKey.DB])
    uri = URI_TEMPLATE.format(host=host, uname=uname, pwd=pwd, db=db)
    client = MongoClient(uri)
    return client


_COLLECTION_NAME = 'cachier_test_{}_{}.{}.{}'.format(
    platform.system(), sys.version_info[0], sys.version_info[1],
    sys.version_info[2])


def _test_mongetter():
    if not hasattr(_test_mongetter, 'client'):
        _test_mongetter.client = _get_cachier_db_mongo_client()
    db_obj = _test_mongetter.client['cachier_test']
    if _COLLECTION_NAME not in db_obj.list_collection_names():
        db_obj.create_collection(_COLLECTION_NAME)
    return db_obj[_COLLECTION_NAME]


# === Mongo core tests ===


def test_information():
    print("\npymongo version: ", end="")
    print(pymongo.__version__)


@cachier(mongetter=_test_mongetter)
def _test_mongo_caching(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2


@pytest.mark.mongo
def test_mongo_index_creation():
    """Basic Mongo core functionality."""
    collection = _test_mongetter()
    _test_mongo_caching.clear_cache()
    val1 = _test_mongo_caching(1, 2)
    val2 = _test_mongo_caching(1, 2)
    assert val1 == val2
    assert _MongoCore._INDEX_NAME in collection.index_information()


@pytest.mark.mongo
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
MONGO_DELTA_LONG = timedelta(seconds=10)


@cachier(mongetter=_test_mongetter, stale_after=MONGO_DELTA, next_time=False)
def _stale_after_mongo(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2


@pytest.mark.mongo
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
    sleep(3)
    return random() + arg_1 + arg_2


def _calls_takes_time(res_queue):
    res = _takes_time(34, 82.3)
    res_queue.put(res)


@pytest.mark.mongo
def test_mongo_being_calculated():
    """Testing MongoDB core handling of being calculated scenarios."""
    _takes_time.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread2 = threading.Thread(
        target=_calls_takes_time, kwargs={'res_queue': res_queue})
    thread1.start()
    sleep(1)
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

    def delete_many(self, *args, **kwargs):  # skipcq: PYL-R0201, PYL-W0613
        pass

    def update_many(self, *args, **kwargs):  # skipcq: PYL-R0201, PYL-W0613

        pass

    def update_one(self, *args, **kwargs):  # skipcq: PYL-R0201, PYL-W0613
        raise OperationFailure(Exception())


def _bad_mongetter():
    return _BadMongoCollection(_test_mongetter)


@cachier(mongetter=_bad_mongetter)
def _func_w_bad_mongo(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2


@pytest.mark.mongo
def test_mongo_write_failure():
    """Testing MongoDB core handling of writing failure scenarios."""
    with pytest.raises(OperationFailure):
        val1 = _func_w_bad_mongo(1, 2)
        val2 = _func_w_bad_mongo(1, 2)
        assert val1 == val2


@pytest.mark.mongo
def test_mongo_clear_being_calculated():
    """Testing MongoDB core clear_being_calculated."""
    _func_w_bad_mongo.clear_being_calculated()


@pytest.mark.mongo
def test_stalled_mongo_db_cache():
    @cachier(mongetter=_test_mongetter)
    def _stalled_func():
        return 1
    core = _MongoCore(_test_mongetter, None, False, 0)
    core.set_func(_stalled_func)
    core.clear_cache()
    with pytest.raises(RecalculationNeeded):
        core.wait_on_entry_calc(key=None)


@pytest.mark.mongo
def test_stalled_mong_db_core(monkeypatch):

    def mock_get_entry(self, args, kwargs, hash_params):  # skipcq: PYL-R0201, PYL-W0613  # noqa: E501
        return "key", {'being_calculated': True}

    def mock_get_entry_by_key(self, key):  # skipcq: PYL-R0201, PYL-W0613
        return "key", None

    monkeypatch.setattr(
        "cachier.mongo_core._MongoCore.get_entry", mock_get_entry)
    monkeypatch.setattr(
        "cachier.mongo_core._MongoCore.get_entry_by_key", mock_get_entry_by_key
    )

    @cachier(mongetter=_test_mongetter)
    def _stalled_func():
        return 1
    res = _stalled_func()
    assert res == 1

    def mock_get_entry_2(self, args, kwargs, hash_params):  # skipcq: PYL-W0613
        entry = {
            'being_calculated': True,
            "value": 1,
            "time": datetime.datetime.now() - datetime.timedelta(seconds=10)
        }
        return "key", entry

    monkeypatch.setattr(
        "cachier.mongo_core._MongoCore.get_entry", mock_get_entry_2)

    stale_after = datetime.timedelta(seconds=1)

    @cachier(mongetter=_test_mongetter, stale_after=stale_after)
    def _stalled_func_2():
        """Testing stalled function"""
        return 2

    res = _stalled_func_2()
    assert res == 2


@pytest.mark.mongo
def test_callable_hash_param():

    def _hash_params(args, kwargs):
        def _hash(obj):
            if isinstance(obj, pd.core.frame.DataFrame):
                return hashlib.sha256(
                    pd.util.hash_pandas_object(obj).values.tobytes()
                ).hexdigest()
            return obj

        k_args = tuple(map(_hash, args))
        k_kwargs = tuple(sorted({
            k: _hash(v) for k, v in kwargs.items()}.items()))
        return k_args + k_kwargs

    @cachier(mongetter=_test_mongetter, hash_params=_hash_params)
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
