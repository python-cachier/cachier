"""Testing the MongoDB core of cachier."""

# standard library imports
import datetime
import hashlib
import platform
import queue
import sys
import threading
from random import random
from time import sleep, time
from urllib.parse import quote_plus

# third-party imports
import pytest
from birch import Birch  # type: ignore[import-not-found]

try:
    import pandas as pd
except (ImportError, ModuleNotFoundError):
    pd = None
    print("pandas is not installed; tests requiring pandas will fail!")

try:
    import pymongo
    from pymongo.errors import OperationFailure
    from pymongo.mongo_client import MongoClient

    from cachier.cores.mongo import MissingMongetter
except (ImportError, ModuleNotFoundError):
    print("pymongo is not installed; tests requiring pymongo will fail!")
    pymongo = None
    OperationFailure = None
    MissingMongetter = None

    # define a mock MongoClient class that will raise an exception
    # on init, warning that pymongo is not installed
    class MongoClient:
        """Mock MongoClient class raising ImportError on missing pymongo."""

        def __init__(self, *args, **kwargs):
            """Initialize the mock MongoClient."""
            raise ImportError("pymongo is not installed!")


try:
    from pymongo_inmemory import MongoClient as InMemoryMongoClient
except (ImportError, ModuleNotFoundError):

    class InMemoryMongoClient:
        """Mock InMemoryMongoClient class.

        Raises an ImportError on missing pymongo_inmemory.

        """

        def __init__(self, *args, **kwargs):
            """Initialize the mock InMemoryMongoClient."""
            raise ImportError("pymongo_inmemory is not installed!")

    print(
        "pymongo_inmemory is not installed; in-memory MongoDB tests will fail!"
    )

# local imports
from cachier import cachier
from cachier.config import CacheEntry
from cachier.cores.base import RecalculationNeeded
from cachier.cores.mongo import _MongoCore

# === Enables testing vs a real MongoDB instance ===


class CfgKey:
    HOST = "TEST_HOST"
    PORT = "TEST_PORT"
    # UNAME = "TEST_USERNAME"
    # PWD = "TEST_PASSWORD"
    # DB = "TEST_DB"
    TEST_VS_DOCKERIZED_MONGO = "TEST_VS_DOCKERIZED_MONGO"


CFG = Birch(
    namespace="cachier",
    defaults={CfgKey.TEST_VS_DOCKERIZED_MONGO: False},
)


# URI_TEMPLATE = "mongodb://myUser:myPassword@localhost:27017/"
URI_TEMPLATE = "mongodb://{host}:{port}?retrywrites=true&w=majority"


def _get_cachier_db_mongo_client():
    host = quote_plus(CFG[CfgKey.HOST])
    port = quote_plus(CFG[CfgKey.PORT])
    # uname = quote_plus(CFG[CfgKey.UNAME])
    # pwd = quote_plus(CFG[CfgKey.PWD])
    # db = quote_plus(CFG[CfgKey.DB])
    uri = f"mongodb://{host}:{port}?retrywrites=true&w=majority"
    return MongoClient(uri)


_COLLECTION_NAME = (
    f"cachier_test_{platform.system()}"
    f"_{'.'.join(map(str, sys.version_info[:3]))}"
)


def _test_mongetter():
    if not hasattr(_test_mongetter, "client"):
        if str(CFG.mget(CfgKey.TEST_VS_DOCKERIZED_MONGO)).lower() == "true":
            print("Using live MongoDB instance for testing.")
            _test_mongetter.client = _get_cachier_db_mongo_client()
        else:
            print("Using in-memory MongoDB instance for testing.")
            _test_mongetter.client = InMemoryMongoClient()
    db_obj = _test_mongetter.client["cachier_test"]
    if _COLLECTION_NAME not in db_obj.list_collection_names():
        db_obj.create_collection(_COLLECTION_NAME)
    return db_obj[_COLLECTION_NAME]


# === Mongo core tests ===


@pytest.mark.mongo
def test_missing_mongetter():
    # Test that the appropriate exception is thrown
    # when forgetting to specify the mongetter.
    with pytest.raises(MissingMongetter):

        @cachier(backend="mongo", mongetter=None)
        def dummy_func():
            pass


@pytest.mark.mongo
def test_information():
    print("\npymongo version: ", end="")
    print(pymongo.__version__)


@pytest.mark.mongo
def test_mongo_index_creation():
    """Basic Mongo core functionality."""

    @cachier(mongetter=_test_mongetter)
    def _decorated(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    collection = _test_mongetter()
    _decorated.clear_cache()
    val1 = _decorated(1, 2)
    val2 = _decorated(1, 2)
    assert val1 == val2
    assert _MongoCore._INDEX_NAME in collection.index_information()


@pytest.mark.mongo
def test_mongo_core_basic():
    """Basic Mongo core functionality."""

    @cachier(mongetter=_test_mongetter)
    def _funci(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    _funci.clear_cache()
    val1 = _funci(1, 2)
    val2 = _funci(1, 2)
    assert val1 == val2
    val3 = _funci(1, 2, cachier__skip_cache=True)
    assert val3 != val1
    val4 = _funci(1, 2)
    assert val4 == val1
    val5 = _funci(1, 2, cachier__overwrite_cache=True)
    assert val5 != val1
    val6 = _funci(1, 2)
    assert val6 == val5


@pytest.mark.mongo
def test_mongo_core_keywords():
    """Basic Mongo core functionality with keyword arguments."""

    @cachier(mongetter=_test_mongetter)
    def _func_keywords(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    _func_keywords.clear_cache()
    val1 = _func_keywords(1, arg_2=2)
    val2 = _func_keywords(1, arg_2=2)
    assert val1 == val2
    val3 = _func_keywords(1, arg_2=2, cachier__skip_cache=True)
    assert val3 != val1
    val4 = _func_keywords(1, arg_2=2)
    assert val4 == val1
    val5 = _func_keywords(1, arg_2=2, cachier__overwrite_cache=True)
    assert val5 != val1
    val6 = _func_keywords(1, arg_2=2)
    assert val6 == val5


@pytest.mark.mongo
def test_mongo_stale_after():
    """Testing MongoDB core stale_after functionality."""

    @cachier(
        mongetter=_test_mongetter,
        stale_after=datetime.timedelta(seconds=3),
        next_time=False,
    )
    def _stale_after_mongo(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    _stale_after_mongo.clear_cache()
    val1 = _stale_after_mongo(1, 2)
    val2 = _stale_after_mongo(1, 2)
    assert val1 == val2
    sleep(3)
    val3 = _stale_after_mongo(1, 2)
    assert val3 != val1


def _calls_takes_time(res_queue):
    @cachier(mongetter=_test_mongetter)
    def _takes_time(arg_1, arg_2):
        """Some function."""
        sleep(3)
        return random() + arg_1 + arg_2

    res = _takes_time(34, 82.3)
    res_queue.put(res)


@pytest.mark.mongo
def test_mongo_being_calculated():
    """Testing MongoDB core handling of being calculated scenarios."""

    @cachier(mongetter=_test_mongetter)
    def _takes_time(arg_1, arg_2):
        """Some function."""
        sleep(3)
        return random() + arg_1 + arg_2

    _takes_time.clear_cache()
    res_queue = queue.Queue()
    thread1 = threading.Thread(
        target=_calls_takes_time, kwargs={"res_queue": res_queue}, daemon=True
    )
    thread2 = threading.Thread(
        target=_calls_takes_time, kwargs={"res_queue": res_queue}, daemon=True
    )
    thread1.start()
    sleep(1)
    thread2.start()
    thread1.join(timeout=4)
    thread2.join(timeout=4)
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


@pytest.mark.mongo
def test_mongo_write_failure():
    """Testing MongoDB core handling of writing failure scenarios."""

    @cachier(mongetter=_bad_mongetter)
    def _func_w_bad_mongo(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    with pytest.raises(OperationFailure):
        _func_w_bad_mongo(1, 2)
    with pytest.raises(OperationFailure):
        _func_w_bad_mongo(1, 2)
    # assert val1 == val2


@pytest.mark.mongo
def test_mongo_clear_being_calculated():
    """Testing MongoDB core clear_being_calculated."""

    @cachier(mongetter=_bad_mongetter)
    def _func_w_bad_mongo(arg_1, arg_2):
        """Some function."""
        return random() + arg_1 + arg_2

    _func_w_bad_mongo.clear_being_calculated()


@pytest.mark.mongo
def test_stalled_mongo_db_cache():
    @cachier(mongetter=_test_mongetter)
    def _stalled_func():
        return 1

    core = _MongoCore(None, _test_mongetter, 0)
    core.set_func(_stalled_func)
    core.clear_cache()
    with pytest.raises(RecalculationNeeded):
        core.wait_on_entry_calc(key=None)


@pytest.mark.mongo
def test_stalled_mong_db_core(monkeypatch):
    def mock_get_entry(self, args, kwargs):
        return "key", CacheEntry(
            _processing=True, value=None, time=None, stale=None
        )

    def mock_get_entry_by_key(self, key):
        return "key", None

    monkeypatch.setattr(
        "cachier.cores.mongo._MongoCore.get_entry", mock_get_entry
    )
    monkeypatch.setattr(
        "cachier.cores.mongo._MongoCore.get_entry_by_key",
        mock_get_entry_by_key,
    )

    @cachier(mongetter=_test_mongetter)
    def _stalled_func():
        return 1

    res = _stalled_func()
    assert res == 1

    def mock_get_entry_2(self, args, kwargs):
        return "key", CacheEntry(
            value=1,
            time=datetime.datetime.now() - datetime.timedelta(seconds=10),
            _processing=True,
            stale=None,
        )

    monkeypatch.setattr(
        "cachier.cores.mongo._MongoCore.get_entry", mock_get_entry_2
    )

    stale_after = datetime.timedelta(seconds=1)

    @cachier(mongetter=_test_mongetter, stale_after=stale_after)
    def _stalled_func_2():
        """Testing stalled function."""
        return 2

    res = _stalled_func_2()
    assert res == 2

    @cachier(
        mongetter=_test_mongetter, stale_after=stale_after, next_time=True
    )
    def _stalled_func_3():
        """Testing stalled function."""
        return 3

    res = _stalled_func_3()
    assert res == 1


@pytest.mark.mongo
def test_callable_hash_param():
    def _hash_func(args, kwargs):
        def _hash(obj):
            if isinstance(obj, pd.core.frame.DataFrame):
                return hashlib.sha256(
                    pd.util.hash_pandas_object(obj).values.tobytes()
                ).hexdigest()
            return obj

        k_args = tuple(map(_hash, args))
        k_kwargs = tuple(
            sorted({k: _hash(v) for k, v in kwargs.items()}.items())
        )
        return k_args + k_kwargs

    @cachier(mongetter=_test_mongetter, hash_func=_hash_func)
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


# ==== Imported from test_general.py ===

MONGO_DELTA_LONG = datetime.timedelta(seconds=10)


@pytest.mark.mongo
@pytest.mark.parametrize("separate_files", [True, False])
def test_wait_for_calc_timeout_ok(separate_files):
    @cachier(
        mongetter=_test_mongetter,
        stale_after=MONGO_DELTA_LONG,
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


@pytest.mark.mongo
@pytest.mark.parametrize("separate_files", [True, False])
@pytest.mark.flaky(reruns=5, reruns_delay=0.5)
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

    @cachier(
        mongetter=_test_mongetter,
        stale_after=MONGO_DELTA_LONG,
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


@pytest.mark.mongo
def test_precache_value():
    @cachier(mongetter=_test_mongetter)
    def dummy_func(arg_1, arg_2):
        """Some function."""
        return arg_1 + arg_2

    assert dummy_func.precache_value(2, 2, value_to_cache=5) == 5
    assert dummy_func(2, 2) == 5
    dummy_func.clear_cache()
    assert dummy_func(2, 2) == 4
    assert dummy_func.precache_value(2, arg_2=2, value_to_cache=5) == 5
    assert dummy_func(2, arg_2=2) == 5


@pytest.mark.mongo
def test_ignore_self_in_methods():
    class DummyClass:
        @cachier(mongetter=_test_mongetter)
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
