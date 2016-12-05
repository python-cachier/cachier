"""Test for the Cachier python package."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

from os.path import (
    realpath,
    dirname
)
from time import (
    time,
    sleep
)
from datetime import timedelta
from random import random
import yaml

from cachier import cachier
from pymongo.mongo_client import MongoClient
try:
    from functools import lru_cache
except ImportError:
    from repoze.lru import lru_cache


CRED_FILE_NAME = 'cachier_test_mongo_cred.yml'

def _get_mongo_cred():
    try:
        current_dir = dirname(realpath(__file__))
        cred_file_path = current_dir + '/' + CRED_FILE_NAME
        with open(cred_file_path, 'r') as mongo_cred_file:
            return yaml.load(mongo_cred_file)
    except FileNotFoundError:
        msg = 'A MongoDB credentials file is missing. '
        msg += 'Please add a file named cachier_test_mongo_cred.yml to the'
        msg += ' tests directory of cachier, pointing to a MongoDB instance'
        msg += 'to be used for testing, with the following format:\n'
        msg += '---- Format begins below ----\n'
        msg += 'host: some_host.com\n'
        msg += 'port: 27017\n'
        msg += 'username: my_username\n'
        msg += 'password: my_password\n'
        msg += '---- Format ended above ----\n'
        raise FileNotFoundError(msg)


def _build_mongo_uri(mongo_cred):
    uri = 'mongodb://{username}:{password}@{host}:{port}'.format(**mongo_cred)
    print(uri)
    return uri


def _get_cachier_db_mongo_client():
    # mongo_uri = _build_mongo_uri(_get_mongo_cred())
    # return MongoClient(host=mongo_uri)
    mongo_cred = _get_mongo_cred()
    client = MongoClient(host=mongo_cred['host'], port=mongo_cred['port'])
    client.cachier_test.authenticate(
        name=mongo_cred['username'],
        password=mongo_cred['password'],
        mechanism='SCRAM-SHA-1'
    )
    return client


@lru_cache(2)
def _mongo_getter():
    return _get_cachier_db_mongo_client()['cachier_test']['cachier_test']


# Pickle core tests

@cachier(next_time=True)
def _test_int_pickling(int_1, int_2):
    """Add the two given ints."""
    return int_1 + int_2


def _test_int_pickling_compare(int_1, int_2):
    """Add the two given ints."""
    return int_1 + int_2


def test_pickle_speed():
    """Test speeds"""
    print("    * Comparing speeds of decorated vs non-decorated functions...")
    num_of_vals = 100
    times = []
    for i in range(1, num_of_vals):
        tic = time()
        _test_int_pickling_compare(i, i + 1)
        toc = time()
        times.append(toc - tic)
    print('      - Non-decorated average = {:.8f}'.format(
        sum(times) / num_of_vals))

    _test_int_pickling.clear_cache()
    times = []
    for i in range(1, num_of_vals):
        tic = time()
        _test_int_pickling(i, i + 1)
        toc = time()
        times.append(toc - tic)
    print('      - Decorated average = {:.8f}'.format(
        sum(times) / num_of_vals))


@cachier(next_time=False)
def _takes_5_seconds(arg_1, arg_2):
    """Some function."""
    sleep(5)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


def test_pickle_core():
    """Basic Pickle core functionality."""
    print("    * Testing basic Pickle core functionality.")
    _takes_5_seconds.clear_cache()
    stringi = _takes_5_seconds('a', 'b')
    start = time()
    stringi = _takes_5_seconds('a', 'b')
    end = time()
    assert end - start < 1


DELTA = timedelta(seconds=3)

@cachier(stale_after=DELTA, next_time=False)
def _stale_after_seconds(arg_1, arg_2):
    """Some function."""
    return random()


def test_stale_after():
    """Testing the stale_after functionality."""
    print("    * Testing the stale_after functionality.")
    _stale_after_seconds.clear_cache()
    val1 = _stale_after_seconds(1, 2)
    val2 = _stale_after_seconds(1, 2)
    val3 = _stale_after_seconds(1, 3)
    assert val1 == val2
    assert val1 != val3
    sleep(3)
    val4 = _stale_after_seconds(1, 2)
    assert val4 != val1


@cachier(stale_after=DELTA, next_time=True)
def _stale_after_next_time(arg_1, arg_2):
    """Some function."""
    return random()


def test_stale_after_next_time():
    """Testing the stale_after with next_time functionality."""
    print("    * Testing the stale_after with next_time functionality.")
    _stale_after_next_time.clear_cache()
    val1 = _stale_after_next_time(1, 2)
    val2 = _stale_after_next_time(1, 2)
    val3 = _stale_after_next_time(1, 3)
    assert val1 == val2
    assert val1 != val3
    sleep(3)
    val4 = _stale_after_next_time(1, 2)
    assert val4 == val1
    val5 = _stale_after_next_time(1, 2)
    assert val5 != val1


@cachier()
def _random_num():
    return random()


@cachier()
def _random_num_with_arg(a):
    # print(a)
    return random()


def test_overwrite_cache():
    """Tests that the overwrite feature works correctly."""
    print("    * Tests that the overwrite feature works correctly.")
    _random_num.clear_cache()
    int1 = _random_num()
    int2 = _random_num()
    assert int2 == int1
    int3 = _random_num(overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 == int3

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg('a')
    int2 = _random_num_with_arg('a')
    assert int2 == int1
    int3 = _random_num_with_arg('a', overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg('a')
    assert int4 == int3


def test_ignore_cache():
    """Tests that the ignore_cache feature works correctly."""
    print("    * Tests that the ignore_cache feature works correctly.")
    _random_num.clear_cache()
    int1 = _random_num()
    int2 = _random_num()
    assert int2 == int1
    int3 = _random_num(ignore_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 != int3
    assert int4 == int1

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg('a')
    int2 = _random_num_with_arg('a')
    assert int2 == int1
    int3 = _random_num_with_arg('a', ignore_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg('a')
    assert int4 != int3
    assert int4 == int1


# Mongo core tests

@cachier(mongetter=_mongo_getter)
def _test_mongo_caching(arg_1, arg_2):
    """Some function."""
    return random() + arg_1 + arg_2


def test_mongo_core():
    """Basic Mongo core functionality."""
    print("    * Testing basic MongoDB core functionality.")
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
    """Basic Mongo core functionality."""
    print("    * Testing MongoDB core stale_after functionality.")
    _stale_after_mongo.clear_cache()
    val1 = _stale_after_mongo(1, 2)
    val2 = _stale_after_mongo(1, 2)
    assert val1 == val2
    sleep(3)
    val3 = _stale_after_mongo(1, 2)
    assert val3 != val1


# Main

def main():
    """Calling all tests."""
    print("\nCalling all tests for Cachier.\n")
    print("--- Calling all Pickle core tests...")
    test_pickle_core()
    test_stale_after()
    test_stale_after_next_time()
    test_overwrite_cache()
    test_ignore_cache()
    test_pickle_speed()
    print("=== All Pickle core tests passed.\n")
    print("--- Calling all MongoDB core tests...")
    test_mongo_core()
    test_mongo_stale_after()
    print("=== All MongoDB core tests passed.\n")
    print("All tests passed.")


if __name__ == "__main__":
    main()
