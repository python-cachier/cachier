"""Test for the Cachier python package."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

# from os.path import (
#     realpath,
#     dirname
# )
from time import (
    time,
    sleep
)
from datetime import timedelta
from random import random

from cachier import cachier


# Pickle core tests

@cachier(next_time=False)
def _takes_5_seconds(arg_1, arg_2):
    """Some function."""
    sleep(5)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


def test_pickle_core():
    """Basic Pickle core functionality."""
    _takes_5_seconds.clear_cache()
    stringi = _takes_5_seconds('a', 'b')
    start = time()
    stringi = _takes_5_seconds('a', 'b')
    end = time()
    assert end - start < 1
    _takes_5_seconds.clear_cache()


DELTA = timedelta(seconds=3)

@cachier(stale_after=DELTA, next_time=False)
def _stale_after_seconds(arg_1, arg_2):
    """Some function."""
    return random()


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


@cachier(stale_after=DELTA, next_time=True)
def _stale_after_next_time(arg_1, arg_2):
    """Some function."""
    return random()


def test_stale_after_next_time():
    """Testing the stale_after with next_time functionality."""
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
    _stale_after_next_time.clear_cache()



@cachier()
def _random_num():
    return random()


@cachier()
def _random_num_with_arg(a):
    # print(a)
    return random()


def test_overwrite_cache():
    """Tests that the overwrite feature works correctly."""
    _random_num.clear_cache()
    int1 = _random_num()
    int2 = _random_num()
    assert int2 == int1
    int3 = _random_num(overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 == int3
    _random_num.clear_cache()

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg('a')
    int2 = _random_num_with_arg('a')
    assert int2 == int1
    int3 = _random_num_with_arg('a', overwrite_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg('a')
    assert int4 == int3
    _random_num_with_arg.clear_cache()


def test_ignore_cache():
    """Tests that the ignore_cache feature works correctly."""
    _random_num.clear_cache()
    int1 = _random_num()
    int2 = _random_num()
    assert int2 == int1
    int3 = _random_num(ignore_cache=True)
    assert int3 != int1
    int4 = _random_num()
    assert int4 != int3
    assert int4 == int1
    _random_num.clear_cache()

    _random_num_with_arg.clear_cache()
    int1 = _random_num_with_arg('a')
    int2 = _random_num_with_arg('a')
    assert int2 == int1
    int3 = _random_num_with_arg('a', ignore_cache=True)
    assert int3 != int1
    int4 = _random_num_with_arg('a')
    assert int4 != int3
    assert int4 == int1
    _random_num_with_arg.clear_cache()

