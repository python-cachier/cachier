"""Test for the Cachier python package."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

import time
import datetime
from cachier import cachier
from datapy.mongo import get_collection


def _mongo_getter():
    return get_collection('cachier', server_name='production', mode='writing')


# Pickle core tests

@cachier()
def test_int_pickling(int_1, int_2):
    """Add the two given ints."""
    return int_1 + int_2


@cachier(next_time=False)
def takes_30_seconds(arg_1, arg_2):
    """Some function."""
    time.sleep(30)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


DELTA = datetime.timedelta(seconds=10)


@cachier(stale_after=DELTA, next_time=False)
def stale_after_seconds(arg_1, arg_2):
    """Some function."""
    return {'arg_1': arg_1, 'arg_2': arg_2}


# Mongo core tests

@cachier(mongetter=_mongo_getter)
def test_mongo_caching(arg_1, arg_2):
    """Some function."""
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)


@cachier(mongetter=_mongo_getter, next_time=False)
def takes_30_seconds_mongo(arg_1, arg_2):
    """Some function."""
    time.sleep(30)
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)

MONGO_DELTA = datetime.timedelta(seconds=30)


@cachier(mongetter=_mongo_getter, stale_after=MONGO_DELTA, next_time=False)
def stale_after_mongo(arg_1, arg_2):
    """Some function."""
    return {'arg_1': arg_1, 'arg_2': arg_2}
