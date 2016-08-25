"""Test for the Cachier python package."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

from cachier import cachier
from datapy.mongo import get_collection


def _mongo_getter():
    return get_collection('cachier', server_name='production', mode='writing')


@cachier()
def test_int_pickling(int_1, int_2):
    """Add the two given ints."""
    return int_1 + int_2


@cachier(mongetter=_mongo_getter)
def test_mongo_caching(arg_1, arg_2):
    """Some function."""
    return 'arg_1:{}, arg_2:{}'.format(arg_1, arg_2)
