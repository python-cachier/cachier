"""Testing pickling of lists and dicts for the Cachier python package."""

# This file is part of Cachier.
# https://github.com/shaypal5/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

from time import sleep, time

from cachier import cachier


@cachier()
def _list_takes_2_seconds(a):
    """ Numpy cache """
    sleep(2)
    return a * 2


def test_list():
    a = [1, 2, 3]
    _list_takes_2_seconds.clear_cache()
    _list_takes_2_seconds(a)
    start = time()
    _list_takes_2_seconds(a)
    end = time()
    assert end - start < 1

    a = [1, 2, 4]
    start = time()
    _list_takes_2_seconds(a)
    end = time()
    assert end - start > 2.0

    _list_takes_2_seconds.clear_cache()


@cachier()
def _dict_takes_2_seconds(a):
    """ Numpy cache """
    sleep(2)
    return {k: 2 * v for k, v in a.items()}


def test_dict():
    a = {'a': 1, 'b': 2}
    _dict_takes_2_seconds.clear_cache()
    _dict_takes_2_seconds(a)
    start = time()
    _dict_takes_2_seconds(a)
    end = time()
    assert end - start < 1

    a = {'a': 1, 'b': 3}
    start = time()
    _dict_takes_2_seconds(a)
    end = time()
    assert end - start > 2.0

    _dict_takes_2_seconds.clear_cache()
