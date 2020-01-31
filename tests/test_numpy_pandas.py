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
from time import sleep, time

import numpy as np
import pandas as pd

from cachier import cachier

# Numpy and pandas tests


@cachier()
def _numpy_sum_takes_2_seconds(a):
    """ Numpy cache """
    sleep(2)
    return a.sum()


@cachier()
def _pandas_sum_takes_2_seconds(df):
    """ Numpy cache """
    sleep(2)
    return df.sum()


def test_numpy_narray():
    """Basic numpy core functionality."""
    a = np.zeros(1000)
    _numpy_sum_takes_2_seconds.clear_cache()
    _numpy_sum_takes_2_seconds(a)
    start = time()
    _numpy_sum_takes_2_seconds(a)
    end = time()
    assert end - start < 1

    a[0] = 3
    start = time()
    _numpy_sum_takes_2_seconds(a)
    end = time()
    assert end - start > 2.0

    _numpy_sum_takes_2_seconds.clear_cache()


def test_pandas_dataframe():
    """Basic Pickle core functionality."""
    a = np.zeros(1000)
    df = pd.DataFrame(a)
    _numpy_sum_takes_2_seconds.clear_cache()
    _numpy_sum_takes_2_seconds(df)
    start = time()
    _numpy_sum_takes_2_seconds(df)
    end = time()
    assert end - start < 1
    _numpy_sum_takes_2_seconds.clear_cache()

    df.iloc[0, 0] = 3
    start = time()
    _numpy_sum_takes_2_seconds(a)
    end = time()
    assert end - start > 2.0
