import os
import pickle
import time
from dataclasses import replace
from datetime import timedelta

import pytest

import cachier
from cachier import cachier as cachier_dec

_copied_defaults = replace(cachier.get_global_params())


def setup_function() -> None:
    cachier.set_global_params(**vars(_copied_defaults))


def teardown_function() -> None:
    cachier.set_global_params(**vars(_copied_defaults))


@pytest.mark.pickle
def test_cleanup_stale_entries(tmp_path):
    @cachier_dec(
        cache_dir=tmp_path, stale_after=timedelta(seconds=1), cleanup_stale=True, cleanup_interval=timedelta(seconds=0)
    )
    def add(x):
        return x + 1

    add.clear_cache()
    add(1)
    add(2)
    fname = f".{add.__module__}.{add.__qualname__}".replace("<", "_").replace(">", "_")
    cache_path = os.path.join(add.cache_dpath(), fname)
    with open(cache_path, "rb") as fh:
        data = pickle.load(fh)
    assert len(data) == 2
    time.sleep(1.1)
    add(1)
    time.sleep(0.2)
    with open(cache_path, "rb") as fh:
        data = pickle.load(fh)
    assert len(data) == 1
