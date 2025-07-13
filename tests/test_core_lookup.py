"""Testing a few basic cachier interfaces."""

import pytest

from cachier import cachier, get_global_params
from cachier.cores.mongo import MissingMongetter


def test_get_default_params():
    params = get_global_params()
    assert sorted(vars(params).keys()) == [
        "allow_none",
        "backend",
        "cache_dir",
        "caching_enabled",
        "hash_func",
        "mongetter",
        "next_time",
        "pickle_reload",
        "separate_files",
        "stale_after",
        "wait_for_calc_timeout",
    ]


def test_bad_name():
    # Test that the appropriate exception is thrown
    # when an invalid backend is given.
    invalid_core = "bad_core"
    expctd = f"specified an invalid core: {invalid_core}"
    with pytest.raises(ValueError, match=expctd):

        @cachier(backend=invalid_core)
        def dummy_func():
            pass
