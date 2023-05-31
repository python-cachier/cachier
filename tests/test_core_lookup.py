"""Testing a few basic cachier interfaces."""

from cachier import cachier, get_default_params
from cachier.core import MissingMongetter


def test_get_default_params():
    params = get_default_params()
    assert tuple(sorted(params)) == (
        'allow_none',
        'backend',
        'cache_dir',
        'caching_enabled',
        'hash_func',
        'mongetter',
        'next_time',
        'pickle_reload',
        'separate_files',
        'stale_after',
        'wait_for_calc_timeout',
    )


def test_bad_name():
    """Test that the appropriate exception is thrown when an invalid backend
    is given."""

    name = 'nope'
    try:
        @cachier(backend=name)
        def func():
            pass
    except ValueError as e:
        assert name in e.args[0]
    else:
        assert False


def test_missing_mongetter():
    """Test that the appropriate exception is thrown when forgetting to
    specify the mongetter."""
    try:
        @cachier(backend='mongo', mongetter=None)
        def func():
            pass
    except MissingMongetter:
        assert True
    else:
        assert False
