"""Testing a few basic cachier interfaces."""
import pytest

from cachier import cachier, get_default_params
from cachier.core import MissingMongetter


def test_get_default_params():
    params = get_default_params()
    assert tuple(sorted(params)) == (
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
    )


def test_bad_name(name="nope"):
    """Test that the appropriate exception is thrown when an invalid backend is
    given."""

    with pytest.raises(ValueError, match=f"specified an invalid core: {name}"):

        @cachier(backend=name)
        def dummy_func():
            pass


def test_missing_mongetter():
    """Test that the appropriate exception is thrown when forgetting to specify
    the mongetter."""
    with pytest.raises(MissingMongetter):

        @cachier(backend="mongo", mongetter=None)
        def dummy_func():
            pass
