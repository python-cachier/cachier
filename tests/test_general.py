"""Non-core-specific tests for cachier."""

import os

from cachier.core import (
    MAX_WORKERS_ENVAR_NAME,
    DEFAULT_MAX_WORKERS,
    _max_workers,
    _set_max_workers,
    _get_executor
)


def test_max_workers():
    """Just call this function for coverage."""
    try:
        del os.environ[MAX_WORKERS_ENVAR_NAME]
    except KeyError:
        pass
    assert _max_workers() == DEFAULT_MAX_WORKERS


def test_get_executor():
    """Just call this function for coverage."""
    _get_executor()
    _get_executor(False)
    _get_executor(True)


def test_set_max_workers():
    """Just call this function for coverage."""
    _set_max_workers(9)
