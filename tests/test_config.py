"""Additional tests for config module to improve coverage."""

import warnings

import pytest

from cachier.config import get_default_params, set_default_params


def test_set_default_params_deprecated():
    """Test that set_default_params shows deprecation warning."""
    # Test lines 103-111: deprecation warning
    with pytest.warns(DeprecationWarning, match="set_default_params.*deprecated.*set_global_params"):
        set_default_params(stale_after=60)


def test_get_default_params_deprecated():
    """Test that get_default_params shows deprecation warning."""
    # Test lines 143-151: deprecation warning
    with pytest.warns(DeprecationWarning, match="get_default_params.*deprecated.*get_global_params"):
        params = get_default_params()
        assert params is not None