"""Additional tests for base core to improve coverage."""

from unittest.mock import patch

from cachier.cores.base import _BaseCore


class ConcreteCachingCore(_BaseCore):
    """Concrete implementation of _BaseCore for testing."""

    def get_entry_by_key(self, key, reload=False):
        """Retrieve an entry by its key."""
        return key, None

    def set_entry(self, key, func_res):
        """Store an entry in the cache."""
        return True

    def mark_entry_being_calculated(self, key):
        """Mark an entry as being calculated."""
        pass

    def mark_entry_not_calculated(self, key):
        """Mark an entry as not being calculated."""
        pass

    def wait_on_entry_calc(self, key):
        """Wait for an entry calculation to complete."""
        return None

    def clear_cache(self):
        """Clear the cache."""
        pass

    def clear_being_calculated(self):
        """Clear entries that are being calculated."""
        pass

    def delete_stale_entries(self, stale_after):
        """Delete stale entries from the cache."""
        pass


def test_estimate_size_fallback():
    """Test _estimate_size falls back to sys.getsizeof when asizeof fails."""
    # Test lines 101-102: exception handling in _estimate_size
    core = ConcreteCachingCore(hash_func=None, wait_for_calc_timeout=10, entry_size_limit=1000)

    # Mock asizeof to raise exception
    with patch(
        "cachier.cores.base.asizeof.asizeof",
        side_effect=Exception("asizeof failed"),
    ):
        # Should fall back to sys.getsizeof
        size = core._estimate_size("test_value")
        assert size > 0  # sys.getsizeof should return a positive value


def test_should_store_exception():
    """Test _should_store returns True when size estimation fails."""
    # Test lines 109-110: exception handling in _should_store
    core = ConcreteCachingCore(hash_func=None, wait_for_calc_timeout=10, entry_size_limit=1000)

    # Mock both size estimation methods to fail
    patch1 = patch(
        "cachier.cores.base.asizeof.asizeof",
        side_effect=Exception("asizeof failed"),
    )
    patch2 = patch("sys.getsizeof", side_effect=Exception("getsizeof failed"))
    with patch1, patch2:
        # Should return True (allow storage) when size can't be determined
        assert core._should_store("test_value") is True
