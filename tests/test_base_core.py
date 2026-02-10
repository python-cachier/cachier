"""Additional tests for base core to improve coverage."""

from datetime import timedelta
from unittest.mock import patch

import pytest

from cachier.cores.base import _BaseCore


class ConcreteCachingCore(_BaseCore):
    """Concrete implementation of _BaseCore for testing."""

    def __init__(self, hash_func, wait_for_calc_timeout, entry_size_limit=None):
        """Initialize test core state."""
        super().__init__(hash_func, wait_for_calc_timeout, entry_size_limit)
        self.last_set = None
        self.last_mark_calc = None
        self.last_mark_not_calc = None
        self.last_wait_key = None
        self.clear_cache_called = False
        self.clear_being_calculated_called = False
        self.last_deleted_stale_after = None

    def get_entry_by_key(self, key, reload=False):
        """Retrieve an entry by its key."""
        return key, None

    def set_entry(self, key, func_res):
        """Store an entry in the cache."""
        self.last_set = (key, func_res)
        return True

    def mark_entry_being_calculated(self, key):
        """Mark an entry as being calculated."""
        self.last_mark_calc = key

    def mark_entry_not_calculated(self, key):
        """Mark an entry as not being calculated."""
        self.last_mark_not_calc = key

    def wait_on_entry_calc(self, key):
        """Wait for an entry calculation to complete."""
        self.last_wait_key = key
        return None

    def clear_cache(self):
        """Clear the cache."""
        self.clear_cache_called = True

    def clear_being_calculated(self):
        """Clear entries that are being calculated."""
        self.clear_being_calculated_called = True

    def delete_stale_entries(self, stale_after):
        """Delete stale entries from the cache."""
        self.last_deleted_stale_after = stale_after


def test_estimate_size_fallback():
    """Test _estimate_size falls back to sys.getsizeof when asizeof fails."""
    # Test lines 101-102: exception handling in _estimate_size
    core = ConcreteCachingCore(hash_func=None, wait_for_calc_timeout=10, entry_size_limit=1000)

    # Mock asizeof to raise exception
    with patch("cachier.cores.base.asizeof.asizeof", side_effect=Exception("asizeof failed")):
        # Should fall back to sys.getsizeof
        size = core._estimate_size("test_value")
        assert size > 0  # sys.getsizeof should return a positive value


def test_should_store_exception():
    """Test _should_store returns True when size estimation fails."""
    # Test lines 109-110: exception handling in _should_store
    core = ConcreteCachingCore(hash_func=None, wait_for_calc_timeout=10, entry_size_limit=1000)

    # Mock both size estimation methods to fail
    patch1 = patch("cachier.cores.base.asizeof.asizeof", side_effect=Exception("asizeof failed"))
    patch2 = patch("sys.getsizeof", side_effect=Exception("getsizeof failed"))
    with patch1, patch2:
        # Should return True (allow storage) when size can't be determined
        assert core._should_store("test_value") is True


@pytest.mark.asyncio
async def test_base_core_async_default_wrappers():
    """Test async default wrappers delegate correctly to sync methods."""
    core = ConcreteCachingCore(hash_func=None, wait_for_calc_timeout=10, entry_size_limit=1000)

    async def fake_aset_entry(key, value):
        core.last_set = (key, value)
        return True

    core.aset_entry = fake_aset_entry

    value = await core.aprecache_value(args=(), kwds={"x": 1}, value_to_cache=42)
    assert value == 42
    assert core.last_set is not None

    key, entry = await core.aget_entry_by_key("abc")
    assert key == "abc"
    assert entry is None

    await core.amark_entry_being_calculated("k1")
    await core.amark_entry_not_calculated("k1")
    assert core.last_mark_calc == "k1"
    assert core.last_mark_not_calc == "k1"

    await core.await_on_entry_calc("k2")
    assert core.last_wait_key == "k2"

    await core.aclear_cache()
    assert core.clear_cache_called is True

    await core.aclear_being_calculated()
    assert core.clear_being_calculated_called is True

    stale_after = timedelta(seconds=5)
    await core.adelete_stale_entries(stale_after)
    assert core.last_deleted_stale_after == stale_after
