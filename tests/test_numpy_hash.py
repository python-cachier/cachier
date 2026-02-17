"""Tests for NumPy-aware default hash behavior."""

from datetime import timedelta

import pytest

from cachier import cachier

np = pytest.importorskip("numpy")


@pytest.mark.parametrize("backend", ["memory", "pickle"])
def test_default_hash_func_uses_array_content_for_cache_keys(backend, tmp_path):
    """Verify equal arrays map to a cache hit and different arrays miss."""
    call_count = 0

    decorator_kwargs = {"backend": backend, "stale_after": timedelta(seconds=120)}
    if backend == "pickle":
        decorator_kwargs["cache_dir"] = tmp_path

    @cachier(**decorator_kwargs)
    def array_sum(values):
        nonlocal call_count
        call_count += 1
        return int(values.sum())

    arr = np.arange(100_000, dtype=np.int64)
    arr_copy = arr.copy()
    changed = arr.copy()
    changed[-1] = -1

    first = array_sum(arr)
    assert call_count == 1

    second = array_sum(arr_copy)
    assert second == first
    assert call_count == 1

    third = array_sum(changed)
    assert third != first
    assert call_count == 2

    array_sum.clear_cache()
