"""Tests for async support in the pickle core."""

import asyncio
from datetime import timedelta

import pytest

from cachier import cachier


@pytest.mark.pickle
@pytest.mark.asyncio
@pytest.mark.parametrize("separate_files", [False, True])
async def test_async_pickle_basic_caching(tmp_path, separate_files):
    """Ensure async functions are cached by the pickle backend."""
    call_count = 0

    @cachier(
        backend="pickle",
        cache_dir=tmp_path,
        separate_files=separate_files,
    )
    async def async_pickle_cached(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return x + call_count

    async_pickle_cached.clear_cache()
    try:
        value1 = await async_pickle_cached(5)
        value2 = await async_pickle_cached(5)
        assert value1 == value2 == 6
        assert call_count == 1
    finally:
        async_pickle_cached.clear_cache()


@pytest.mark.pickle
@pytest.mark.asyncio
@pytest.mark.parametrize("separate_files", [False, True])
async def test_async_pickle_next_time_returns_stale_then_updates(tmp_path, separate_files):
    """Ensure next_time returns stale pickle values and updates asynchronously."""
    call_count = 0

    @cachier(
        backend="pickle",
        cache_dir=tmp_path,
        separate_files=separate_files,
        stale_after=timedelta(milliseconds=150),
        next_time=True,
    )
    async def async_pickle_next_time(_: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return call_count

    async_pickle_next_time.clear_cache()
    try:
        first = await async_pickle_next_time(1)
        assert first == 1

        await asyncio.sleep(0.2)

        stale = await async_pickle_next_time(1)
        assert stale == 1

        updated = stale
        for _ in range(10):
            await asyncio.sleep(0.05)
            updated = await async_pickle_next_time(1)
            if updated > 1:
                break

        assert updated > 1
        assert call_count >= 2
        await asyncio.sleep(0.1)
    finally:
        async_pickle_next_time.clear_cache()
