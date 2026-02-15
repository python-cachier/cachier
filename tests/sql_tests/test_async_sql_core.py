"""Tests for async/coroutine support with the SQL core."""

import asyncio
from datetime import timedelta
from random import random

import pytest

from cachier import cachier
from cachier.cores.sql import _SQLCore


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_client_over_sync_async_functions(async_sql_engine):
    @cachier(backend="sql", sql_engine=async_sql_engine)
    async def async_sql_with_async_engine(_: int) -> int:
        return 1

    assert callable(async_sql_with_async_engine)

    with pytest.raises(TypeError, match="Async SQL engines require an async cached function."):

        @cachier(backend="sql", sql_engine=async_sql_engine)
        def sync_sql_with_async_engine(_: int) -> int:
            return 1


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_basic_caching(async_sql_engine):
    call_count = 0

    @cachier(backend="sql", sql_engine=async_sql_engine)
    async def async_sql_basic(x: int) -> float:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return random() + x

    await async_sql_basic.aclear_cache()
    call_count = 0

    val1 = await async_sql_basic(5)
    val2 = await async_sql_basic(5)
    assert val1 == val2
    assert call_count == 1

    await async_sql_basic.aclear_cache()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_next_time_returns_stale_then_updates(async_sql_engine):
    call_count = 0

    @cachier(
        backend="sql",
        sql_engine=async_sql_engine,
        stale_after=timedelta(seconds=0.5),
        next_time=True,
    )
    async def async_sql_next_time(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return call_count

    await async_sql_next_time.aclear_cache()
    call_count = 0

    val1 = await async_sql_next_time(1)
    assert val1 == 1

    await asyncio.sleep(0.6)

    val2 = await async_sql_next_time(1)
    assert val2 == 1

    await asyncio.sleep(0.2)
    assert call_count >= 2

    val3 = await async_sql_next_time(1)
    assert val3 == 2
    assert call_count == 2

    await async_sql_next_time.aclear_cache()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_recalculates_after_expiry(async_sql_engine):
    call_count = 0

    @cachier(
        backend="sql",
        sql_engine=async_sql_engine,
        stale_after=timedelta(seconds=0.2),
        next_time=False,
    )
    async def async_sql_expiry(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return call_count

    await async_sql_expiry.aclear_cache()
    call_count = 0

    val1 = await async_sql_expiry(1)
    val2 = await async_sql_expiry(1)
    assert val1 == val2 == 1
    assert call_count == 1

    await asyncio.sleep(0.25)

    val3 = await async_sql_expiry(1)
    assert val3 == 2
    assert call_count == 2

    await async_sql_expiry.aclear_cache()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_sqlcore_sync_session_requires_sync_engine(async_sql_engine):
    core = _SQLCore(hash_func=None, sql_engine=async_sql_engine)
    with pytest.raises(TypeError, match="Sync SQL operations require a sync SQLAlchemy Engine."):
        core._get_sync_session()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_sqlcore_async_session_requires_async_engine():
    core = _SQLCore(hash_func=None, sql_engine="sqlite:///:memory:")
    core.set_func(lambda x: x)
    with pytest.raises(TypeError, match="Async SQL operations require an AsyncEngine sql_engine."):
        await core._get_async_session()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_sqlcore_async_session_creates_tables_once(async_sql_engine):
    core = _SQLCore(hash_func=None, sql_engine=async_sql_engine)
    core.set_func(lambda x: x)

    class _CountingAsyncEngine:
        def __init__(self, engine):
            self._engine = engine
            self.begin_calls = 0

        def begin(self):
            self.begin_calls += 1
            return self._engine.begin()

    counting_engine = _CountingAsyncEngine(async_sql_engine)
    core._async_engine = counting_engine  # type: ignore[assignment]

    assert core._async_tables_created is False
    first_session = await core._get_async_session()
    assert core._async_tables_created is True
    second_session = await core._get_async_session()
    assert first_session is second_session
    assert counting_engine.begin_calls == 1
