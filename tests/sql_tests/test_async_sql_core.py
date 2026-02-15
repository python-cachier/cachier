"""Tests for async/coroutine support with the SQL core."""

import asyncio
import os
from datetime import timedelta
from random import random

import pytest
import pytest_asyncio

from cachier import cachier


def _get_async_sql_conn_str() -> str:
    conn_str = os.environ.get("SQLALCHEMY_DATABASE_URL")
    if conn_str is None:
        pytest.importorskip("aiosqlite")
        return "sqlite+aiosqlite:///:memory:"
    if conn_str.startswith("sqlite://") and not conn_str.startswith("sqlite+aiosqlite://"):
        pytest.importorskip("aiosqlite")
        return conn_str.replace("sqlite://", "sqlite+aiosqlite://", 1)
    return conn_str


@pytest_asyncio.fixture
async def async_sql_engine():
    pytest.importorskip("sqlalchemy.ext.asyncio")
    pytest.importorskip("greenlet")
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(_get_async_sql_conn_str(), future=True)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_requires_async_engine():
    with pytest.raises(
        TypeError,
        match="Async cached functions with SQL backend require an AsyncEngine sql_engine.",
    ):

        @cachier(backend="sql", sql_engine="sqlite:///:memory:")
        async def async_sql_requires_async_engine(_: int) -> int:
            return 1


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_rejects_async_engine_for_sync_function(async_sql_engine):
    with pytest.raises(
        TypeError,
        match="Async SQL engines require an async cached function.",
    ):

        @cachier(backend="sql", sql_engine=async_sql_engine)
        def sync_sql_rejects_async_engine(_: int) -> int:
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
