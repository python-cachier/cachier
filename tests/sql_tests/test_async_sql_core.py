"""Tests for async/coroutine support with the SQL core."""

import asyncio
import os
from datetime import timedelta
from random import random

import pytest

from cachier import cachier

SQL_CONN_STR = os.environ.get("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_basic_caching():
    call_count = 0

    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    async def async_sql_basic(x: int) -> float:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return random() + x

    async_sql_basic.clear_cache()
    call_count = 0

    val1 = await async_sql_basic(5)
    val2 = await async_sql_basic(5)
    assert val1 == val2
    assert call_count == 1

    async_sql_basic.clear_cache()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_next_time_returns_stale_then_updates():
    call_count = 0

    @cachier(
        backend="sql",
        sql_engine=SQL_CONN_STR,
        stale_after=timedelta(seconds=0.5),
        next_time=True,
    )
    async def async_sql_next_time(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return call_count

    async_sql_next_time.clear_cache()
    call_count = 0

    val1 = await async_sql_next_time(1)
    assert val1 == 1

    await asyncio.sleep(0.6)

    val2 = await async_sql_next_time(1)
    assert val2 == 1  # stale value returned immediately

    # Give the background calculation time to complete and persist.
    await asyncio.sleep(0.2)
    assert call_count >= 2

    val3 = await async_sql_next_time(1)
    assert val3 == 2
    assert call_count == 2  # cached value used, no extra call

    async_sql_next_time.clear_cache()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_recalculates_after_expiry():
    call_count = 0

    @cachier(
        backend="sql",
        sql_engine=SQL_CONN_STR,
        stale_after=timedelta(seconds=0.2),
        next_time=False,
    )
    async def async_sql_expiry(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return call_count

    async_sql_expiry.clear_cache()
    call_count = 0

    val1 = await async_sql_expiry(1)
    val2 = await async_sql_expiry(1)
    assert val1 == val2 == 1
    assert call_count == 1

    await asyncio.sleep(0.25)

    val3 = await async_sql_expiry(1)
    assert val3 == 2
    assert call_count == 2

    async_sql_expiry.clear_cache()
