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


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_set_entry_skips_when_too_large(async_sql_engine):
    """aset_entry returns False without storing when _should_store rejects the value."""
    core = _SQLCore(hash_func=None, sql_engine=async_sql_engine, entry_size_limit=1)
    core.set_func(lambda x: x)

    result = await core.aset_entry("too_large_key", "a value that exceeds the tiny limit")
    assert result is False

    _, entry = await core.aget_entry_by_key("too_large_key")
    assert entry is None


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_clear_being_calculated(async_sql_engine):
    """aclear_being_calculated resets all processing flags for this function."""
    core = _SQLCore(hash_func=None, sql_engine=async_sql_engine)
    core.set_func(lambda x: x)

    await core.amark_entry_being_calculated("calc_key_1")
    await core.amark_entry_being_calculated("calc_key_2")

    _, e1 = await core.aget_entry_by_key("calc_key_1")
    _, e2 = await core.aget_entry_by_key("calc_key_2")
    assert e1._processing is True
    assert e2._processing is True

    await core.aclear_being_calculated()

    _, e1_after = await core.aget_entry_by_key("calc_key_1")
    _, e2_after = await core.aget_entry_by_key("calc_key_2")
    assert e1_after._processing is False
    assert e2_after._processing is False

    await core.aclear_cache()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_delete_stale_entries(async_sql_engine):
    """adelete_stale_entries removes old entries and keeps recent ones."""
    core = _SQLCore(hash_func=None, sql_engine=async_sql_engine)
    core.set_func(lambda x: x)

    await core.aset_entry("keep_key", "recent_value")

    # Nothing older than 1 hour exists, so keep_key should survive.
    await core.adelete_stale_entries(timedelta(hours=1))
    _, entry = await core.aget_entry_by_key("keep_key")
    assert entry is not None

    # Delete everything (stale_after=0 s means all entries are older than threshold).
    await core.adelete_stale_entries(timedelta(seconds=0))
    _, entry_gone = await core.aget_entry_by_key("keep_key")
    assert entry_gone is None

    await core.aclear_cache()


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_set_entry_executes_conflict_statement(async_sql_engine, monkeypatch):
    """aset_entry executes the upsert statement when on_conflict_do_update is available."""
    from sqlalchemy.ext.asyncio import AsyncSession

    sentinel_stmt = object()
    seen_stmt: dict = {"value": None}

    class FakeInsert:
        def values(self, **kwargs):
            return self

        def on_conflict_do_update(self, **kwargs):
            return sentinel_stmt

    def fake_insert(_table):
        return FakeInsert()

    async def fake_execute(self, stmt, *args, **kwargs):
        seen_stmt["value"] = stmt

        class DummyResult:
            def scalar_one_or_none(self):
                return None

        return DummyResult()

    monkeypatch.setitem(_SQLCore.aset_entry.__globals__, "insert", fake_insert)
    monkeypatch.setattr(AsyncSession, "execute", fake_execute)

    core = _SQLCore(hash_func=None, sql_engine=async_sql_engine)
    core.set_func(lambda x: x)
    assert await core.aset_entry("upsert_key", 123) is True
    assert seen_stmt["value"] is sentinel_stmt


@pytest.mark.sql
@pytest.mark.asyncio
async def test_async_sql_set_entry_fallback_without_on_conflict(async_sql_engine):
    """aset_entry uses insert/update fallback when on_conflict_do_update is absent."""
    from unittest.mock import patch

    core = _SQLCore(hash_func=None, sql_engine=async_sql_engine)
    core.set_func(lambda x: x)
    await core.aclear_cache()

    _real_hasattr = hasattr

    def _no_on_conflict(obj, name):
        if name == "on_conflict_do_update":
            return False
        return _real_hasattr(obj, name)

    with patch("builtins.hasattr", _no_on_conflict):
        # First call: no existing row → session.add (the else branch).
        assert await core.aset_entry("fb_key", "val1") is True
        # Second call: row already exists → update (the if-row branch).
        assert await core.aset_entry("fb_key", "val2") is True

    _, entry = await core.aget_entry_by_key("fb_key")
    assert entry is not None
    assert entry.value == "val2"

    await core.aclear_cache()
