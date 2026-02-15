"""Shared SQL test configuration and fixtures."""

import os

import pytest
import pytest_asyncio

SQL_CONN_STR = os.environ.get("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")


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
