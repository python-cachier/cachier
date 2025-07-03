import queue
import threading
from datetime import timedelta
from random import random
from time import sleep
import sys
import os

import pytest

from cachier import cachier
from cachier.cores.sql import _SQLCore

SQL_CONN_STR = os.environ.get("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")


@pytest.mark.sql
def test_sql_core_basic():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x, y):
        return random() + x + y

    f.clear_cache()
    v1 = f(1, 2)
    v2 = f(1, 2)
    assert v1 == v2
    v3 = f(1, 2, cachier__skip_cache=True)
    assert v3 != v1
    v4 = f(1, 2)
    assert v4 == v1
    v5 = f(1, 2, cachier__overwrite_cache=True)
    assert v5 != v1
    v6 = f(1, 2)
    assert v6 == v5


@pytest.mark.sql
def test_sql_core_keywords():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x, y):
        return random() + x + y

    f.clear_cache()
    v1 = f(1, y=2)
    v2 = f(1, y=2)
    assert v1 == v2
    v3 = f(1, y=2, cachier__skip_cache=True)
    assert v3 != v1
    v4 = f(1, y=2)
    assert v4 == v1
    v5 = f(1, y=2, cachier__overwrite_cache=True)
    assert v5 != v1
    v6 = f(1, y=2)
    assert v6 == v5


@pytest.mark.sql
def test_sql_stale_after():
    @cachier(
        backend="sql",
        sql_engine=SQL_CONN_STR,
        stale_after=timedelta(seconds=2),
        next_time=False,
    )
    def f(x, y):
        return random() + x + y

    f.clear_cache()
    v1 = f(1, 2)
    v2 = f(1, 2)
    assert v1 == v2
    sleep(2)
    v3 = f(1, 2)
    assert v3 != v1


@pytest.mark.sql
def test_sql_overwrite_and_skip_cache():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x):
        return random() + x

    f.clear_cache()
    v1 = f(1)
    v2 = f(1)
    assert v1 == v2
    v3 = f(1, cachier__skip_cache=True)
    assert v3 != v1
    v4 = f(1, cachier__overwrite_cache=True)
    assert v4 != v1
    v5 = f(1)
    assert v5 == v4


@pytest.mark.sql
def test_sql_concurrency():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def slow_func(x):
        sleep(1)
        return random() + x

    slow_func.clear_cache()
    res_queue = queue.Queue()

    def call():
        res = slow_func(5)
        res_queue.put(res)

    t1 = threading.Thread(target=call)
    t2 = threading.Thread(target=call)
    t1.start()
    sleep(0.2)
    t2.start()
    t1.join(timeout=3)
    t2.join(timeout=3)
    assert res_queue.qsize() == 2
    r1 = res_queue.get()
    r2 = res_queue.get()
    assert r1 == r2


@pytest.mark.sql
def test_sql_clear_being_calculated():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def slow_func(x):
        sleep(1)
        return random() + x

    slow_func.clear_cache()
    slow_func(1)
    slow_func.clear_being_calculated()
    # Should not raise
    slow_func(1)


@pytest.mark.sql
def test_sql_missing_entry():
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x):
        return x

    f.clear_cache()
    # Should not raise
    assert f(123) == 123


@pytest.mark.sql
def test_sql_failed_write(monkeypatch):
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x):
        return x

    f.clear_cache()
    # Simulate DB failure by monkeypatching set_entry
    orig = _SQLCore.set_entry

    def fail_set_entry(self, key, func_res):
        raise Exception("fail")

    monkeypatch.setattr(_SQLCore, "set_entry", fail_set_entry)
    with pytest.raises(Exception):
        f(1)
    monkeypatch.setattr(_SQLCore, "set_entry", orig)


@pytest.mark.sql
def test_import_cachier_without_sqlalchemy(monkeypatch):
    """Test that importing cachier works when SQLAlchemy is missing.

    This should work unless SQL core is used."""
    # Simulate SQLAlchemy not installed
    modules_backup = sys.modules.copy()
    sys.modules["sqlalchemy"] = None
    sys.modules["sqlalchemy.orm"] = None
    sys.modules["sqlalchemy.engine"] = None
    try:
        import importlib  # noqa: F401
        import cachier  # noqa: F401

        # Should import fine
    finally:
        sys.modules.clear()
        sys.modules.update(modules_backup)


@pytest.mark.sql
def test_sqlcore_importerror_without_sqlalchemy(monkeypatch):
    """Test that using SQL core without SQLAlchemy raises an ImportError."""
    # Simulate SQLAlchemy not installed
    modules_backup = sys.modules.copy()
    sys.modules["sqlalchemy"] = None
    sys.modules["sqlalchemy.orm"] = None
    sys.modules["sqlalchemy.engine"] = None
    try:
        import importlib

        sql_mod = importlib.import_module("cachier.cores.sql")
        with pytest.raises(ImportError) as excinfo:
            sql_mod._SQLCore(hash_func=None, sql_engine="sqlite:///:memory:")
        assert "SQLAlchemy is required" in str(excinfo.value)
    finally:
        sys.modules.clear()
        sys.modules.update(modules_backup)
