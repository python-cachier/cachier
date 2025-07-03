import os
import queue
import sys
import threading
from datetime import datetime, timedelta
from random import random
from time import sleep

import pytest

from cachier import cachier
from cachier.cores.base import RecalculationNeeded
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


class DummyWriteError(Exception):
    pass


@pytest.mark.sql
def test_sql_failed_write(monkeypatch):
    @cachier(backend="sql", sql_engine=SQL_CONN_STR)
    def f(x):
        return x

    f.clear_cache()
    # Simulate DB failure by monkeypatching set_entry
    orig = _SQLCore.set_entry

    def fail_set_entry(self, key, func_res):
        raise DummyWriteError("fail")

    monkeypatch.setattr(_SQLCore, "set_entry", fail_set_entry)
    with pytest.raises(DummyWriteError, match="fail"):
        f(1)
    monkeypatch.setattr(_SQLCore, "set_entry", orig)


@pytest.mark.sql
def test_import_cachier_without_sqlalchemy(monkeypatch):
    """Test that importing cachier works when SQLAlchemy is missing.

    This should work unless SQL core is used.

    """
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


@pytest.mark.pickle
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


@pytest.mark.sql
def test_sqlcore_invalid_sql_engine():
    with pytest.raises(
        ValueError, match="sql_engine must be a SQLAlchemy Engine"
    ):
        _SQLCore(hash_func=None, sql_engine=12345)


@pytest.mark.sql
def test_sqlcore_get_entry_by_key_none_value():
    import pytest

    pytest.importorskip("sqlalchemy")
    import cachier.cores.sql as sql_mod
    from cachier.cores.sql import _SQLCore

    CacheTable = getattr(sql_mod, "CacheTable", None)
    if CacheTable is None:
        pytest.skip("CacheTable not available (SQLAlchemy missing)")
    core = _SQLCore(hash_func=None, sql_engine=SQL_CONN_STR)
    core.set_func(lambda x: x)
    # Insert a row with value=None
    with core._Session() as session:
        session.add(
            CacheTable(
                id="testfunc:abc",
                function_id=core._func_str,
                key="abc",
                value=None,
                timestamp=datetime.now(),
                stale=False,
                processing=False,
                completed=True,
            )
        )
        session.commit()
    key, entry = core.get_entry_by_key("abc")
    assert entry is not None
    assert entry.value is None


@pytest.mark.sql
def test_sqlcore_set_entry_fallback(monkeypatch):
    core = _SQLCore(hash_func=None, sql_engine=SQL_CONN_STR)
    core.set_func(lambda x: x)
    # Monkeypatch insert to not have on_conflict_do_update
    orig_insert = core._Session().execute

    def fake_insert(stmt):
        class FakeInsert:
            def __init__(self):
                pass

        return FakeInsert()

    monkeypatch.setattr(core._Session(), "execute", fake_insert)
    # Should not raise
    core.set_entry("fallback", 123)
    monkeypatch.setattr(core._Session(), "execute", orig_insert)


@pytest.mark.sql
def test_sqlcore_wait_on_entry_calc_recalculation():
    core = _SQLCore(hash_func=None, sql_engine=SQL_CONN_STR)
    core.set_func(lambda x: x)
    with pytest.raises(RecalculationNeeded):
        core.wait_on_entry_calc("missing_key")


@pytest.mark.sql
def test_sqlcore_clear_being_calculated_empty():
    core = _SQLCore(hash_func=None, sql_engine=SQL_CONN_STR)
    core.set_func(lambda x: x)
    # Should not raise even if nothing is being calculated
    core.clear_being_calculated()
