"""A SQLAlchemy-based caching core for cachier."""

import pickle
import threading
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Tuple, Union, cast

try:
    from sqlalchemy import (
        Boolean,
        Column,
        DateTime,
        Index,
        LargeBinary,
        String,
        and_,
        create_engine,
        delete,
        insert,
        select,
        update,
    )
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
    from sqlalchemy.orm import declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from .._types import HashFunc
from ..config import CacheEntry
from .base import RecalculationNeeded, _BaseCore, _get_func_str

if SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class CacheTable(Base):  # type: ignore[misc, valid-type]
        """SQLAlchemy model for cachier cache entries."""

        __tablename__ = "cachier_cache"
        id = Column(String, primary_key=True)
        function_id = Column(String, index=True, nullable=False)
        key = Column(String, index=True, nullable=False)
        value = Column(LargeBinary, nullable=True)
        timestamp = Column(DateTime, nullable=False)
        stale = Column(Boolean, default=False)
        processing = Column(Boolean, default=False)
        completed = Column(Boolean, default=False)
        __table_args__ = (Index("ix_func_key", "function_id", "key", unique=True),)


class _SQLCore(_BaseCore):
    """SQLAlchemy-based core for Cachier, supporting SQL-based backends.

    This should work with SQLite, PostgreSQL and so on.

    """

    _ENGINE_ERROR = (
        "sql_engine must be a SQLAlchemy Engine, AsyncEngine, connection string, or callable returning an Engine."
    )

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        sql_engine: Optional[Union[str, "Engine", "AsyncEngine", Callable[[], "Engine"]]],
        wait_for_calc_timeout: Optional[int] = None,
        entry_size_limit: Optional[int] = None,
    ):
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError("SQLAlchemy is required for the SQL core. Install with `pip install SQLAlchemy`.")
        super().__init__(
            hash_func=hash_func,
            wait_for_calc_timeout=wait_for_calc_timeout,
            entry_size_limit=entry_size_limit,
        )
        self._lock = threading.RLock()
        self._func_str = None

        self._engine: Optional[Engine] = None
        self._Session = None

        self._async_engine: Optional[AsyncEngine] = None
        self._AsyncSession = None
        self._async_tables_created = False

        if isinstance(sql_engine, AsyncEngine):
            self._async_engine = sql_engine
            self._AsyncSession = sessionmaker(bind=self._async_engine, class_=AsyncSession, expire_on_commit=False)
            return

        self._engine = self._resolve_engine(sql_engine)
        self._Session = sessionmaker(bind=self._engine)
        Base.metadata.create_all(self._engine)

    def __del__(self) -> None:
        engine = getattr(self, "_engine", None)
        if engine is not None:
            with suppress(Exception):
                engine.dispose()
        async_engine = getattr(self, "_async_engine", None)
        if async_engine is not None:
            with suppress(Exception):
                async_engine.sync_engine.dispose()

    def has_async_engine(self) -> bool:
        """Return whether this core was configured with an AsyncEngine."""
        return self._async_engine is not None

    def _resolve_engine(self, sql_engine):
        if isinstance(sql_engine, Engine):
            return sql_engine
        if isinstance(sql_engine, str):
            if sql_engine.startswith("sqlite:///:memory:"):
                return create_engine(
                    sql_engine,
                    future=True,
                    connect_args={"check_same_thread": False},
                    poolclass=StaticPool,
                )
            return create_engine(sql_engine, future=True)
        if callable(sql_engine):
            resolved = sql_engine()
            if isinstance(resolved, Engine):
                return resolved
            raise ValueError(_SQLCore._ENGINE_ERROR)
        raise ValueError(_SQLCore._ENGINE_ERROR)

    def _get_sync_session(self):
        if self._Session is None:
            msg = "Sync SQL operations require a sync SQLAlchemy Engine."
            raise TypeError(msg)
        return self._Session

    async def _get_async_session(self):
        if self._AsyncSession is None or self._async_engine is None:
            msg = "Async SQL operations require an AsyncEngine sql_engine."
            raise TypeError(msg)
        if not self._async_tables_created:
            async with self._async_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            self._async_tables_created = True
        return self._AsyncSession

    def set_func(self, func):
        super().set_func(func)
        self._func_str = _get_func_str(func)

    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        session_factory = self._get_sync_session()
        with self._lock, session_factory() as session:
            row = session.execute(
                select(CacheTable).where(
                    and_(
                        CacheTable.function_id == self._func_str,
                        CacheTable.key == key,
                    )
                )
            ).scalar_one_or_none()
            if not row:
                return key, None
            value = pickle.loads(row.value) if row.value is not None else None
            entry = CacheEntry(
                value=value,
                time=cast(datetime, row.timestamp),
                stale=cast(bool, row.stale),
                _processing=cast(bool, row.processing),
                _completed=cast(bool, row.completed),
            )
            return key, entry

    async def aget_entry(self, args, kwds) -> Tuple[str, Optional[CacheEntry]]:
        key = self.get_key(args, kwds)
        return await self.aget_entry_by_key(key)

    async def aget_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        session_factory = await self._get_async_session()
        async with session_factory() as session:
            row = (
                await session.execute(
                    select(CacheTable).where(
                        and_(
                            CacheTable.function_id == self._func_str,
                            CacheTable.key == key,
                        )
                    )
                )
            ).scalar_one_or_none()
            if not row:
                return key, None
            value = pickle.loads(row.value) if row.value is not None else None
            entry = CacheEntry(
                value=value,
                time=cast(datetime, row.timestamp),
                stale=cast(bool, row.stale),
                _processing=cast(bool, row.processing),
                _completed=cast(bool, row.completed),
            )
            return key, entry

    def set_entry(self, key: str, func_res: Any) -> bool:
        if not self._should_store(func_res):
            return False
        session_factory = self._get_sync_session()
        with self._lock, session_factory() as session:
            thebytes = pickle.dumps(func_res)
            now = datetime.now()
            base_insert = insert(CacheTable)
            stmt = (
                base_insert.values(
                    id=f"{self._func_str}:{key}",
                    function_id=self._func_str,
                    key=key,
                    value=thebytes,
                    timestamp=now,
                    stale=False,
                    processing=False,
                    completed=True,
                ).on_conflict_do_update(
                    index_elements=[CacheTable.function_id, CacheTable.key],
                    set_={"value": thebytes, "timestamp": now, "stale": False, "processing": False, "completed": True},
                )
                if hasattr(base_insert, "on_conflict_do_update")
                else None
            )
            if stmt:
                session.execute(stmt)
            else:
                row = session.execute(
                    select(CacheTable).where(
                        and_(
                            CacheTable.function_id == self._func_str,
                            CacheTable.key == key,
                        )
                    )
                ).scalar_one_or_none()
                if row:
                    session.execute(
                        update(CacheTable)
                        .where(and_(CacheTable.function_id == self._func_str, CacheTable.key == key))
                        .values(value=thebytes, timestamp=now, stale=False, processing=False, completed=True)
                    )
                else:
                    session.add(
                        CacheTable(
                            id=f"{self._func_str}:{key}",
                            function_id=self._func_str,
                            key=key,
                            value=thebytes,
                            timestamp=now,
                            stale=False,
                            processing=False,
                            completed=True,
                        )
                    )
            session.commit()
        return True

    async def aset_entry(self, key: str, func_res: Any) -> bool:
        if not self._should_store(func_res):
            return False
        session_factory = await self._get_async_session()
        async with session_factory() as session:
            thebytes = pickle.dumps(func_res)
            now = datetime.now()
            base_insert = insert(CacheTable)
            stmt = (
                base_insert.values(
                    id=f"{self._func_str}:{key}",
                    function_id=self._func_str,
                    key=key,
                    value=thebytes,
                    timestamp=now,
                    stale=False,
                    processing=False,
                    completed=True,
                ).on_conflict_do_update(
                    index_elements=[CacheTable.function_id, CacheTable.key],
                    set_={"value": thebytes, "timestamp": now, "stale": False, "processing": False, "completed": True},
                )
                if hasattr(base_insert, "on_conflict_do_update")
                else None
            )
            if stmt:
                await session.execute(stmt)
            else:
                row = (
                    await session.execute(
                        select(CacheTable).where(
                            and_(
                                CacheTable.function_id == self._func_str,
                                CacheTable.key == key,
                            )
                        )
                    )
                ).scalar_one_or_none()
                if row:
                    await session.execute(
                        update(CacheTable)
                        .where(and_(CacheTable.function_id == self._func_str, CacheTable.key == key))
                        .values(value=thebytes, timestamp=now, stale=False, processing=False, completed=True)
                    )
                else:
                    session.add(
                        CacheTable(
                            id=f"{self._func_str}:{key}",
                            function_id=self._func_str,
                            key=key,
                            value=thebytes,
                            timestamp=now,
                            stale=False,
                            processing=False,
                            completed=True,
                        )
                    )
            await session.commit()
        return True

    def mark_entry_being_calculated(self, key: str) -> None:
        session_factory = self._get_sync_session()
        with self._lock, session_factory() as session:
            row = session.execute(
                select(CacheTable).where(
                    and_(
                        CacheTable.function_id == self._func_str,
                        CacheTable.key == key,
                    )
                )
            ).scalar_one_or_none()
            if row:
                session.execute(
                    update(CacheTable)
                    .where(and_(CacheTable.function_id == self._func_str, CacheTable.key == key))
                    .values(processing=True)
                )
            else:
                session.add(
                    CacheTable(
                        id=f"{self._func_str}:{key}",
                        function_id=self._func_str,
                        key=key,
                        value=None,
                        timestamp=datetime.now(),
                        stale=False,
                        processing=True,
                        completed=False,
                    )
                )
            session.commit()

    async def amark_entry_being_calculated(self, key: str) -> None:
        session_factory = await self._get_async_session()
        async with session_factory() as session:
            row = (
                await session.execute(
                    select(CacheTable).where(
                        and_(
                            CacheTable.function_id == self._func_str,
                            CacheTable.key == key,
                        )
                    )
                )
            ).scalar_one_or_none()
            if row:
                await session.execute(
                    update(CacheTable)
                    .where(and_(CacheTable.function_id == self._func_str, CacheTable.key == key))
                    .values(processing=True)
                )
            else:
                session.add(
                    CacheTable(
                        id=f"{self._func_str}:{key}",
                        function_id=self._func_str,
                        key=key,
                        value=None,
                        timestamp=datetime.now(),
                        stale=False,
                        processing=True,
                        completed=False,
                    )
                )
            await session.commit()

    def mark_entry_not_calculated(self, key: str) -> None:
        session_factory = self._get_sync_session()
        with self._lock, session_factory() as session:
            session.execute(
                update(CacheTable)
                .where(and_(CacheTable.function_id == self._func_str, CacheTable.key == key))
                .values(processing=False)
            )
            session.commit()

    async def amark_entry_not_calculated(self, key: str) -> None:
        session_factory = await self._get_async_session()
        async with session_factory() as session:
            await session.execute(
                update(CacheTable)
                .where(and_(CacheTable.function_id == self._func_str, CacheTable.key == key))
                .values(processing=False)
            )
            await session.commit()

    def wait_on_entry_calc(self, key: str) -> Any:
        import time

        time_spent = 0
        session_factory = self._get_sync_session()
        while True:
            with self._lock, session_factory() as session:
                row = session.execute(
                    select(CacheTable).where(and_(CacheTable.function_id == self._func_str, CacheTable.key == key))
                ).scalar_one_or_none()
                if not row:
                    raise RecalculationNeeded()
                if not row.processing:
                    return pickle.loads(row.value) if row.value is not None else None
            time.sleep(1)
            time_spent += 1
            self.check_calc_timeout(time_spent)

    def clear_cache(self) -> None:
        session_factory = self._get_sync_session()
        with self._lock, session_factory() as session:
            session.execute(delete(CacheTable).where(CacheTable.function_id == self._func_str))
            session.commit()

    async def aclear_cache(self) -> None:
        session_factory = await self._get_async_session()
        async with session_factory() as session:
            await session.execute(delete(CacheTable).where(CacheTable.function_id == self._func_str))
            await session.commit()

    def clear_being_calculated(self) -> None:
        session_factory = self._get_sync_session()
        with self._lock, session_factory() as session:
            session.execute(
                update(CacheTable)
                .where(and_(CacheTable.function_id == self._func_str, CacheTable.processing))
                .values(processing=False)
            )
            session.commit()

    async def aclear_being_calculated(self) -> None:
        session_factory = await self._get_async_session()
        async with session_factory() as session:
            await session.execute(
                update(CacheTable)
                .where(and_(CacheTable.function_id == self._func_str, CacheTable.processing))
                .values(processing=False)
            )
            await session.commit()

    def delete_stale_entries(self, stale_after: timedelta) -> None:
        """Delete stale entries from the SQL cache."""
        threshold = datetime.now() - stale_after
        session_factory = self._get_sync_session()
        with self._lock, session_factory() as session:
            session.execute(
                delete(CacheTable).where(
                    and_(
                        CacheTable.function_id == self._func_str,
                        CacheTable.timestamp < threshold,
                    )
                )
            )
            session.commit()

    async def adelete_stale_entries(self, stale_after: timedelta) -> None:
        """Delete stale entries from the SQL cache asynchronously."""
        threshold = datetime.now() - stale_after
        session_factory = await self._get_async_session()
        async with session_factory() as session:
            await session.execute(
                delete(CacheTable).where(
                    and_(
                        CacheTable.function_id == self._func_str,
                        CacheTable.timestamp < threshold,
                    )
                )
            )
            await session.commit()
