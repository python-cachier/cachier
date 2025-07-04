"""A SQLAlchemy-based caching core for cachier."""

import pickle
import threading
from datetime import datetime
from typing import Any, Callable, Optional, Tuple, Union

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
    from sqlalchemy.orm import declarative_base, sessionmaker

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
        __table_args__ = (
            Index("ix_func_key", "function_id", "key", unique=True),
        )


class _SQLCore(_BaseCore):
    """SQLAlchemy-based core for Cachier, supporting SQL-based backends.

    This should work with SQLite, PostgreSQL and so on.

    """

    def __init__(
        self,
        hash_func: Optional[HashFunc],
        sql_engine: Optional[Union[str, "Engine", Callable[[], "Engine"]]],
        wait_for_calc_timeout: Optional[int] = None,
    ):
        if not SQLALCHEMY_AVAILABLE:
            raise ImportError(
                "SQLAlchemy is required for the SQL core. "
                "Install with `pip install SQLAlchemy`."
            )
        super().__init__(
            hash_func=hash_func, wait_for_calc_timeout=wait_for_calc_timeout
        )
        self._engine = self._resolve_engine(sql_engine)
        self._Session = sessionmaker(bind=self._engine)
        Base.metadata.create_all(self._engine)
        self._lock = threading.RLock()
        self._func_str = None

    def _resolve_engine(self, sql_engine):
        if isinstance(sql_engine, Engine):
            return sql_engine
        if isinstance(sql_engine, str):
            return create_engine(sql_engine, future=True)
        if callable(sql_engine):
            return sql_engine()
        raise ValueError(
            "sql_engine must be a SQLAlchemy Engine, connection string, "
            "or callable returning an Engine."
        )

    def set_func(self, func):
        super().set_func(func)
        self._func_str = _get_func_str(func)

    def get_entry_by_key(self, key: str) -> Tuple[str, Optional[CacheEntry]]:
        with self._lock, self._Session() as session:
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
                time=row.timestamp,
                stale=row.stale,
                _processing=row.processing,
                _completed=row.completed,
            )
            return key, entry

    def set_entry(self, key: str, func_res: Any) -> None:
        with self._lock, self._Session() as session:
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
                    set_={
                        "value": thebytes,
                        "timestamp": now,
                        "stale": False,
                        "processing": False,
                        "completed": True,
                    },
                )
                if hasattr(base_insert, "on_conflict_do_update")
                else None
            )
            # Fallback for non-SQLite/Postgres: try update, else insert
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
                        .where(
                            and_(
                                CacheTable.function_id == self._func_str,
                                CacheTable.key == key,
                            )
                        )
                        .values(
                            value=thebytes,
                            timestamp=now,
                            stale=False,
                            processing=False,
                            completed=True,
                        )
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

    def mark_entry_being_calculated(self, key: str) -> None:
        with self._lock, self._Session() as session:
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
                    .where(
                        and_(
                            CacheTable.function_id == self._func_str,
                            CacheTable.key == key,
                        )
                    )
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

    def mark_entry_not_calculated(self, key: str) -> None:
        with self._lock, self._Session() as session:
            session.execute(
                update(CacheTable)
                .where(
                    and_(
                        CacheTable.function_id == self._func_str,
                        CacheTable.key == key,
                    )
                )
                .values(processing=False)
            )
            session.commit()

    def wait_on_entry_calc(self, key: str) -> Any:
        import time

        time_spent = 0
        while True:
            with self._lock, self._Session() as session:
                row = session.execute(
                    select(CacheTable).where(
                        and_(
                            CacheTable.function_id == self._func_str,
                            CacheTable.key == key,
                        )
                    )
                ).scalar_one_or_none()
                if not row:
                    raise RecalculationNeeded()
                if not row.processing:
                    return (
                        pickle.loads(row.value)
                        if row.value is not None
                        else None
                    )
            time.sleep(1)
            time_spent += 1
            self.check_calc_timeout(time_spent)

    def clear_cache(self) -> None:
        with self._lock, self._Session() as session:
            session.execute(
                delete(CacheTable).where(
                    CacheTable.function_id == self._func_str
                )
            )
            session.commit()

    def clear_being_calculated(self) -> None:
        with self._lock, self._Session() as session:
            session.execute(
                update(CacheTable)
                .where(
                    and_(
                        CacheTable.function_id == self._func_str,
                        CacheTable.processing,
                    )
                )
                .values(processing=False)
            )
            session.commit()
