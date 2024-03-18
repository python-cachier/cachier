"""A pyodbc-based caching core for cachier."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

# standard library imports
import pickle
import time
import datetime

pyodbc = None
# third party imports
with suppress(ImportError):
    import pyodbc

# local imports
from .base import _BaseCore, RecalculationNeeded

class _OdbcCore(_BaseCore):

    def __init__(
            self,
            hash_func,
            wait_for_calc_timeout,
            connection_string,
            table_name,
    ):
        if "pyodbc" not in sys.modules:
            warnings.warn(
                "`pyodbc` was not found. pyodbc cores will not function.",
                ImportWarning,
                stacklevel=2,
            )  # pragma: no cover
        super().__init__(hash_func, wait_for_calc_timeout)
        self.connection_string = connection_string
        self.table_name = table_name
        self.ensure_table_exists()

    def ensure_table_exists(self):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{self.table_name}')
                BEGIN
                    CREATE TABLE {self.table_name} (
                        key NVARCHAR(255),
                        value VARBINARY(MAX),
                        time DATETIME,
                        being_calculated BIT,
                        PRIMARY KEY (key)
                    );
                END
            """)
            conn.commit()

    def get_entry_by_key(self, key):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT value, time, being_calculated FROM {self.table_name} WHERE key = ?", key)
            row = cursor.fetchone()
            if row:
                return {
                    "value": pickle.loads(row.value),
                    "time": row.time,
                    "being_calculated": row.being_calculated,
                }
            return None

    def set_entry(self, key, func_res):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                MERGE INTO {self.table_name} USING (SELECT 1 AS dummy) AS src ON (key = ?)
                WHEN MATCHED THEN
                    UPDATE SET value = ?, time = GETDATE(), being_calculated = 0
                WHEN NOT MATCHED THEN
                    INSERT (key, value, time, being_calculated) VALUES (?, ?, GETDATE(), 0);
            """, key, pickle.dumps(func_res), key, pickle.dumps(func_res))
            conn.commit()

    def mark_entry_being_calculated(self, key):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {self.table_name} SET being_calculated = 1 WHERE key = ?", key)
            conn.commit()

    def mark_entry_not_calculated(self, key):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {self.table_name} SET being_calculated = 0 WHERE key = ?", key)
            conn.commit()

    def wait_on_entry_calc(self, key):
        start_time = datetime.datetime.now()
        while True:
            entry = self.get_entry_by_key(key)
            if entry and not entry['being_calculated']:
                return entry['value']
            if (datetime.datetime.now() - start_time).total_seconds() > self.wait_for_calc_timeout:
                raise RecalculationNeeded()
            time.sleep(1)

    def clear_cache(self):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {self.table_name}")
            conn.commit()

    def clear_being_calculated(self):
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {self.table_name} SET being_calculated = 0")
            conn.commit()
