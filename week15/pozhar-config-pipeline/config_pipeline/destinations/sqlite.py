"""
sqlite destination - writes a DataFrame to a SQLite table, creating the
table from whatever columns the DataFrame happens to have (no hardcoded
schema anywhere, so this works for any pipeline's output).
"""

import sqlite3
from pathlib import Path

import pandas as pd

_DTYPE_MAP = {
    "int64": "INTEGER",
    "Int64": "INTEGER",
    "float64": "REAL",
    "bool": "INTEGER",
    "datetime64[ns]": "TEXT",
    "datetime64[ns, UTC]": "TEXT",
}


def _sql_type(dtype):
    return _DTYPE_MAP.get(str(dtype), "TEXT")


class Destination:
    def __init__(self, config):
        self.path = config["path"]
        self.table = config["table"]
        self.primary_key = config.get("primary_key", [])
        self.on_conflict = config.get("on_conflict", "fail")  # ignore | replace | fail

    def write(self, df):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        try:
            self._create_table_if_needed(conn, df)
            rows_written = self._insert(conn, df)
            conn.commit()
            return {"rows_written": rows_written, "table": self.table, "path": self.path}
        finally:
            conn.close()

    def _create_table_if_needed(self, conn, df):
        columns_sql = ", ".join(f'"{col}" {_sql_type(df[col].dtype)}' for col in df.columns)
        pk_sql = f", PRIMARY KEY ({', '.join(self.primary_key)})" if self.primary_key else ""
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{self.table}" ({columns_sql}{pk_sql})')

    def _insert(self, conn, df):
        # sqlite3's parameter binding only understands plain Python types
        # (str, int, float, bytes, None) - a pandas Timestamp isn't one of
        # them, so any datetime columns need converting to strings first
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)

        cols = list(df.columns)
        placeholders = ", ".join(["?"] * len(cols))
        col_list = ", ".join(f'"{c}"' for c in cols)

        verb = "INSERT"
        if self.on_conflict == "ignore":
            verb = "INSERT OR IGNORE"
        elif self.on_conflict == "replace":
            verb = "INSERT OR REPLACE"
        elif self.on_conflict != "fail":
            raise ValueError(f"sqlite destination: unknown on_conflict '{self.on_conflict}'")

        sql = f'{verb} INTO "{self.table}" ({col_list}) VALUES ({placeholders})'

        # pandas NaN -> None, so sqlite stores NULL instead of the string "nan"
        records = df.where(pd.notnull(df), None).values.tolist()

        cursor = conn.cursor()
        before = cursor.execute(f'SELECT COUNT(*) FROM "{self.table}"').fetchone()[0]
        cursor.executemany(sql, records)
        after = cursor.execute(f'SELECT COUNT(*) FROM "{self.table}"').fetchone()[0]
        return after - before
