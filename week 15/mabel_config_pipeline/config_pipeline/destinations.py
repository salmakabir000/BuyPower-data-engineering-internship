"""Destinations: each one has `write(df) -> int` (rows written)."""
from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd

from .base import Component, PipelineError, field

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_pk(config, path):
    pk = config.get("primary_key")
    if pk is not None and (not isinstance(pk, list) or not pk or not all(isinstance(c, str) for c in pk)):
        return [f"{path}.primary_key: must be a non-empty list of column names"]
    return []


class SqliteDestination(Component):
    schema = {
        "path": field(str, required=True),
        "table": field(str, required=True),
        "primary_key": field(list),
        "on_conflict": field(str, default="fail", choices=("fail", "ignore", "replace")),
    }

    @classmethod
    def check(cls, config, path):
        errors = _check_pk(config, path)
        table = config.get("table")
        if isinstance(table, str) and not _IDENT.match(table):
            errors.append(f"{path}.table: '{table}' is not a valid table name (letters, digits, underscore)")
        return errors

    @staticmethod
    def _sql_type(dtype) -> str:
        if pd.api.types.is_bool_dtype(dtype) or pd.api.types.is_integer_dtype(dtype):
            return "INTEGER"
        if pd.api.types.is_float_dtype(dtype):
            return "REAL"
        return "TEXT"

    @staticmethod
    def _py(value):
        if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        if hasattr(value, "item"):  # numpy scalar -> python scalar
            return value.item()
        return value

    def write(self, df: pd.DataFrame) -> int:
        cfg = self.config
        if df.empty and len(df.columns) == 0:
            return 0
        pk = cfg["primary_key"] or []
        missing_pk = [c for c in pk if c not in df.columns]
        if missing_pk:
            raise PipelineError(f"sqlite: primary_key columns not in data: {missing_pk}")

        Path(cfg["path"]).parent.mkdir(parents=True, exist_ok=True)
        cols = list(df.columns)
        q = lambda name: '"' + str(name).replace('"', '""') + '"'

        with closing(sqlite3.connect(cfg["path"])) as conn:
            defs = [f"{q(c)} {self._sql_type(df[c].dtype)}" for c in cols]
            if pk:
                defs.append(f"PRIMARY KEY ({', '.join(q(c) for c in pk)})")
            conn.execute(f"CREATE TABLE IF NOT EXISTS {q(cfg['table'])} ({', '.join(defs)})")

            existing = {r[1] for r in conn.execute(f"PRAGMA table_info({q(cfg['table'])})")}
            extra = [c for c in cols if c not in existing]
            if extra:
                raise PipelineError(f"sqlite: table '{cfg['table']}' has no columns {extra}")

            verb = {"fail": "INSERT", "ignore": "INSERT OR IGNORE", "replace": "INSERT OR REPLACE"}[cfg["on_conflict"]]
            sql = f"{verb} INTO {q(cfg['table'])} ({', '.join(q(c) for c in cols)}) VALUES ({', '.join('?' * len(cols))})"
            rows = [tuple(self._py(v) for v in row) for row in df.itertuples(index=False, name=None)]
            before = conn.total_changes
            try:
                conn.executemany(sql, rows)
                conn.commit()
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                raise PipelineError(f"sqlite: {exc} (set on_conflict to 'ignore' or 'replace' to allow re-runs)")
            return conn.total_changes - before


class ParquetFileDestination(Component):
    schema = {
        "path": field(str, required=True),
        "mode": field(str, default="overwrite", choices=("overwrite", "append")),
        "primary_key": field(list),
    }

    @classmethod
    def check(cls, config, path):
        return _check_pk(config, path)

    def write(self, df: pd.DataFrame) -> int:
        cfg = self.config
        out = Path(cfg["path"])
        out.parent.mkdir(parents=True, exist_ok=True)
        combined = df
        if cfg["mode"] == "append" and out.exists():
            combined = pd.concat([pd.read_parquet(out), df], ignore_index=True)
        if cfg["primary_key"]:
            missing = [c for c in cfg["primary_key"] if c not in combined.columns]
            if missing:
                raise PipelineError(f"parquet_file: primary_key columns not in data: {missing}")
            combined = combined.drop_duplicates(subset=cfg["primary_key"], keep="last")
        combined.to_parquet(out, index=False)
        return len(df)
