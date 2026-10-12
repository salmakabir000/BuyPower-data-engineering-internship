"""
SQLite-backed persistence for DAG and task run history.

Schema matches the spec exactly:

    dag_runs(run_id PK, dag_name, started_at, finished_at, status)
    task_runs(run_id, task_name, started_at, finished_at, status, error_message)
        PRIMARY KEY (run_id, task_name)
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _default_db_path() -> Path:
    # Overridable via env var so tests (and curious readers) don't touch
    # the real ~/.mini_orchestrator/state.db.
    override = os.environ.get("MINI_ORCH_DB_PATH")
    if override:
        return Path(override)
    return Path.home() / ".mini_orchestrator" / "state.db"


DB_PATH = _default_db_path()

SCHEMA = """
CREATE TABLE IF NOT EXISTS dag_runs (
    run_id TEXT PRIMARY KEY,
    dag_name TEXT NOT NULL,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT,
    task_name TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT,
    error_message TEXT,
    PRIMARY KEY (run_id, task_name)
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def create_dag_run(run_id: str, dag_name: str, started_at: datetime) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO dag_runs (run_id, dag_name, started_at, finished_at, status) "
            "VALUES (?, ?, ?, NULL, 'running')",
            (run_id, dag_name, started_at.isoformat()),
        )


def finish_dag_run(run_id: str, finished_at: datetime, status: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE dag_runs SET finished_at = ?, status = ? WHERE run_id = ?",
            (finished_at.isoformat(), status, run_id),
        )


def create_task_run(run_id: str, task_name: str, started_at: datetime) -> None:
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO task_runs (run_id, task_name, started_at, finished_at, status, error_message) "
            "VALUES (?, ?, ?, NULL, 'running', NULL)",
            (run_id, task_name, started_at.isoformat()),
        )


def finish_task_run(
    run_id: str, task_name: str, finished_at: datetime, status: str, error_message: Optional[str] = None
) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE task_runs SET finished_at = ?, status = ?, error_message = ? "
            "WHERE run_id = ? AND task_name = ?",
            (finished_at.isoformat(), status, error_message, run_id, task_name),
        )


def mark_task_skipped(run_id: str, task_name: str) -> None:
    now = utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO task_runs (run_id, task_name, started_at, finished_at, status, error_message) "
            "VALUES (?, ?, ?, ?, 'skipped', NULL)",
            (run_id, task_name, now, now),
        )


def get_recent_runs(dag_name: str, limit: int = 10) -> List[Dict]:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM dag_runs WHERE dag_name = ? ORDER BY started_at DESC LIMIT ?",
            (dag_name, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_task_runs(run_id: str) -> List[Dict]:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM task_runs WHERE run_id = ? ORDER BY started_at",
            (run_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_first_failed_task(run_id: str) -> Optional[Dict]:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM task_runs WHERE run_id = ? AND status = 'failed' "
            "ORDER BY started_at LIMIT 1",
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
