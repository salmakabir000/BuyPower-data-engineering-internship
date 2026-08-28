"""
state.py - persists run history to SQLite.

Schema matches the assignment spec exactly:
  dag_runs(run_id TEXT PK, dag_name, started_at, finished_at, status)
  task_runs(run_id, task_name, started_at, finished_at, status, error_message)
"""

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB_PATH = Path.home() / ".mini_orchestrator" / "state.db"
VALID_STATUSES = {"pending", "running", "success", "failed", "skipped"}


def _now():
    return datetime.now(timezone.utc).isoformat()


def new_run_id():
    """Short text run id, e.g. 'a1b2c3' - matches the spec's example format."""
    return uuid.uuid4().hex[:6]


class StateStore:
    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self):
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS dag_runs (
                    run_id      TEXT PRIMARY KEY,
                    dag_name    TEXT NOT NULL,
                    started_at  TIMESTAMP,
                    finished_at TIMESTAMP,
                    status      TEXT
                );

                CREATE TABLE IF NOT EXISTS task_runs (
                    run_id        TEXT,
                    task_name     TEXT,
                    started_at    TIMESTAMP,
                    finished_at   TIMESTAMP,
                    status        TEXT,
                    error_message TEXT,
                    PRIMARY KEY (run_id, task_name)
                );

                CREATE INDEX IF NOT EXISTS idx_dag_runs_dag_name
                    ON dag_runs(dag_name);
                """
            )
            conn.commit()
        finally:
            conn.close()

    # ---------- DAG run lifecycle ----------

    def create_run(self, dag_name, task_names):
        run_id = new_run_id()
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO dag_runs (run_id, dag_name, started_at, status) "
                "VALUES (?, ?, ?, ?)",
                (run_id, dag_name, _now(), "running"),
            )
            conn.executemany(
                "INSERT INTO task_runs (run_id, task_name, status) VALUES (?, ?, ?)",
                [(run_id, name, "pending") for name in task_names],
            )
            conn.commit()
            return run_id
        finally:
            conn.close()

    def finish_run(self, run_id, status):
        assert status in ("success", "failed")
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE dag_runs SET status = ?, finished_at = ? WHERE run_id = ?",
                (status, _now(), run_id),
            )
            conn.commit()
        finally:
            conn.close()

    # ---------- Task state within a run ----------

    def set_task_status(self, run_id, task_name, status, error_message=None):
        assert status in VALID_STATUSES, f"invalid status: {status}"
        conn = self._connect()
        try:
            if status == "running":
                conn.execute(
                    "UPDATE task_runs SET status = ?, started_at = ? "
                    "WHERE run_id = ? AND task_name = ?",
                    (status, _now(), run_id, task_name),
                )
            elif status in ("success", "failed", "skipped"):
                conn.execute(
                    "UPDATE task_runs SET status = ?, finished_at = ?, error_message = ? "
                    "WHERE run_id = ? AND task_name = ?",
                    (status, _now(), error_message, run_id, task_name),
                )
            else:
                conn.execute(
                    "UPDATE task_runs SET status = ? WHERE run_id = ? AND task_name = ?",
                    (status, run_id, task_name),
                )
            conn.commit()
        finally:
            conn.close()

    # ---------- Querying history ----------

    def get_run(self, run_id):
        conn = self._connect()
        try:
            run = conn.execute(
                "SELECT * FROM dag_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                return None
            tasks = conn.execute(
                "SELECT * FROM task_runs WHERE run_id = ? ORDER BY rowid", (run_id,)
            ).fetchall()
            return {"run": dict(run), "tasks": [dict(t) for t in tasks]}
        finally:
            conn.close()

    def recent_runs(self, dag_name, limit=10):
        """Most recent runs for one DAG, newest first - powers the status command."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM dag_runs WHERE dag_name = ? "
                "ORDER BY started_at DESC LIMIT ?",
                (dag_name, limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def runs_on_date(self, dag_name, date_str):
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM dag_runs WHERE dag_name = ? "
                "AND started_at LIKE ? ORDER BY started_at",
                (dag_name, f"{date_str}%"),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
