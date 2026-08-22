"""
state.py - persists run history to SQLite so we can answer questions like
"did Tuesday's job succeed?" after the fact.

Two tables:
  dag_runs  - one row per time a DAG was run (overall status, start/end time)
  task_runs - one row per task within a run (that task's status, error if any)

Task/run status follows this state machine:
  pending -> running -> success
                      -> failed
  pending -> skipped   (when an upstream dependency failed)
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB_PATH = Path.home() / ".mini_orchestrator" / "state.db"

# Valid states - used to guard against typos elsewhere in the code
VALID_STATUSES = {"pending", "running", "success", "failed", "skipped"}


def _now():
    return datetime.now(timezone.utc).isoformat()


class StateStore:
    """Wraps a SQLite database that tracks DAG run history."""

    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        # check_same_thread=False: our CLI is single-threaded anyway, but this
        # avoids surprises if we ever call this from a different thread.
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
                    run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    dag_name    TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    started_at  TEXT NOT NULL,
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS task_runs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id      INTEGER NOT NULL REFERENCES dag_runs(run_id),
                    task_name   TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    started_at  TEXT,
                    finished_at TEXT,
                    error       TEXT,
                    UNIQUE(run_id, task_name)
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
        """Starts a new run: one dag_runs row + one pending task_runs row per task."""
        conn = self._connect()
        try:
            cur = conn.execute(
                "INSERT INTO dag_runs (dag_name, status, started_at) VALUES (?, ?, ?)",
                (dag_name, "running", _now()),
            )
            run_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO task_runs (run_id, task_name, status) VALUES (?, ?, ?)",
                [(run_id, name, "pending") for name in task_names],
            )
            conn.commit()
            return run_id
        finally:
            conn.close()

    def finish_run(self, run_id, status):
        """status should be 'success' or 'failed' - the overall run outcome."""
        assert status in ("success", "failed"), f"invalid final run status: {status}"
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

    def set_task_status(self, run_id, task_name, status, error=None):
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
                    "UPDATE task_runs SET status = ?, finished_at = ?, error = ? "
                    "WHERE run_id = ? AND task_name = ?",
                    (status, _now(), error, run_id, task_name),
                )
            else:  # pending - shouldn't normally be re-set, but handle it anyway
                conn.execute(
                    "UPDATE task_runs SET status = ? WHERE run_id = ? AND task_name = ?",
                    (status, run_id, task_name),
                )
            conn.commit()
        finally:
            conn.close()

    # ---------- Querying history ----------

    def get_run(self, run_id):
        """Full detail for one run: the dag_runs row + all its task_runs rows."""
        conn = self._connect()
        try:
            run = conn.execute(
                "SELECT * FROM dag_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                return None
            tasks = conn.execute(
                "SELECT * FROM task_runs WHERE run_id = ? ORDER BY id", (run_id,)
            ).fetchall()
            return {"run": dict(run), "tasks": [dict(t) for t in tasks]}
        finally:
            conn.close()

    def list_runs(self, dag_name=None, limit=20):
        """Recent runs, newest first. Filter by dag_name if given."""
        conn = self._connect()
        try:
            if dag_name:
                rows = conn.execute(
                    "SELECT * FROM dag_runs WHERE dag_name = ? "
                    "ORDER BY run_id DESC LIMIT ?",
                    (dag_name, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM dag_runs ORDER BY run_id DESC LIMIT ?", (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def runs_on_date(self, dag_name, date_str):
        """Answers 'did <dag_name>'s job succeed on <date_str>?'
        date_str format: 'YYYY-MM-DD'. Matches on started_at."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM dag_runs WHERE dag_name = ? "
                "AND started_at LIKE ? ORDER BY run_id",
                (dag_name, f"{date_str}%"),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
