import sqlite3
from pathlib import Path
from datetime import datetime


STATE_DIR = Path.home() / ".mini_orchestrator"
STATE_DB = STATE_DIR / "state.db"


def get_connection():
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(STATE_DB)
    connection.row_factory = sqlite3.Row

    return connection


def initialize_database():
    connection = get_connection()

    connection.execute("""
        CREATE TABLE IF NOT EXISTS dag_runs (
            run_id TEXT PRIMARY KEY,
            dag_name TEXT NOT NULL,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT
        )
    """)

    connection.execute("""
        CREATE TABLE IF NOT EXISTS task_runs (
            run_id TEXT,
            task_name TEXT,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT,
            error_message TEXT,
            PRIMARY KEY (run_id, task_name)
        )
    """)

    connection.commit()
    connection.close()


def start_dag_run(run_id, dag_name):
    connection = get_connection()

    connection.execute(
        """
        INSERT INTO dag_runs
        (run_id, dag_name, started_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, dag_name, datetime.now().isoformat(), "running"),
    )

    connection.commit()
    connection.close()


def finish_dag_run(run_id, status):
    connection = get_connection()

    connection.execute(
        """
        UPDATE dag_runs
        SET finished_at = ?, status = ?
        WHERE run_id = ?
        """,
        (datetime.now().isoformat(), status, run_id),
    )

    connection.commit()
    connection.close()


def save_task_run(
    run_id,
    task_name,
    started_at,
    finished_at,
    status,
    error_message=None,
):
    connection = get_connection()

    connection.execute(
        """
        INSERT OR REPLACE INTO task_runs
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            task_name,
            started_at,
            finished_at,
            status,
            error_message,
        ),
    )

    connection.commit()
    connection.close()


def get_dag_runs(dag_name, limit=10):
    connection = get_connection()

    rows = connection.execute(
        """
        SELECT *
        FROM dag_runs
        WHERE dag_name = ?
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (dag_name, limit),
    ).fetchall()

    connection.close()

    return rows


def get_task_runs(run_id):
    connection = get_connection()

    rows = connection.execute(
        """
        SELECT *
        FROM task_runs
        WHERE run_id = ?
        ORDER BY started_at
        """,
        (run_id,),
    ).fetchall()

    connection.close()

    return rows