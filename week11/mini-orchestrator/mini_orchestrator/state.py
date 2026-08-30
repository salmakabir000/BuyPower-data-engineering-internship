import uuid
import sqlite3
from pathlib import Path
from datetime import datetime


PENDING = "pending"
RUNNING = "running"
SUCCESS = "success"
FAILED = "failed"
SKIPPED = "skipped"


DB_PATH = Path.home() / ".mini_orchestrator" / "state.db"


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)

def create_run_id():
    return uuid.uuid4().hex[:6]

def init_db():
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
        INSERT INTO dag_runs (run_id, dag_name, started_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, dag_name, datetime.now(), RUNNING)
    )

    connection.commit()
    connection.close()

def record_task_run(
    run_id,
    task_name,
    started_at,
    finished_at,
    status,
    error_message=None
):
    connection = get_connection()

    connection.execute(
        """
        INSERT INTO task_runs (
            run_id,
            task_name,
            started_at,
            finished_at,
            status,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            task_name,
            started_at,
            finished_at,
            status,
            error_message
        )
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
        (datetime.now(), status, run_id)
    )

    connection.commit()
    connection.close()

def get_dag_runs(dag_name):
    connection = get_connection()

    rows = connection.execute(
        """
        SELECT run_id, status, started_at, finished_at
        FROM dag_runs
        WHERE dag_name = ?
        ORDER BY started_at DESC
        """,
        (dag_name,)
    ).fetchall()

    connection.close()

    return rows

def get_task_runs(run_id):
    connection = get_connection()

    rows = connection.execute(
        """
        SELECT task_name, status, started_at, finished_at, error_message
        FROM task_runs
        WHERE run_id = ?
        ORDER BY started_at ASC
        """,
        (run_id,)
    ).fetchall()

    connection.close()

    return rows
