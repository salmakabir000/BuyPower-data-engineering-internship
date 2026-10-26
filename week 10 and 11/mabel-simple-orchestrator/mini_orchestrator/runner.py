"""
Executes a DAG: resolves order via topological sort, runs each task
through pending -> running -> success/failed/skipped, and persists
everything to SQLite as it happens (not just at the end) so a crash
mid-run still leaves an accurate trail.
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple

from . import state
from .dag import DAG


def _can_run(task_status: Dict[str, str], task) -> bool:
    """A task may run only if every upstream dependency already
    succeeded. If a dependency failed OR was itself skipped, this task
    is skipped too — that's how a failure cascades down the whole
    downstream subtree, one level at a time."""
    return all(task_status.get(dep.name) == "success" for dep in task.depends_on)


def _execute_task(run_id: str, task) -> Tuple[str, str, str | None]:
    started = state.utcnow()
    state.create_task_run(run_id, task.name, started)
    try:
        task.run()
        state.finish_task_run(run_id, task.name, state.utcnow(), "success")
        return task.name, "success", None
    except Exception as exc:  # noqa: BLE001 - we want to capture *any* task failure
        error_message = f"{type(exc).__name__}: {exc}"
        state.finish_task_run(run_id, task.name, state.utcnow(), "failed", error_message)
        return task.name, "failed", error_message


def run_dag(d: DAG, parallel: bool = False) -> str:
    """Runs one instance of the DAG and returns its run_id.

    Sequential mode processes tasks in topological order one at a time.
    Parallel mode (the stretch goal) groups tasks into dependency
    "levels" and runs every task in a level concurrently via
    ThreadPoolExecutor, since none of them depend on each other.
    """
    state.init_db()

    run_id = uuid.uuid4().hex[:12]
    started_at = state.utcnow()
    state.create_dag_run(run_id, d.name, started_at)

    task_status: Dict[str, str] = {}
    dag_failed = False

    if parallel:
        for level in d.levels():
            runnable = [d.tasks[name] for name in level if _can_run(task_status, d.tasks[name])]
            not_runnable = [name for name in level if name not in {t.name for t in runnable}]

            for name in not_runnable:
                state.mark_task_skipped(run_id, name)
                task_status[name] = "skipped"

            if runnable:
                with ThreadPoolExecutor(max_workers=len(runnable)) as pool:
                    futures = [pool.submit(_execute_task, run_id, t) for t in runnable]
                    for fut in as_completed(futures):
                        name, status, _ = fut.result()
                        task_status[name] = status
                        if status == "failed":
                            dag_failed = True
    else:
        for name in d.topological_order():
            task = d.tasks[name]
            if not _can_run(task_status, task):
                state.mark_task_skipped(run_id, name)
                task_status[name] = "skipped"
                continue
            _, status, _ = _execute_task(run_id, task)
            task_status[name] = status
            if status == "failed":
                dag_failed = True

    finished_at = state.utcnow()
    overall_status = "failed" if dag_failed else "success"
    state.finish_dag_run(run_id, finished_at, overall_status)
    return run_id
