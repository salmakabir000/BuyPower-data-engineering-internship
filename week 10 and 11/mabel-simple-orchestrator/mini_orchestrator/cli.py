"""
Command-line interface.

    mini_orchestrator list
    mini_orchestrator run <dag_name> [--parallel]
    mini_orchestrator status <dag_name>
    mini_orchestrator logs <dag_name> <run_id>

DAG discovery: this tool doesn't know about your DAGs until their
module has been imported (that's what triggers the @dag decorator).
By default it imports a module named `dags` from the current working
directory; override with the MINI_ORCH_DAGS_MODULE env var if your
DAG definitions live somewhere else, e.g.:

    MINI_ORCH_DAGS_MODULE=my_pipelines.dags mini_orchestrator list
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from datetime import datetime

from . import state
from .dag import all_dags, get_dag
from .runner import run_dag


def _load_dags() -> None:
    sys.path.insert(0, os.getcwd())
    module_name = os.environ.get("MINI_ORCH_DAGS_MODULE", "dags")
    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            # No user DAG module found — fine if DAGs were already
            # registered some other way (e.g. in tests).
            return
        raise


def _fmt_duration(started: str | None, finished: str | None) -> str:
    if not started or not finished:
        return "-"
    delta = datetime.fromisoformat(finished) - datetime.fromisoformat(started)
    return f"{delta.total_seconds():.1f}s"


def _fmt_started(started: str | None) -> str:
    if not started:
        return "-"
    return started[:19].replace("T", " ")


def cmd_list(_args: argparse.Namespace) -> None:
    _load_dags()
    dags = all_dags()
    if not dags:
        print("No DAGs registered. Set MINI_ORCH_DAGS_MODULE or add a dags.py to your cwd.")
        return
    for name, d in sorted(dags.items()):
        print(f"{name}\tschedule={d.schedule or '-'}\ttasks={len(d.tasks)}")


def cmd_run(args: argparse.Namespace) -> None:
    _load_dags()
    d = get_dag(args.dag_name)
    run_id = run_dag(d, parallel=args.parallel)
    runs = state.get_recent_runs(d.name, limit=1)
    status = runs[0]["status"] if runs else "unknown"
    print(f"Run {run_id} finished with status: {status}")
    if status == "failed":
        failed = state.get_first_failed_task(run_id)
        if failed:
            print(f"  first failure: {failed['task_name']} -- {failed['error_message']}")
        sys.exit(1)


def cmd_status(args: argparse.Namespace) -> None:
    runs = state.get_recent_runs(args.dag_name, limit=10)
    print(f"DAG: {args.dag_name}")
    if not runs:
        print("No runs found.")
        return
    print(f"{'Run ID':<14}| {'Started':<21}| {'Duration':<10}| Result")
    for r in runs:
        duration = _fmt_duration(r["started_at"], r["finished_at"])
        result = r["status"] or "running"
        if result == "failed":
            failed = state.get_first_failed_task(r["run_id"])
            if failed:
                result = f"failed ({failed['task_name']}: {failed['error_message']})"
        print(f"{r['run_id']:<14}| {_fmt_started(r['started_at']):<21}| {duration:<10}| {result}")


def cmd_logs(args: argparse.Namespace) -> None:
    tasks = state.get_task_runs(args.run_id)
    if not tasks:
        print(f"No task runs found for run_id {args.run_id}")
        return
    print(f"DAG: {args.dag_name}  Run: {args.run_id}")
    for t in tasks:
        duration = _fmt_duration(t["started_at"], t["finished_at"])
        line = f"  {t['task_name']:<16}{t['status']:<10}{duration}"
        if t["error_message"]:
            line += f"  -- {t['error_message']}"
        print(line)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mini_orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="list registered DAGs").set_defaults(func=cmd_list)

    p_run = sub.add_parser("run", help="run a DAG once")
    p_run.add_argument("dag_name")
    p_run.add_argument("--parallel", action="store_true", help="run independent tasks concurrently")
    p_run.set_defaults(func=cmd_run)

    p_status = sub.add_parser("status", help="show last 10 runs of a DAG")
    p_status.add_argument("dag_name")
    p_status.set_defaults(func=cmd_status)

    p_logs = sub.add_parser("logs", help="show task statuses and timing for a run")
    p_logs.add_argument("dag_name")
    p_logs.add_argument("run_id")
    p_logs.set_defaults(func=cmd_logs)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
