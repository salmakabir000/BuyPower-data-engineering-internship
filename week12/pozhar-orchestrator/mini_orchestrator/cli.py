"""
cli.py - command-line interface for mini_orchestrator.

Usage:
    mini_orchestrator list
    mini_orchestrator run <dag_name> [--parallel]
    mini_orchestrator status <dag_name>
    mini_orchestrator logs <dag_name> <run_id>

DAGs are auto-discovered from a "dags/" folder in the current directory
(override with --dags-dir).
"""

import argparse
import sys
from datetime import datetime

from .registry import discover_dags
from .runner import Runner, CycleError
from .state import StateStore


def _parse_ts(ts):
    """ISO timestamp string -> datetime, or None if ts is None/empty."""
    if not ts:
        return None
    return datetime.fromisoformat(ts)


def _fmt_started(ts):
    dt = _parse_ts(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "-"


def _fmt_duration(started_at, finished_at):
    start = _parse_ts(started_at)
    end = _parse_ts(finished_at)
    if not start or not end:
        return "-"
    return f"{(end - start).total_seconds():.1f}s"


def _fmt_result(store, run_row):
    """'success' or 'failed (task_name: ExceptionType)'."""
    if run_row["status"] != "failed":
        return run_row["status"] or "running"

    detail = store.get_run(run_row["run_id"])
    failed_task = next((t for t in detail["tasks"] if t["status"] == "failed"), None)
    if failed_task is None:
        return "failed"

    exc_type = "Error"
    if failed_task["error_message"] and ":" in failed_task["error_message"]:
        exc_type = failed_task["error_message"].split(":", 1)[0]
    return f"failed ({failed_task['task_name']}: {exc_type})"


def cmd_list(args):
    dags = discover_dags(args.dags_dir)
    if not dags:
        print(f"No DAGs found in '{args.dags_dir}/'.")
        return
    print("Registered DAGs:")
    for name in sorted(dags):
        print(f"  - {name}")


def cmd_run(args):
    dags = discover_dags(args.dags_dir)
    if args.dag_name not in dags:
        available = sorted(dags)
        print(
            f"Error: no DAG named '{args.dag_name}' found in '{args.dags_dir}/'. "
            f"Available: {available}",
            file=sys.stderr,
        )
        sys.exit(1)

    built_dag = dags[args.dag_name]()
    runner = Runner()
    try:
        run_id = runner.run(built_dag, parallel=args.parallel)
    except CycleError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    detail = runner.state.get_run(run_id)
    sys.exit(0 if detail["run"]["status"] == "success" else 1)


def cmd_status(args):
    store = StateStore()
    runs = store.recent_runs(args.dag_name, limit=10)

    print(f"DAG: {args.dag_name}")
    if not runs:
        print("No runs found.")
        return

    print(f"{'Run ID':<9} | {'Started':<20} | {'Duration':<8} | Result")
    for r in runs:
        run_id = r["run_id"]
        started = _fmt_started(r["started_at"])
        duration = _fmt_duration(r["started_at"], r["finished_at"])
        result = _fmt_result(store, r)
        print(f"{run_id:<9} | {started:<20} | {duration:<8} | {result}")


def cmd_logs(args):
    store = StateStore()
    detail = store.get_run(args.run_id)

    if detail is None or detail["run"]["dag_name"] != args.dag_name:
        print(f"No run '{args.run_id}' found for DAG '{args.dag_name}'.")
        return

    run = detail["run"]
    print(f"DAG: {run['dag_name']}  Run ID: {run['run_id']}  Status: {run['status']}")
    print(f"Started:  {_fmt_started(run['started_at'])}")
    print(f"Finished: {_fmt_started(run['finished_at'])}")
    print()
    print(f"{'Task':<15} | {'Status':<8} | {'Duration':<8} | Error")
    for t in detail["tasks"]:
        duration = _fmt_duration(t["started_at"], t["finished_at"])
        error = t["error_message"] or ""
        print(f"{t['task_name']:<15} | {t['status']:<8} | {duration:<8} | {error}")


def main():
    parser = argparse.ArgumentParser(
        prog="mini_orchestrator",
        description="A tiny DAG orchestrator.",
    )
    parser.add_argument(
        "--dags-dir", default="dags", help="Folder to scan for DAG files (default: dags/)"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_p = subparsers.add_parser("list", help="List registered DAGs")
    list_p.set_defaults(func=cmd_list)

    run_p = subparsers.add_parser("run", help="Run a DAG once")
    run_p.add_argument("dag_name")
    run_p.add_argument(
        "--parallel", action="store_true",
        help="Run tasks at the same dependency level concurrently"
    )
    run_p.set_defaults(func=cmd_run)

    status_p = subparsers.add_parser("status", help="Show last 10 runs of a DAG")
    status_p.add_argument("dag_name")
    status_p.set_defaults(func=cmd_status)

    logs_p = subparsers.add_parser("logs", help="Show task statuses/timing for one run")
    logs_p.add_argument("dag_name")
    logs_p.add_argument("run_id")
    logs_p.set_defaults(func=cmd_logs)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
