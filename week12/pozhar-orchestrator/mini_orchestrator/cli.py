"""
cli.py - command-line interface for mini_orchestrator.

Usage:
    python -m mini_orchestrator run path/to/dag_file.py [--dag NAME]
    python -m mini_orchestrator history [--dag NAME] [--limit N]
    python -m mini_orchestrator status DAG_NAME YYYY-MM-DD
"""

import argparse
import importlib.util
import sys
from pathlib import Path

from .runner import Runner, CycleError
from .state import StateStore


def _load_dag_functions(file_path):
    """Imports a Python file and returns every function in it that was
    built with @dag (found via the marker the decorator attaches)."""
    file_path = Path(file_path).resolve()
    if not file_path.exists():
        print(f"Error: file not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        print(f"Error: failed to import '{file_path}': {e}", file=sys.stderr)
        sys.exit(1)

    dag_funcs = [
        obj
        for obj in vars(module).values()
        if callable(obj) and hasattr(obj, "_mini_orchestrator_dag_name")
    ]
    return dag_funcs


def cmd_run(args):
    dag_funcs = _load_dag_functions(args.file)

    if not dag_funcs:
        print(f"Error: no @dag-decorated function found in '{args.file}'", file=sys.stderr)
        sys.exit(1)

    if args.dag:
        matches = [f for f in dag_funcs if f._mini_orchestrator_dag_name == args.dag]
        if not matches:
            available = [f._mini_orchestrator_dag_name for f in dag_funcs]
            print(
                f"Error: no DAG named '{args.dag}' in '{args.file}'. "
                f"Available: {available}",
                file=sys.stderr,
            )
            sys.exit(1)
        dag_func = matches[0]
    elif len(dag_funcs) == 1:
        dag_func = dag_funcs[0]
    else:
        available = [f._mini_orchestrator_dag_name for f in dag_funcs]
        print(
            f"Error: multiple DAGs found in '{args.file}': {available}. "
            "Use --dag NAME to pick one.",
            file=sys.stderr,
        )
        sys.exit(1)

    built_dag = dag_func()

    runner = Runner()
    try:
        run_id = runner.run(built_dag)
    except CycleError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    detail = runner.state.get_run(run_id)
    sys.exit(0 if detail["run"]["status"] == "success" else 1)


def cmd_history(args):
    store = StateStore()
    runs = store.list_runs(dag_name=args.dag, limit=args.limit)

    if not runs:
        scope = f"for DAG '{args.dag}'" if args.dag else ""
        print(f"No runs found {scope}".strip())
        return

    print(f"{'run_id':<8} {'dag_name':<20} {'status':<10} {'started_at':<28} {'finished_at':<28}")
    print("-" * 96)
    for r in runs:
        print(
            f"{r['run_id']:<8} {r['dag_name']:<20} {r['status']:<10} "
            f"{r['started_at']:<28} {str(r['finished_at']):<28}"
        )


def cmd_status(args):
    store = StateStore()
    runs = store.runs_on_date(args.dag_name, args.date)

    if not runs:
        print(f"No runs of '{args.dag_name}' found on {args.date}.")
        return

    for r in runs:
        print(f"Run {r['run_id']}: status={r['status']}  started_at={r['started_at']}")
        detail = store.get_run(r["run_id"])
        for t in detail["tasks"]:
            line = f"    {t['task_name']}: {t['status']}"
            if t["error"]:
                line += f" (error: {t['error']})"
            print(line)


def main():
    parser = argparse.ArgumentParser(
        prog="mini_orchestrator",
        description="A tiny DAG orchestrator - define pipelines as Python, run them, check history.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_p = subparsers.add_parser("run", help="Run a DAG defined in a Python file")
    run_p.add_argument("file", help="Path to the .py file containing the @dag function")
    run_p.add_argument("--dag", help="DAG name to run, if the file has more than one")
    run_p.set_defaults(func=cmd_run)

    hist_p = subparsers.add_parser("history", help="List recent DAG runs")
    hist_p.add_argument("--dag", help="Only show runs for this DAG name")
    hist_p.add_argument("--limit", type=int, default=20, help="Max runs to show (default 20)")
    hist_p.set_defaults(func=cmd_history)

    status_p = subparsers.add_parser("status", help="Check if a DAG succeeded on a given date")
    status_p.add_argument("dag_name", help="DAG name, e.g. crypto_etl")
    status_p.add_argument("date", help="Date in YYYY-MM-DD format")
    status_p.set_defaults(func=cmd_status)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
