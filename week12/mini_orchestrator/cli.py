import argparse
import dags
import crypto_etl

from .dag import get_dag, list_dags
from .runner import run_dag
from .state import get_dag_runs, get_task_runs


def main():
    parser = argparse.ArgumentParser(description="Mini Orchestrator CLI")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List registered DAGs")

    run_parser = subparsers.add_parser("run", help="Run a DAG")
    run_parser.add_argument("dag_name")

    status_parser = subparsers.add_parser(
        "status",
        help="Show DAG run history"
    )
    status_parser.add_argument("dag_name")

    logs_parser = subparsers.add_parser(
        "logs",
        help="Show task run details"
    )
    logs_parser.add_argument("dag_name")
    logs_parser.add_argument("run_id")

    args = parser.parse_args()

    if args.command == "list":
        dags_list = list_dags()

        if not dags_list:
            print("No DAGs registered.")
            return

        for registered_dag in dags_list:
            print(registered_dag.name)

    elif args.command == "run":
        selected_dag = get_dag(args.dag_name)

        if selected_dag is None:
            print(f"DAG not found: {args.dag_name}")
            return

        run_dag(selected_dag)

    elif args.command == "status":
        runs = get_dag_runs(args.dag_name)

        print(f"DAG: {args.dag_name}")

        if not runs:
            print("No runs found.")
            return

        print("Run ID | Started | Finished | Result")

        for run in runs:
            print(
                f"{run['run_id']} | {run['started_at']} | "
                f"{run['finished_at']} | {run['status']}"
            )

    elif args.command == "logs":
        task_runs = get_task_runs(args.run_id)

        print(f"DAG: {args.dag_name}")
        print(f"Run ID: {args.run_id}")

        if not task_runs:
            print("No task runs found.")
            return

        for task_run in task_runs:
            print(
                f"{task_run['task_name']} | {task_run['status']} | "
                f"Started: {task_run['started_at']} | "
                f"Finished: {task_run['finished_at']}"
            )

            if task_run["error_message"]:
                print(f"Error: {task_run['error_message']}")


if __name__ == "__main__":
    main()
