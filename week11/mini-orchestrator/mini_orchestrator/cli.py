import sys

from mini_orchestrator.dags import crypto_etl
from mini_orchestrator.runner import run_dag
from mini_orchestrator.state import get_dag_runs, get_task_runs


DAGS = {
    "crypto_etl": crypto_etl
}


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 -m mini_orchestrator.cli <command>")
        return

    command = sys.argv[1]

    if command == "list":
        for dag_name in DAGS:
            print(dag_name)

    elif command == "run":
        if len(sys.argv) < 3:
            print("Usage: python3 -m mini_orchestrator.cli run <dag_name>")
            return

        dag_name = sys.argv[2]

        if dag_name not in DAGS:
            print(f"DAG not found: {dag_name}")
            return

        dag = DAGS[dag_name]
        run_id, states = run_dag(dag)

        print(f"Run ID: {run_id}")

    elif command == "status":
        if len(sys.argv) < 3:
            print("Usage: python3 -m mini_orchestrator.cli status <dag_name>")
            return

        dag_name = sys.argv[2]

        if dag_name not in DAGS:
            print(f"DAG not found: {dag_name}")
            return

        runs = get_dag_runs(dag_name)

        print("Run ID    Status     Started")

        for run in runs:
            run_id, status, started_at, finished_at = run
            print(f"{run_id:<9} {status:<10} {started_at}")

    elif command == "logs":
        if len(sys.argv) < 4:
            print("Usage: python3 -m mini_orchestrator.cli logs <dag_name> <run_id>")
            return

        dag_name = sys.argv[2]
        run_id = sys.argv[3]

        if dag_name not in DAGS:
            print(f"DAG not found: {dag_name}")
            return

        runs = get_task_runs(run_id)

        for run in runs:
            print(run)

    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
