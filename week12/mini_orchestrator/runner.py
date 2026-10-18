import uuid
from datetime import datetime

from .state import (
    initialize_database,
    start_dag_run,
    finish_dag_run,
    save_task_run,
)


def run_dag(dag):
    initialize_database()

    run_id = uuid.uuid4().hex[:6]

    print(f"Running DAG: {dag.name}")
    print(f"Run ID: {run_id}")

    start_dag_run(run_id, dag.name)

    failed = False

    for task in dag.topological_sort():

        if failed:
            now = datetime.now().isoformat()

            save_task_run(
                run_id,
                task.name,
                now,
                now,
                "skipped",
            )

            print(f"Skipping task: {task.name}")
            continue

        started_at = datetime.now().isoformat()

        print(f"Running task: {task.name}")

        try:
            task.run()

            finished_at = datetime.now().isoformat()

            save_task_run(
                run_id,
                task.name,
                started_at,
                finished_at,
                "success",
            )

            print(f"Task succeeded: {task.name}")

        except Exception as error:
            finished_at = datetime.now().isoformat()

            save_task_run(
                run_id,
                task.name,
                started_at,
                finished_at,
                "failed",
                str(error),
            )

            print(
                f"Task failed: {task.name} - {error}"
            )

            failed = True

    if failed:
        finish_dag_run(run_id, "failed")
        print("DAG failed.")
    else:
        finish_dag_run(run_id, "success")
        print("DAG completed successfully.")

    return run_id