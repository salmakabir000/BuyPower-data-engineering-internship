from .dag import DAG
from datetime import datetime

from .state import (
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED,
    SKIPPED,
    create_run_id,
    start_dag_run,
    record_task_run,
    finish_dag_run,
)

def topological_sort(dag):
    in_degree = {}

    for task in dag.tasks:
        in_degree[task] = len(task.depends_on)

    ready = [
        task for task in dag.tasks
        if in_degree[task] == 0
    ]

    ordered = []

    while ready:
        current = ready.pop(0)
        ordered.append(current)

        for task in dag.tasks:
            if current in task.depends_on:
                in_degree[task] -= 1

                if in_degree[task] == 0:
                    ready.append(task)

    if len(ordered) != len(dag.tasks):
        raise ValueError("DAG contains a cycle")

    return ordered


def run_dag(dag):
    run_id = create_run_id()
    start_dag_run(run_id, dag.name)

    ordered_tasks = topological_sort(dag)

    states = {
        task: PENDING
        for task in dag.tasks
    }

    for task in ordered_tasks:

        if any(
            states[dependency] != SUCCESS
            for dependency in task.depends_on
        ):
            states[task] = SKIPPED

            record_task_run(
                run_id,
                task.name,
                None,
                None,
                SKIPPED
            )

            print(f"Task skipped: {task.name}")
            continue

        states[task] = RUNNING
        started_at = datetime.now()

        print(f"Running task: {task.name}")

        try:
            task.run()

            states[task] = SUCCESS
            finished_at = datetime.now()

            record_task_run(
                run_id,
                task.name,
                started_at,
                finished_at,
                SUCCESS
            )

            print(f"Task succeeded: {task.name}")

        except Exception as error:
            states[task] = FAILED
            finished_at = datetime.now()

            record_task_run(
                run_id,
                task.name,
                started_at,
                finished_at,
                FAILED,
                str(error)
            )

            print(f"Task failed: {task.name} - {error}")

    if FAILED in states.values():
        finish_dag_run(run_id, FAILED)
    else:
        finish_dag_run(run_id, SUCCESS)

    return run_id, states
