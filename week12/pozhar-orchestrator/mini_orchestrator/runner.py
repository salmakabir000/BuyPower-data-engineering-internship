"""
runner.py - executes a DAG.

Two phases:
  1. Topological sort: figure out a valid order to attempt tasks in
     (also catches cycles - a broken DAG - before anything runs).
  2. Execution: walk that order. For each task, check whether all of its
     dependencies succeeded.
       - all deps succeeded (or no deps) -> run it -> success/failed
       - any dep failed or was skipped   -> skip it, don't run it
     This naturally cascades: if A fails, B (depends on A) gets skipped,
     and C (depends on B) sees B wasn't a success, so C gets skipped too.
"""

from .state import StateStore


class CycleError(ValueError):
    """Raised when a DAG's dependencies form a loop and can't be ordered."""
    pass


def topological_sort(dag_obj):
    """Returns a list of task names in a valid execution order.
    Raises CycleError if the DAG's dependencies contain a loop."""

    in_degree = {name: len(t.depends_on) for name, t in dag_obj.tasks.items()}
    dependents = {name: [] for name in dag_obj.tasks}
    for name, t in dag_obj.tasks.items():
        for dep_name in t.depends_on:
            dependents[dep_name].append(name)

    ready = sorted([name for name, deg in in_degree.items() if deg == 0])
    order = []

    while ready:
        current = ready.pop(0)
        order.append(current)
        for dependent_name in dependents[current]:
            in_degree[dependent_name] -= 1
            if in_degree[dependent_name] == 0:
                ready.append(dependent_name)
        ready.sort()

    if len(order) != len(dag_obj.tasks):
        unresolved = set(dag_obj.tasks) - set(order)
        raise CycleError(
            f"DAG '{dag_obj.name}' has a cycle in these tasks (or their "
            f"dependencies): {sorted(unresolved)}. Every task's dependencies "
            "must eventually resolve to tasks with no dependencies."
        )

    return order


class Runner:
    """Executes a DAG and persists progress to a StateStore."""

    def __init__(self, state_store=None):
        self.state = state_store or StateStore()

    def run(self, dag_obj, verbose=True):
        order = topological_sort(dag_obj)

        run_id = self.state.create_run(dag_obj.name, list(dag_obj.tasks.keys()))
        if verbose:
            print(f"Starting run of DAG '{dag_obj.name}' (run_id={run_id})")
            print(f"Execution order: {order}")

        task_status = {}

        for name in order:
            task_obj = dag_obj.tasks[name]
            deps_ok = all(task_status.get(dep) == "success" for dep in task_obj.depends_on)

            if not deps_ok:
                task_status[name] = "skipped"
                self.state.set_task_status(run_id, name, "skipped")
                if verbose:
                    print(f"  [SKIPPED] {name} (an upstream dependency did not succeed)")
                continue

            self.state.set_task_status(run_id, name, "running")
            if verbose:
                print(f"  [RUNNING] {name}...")
            try:
                task_obj.run()
            except Exception as e:
                task_status[name] = "failed"
                self.state.set_task_status(run_id, name, "failed", error=str(e))
                if verbose:
                    print(f"  [FAILED]  {name}: {e}")
            else:
                task_status[name] = "success"
                self.state.set_task_status(run_id, name, "success")
                if verbose:
                    print(f"  [SUCCESS] {name}")

        overall_status = "failed" if "failed" in task_status.values() else "success"
        self.state.finish_run(run_id, overall_status)

        if verbose:
            print(f"Run {run_id} finished with status: {overall_status}")

        return run_id
