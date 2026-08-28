"""
runner.py - executes a DAG.

Two execution modes:
  - sequential (default): topological sort, then run tasks one at a time
  - parallel (--parallel flag): tasks are grouped into "levels" by
    dependency depth (a task's level = 1 + the deepest level among its
    dependencies). Tasks within the same level don't depend on each other
    by definition, so they can safely run concurrently via
    ThreadPoolExecutor. We wait for a whole level to finish before moving
    to the next, since the next level may depend on this one's results.

Either way, a task only runs if every one of its dependencies succeeded.
If a dependency failed or was skipped, the task is marked 'skipped'
instead of running - this cascades naturally through the graph.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

from .state import StateStore


class CycleError(ValueError):
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
            f"DAG '{dag_obj.name}' has a cycle involving: {sorted(unresolved)}."
        )

    return order


def compute_levels(dag_obj, order):
    """Groups tasks into levels by dependency depth, for parallel execution.
    A task's level = 1 + the max level of its dependencies (0 if none)."""
    level = {}
    for name in order:  # order guarantees deps are computed before dependents
        deps = dag_obj.tasks[name].depends_on
        level[name] = 0 if not deps else 1 + max(level[d] for d in deps)

    levels = {}
    for name, lvl in level.items():
        levels.setdefault(lvl, []).append(name)
    return [levels[lvl] for lvl in sorted(levels)]


class Runner:
    def __init__(self, state_store=None):
        self.state = state_store or StateStore()

    def run(self, dag_obj, parallel=False, verbose=True):
        order = topological_sort(dag_obj)
        run_id = self.state.create_run(dag_obj.name, list(dag_obj.tasks.keys()))

        if verbose:
            mode = "parallel (by level)" if parallel else "sequential"
            print(f"Starting run of DAG '{dag_obj.name}' (run_id={run_id}, mode={mode})")

        task_status = {}

        if parallel:
            levels = compute_levels(dag_obj, order)
            if verbose:
                print(f"Levels: {levels}")
            for level_tasks in levels:
                self._run_level_parallel(dag_obj, level_tasks, run_id, task_status, verbose)
        else:
            for name in order:
                self._run_one_task(dag_obj, name, run_id, task_status, verbose)

        overall_status = "failed" if "failed" in task_status.values() else "success"
        self.state.finish_run(run_id, overall_status)

        if verbose:
            print(f"Run {run_id} finished with status: {overall_status}")

        return run_id

    def _run_one_task(self, dag_obj, name, run_id, task_status, verbose):
        task_obj = dag_obj.tasks[name]
        deps_ok = all(task_status.get(dep) == "success" for dep in task_obj.depends_on)

        if not deps_ok:
            task_status[name] = "skipped"
            self.state.set_task_status(run_id, name, "skipped")
            if verbose:
                print(f"  [SKIPPED] {name} (an upstream dependency did not succeed)")
            return

        self.state.set_task_status(run_id, name, "running")
        if verbose:
            print(f"  [RUNNING] {name}...")
        try:
            task_obj.run()
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}"
            task_status[name] = "failed"
            self.state.set_task_status(run_id, name, "failed", error_message=error_message)
            if verbose:
                print(f"  [FAILED]  {name}: {error_message}")
        else:
            task_status[name] = "success"
            self.state.set_task_status(run_id, name, "success")
            if verbose:
                print(f"  [SUCCESS] {name}")

    def _run_level_parallel(self, dag_obj, level_tasks, run_id, task_status, verbose):
        # tasks whose deps already failed/skipped get marked immediately, no thread needed
        runnable = []
        for name in level_tasks:
            task_obj = dag_obj.tasks[name]
            deps_ok = all(task_status.get(dep) == "success" for dep in task_obj.depends_on)
            if deps_ok:
                runnable.append(name)
            else:
                task_status[name] = "skipped"
                self.state.set_task_status(run_id, name, "skipped")
                if verbose:
                    print(f"  [SKIPPED] {name} (an upstream dependency did not succeed)")

        if not runnable:
            return

        for name in runnable:
            self.state.set_task_status(run_id, name, "running")
            if verbose:
                print(f"  [RUNNING] {name}...")

        with ThreadPoolExecutor(max_workers=len(runnable)) as executor:
            futures = {executor.submit(dag_obj.tasks[name].run): name for name in runnable}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    error_message = f"{type(e).__name__}: {e}"
                    task_status[name] = "failed"
                    self.state.set_task_status(
                        run_id, name, "failed", error_message=error_message
                    )
                    if verbose:
                        print(f"  [FAILED]  {name}: {error_message}")
                else:
                    task_status[name] = "success"
                    self.state.set_task_status(run_id, name, "success")
                    if verbose:
                        print(f"  [SUCCESS] {name}")
