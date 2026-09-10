"""
runner.py - executes a DAG.

Execution modes: sequential (default) or parallel (--parallel, groups tasks
into dependency "levels" and runs each level concurrently).

Retry logic (new this week): each task retries on failure using exponential
backoff with jitter. Delay before attempt N (N > 1) is:

    retry_delay_seconds * 2^(attempt - 1) + random(0, 30)

So with the default retry_delay_seconds=60: ~60-90s before attempt 2,
~120-150s before attempt 3, ~240-270s before attempt 4, etc. This spaces
retries out further each time, so a task hammering a struggling downstream
system backs off instead of making things worse.

Errors are classified as retryable or not (see retry.py) - a non-retryable
error skips straight to failed_permanent instead of wasting time retrying
something that will never succeed.
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .state import StateStore
from .retry import is_retryable


class CycleError(ValueError):
    pass


def topological_sort(dag_obj):
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
    level = {}
    for name in order:
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
                task_obj = dag_obj.tasks[name]
                deps_ok = all(
                    task_status.get(dep) == "success" for dep in task_obj.depends_on
                )
                if not deps_ok:
                    task_status[name] = "skipped"
                    self.state.set_task_status(run_id, name, "skipped")
                    if verbose:
                        print(f"  [SKIPPED] {name} (an upstream dependency did not succeed)")
                    continue
                final_status = self._execute_with_retries(task_obj, run_id, verbose)
                task_status[name] = final_status

        overall_status = (
            "failed"
            if any(s in ("failed", "failed_permanent") for s in task_status.values())
            else "success"
        )
        self.state.finish_run(run_id, overall_status)

        if verbose:
            print(f"Run {run_id} finished with status: {overall_status}")

        return run_id

    def _execute_with_retries(self, task_obj, run_id, verbose):
        """Runs one task, retrying on retryable failures with exponential
        backoff + jitter. Returns the final status: 'success' or 'failed_permanent'."""
        max_attempts = task_obj.retries + 1  # first try + N retries
        attempt = 1

        while True:
            self.state.set_task_status(run_id, task_obj.name, "running")
            if verbose:
                suffix = f" (attempt {attempt}/{max_attempts})" if attempt > 1 else ""
                print(f"  [RUNNING] {task_obj.name}{suffix}...")

            try:
                task_obj.run()
            except Exception as e:
                error_message = f"{type(e).__name__}: {e}"
                retryable = is_retryable(e)

                if not retryable or attempt >= max_attempts:
                    self.state.set_task_status(
                        run_id, task_obj.name, "failed_permanent", error_message=error_message
                    )
                    if verbose:
                        reason = "non-retryable error" if not retryable else "retries exhausted"
                        print(f"  [FAILED_PERMANENT] {task_obj.name}: {error_message} ({reason})")
                    return "failed_permanent"

                delay = task_obj.retry_delay_seconds * (2 ** (attempt - 1)) + random.uniform(0, 30)
                self.state.set_task_status(
                    run_id, task_obj.name, "retrying", error_message=error_message
                )
                if verbose:
                    print(
                        f"  [RETRYING] {task_obj.name} failed (attempt {attempt}/{max_attempts}): "
                        f"{error_message}. Waiting {delay:.1f}s before retry..."
                    )
                time.sleep(delay)
                attempt += 1
            else:
                self.state.set_task_status(run_id, task_obj.name, "success")
                if verbose:
                    suffix = f" (attempt {attempt}/{max_attempts})" if attempt > 1 else ""
                    print(f"  [SUCCESS] {task_obj.name}{suffix}")
                return "success"

    def _run_level_parallel(self, dag_obj, level_tasks, run_id, task_status, verbose):
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

        with ThreadPoolExecutor(max_workers=len(runnable)) as executor:
            futures = {
                executor.submit(
                    self._execute_with_retries, dag_obj.tasks[name], run_id, verbose
                ): name
                for name in runnable
            }
            for future in as_completed(futures):
                name = futures[future]
                task_status[name] = future.result()
