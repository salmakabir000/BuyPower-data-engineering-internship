"""
DAG and Task definitions.

A DAG is built by calling a plain Python function decorated with @dag.
Inside that function, task() calls register Task objects onto a
thread-local "currently being built" DAG. This mirrors how Airflow's
context manager works, but with a decorator instead, to keep the
example DAG syntax from the spec working unmodified:

    @dag(name="crypto_etl", schedule="0 * * * *")
    def crypto_etl():
        extract = task("extract", run=extract_crypto)
        transform = task("transform", run=transform_crypto, depends_on=[extract])
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Callable, Dict, List, Optional


_REGISTRY: Dict[str, "DAG"] = {}
_build_context = threading.local()


class Task:
    """A single unit of work inside a DAG.

    `run` is a zero-argument callable. Real orchestrators pass along
    context (execution date, upstream outputs, etc.) — that's a natural
    extension point here, deliberately left out to keep the state
    machine and scheduling logic the focus of this project.
    """

    def __init__(self, name: str, run: Callable[[], None], depends_on: Optional[List["Task"]] = None):
        self.name = name
        self.run = run
        self.depends_on: List["Task"] = list(depends_on or [])

        current = _get_current_dag()
        if current is None:
            raise RuntimeError(
                "task() was called outside of a @dag-decorated function. "
                "Tasks must be declared inside the function you decorate with @dag."
            )
        current.add_task(self)

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        deps = [d.name for d in self.depends_on]
        return f"Task(name={self.name!r}, depends_on={deps})"


class DAG:
    """A named collection of tasks with dependency edges between them."""

    def __init__(self, name: str, schedule: Optional[str] = None):
        self.name = name
        self.schedule = schedule
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task_obj: Task) -> None:
        if task_obj.name in self.tasks:
            raise ValueError(f"Duplicate task name {task_obj.name!r} in DAG {self.name!r}")
        self.tasks[task_obj.name] = task_obj

    def topological_order(self) -> List[str]:
        """Kahn's algorithm. Raises ValueError if the graph has a cycle
        or references an unknown dependency."""
        in_degree = {name: 0 for name in self.tasks}
        children: Dict[str, List[str]] = {name: [] for name in self.tasks}

        for t in self.tasks.values():
            for dep in t.depends_on:
                if dep.name not in self.tasks:
                    raise ValueError(
                        f"Task {t.name!r} depends on {dep.name!r}, which is not "
                        f"registered in DAG {self.name!r}"
                    )
                in_degree[t.name] += 1
                children[dep.name].append(t.name)

        queue = deque(sorted(name for name, deg in in_degree.items() if deg == 0))
        order: List[str] = []

        while queue:
            name = queue.popleft()
            order.append(name)
            for child in sorted(children[name]):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(order) != len(self.tasks):
            remaining = sorted(set(self.tasks) - set(order))
            raise ValueError(f"Cycle detected in DAG {self.name!r} among tasks: {remaining}")

        return order

    def levels(self) -> List[List[str]]:
        """Group tasks by "depth" from the roots, so tasks in the same
        level have no dependency relationship to each other and can run
        concurrently. Used by the --parallel stretch goal."""
        order = self.topological_order()
        depth: Dict[str, int] = {}
        for name in order:
            t = self.tasks[name]
            depth[name] = 0 if not t.depends_on else 1 + max(depth[d.name] for d in t.depends_on)

        by_level: Dict[int, List[str]] = {}
        for name, lvl in depth.items():
            by_level.setdefault(lvl, []).append(name)

        return [sorted(by_level[lvl]) for lvl in sorted(by_level)]


def _get_current_dag() -> Optional[DAG]:
    return getattr(_build_context, "dag", None)


def dag(name: str, schedule: Optional[str] = None):
    """Decorator. Runs the decorated function once, immediately, to
    collect its task() calls into a new DAG, then registers that DAG
    under `name`. Returns the DAG object (not the original function) —
    matching the spec's example, where `crypto_etl` after decoration
    is the thing the runner operates on.
    """

    def decorator(build_fn: Callable[[], None]) -> DAG:
        new_dag = DAG(name=name, schedule=schedule)
        previous = _get_current_dag()
        _build_context.dag = new_dag
        try:
            build_fn()
        finally:
            _build_context.dag = previous
        _REGISTRY[name] = new_dag
        return new_dag

    return decorator


def task(name: str, run: Callable[[], None], depends_on: Optional[List[Task]] = None) -> Task:
    return Task(name=name, run=run, depends_on=depends_on)


def get_dag(name: str) -> DAG:
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise KeyError(f"No DAG registered as {name!r}. Registered DAGs: {available}")
    return _REGISTRY[name]


def all_dags() -> Dict[str, DAG]:
    return dict(_REGISTRY)


def clear_registry() -> None:
    """Mainly for tests, so each test starts from a clean registry."""
    _REGISTRY.clear()
