"""
dag.py - defines what a DAG and a Task are.
"""


class Task:
    def __init__(self, name, run, depends_on=None, retries=3, retry_delay_seconds=60):
        self.name = name
        self.run = run
        depends_on = depends_on or []
        self.depends_on = [
            d.name if isinstance(d, Task) else d for d in depends_on
        ]
        self.retries = retries  # max retry attempts after the first failure
        self.retry_delay_seconds = retry_delay_seconds  # base delay for backoff

    def __repr__(self):
        return f"Task(name={self.name!r}, depends_on={self.depends_on!r})"


class DAG:
    def __init__(self, name, schedule=None, timezone=None):
        self.name = name
        self.schedule = schedule  # cron expression, e.g. "*/2 * * * *"
        self.timezone = timezone  # e.g. "Africa/Lagos" - stretch goal
        self.tasks = {}

    def add_task(self, task_obj):
        if task_obj.name in self.tasks:
            raise ValueError(
                f"Task '{task_obj.name}' already exists in DAG '{self.name}'."
            )
        for dep_name in task_obj.depends_on:
            if dep_name not in self.tasks:
                raise ValueError(
                    f"Task '{task_obj.name}' depends on '{dep_name}', "
                    f"but '{dep_name}' hasn't been defined yet in DAG '{self.name}'."
                )
        self.tasks[task_obj.name] = task_obj

    def __repr__(self):
        return f"DAG(name={self.name!r}, tasks={list(self.tasks.keys())!r})"


_dag_build_stack = []


def _current_dag():
    if not _dag_build_stack:
        raise RuntimeError(
            "task() was called outside of a @dag-decorated function."
        )
    return _dag_build_stack[-1]


def dag(name, schedule=None, timezone=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            new_dag = DAG(name=name, schedule=schedule, timezone=timezone)
            _dag_build_stack.append(new_dag)
            try:
                func(*args, **kwargs)
            finally:
                _dag_build_stack.pop()
            return new_dag

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper._mini_orchestrator_dag_name = name
        return wrapper

    return decorator


def task(name, run, depends_on=None, retries=3, retry_delay_seconds=60):
    t = Task(
        name=name, run=run, depends_on=depends_on,
        retries=retries, retry_delay_seconds=retry_delay_seconds,
    )
    _current_dag().add_task(t)
    return t
