"""
dag.py - defines what a DAG and a Task are.

A DAG is just: a name + a collection of Tasks.
A Task is just: a name + a Python function to run + a list of tasks it depends on.

The @dag decorator and task() function below let you write DAGs like this:

    from mini_orchestrator import dag, task

    @dag(name="crypto_etl", schedule="0 * * * *")
    def crypto_etl():
        extract = task("extract", run=extract_crypto)
        transform = task("transform", run=transform_crypto, depends_on=[extract])

Calling crypto_etl() returns a built DAG object, ready to hand to the runner.
"""


class Task:
    """One unit of work inside a DAG."""

    def __init__(self, name, run, depends_on=None):
        self.name = name
        self.run = run  # the actual Python function to call
        # depends_on is a list of Task objects (or task names) this task waits on.
        # We store just the names internally - that's all the runner needs.
        depends_on = depends_on or []
        self.depends_on = [
            d.name if isinstance(d, Task) else d for d in depends_on
        ]

    def __repr__(self):
        return f"Task(name={self.name!r}, depends_on={self.depends_on!r})"


class DAG:
    """A named collection of Tasks with dependencies between them."""

    def __init__(self, name, schedule=None):
        self.name = name
        self.schedule = schedule
        self.tasks = {}  # task name -> Task object

    def add_task(self, task_obj):
        if task_obj.name in self.tasks:
            raise ValueError(
                f"Task '{task_obj.name}' already exists in DAG '{self.name}'. "
                "Task names must be unique within a DAG."
            )
        # every dependency must already exist in this DAG - catches typos early
        for dep_name in task_obj.depends_on:
            if dep_name not in self.tasks:
                raise ValueError(
                    f"Task '{task_obj.name}' depends on '{dep_name}', "
                    f"but '{dep_name}' hasn't been defined yet in DAG '{self.name}'. "
                    "Define tasks in dependency order (dependencies first)."
                )
        self.tasks[task_obj.name] = task_obj

    def __repr__(self):
        return f"DAG(name={self.name!r}, tasks={list(self.tasks.keys())!r})"


# --- Context tracking so task() knows which DAG it's currently being built for ---
# This is a simple stack rather than a single variable so nested/future use is safe,
# though in practice we only ever build one DAG at a time.
_dag_build_stack = []


def _current_dag():
    if not _dag_build_stack:
        raise RuntimeError(
            "task() was called outside of a @dag-decorated function. "
            "task() can only be used inside a function decorated with @dag."
        )
    return _dag_build_stack[-1]


def dag(name, schedule=None):
    """Decorator that turns a plain function (which calls task() internally)
    into a function that returns a fully built DAG object."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            new_dag = DAG(name=name, schedule=schedule)
            _dag_build_stack.append(new_dag)
            try:
                func(*args, **kwargs)
            finally:
                _dag_build_stack.pop()
            return new_dag

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        # marker so the CLI can find dag-decorated functions inside a file
        # without having to call every function in the file to check
        wrapper._mini_orchestrator_dag_name = name
        return wrapper

    return decorator


def task(name, run, depends_on=None):
    """Defines one task and registers it with the DAG currently being built."""
    t = Task(name=name, run=run, depends_on=depends_on)
    _current_dag().add_task(t)
    return t
