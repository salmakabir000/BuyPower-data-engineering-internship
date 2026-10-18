DAG_REGISTRY = {}
_CURRENT_DAG = None


class Task:
    def __init__(self, name, run, depends_on=None):
        self.name = name
        self.run = run
        self.depends_on = depends_on or []

    def __repr__(self):
        return f"Task({self.name})"


class DAG:
    def __init__(self, name, schedule=None):
        self.name = name
        self.schedule = schedule
        self.tasks = {}

    def add_task(self, task):
        self.tasks[task.name] = task

    def topological_sort(self):
        ordered = []
        remaining = set(self.tasks)

        while remaining:
            progress = False

            for name in list(remaining):
                task = self.tasks[name]

                dependencies = {
                    dep.name for dep in task.depends_on
                }

                if dependencies.issubset(set(ordered)):
                    ordered.append(name)
                    remaining.remove(name)
                    progress = True

            if not progress:
                raise ValueError("Circular dependency detected")

        return [self.tasks[name] for name in ordered]


def task(name, run, depends_on=None):
    """Create and register a task in the current DAG."""
    if _CURRENT_DAG is None:
        raise RuntimeError(
            "task() must be used inside a @dag function"
        )

    new_task = Task(
        name=name,
        run=run,
        depends_on=depends_on,
    )

    _CURRENT_DAG.add_task(new_task)
    return new_task


def dag(name, schedule=None):
    """Decorator used to define a DAG."""

    def decorator(function):
        global _CURRENT_DAG

        new_dag = DAG(
            name=name,
            schedule=schedule,
        )

        _CURRENT_DAG = new_dag

        try:
            function()
        finally:
            _CURRENT_DAG = None

        DAG_REGISTRY[name] = new_dag

        return new_dag

    return decorator


def get_dag(name):
    return DAG_REGISTRY.get(name)


def list_dags():
    return list(DAG_REGISTRY.values())