_current_dag = None

class Task:
    def __init__(self, name, run, depends_on=None):
        self.name = name
        self.run = run
        self.depends_on = depends_on or []


class DAG:
    def __init__(self, name, schedule=None):
        self.name = name
        self.schedule = schedule
        self.tasks = []

def task(name, run, depends_on=None):
    new_task = Task(name, run, depends_on)

    if _current_dag:
        _current_dag.tasks.append(new_task)

    return new_task

def dag(name, schedule=None):
    def decorator(func):
        global _current_dag

        dag_object = DAG(name, schedule)
        _current_dag = dag_object

        func()

        _current_dag = None

        return dag_object

    return decorator
