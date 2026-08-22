# Week 12 - Simple Orchestrator (DAG Runner)

## What this does
`mini_orchestrator` is a small DAG (Directed Acyclic Graph) orchestrator -
a simplified version of what Airflow/Dagster/Prefect do under the hood.

DAGs are defined as Python code:

```python
from mini_orchestrator import dag, task

@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():
    extract = task("extract", run=extract_crypto)
    transform = task("transform", run=transform_crypto, depends_on=[extract])
    load = task("load", run=load_to_warehouse, depends_on=[transform])
    quality_check = task("quality_check", run=run_dq_checks, depends_on=[load])
```

## How it works

- **`dag.py`** - `Task` and `DAG` classes. The `@dag` decorator and `task()`
  function let you define a DAG declaratively; `task()` registers itself
  with whichever DAG is currently being built using a small internal stack.
- **`runner.py`** - runs a DAG in two phases:
  1. Topological sort (Kahn's algorithm) computes a valid execution order,
     and detects cycles up front before anything runs.
  2. Walks that order task by task. A task only runs if all of its
     dependencies succeeded. If any dependency failed or was skipped, the
     task is marked `skipped` instead of running - this cascades naturally,
     so a failure early in the DAG skips everything downstream of it.
- **`state.py`** - persists every run to SQLite at `~/.mini_orchestrator/state.db`.
  Two tables: `dag_runs` (one row per run, overall status) and `task_runs`
  (one row per task per run, individual status + error message if failed).
- **`cli.py`** - command-line interface:
  - `python -m mini_orchestrator run <file.py>` - run a DAG defined in a file
  - `python -m mini_orchestrator history [--dag NAME] [--limit N]` - list recent runs
  - `python -m mini_orchestrator status <dag_name> <date>` - answers
    "did <dag_name>'s job succeed on this date?"

## Task state machine
