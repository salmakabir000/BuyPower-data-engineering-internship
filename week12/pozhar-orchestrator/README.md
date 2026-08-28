# Week 12-13 - Simple Orchestrator (DAG Runner)

## What this does
`mini_orchestrator` is a small DAG (Directed Acyclic Graph) orchestrator - a
simplified version of what Airflow/Dagster/Prefect do under the hood. DAGs
are defined declaratively as Python code:

```python
from mini_orchestrator import dag, task

@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():
    extract = task("extract", run=extract_crypto)
    transform = task("transform", run=transform_crypto, depends_on=[extract])
    load = task("load", run=load_to_warehouse, depends_on=[transform])
```

## CLI commands
- `mini_orchestrator list` - list all registered DAGs (auto-discovered from `dags/`)
- `mini_orchestrator run <dag_name> [--parallel]` - run a DAG once
- `mini_orchestrator status <dag_name>` - show the last 10 runs of a DAG
- `mini_orchestrator logs <dag_name> <run_id>` - show task-by-task status/timing for one run

## Task state machine




## Design doc

**How DAGs are defined and discovered.** A DAG is just a name plus a
dictionary of `Task` objects, where each task stores its own name, the
function to run, and the names of tasks it depends on. The `@dag` decorator
and `task()` function let you write DAGs declaratively rather than manually
building graph objects: `task()` uses a small internal stack to know which
DAG is "currently being defined," so it can register itself automatically
when called inside a `@dag`-decorated function. For the CLI to run a DAG by
name (`mini_orchestrator run crypto_etl`) instead of needing a file path, I
added a `registry.py` module that scans a `dags/` folder, imports every
`.py` file in it, and collects any function marked by the `@dag` decorator
into a `{name: function}` dictionary. This mirrors how Airflow's own
"dags folder" concept works, just far simpler.

**How execution and failure handling work.** The runner works in two
phases. First, it computes a valid execution order using Kahn's topological
sort algorithm - repeatedly finding tasks with no unsatisfied dependencies,
"running" them structurally, and checking what that unblocks next. This
also catches cycles immediately (a DAG where two tasks depend on each
other) before any real code executes. Second, it walks that order and, for
each task, checks whether every one of its dependencies actually succeeded
during this run. If so, the task executes for real and its result
(success/failed) is recorded. If not, the task is marked `skipped` without
running it at all. This check-then-skip approach means failure cascades
happen automatically: if task B fails, task C (which depends on B) sees
that B's status isn't `success`, so C gets skipped too, and this continues
down the whole chain with no extra "propagate failure" code needed. For the
stretch goal, I added an optional `--parallel` mode that groups tasks into
"levels" by dependency depth (a task's level is one more than the deepest
level among its dependencies) and runs every task in a level concurrently
using `ThreadPoolExecutor`, since tasks at the same level can never depend
on each other. I verified this actually saves time, not just structurally:
two independent 1-second tasks took 2.1s sequentially but only 1.1s in
parallel.

**How state is persisted and queried.** Every run writes to a SQLite
database at `~/.mini_orchestrator/state.db`, using two tables: `dag_runs`
(one row per run - overall status, start/finish time) and `task_runs` (one
row per task within a run - its own status, timing, and error message if it
failed). Run IDs are short random hex strings rather than auto-incrementing
numbers, generated with `uuid.uuid4().hex[:6]`, to match a more realistic
production ID style. The `status` command queries `dag_runs` for the most
recent runs of a given DAG name, and for any failed run, looks up which
specific task failed and extracts the exception type from its stored error
message to build a message like `failed (transform: ValueError)`. The
`logs` command does a similar lookup but returns full task-level detail for
one specific run, which is what actually answers "why did this fail" rather
than just "did it fail."

## Wired to the real Week 4 ETL
`dags/crypto_etl_dag.py` wraps the actual CoinGecko ETL from Week 4
(`extract`/`transform`/`load` functions, unchanged) as a 3-task DAG. Since a
Task's `run()` function takes no arguments, I used a `context` dict that
each task reads from and writes to, so `extract`'s output becomes
`transform`'s input, and so on. Verified end-to-end against the live
CoinGecko API - see `example_runs_screenshot.png` for 5 successful runs.

## Example files
- `dags/example_dag.py` - a DAG where every task succeeds
- `dags/broken_dag.py` - a DAG where `transform` fails, demonstrating the
  failure -> skip cascade
- `dags/crypto_etl_dag.py` - the real Week 4 ETL wrapped as a DAG
