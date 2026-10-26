# mini_orchestrator

A small DAG runner built to demystify what Airflow, Dagster, and Prefect are
actually doing underneath: topological sort, a task state machine, and a
persistence layer you can query for run history.

## Install

```bash
pip install -e .
```

This registers a `mini_orchestrator` command (see `pyproject.toml`'s
`[project.scripts]` entry).

## Define a DAG

Create a `dags.py` in your working directory (or point
`MINI_ORCH_DAGS_MODULE` at a different importable module):

```python
from mini_orchestrator import dag, task

@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():
    extract = task("extract", run=extract_crypto)
    transform = task("transform", run=transform_crypto, depends_on=[extract])
    load = task("load", run=load_to_warehouse, depends_on=[transform])
    task("quality_check", run=run_dq_checks, depends_on=[load])
```

`dags.py` in this repo wires this up for real, against the CoinGecko API,
with SQLite as the "warehouse." `demo_dags.py` is a network-free DAG used
only to generate realistic run history for testing/screenshots.

## CLI

```bash
mini_orchestrator list                          # list registered DAGs
mini_orchestrator run crypto_etl                 # run once, sequentially
mini_orchestrator run crypto_etl --parallel      # run, parallelizing same-level tasks
mini_orchestrator status crypto_etl              # last 10 runs
mini_orchestrator logs crypto_etl <run_id>        # per-task status/timing for one run
```

## Run the tests

```bash
pip install -e ".[dev]"  # or: pip install pytest
pytest tests/ -v
```

9 tests cover: topological ordering, diamond dependencies, cycle detection,
failure-cascading skips (including partial failure where an independent
branch still completes), state persistence across repeated runs, and both
sequential and parallel execution modes.

---

## Design doc

**Why a decorator that runs the function eagerly, instead of a context
manager or a declarative dict.** The spec's example DAG syntax calls
`task()` as a bare function inside the body of a function decorated with
`@dag`. To make `depends_on=[extract]` work — referencing a `Task` object
that was itself just returned from an earlier `task()` call — the tasks
need somewhere to register themselves the moment they're constructed. I used
a thread-local "which DAG is currently being built" pointer: `@dag` sets it,
calls the function once, and unsets it in a `finally` block. This is exactly
the trick Airflow's `with DAG(...) as dag:` context manager plays with
`DAG.CURRENT_DAG`, just via decorator instead of context manager, since a
context manager would make you write `with dag(...) as d:` and break the
spec's example. The cost is that `task()` must always be called inside a
`@dag` function body — calling it standalone raises a clear `RuntimeError`
rather than failing silently later.

**Why the state machine only has one failure mode ("skip everything
downstream"), not partial retry or backoff.** Airflow supports per-task
retries, retry delays, and trigger rules (run if any upstream failed, run
regardless, etc.) — I deliberately left all of that out. The learning
objective here is topological sort plus a *correct* pending →
running → success/failed/skipped transition, and adding retry logic would
mean debugging two state machines interacting (the retry loop, and the
DAG-level cascade) instead of one. The skip rule itself is simple and
matches Airflow's default trigger rule (`all_success`): a task runs only if
every direct dependency succeeded; if a dependency failed *or was itself
skipped*, this task is skipped too. Because I check status level-by-level
in topological order, that cascades correctly through arbitrarily long
chains without any special-casing — a failure at the root of a
20-task-deep chain skips all 19 descendants, one propagation step at a
time, not by walking the whole subtree recursively.

**Why writes happen at every state transition instead of buffering results
in memory and writing once at the end.** `create_task_run` fires the moment
a task starts (status `running`), and `finish_task_run` fires the moment it
ends. This means `status`/`logs` are accurate even if the whole Python
process is killed mid-run — you'd see a task stuck at `running` forever
rather than the run vanishing entirely, which is closer to how a real
orchestrator's UI can show "still running" or "orphaned" rows. It costs
more SQLite round-trips than batching, but for this project's scale (a
handful of tasks per run) that's free, and it was the only way to make
"did Tuesday's job succeed" answerable even for a run that crashed instead
of failing gracefully.

## Known limitations / things a real orchestrator adds that this doesn't

- No scheduler loop — `schedule` is stored but nothing reads a cron string
  and triggers runs automatically; you'd wire that to `cron` or a `while
  True: sleep` loop yourself.
- No retries — a failed task is just failed, once.
- No task-to-task data passing (XComs in Airflow) — tasks communicate via
  side effects (files, a database), same as the CoinGecko example.
- SQLite via one file means this won't scale to concurrent *DAG* runs
  writing at high frequency, though normal `sqlite3` write-locking makes it
  safe for the single-machine, run-DAGs-occasionally scale this project
  targets.
