# mini_orchestrator: Scheduler with Retries

Extends the mini_orchestrator (Week 10-11) with a cron-based scheduler
component and per-task retry/backoff logic.

## What's here

```
mini_orchestrator/
  dag.py            Task, DAG, TaskState, DAGRunState, RetryableError, NonRetryableError
  runner.py         DAGRunner: runs a DAG's tasks in order, with retry/backoff
  scheduler.py       Scheduler: cron ticking, due-check, overlap protection
  registry.py        Discovers DAG objects from a dags/ directory
  logging_config.py  Console + file logging setup
  cli.py             `mini_orchestrator scheduler start` command
dags/
  demo_dag.py         */2 * * * * DAG with a flaky task (shows retries)
  bad_config_dag.py   DAG that fails with a NonRetryableError (shows no wasted retries)
  lagos_daily_dag.py  Stretch goal: timezone="Africa/Lagos" DAG
tests/
  test_core.py        Unit tests for backoff math, error classification,
                       retry exhaustion, and overlap protection
demo_fast.py          Time-compressed proof of the "5 triggers over 10
                       minutes" requirement (uses a fake clock so you
                       don't have to wait 10 real minutes)
demo_overlap.py        Standalone proof that a slow-running DAG's next
                       due-time is skipped, not run concurrently
logs/                  Log output lands here
```

## Running it for real

```bash
pip install croniter pytz
python -m mini_orchestrator scheduler start --dags-dir dags --check-interval 30
```

This runs in the foreground, checks every 30 seconds whether any
registered DAG is due, and triggers due DAGs via the runner. Every
scheduling decision is logged — including "nothing to do" ticks — to
both stdout and `logs/scheduler.log`.

Leave `dags/demo_dag.py` registered (schedule `*/2 * * * *`) and let it
run for 10+ minutes to see 5 real triggers, exactly as the assignment
asks. Stop it with Ctrl+C.

## Running the fast demos (no 10-minute wait)

```bash
python3 demo_fast.py     # proves 5 triggers of a */2 * * * * DAG fire correctly
python3 demo_overlap.py  # proves a concurrent due-check is skipped, not double-run
```

These inject a fake clock into the scheduler so cron due-checks
advance in simulated time; actual task execution (including real retry
backoff sleeps) still runs for real. Output also lands in `logs/`.

## Running the tests

```bash
pip install pytest
python3 -m pytest tests/ -v
```

## How scheduling works

- Cron parsing is entirely delegated to `croniter` — no hand-rolled
  cron parser.
- Each DAG remembers `_last_checked_at`. On every tick, the scheduler
  asks croniter for the next fire time after `_last_checked_at`. If
  that time has already passed, the DAG is due.
- This means the scheduler only needs to *tick* — it doesn't need to
  sleep-align to cron boundaries itself. A 30-second check interval
  against a `*/2 * * * *` schedule fires within 30 seconds of the
  actual minute mark, which is fine for this use case (Airflow/most
  schedulers work exactly this way).
- If more than one cron boundary was missed since the last tick (e.g.
  the scheduler process was down), this implementation does **not**
  catch up and fire multiple times — it collapses missed ticks into a
  single check and moves on. That's a deliberate simplification
  (`catchup=False` is the only mode implemented); a production
  scheduler would let you choose catch-up vs skip-ahead per DAG.

## Avoiding double-firing / overlapping runs

Each `DAG` has a `threading.Lock`-guarded `_is_running` flag.
`try_acquire_run_slot()` atomically checks-and-sets it; a background
thread doing the actual DAG run releases it in a `finally` block, so a
crash mid-run doesn't leave the DAG stuck as "always running".

If the scheduler tick finds a DAG both due *and* already running, it
does **not** start a second run. Instead it logs
`decision=skipped_overlap` and moves on — the next scheduled window
still gets its own due-check. This was verified in
`demo_overlap.py` and `tests/test_core.py::test_scheduler_skips_overlapping_trigger_and_logs_it`.

## Retry logic

Each `Task` takes:
- `retries` (default 3) — number of retries *after* the first attempt
  (so 4 total attempts by default)
- `retry_delay_seconds` (default 60) — base delay used in the backoff
  formula below
- `max_retry_delay_seconds` (default 3600) — a cap so backoff doesn't
  grow unboundedly during a long outage

On failure:
1. The raised exception is classified as `retryable` or
   `non_retryable` (see below).
2. If non-retryable, or if retries are exhausted, the task is marked
   `failed_permanent` immediately and the DAG run stops (no downstream
   tasks execute).
3. Otherwise the task enters state `retrying`, sleeps for
   `min(retry_delay_seconds * 2^(attempt-1), max_retry_delay_seconds) + uniform(0, 30)`
   seconds, then attempts again.

### Retryable vs non-retryable errors

- `RetryableError` — raise this for anything transient: network
  timeouts, connection resets, HTTP 429/5xx, a queue temporarily full,
  a database deadlock. Retrying has a real chance of succeeding.
- `NonRetryableError` — raise this for anything retrying can't fix:
  missing/bad config, malformed input, HTTP 4xx, a schema mismatch, an
  assertion or logic bug. Retrying just burns time and jitter for a
  guaranteed-repeat failure.
- Any *other* exception type (something you didn't explicitly
  classify) is treated as retryable by default. Reasoning: most
  unclassified failures in practice are transient infrastructure
  problems (a worker got OOM-killed, a dependency blipped), so the
  safer default is to retry rather than to silently give up on the
  first unexpected error. You opt *out* of retrying by explicitly
  raising `NonRetryableError` for the specific failure modes you know
  are permanent.

### Fixed delay vs. exponential backoff — and when to use each

**Fixed delay** retries after the same wait every time
(`retry_delay_seconds`, unchanged, on every attempt). Use it when:
- The failure is usually a one-off blip that resolves almost
  immediately (e.g. a single dropped TCP packet), so there's no benefit
  to waiting longer on later attempts.
- You're retrying against something you control and know the recovery
  time for (e.g. "this cache always finishes warming in ~5s").
- You want predictable, bounded total retry time and the downstream
  system can handle being hit at a constant rate.

**Exponential backoff (with jitter)** doubles the wait after each
failed attempt (`retry_delay * 2^(attempt-1)`), plus a random jitter
component. Use it when:
- The failure might indicate the downstream system is *struggling*,
  not just blipping — a database under load, a rate-limited API, a
  service mid-outage. Retrying immediately and repeatedly at a fixed
  interval is exactly the "thundering herd" pattern that makes such
  outages worse or delays their recovery.
- You have many callers (many DAG runs/tasks) that could all fail at
  once and all start retrying at the same time — jitter spreads their
  retries out instead of having them all hammer the downstream system
  on the same fixed cadence.
- You don't know how long the failure will last, so you want later
  retries to naturally wait longer, giving the downstream system more
  room to recover before being hit again.

This project uses exponential backoff with jitter for all retryable
task failures, per the assignment spec, because task failures here are
almost always "some downstream system is unhappy" rather than
single-packet blips.

## Stretch goal: timezone support

A `DAG` can set `timezone="Africa/Lagos"` (any IANA tz name). The
scheduler's due-check computes "now" in that timezone (via
`zoneinfo`) before asking croniter whether the DAG's cron expression
is due, so `"0 9 * * MON-FRI"` on a `timezone="Africa/Lagos"` DAG means
9am West Africa Time, not 9am UTC. See `dags/lagos_daily_dag.py`.

## Known simplifications (given the scope of this exercise)

- Tasks run strictly in list order within a DAG — no dependency graph /
  fan-out (that was Week 10-11's job).
- No persistence: DAG run history and `_last_checked_at` live in
  memory and reset if the scheduler process restarts. A real system
  would persist this (e.g. in Postgres) so a restart doesn't cause a
  missed-or-duplicate fire.
- Missed cron ticks (scheduler was down) are not caught up — see
  "How scheduling works" above.
