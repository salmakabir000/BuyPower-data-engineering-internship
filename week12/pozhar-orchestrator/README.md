# Week 14 - Simple Scheduler with Retries

Extends the Week 12-13 orchestrator with a scheduler that runs DAGs on a
cron schedule and automatically retries failed tasks with exponential
backoff.

## New CLI command

mini_orchestrator scheduler start [--interval SECONDS]

Runs in the foreground. Every `interval` seconds (default 30), it checks
every registered DAG's `schedule` (a cron expression) and triggers any DAG
whose scheduled time has passed since the last check. Logs every decision,
including ticks where nothing was due.

## Retries
Each task now accepts `retries` (default 3) and `retry_delay_seconds`
(default 60):
```python
task("extract", run=extract_crypto, retries=3, retry_delay_seconds=60)
```
On failure, a task retries using exponential backoff with jitter:

delay = retry_delay_seconds * 2^(attempt - 1) + random(0, 30)

Errors are classified as retryable or not (`retry.py`). `ValueError`,
`TypeError`, `KeyError`, and a custom `NonRetryableError` skip straight to
`failed_permanent` with no retries, since these usually mean a logic bug
that won't fix itself between attempts. Everything else (network/timeout
errors) is retried.

## Fixed delay vs. exponential backoff

**Fixed delay** means waiting the same amount of time between every retry -
e.g. always 60 seconds, no matter how many times it's failed. This is
simple and fine for something that fails rarely and recovers fast, like a
single dropped network packet. The downside shows up when many things fail
at once, or a downstream system goes down for a while: everything retries
at the same steady rate, which can pile more load onto a system that's
already struggling, and doesn't give it meaningfully more time to recover
on later attempts than on earlier ones.

**Exponential backoff** doubles the wait time after each failure (60s, then
120s, then 240s...), so retries naturally space out further apart the
longer something stays broken. Early on it retries almost as fast as fixed
delay would, but if the problem persists, it backs off aggressively instead
of continuing to hammer a struggling system. Adding jitter (a small random
extra wait) on top prevents many tasks that failed at the same moment from
all retrying in a synchronized burst - which would create it own new load
spike right when a recovering system is most vulnerable.

**When to use which:** fixed delay is fine for something you're confident
fails only briefly (e.g. a rate-limited API with a known cooldown).
Exponential backoff with jitter is the safer default for anything talking
to another service you don't fully control, since it degrades gracefully
whether the failure lasts one second or ten minutes.

## Overlap protection
Before triggering a DAG, the scheduler checks if that DAG name is already
in `running_dags` (an in-memory set, tracked per scheduler process). If so,
it skips the trigger and logs why - no two runs of the same DAG ever
execute at once. Verified live: a DAG scheduled every minute with a 90s
task correctly triggers, sleeps, gets skipped once when its next scheduled
minute arrives mid-run, then triggers again once free.

## Timezone support (stretch goal)
A DAG can declare `timezone="Africa/Lagos"`, and its cron expression is
evaluated in that timezone instead of UTC:
```python
@dag(name="daily_report", schedule="0 9 * * *", timezone="Africa/Lagos")
```
This means "9am Lagos time," not "9am UTC" - the scheduler converts both
its last-check time and the current time into the DAG's timezone before
asking croniter whether a fire time has passed.

## Demo files
- `demo_scheduled_dag.py` - runs every 2 minutes, used for the 10-minute
  unattended scheduler demo (see `scheduler_log_screenshot.png` - 18
  successful triggers over 35+ minutes, no manual intervention)
- `flaky_demo_dag.py` - fails twice, succeeds on the 3rd attempt, shows
  retry + backoff timing
- `always_fails_dag.py` - always fails, shows retries exhausting into
  `failed_permanent`

## New task/run states this week
