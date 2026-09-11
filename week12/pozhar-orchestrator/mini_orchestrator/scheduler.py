"""
scheduler.py - runs DAGs on their cron schedule and retries failed tasks.

Every `interval_seconds` (default 30), the scheduler:
  1. Re-scans the dags/ folder (so newly added DAGs are picked up without
     restarting)
  2. For each DAG with a `schedule` (cron expression), checks whether any
     scheduled fire time has passed since the last check
  3. If due AND the DAG isn't already running, triggers it in a background
     thread (so a slow/retrying DAG doesn't block checking other DAGs)
  4. If due but STILL running from a previous trigger, skips it and logs
     why - this is what prevents overlapping runs of the same DAG
  5. Logs every decision, including "nothing due" ticks

Timezone support (stretch goal): if a DAG declares timezone="Africa/Lagos",
its cron expression is evaluated in that timezone instead of UTC.
"""

import threading
import time
from datetime import datetime, timezone as dt_timezone
from zoneinfo import ZoneInfo

from croniter import croniter

from .registry import discover_dags
from .runner import Runner
from .state import StateStore


class Scheduler:
    def __init__(self, dags_dir="dags", interval_seconds=30, state_store=None):
        self.dags_dir = dags_dir
        self.interval_seconds = interval_seconds
        self.state = state_store or StateStore()
        self.running_dags = set()  # dag names currently executing
        self.lock = threading.Lock()
        # only fires for schedule times AFTER the scheduler started -
        # we don't want to immediately fire every DAG on startup for every
        # minute that passed while the scheduler was off
        self.last_check = datetime.now(dt_timezone.utc)

    def _get_tz(self, dag_obj):
        if dag_obj.timezone:
            return ZoneInfo(dag_obj.timezone)
        return dt_timezone.utc

    def _is_due(self, dag_obj, now):
        """True if this DAG has a scheduled fire time in (last_check, now]."""
        if not dag_obj.schedule:
            return False
        tz = self._get_tz(dag_obj)
        base = self.last_check.astimezone(tz)
        now_local = now.astimezone(tz)
        next_fire = croniter(dag_obj.schedule, base).get_next(datetime)
        return next_fire <= now_local

    def _trigger(self, dag_name, dag_func, now):
        with self.lock:
            if dag_name in self.running_dags:
                print(
                    f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] SKIP  {dag_name}: "
                    "previous run still in progress, not triggering (no overlap)."
                )
                return
            self.running_dags.add(dag_name)

        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] TRIGGER {dag_name}")

        def _run_in_background():
            try:
                built = dag_func()
                runner = Runner(state_store=self.state)
                runner.run(built, parallel=False, verbose=True)
            except Exception as e:
                print(f"Scheduler: unexpected error running '{dag_name}': {e}")
            finally:
                with self.lock:
                    self.running_dags.discard(dag_name)

        threading.Thread(target=_run_in_background, daemon=True).start()

    def tick(self):
        """One scheduling check cycle. Public so it can be called directly
        in tests without waiting for the real interval to pass."""
        now = datetime.now(dt_timezone.utc)
        dags = discover_dags(self.dags_dir)

        any_due = False
        for name, dag_func in dags.items():
            built = dag_func()
            if not built.schedule:
                continue
            if self._is_due(built, now):
                any_due = True
                self._trigger(name, dag_func, now)

        if not any_due:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Nothing due.")

        self.last_check = now

    def run_forever(self):
        print(
            f"Scheduler started. Checking every {self.interval_seconds}s "
            f"for DAGs due to run in '{self.dags_dir}/'. Ctrl+C to stop."
        )
        try:
            while True:
                self.tick()
                time.sleep(self.interval_seconds)
        except KeyboardInterrupt:
            print("\nScheduler stopped.")
