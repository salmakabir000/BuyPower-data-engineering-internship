"""
Scheduler component.

Every `check_interval_seconds` (default 30s), the scheduler asks each
registered DAG: "is your next cron-scheduled fire time due?" If yes and
no run of that DAG is currently in progress, it triggers the DAG via
DAGRunner on a background thread (so a slow/retrying DAG never blocks
the scheduling loop or other DAGs). If a run is already in progress, the
new trigger is skipped and logged -- this is the "no overlapping runs"
requirement.

Cron parsing is delegated entirely to `croniter` -- we never hand-roll
cron field parsing.
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from croniter import croniter

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from .dag import DAG
from .runner import DAGRunner, DAGRunResult

logger = logging.getLogger("mini_orchestrator.scheduler")


def _now_in_tz(tz_name: Optional[str]) -> datetime:
    """Current time, timezone-aware. If tz_name is given, in that zone
    (stretch goal: per-DAG timezone-aware cron); otherwise UTC."""
    if tz_name:
        if ZoneInfo is None:
            raise RuntimeError("zoneinfo not available; install tzdata or use Python 3.9+")
        return datetime.now(ZoneInfo(tz_name))
    return datetime.now(timezone.utc)


class Scheduler:
    """Ticks on a fixed interval and fires due DAGs.

    `clock_fn` is injectable: pass a fake clock to run fast, time-
    compressed demos/tests instead of waiting on real wall-clock time.
    """

    def __init__(
        self,
        check_interval_seconds: float = 30.0,
        clock_fn: Optional[Callable[[Optional[str]], datetime]] = None,
        runner: Optional[DAGRunner] = None,
        on_run_complete: Optional[Callable[[DAGRunResult], None]] = None,
    ):
        self.check_interval_seconds = check_interval_seconds
        self.clock_fn = clock_fn or _now_in_tz
        self.runner = runner or DAGRunner()
        self.on_run_complete = on_run_complete

        self._dags: Dict[str, DAG] = {}
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, dag: DAG) -> None:
        if dag.name in self._dags:
            raise ValueError(f"DAG '{dag.name}' is already registered")
        # Validate the cron expression eagerly so bad config fails fast,
        # not on the first tick.
        croniter(dag.schedule)
        self._dags[dag.name] = dag
        logger.info(
            "event=dag_registered dag=%s schedule=%r timezone=%s",
            dag.name, dag.schedule, dag.timezone or "UTC",
        )

    def register_all(self, dags: List[DAG]) -> None:
        for dag in dags:
            self.register(dag)

    # ------------------------------------------------------------------
    # Core loop
    # ------------------------------------------------------------------

    def start(self, max_ticks: Optional[int] = None) -> None:
        """Run the scheduler loop in the foreground (blocking).

        `max_ticks`: for tests/demos, stop after N ticks instead of
        running forever. None (default) = run until stopped.
        """
        logger.info(
            "event=scheduler_started check_interval_seconds=%s dag_count=%d",
            self.check_interval_seconds, len(self._dags),
        )
        self._stop_event.clear()
        tick_count = 0
        try:
            while not self._stop_event.is_set():
                self.tick()
                tick_count += 1
                if max_ticks is not None and tick_count >= max_ticks:
                    break
                self._stop_event.wait(self.check_interval_seconds)
        except KeyboardInterrupt:
            logger.info("event=scheduler_interrupted")
        finally:
            self.stop()

    def stop(self) -> None:
        self._stop_event.set()
        logger.info("event=scheduler_stopping active_threads=%d", len(
            [t for t in self._threads if t.is_alive()]
        ))
        for t in self._threads:
            t.join(timeout=0.1)

    def tick(self) -> None:
        """One scheduling pass over all registered DAGs. Every decision
        is logged, including 'nothing to do'."""
        if not self._dags:
            logger.info("event=tick_evaluated dag_count=0 note=no_dags_registered")
            return

        for dag in self._dags.values():
            self._evaluate_dag(dag)

    def _evaluate_dag(self, dag: DAG) -> None:
        now = self.clock_fn(dag.timezone)

        if dag._last_checked_at is None:
            # First tick ever for this DAG: establish a baseline instead
            # of firing immediately (avoids a burst-fire at scheduler
            # startup for every registered DAG).
            dag._last_checked_at = now
            logger.info(
                "event=schedule_decision dag=%s decision=baseline_established "
                "now=%s next_check_from=%s",
                dag.name, now.isoformat(), now.isoformat(),
            )
            return

        cron = croniter(dag.schedule, dag._last_checked_at)
        next_fire_time = cron.get_next(datetime)

        if next_fire_time > now:
            logger.info(
                "event=schedule_decision dag=%s decision=not_due now=%s next_fire_time=%s",
                dag.name, now.isoformat(), next_fire_time.isoformat(),
            )
            dag._last_checked_at = now
            return

        # It's due. Check for an overlapping run before triggering.
        if not dag.try_acquire_run_slot():
            logger.warning(
                "event=schedule_decision dag=%s decision=skipped_overlap "
                "now=%s next_fire_time=%s reason=previous_run_still_in_progress",
                dag.name, now.isoformat(), next_fire_time.isoformat(),
            )
            # Do NOT advance _last_checked_at past this fire time -- we
            # want the next tick to notice it's *still* due (or, if you
            # prefer catch-up semantics, this is where you'd decide to
            # drop vs queue the missed run). Here we drop it: the very
            # next successful check becomes the new baseline.
            dag._last_checked_at = now
            return

        logger.info(
            "event=schedule_decision dag=%s decision=triggered now=%s next_fire_time=%s",
            dag.name, now.isoformat(), next_fire_time.isoformat(),
        )
        dag._last_checked_at = now
        dag._last_triggered_at = now

        run_id = uuid.uuid4().hex[:8]
        thread = threading.Thread(
            target=self._run_dag_in_background,
            args=(dag, run_id),
            name=f"dag-run-{dag.name}-{run_id}",
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()
        # Keep the thread list from growing unboundedly over a long-running
        # scheduler process.
        self._threads = [t for t in self._threads if t.is_alive()]

    def _run_dag_in_background(self, dag: DAG, run_id: str) -> None:
        try:
            result = self.runner.run(dag, run_id)
            if self.on_run_complete:
                self.on_run_complete(result)
        finally:
            dag.release_run_slot()
