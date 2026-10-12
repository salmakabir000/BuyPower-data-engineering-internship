"""
Core data model: Task, DAG, task/run states, and the retryable vs
non-retryable error hierarchy.

Design notes
------------
- A Task wraps a plain Python callable. Tasks don't know about
  scheduling or retries themselves -- the DAGRunner drives that.
- Error classification is done via exception type. Raise a
  RetryableError for anything transient (timeouts, connection resets,
  rate limits, 5xx). Raise NonRetryableError (or let something else
  propagate, see runner.py) for anything that retrying can't fix
  (bad config, 4xx, programming bugs, assertion errors).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Optional


# --------------------------------------------------------------------------
# Error classification
# --------------------------------------------------------------------------

class RetryableError(Exception):
    """Raise this for transient failures worth retrying.

    Examples: network timeouts, connection resets, HTTP 429/5xx,
    a downstream queue being temporarily full, a deadlock detected
    by the database.
    """


class NonRetryableError(Exception):
    """Raise this for failures that retrying will never fix.

    Examples: bad credentials, malformed input, a 4xx client error,
    a schema mismatch, an assertion/logic bug.
    """


# Any exception NOT in this tuple that isn't explicitly NonRetryableError
# is treated as retryable by default. This mirrors real-world practice:
# unknown failures are usually transient (a crashed worker, an OOM, a
# flaky dependency) so the safe default is to retry them, while a small
# well-known set of "this will never work" errors short-circuits retries.
NON_RETRYABLE_TYPES = (NonRetryableError,)


def classify_error(exc: BaseException) -> str:
    """Return 'retryable' or 'non_retryable' for a raised exception."""
    if isinstance(exc, NON_RETRYABLE_TYPES):
        return "non_retryable"
    return "retryable"


# --------------------------------------------------------------------------
# States
# --------------------------------------------------------------------------

class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"          # between attempts, waiting out backoff
    SUCCESS = "success"
    FAILED_PERMANENT = "failed_permanent"  # retries exhausted (or non-retryable)


class DAGRunState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


# --------------------------------------------------------------------------
# Task
# --------------------------------------------------------------------------

@dataclass
class Task:
    name: str
    func: Callable[..., None]
    retries: int = 3
    retry_delay_seconds: float = 60.0
    # Max delay cap so backoff doesn't grow unboundedly on flaky, long-lived
    # outages. Not in the spec explicitly but a sane real-world guardrail.
    max_retry_delay_seconds: float = 3600.0

    def __post_init__(self) -> None:
        if self.retries < 0:
            raise ValueError("retries must be >= 0")
        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be >= 0")


# --------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------

@dataclass
class DAG:
    """A DAG is a name, a cron schedule, and an ordered list of tasks.

    Tasks run strictly in list order (this mini orchestrator doesn't do
    dependency-graph fan-out -- that was Week 10-11's job; this project
    is about scheduling, not DAG topology).
    """

    name: str
    schedule: str                      # cron expression, e.g. "*/2 * * * *"
    tasks: List[Task] = field(default_factory=list)
    timezone: Optional[str] = None     # e.g. "Africa/Lagos"; None = local/UTC
    catchup: bool = False              # if True, fire once for every missed
                                        # tick since last check; if False,
                                        # collapse missed ticks into one fire

    # --- scheduler bookkeeping (mutated by Scheduler, not user code) -----
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    _is_running: bool = field(default=False, repr=False, compare=False)
    _last_checked_at = None            # datetime; set on first scheduler tick
    _last_triggered_at = None          # datetime; last time this DAG fired

    def add_task(self, task: Task) -> "DAG":
        self.tasks.append(task)
        return self

    # --- concurrency guard: prevents overlapping runs of the same DAG ----
    def try_acquire_run_slot(self) -> bool:
        """Return True and mark as running if no run is currently in
        progress; return False if a run is already in progress."""
        with self._lock:
            if self._is_running:
                return False
            self._is_running = True
            return True

    def release_run_slot(self) -> None:
        with self._lock:
            self._is_running = False

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._is_running
