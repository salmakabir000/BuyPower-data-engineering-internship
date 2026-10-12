"""
DAGRunner: executes the tasks of a single DAG run, in order, applying
per-task retry policy with exponential backoff + jitter.

This is intentionally the same runner the Week 10-11 orchestrator used
to trigger DAGs on demand -- the scheduler in scheduler.py just calls
into this on a timer instead of on a button press / CLI invocation.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from .dag import DAG, Task, TaskState, DAGRunState, classify_error

logger = logging.getLogger("mini_orchestrator.runner")


@dataclass
class TaskAttemptResult:
    attempt: int
    state: TaskState
    error: Optional[str] = None


@dataclass
class TaskResult:
    task_name: str
    final_state: TaskState
    attempts: List[TaskAttemptResult]


@dataclass
class DAGRunResult:
    dag_name: str
    run_id: str
    state: DAGRunState
    started_at: datetime
    finished_at: datetime
    task_results: List[TaskResult]


def compute_backoff_seconds(
    attempt: int,
    retry_delay_seconds: float,
    max_retry_delay_seconds: float,
    jitter_max_seconds: float = 30.0,
    rand: Optional[random.Random] = None,
) -> float:
    """Exponential backoff with jitter.

    delay = min(retry_delay * 2^(attempt - 1), cap) + uniform(0, jitter_max)

    `attempt` is 1-indexed: this is the delay taken *after* attempt
    number `attempt` has failed, before trying attempt `attempt + 1`.
    """
    rand = rand or random
    base = retry_delay_seconds * (2 ** (attempt - 1))
    base = min(base, max_retry_delay_seconds)
    jitter = rand.uniform(0, jitter_max_seconds)
    return base + jitter


class DAGRunner:
    """Runs one DAG to completion (all tasks, in order), handling retries.

    `sleep_fn` is injectable so tests / demos can run without waiting
    out real backoff delays.
    """

    def __init__(self, sleep_fn=time.sleep, rand: Optional[random.Random] = None):
        self.sleep_fn = sleep_fn
        self.rand = rand or random

    def run(self, dag: DAG, run_id: str) -> DAGRunResult:
        started_at = datetime.now(timezone.utc)
        logger.info("dag=%s run_id=%s event=run_started", dag.name, run_id)

        task_results: List[TaskResult] = []
        dag_failed = False

        for task in dag.tasks:
            result = self._run_task_with_retries(dag.name, run_id, task)
            task_results.append(result)
            if result.final_state != TaskState.SUCCESS:
                dag_failed = True
                logger.error(
                    "dag=%s run_id=%s task=%s event=dag_aborted reason=task_failed_permanent",
                    dag.name, run_id, task.name,
                )
                break  # stop DAG on first permanently-failed task

        finished_at = datetime.now(timezone.utc)
        run_state = DAGRunState.FAILED if dag_failed else DAGRunState.SUCCESS

        logger.info(
            "dag=%s run_id=%s event=run_finished state=%s duration_s=%.2f",
            dag.name, run_id, run_state.value, (finished_at - started_at).total_seconds(),
        )

        return DAGRunResult(
            dag_name=dag.name,
            run_id=run_id,
            state=run_state,
            started_at=started_at,
            finished_at=finished_at,
            task_results=task_results,
        )

    def _run_task_with_retries(self, dag_name: str, run_id: str, task: Task) -> TaskResult:
        attempts: List[TaskAttemptResult] = []
        attempt = 1
        max_attempts = task.retries + 1  # first try + `retries` retries

        while True:
            logger.info(
                "dag=%s run_id=%s task=%s event=task_attempt_started attempt=%d/%d",
                dag_name, run_id, task.name, attempt, max_attempts,
            )
            try:
                task.func()
            except Exception as exc:  # noqa: BLE001 - we classify below
                error_class = classify_error(exc)
                logger.warning(
                    "dag=%s run_id=%s task=%s event=task_attempt_failed attempt=%d/%d "
                    "error_class=%s error=%r",
                    dag_name, run_id, task.name, attempt, max_attempts, error_class, exc,
                )
                attempts.append(TaskAttemptResult(attempt, TaskState.FAILED_PERMANENT, str(exc)))

                non_retryable = error_class == "non_retryable"
                retries_exhausted = attempt >= max_attempts

                if non_retryable or retries_exhausted:
                    reason = "non_retryable_error" if non_retryable else "retries_exhausted"
                    logger.error(
                        "dag=%s run_id=%s task=%s event=task_failed_permanent "
                        "attempts=%d reason=%s",
                        dag_name, run_id, task.name, attempt, reason,
                    )
                    return TaskResult(task.name, TaskState.FAILED_PERMANENT, attempts)

                # Retryable and attempts remain -> back off, then retry.
                delay = compute_backoff_seconds(
                    attempt=attempt,
                    retry_delay_seconds=task.retry_delay_seconds,
                    max_retry_delay_seconds=task.max_retry_delay_seconds,
                    rand=self.rand,
                )
                logger.info(
                    "dag=%s run_id=%s task=%s event=task_state_change state=%s "
                    "backoff_seconds=%.1f next_attempt=%d/%d",
                    dag_name, run_id, task.name, TaskState.RETRYING.value,
                    delay, attempt + 1, max_attempts,
                )
                self.sleep_fn(delay)
                attempt += 1
                continue
            else:
                logger.info(
                    "dag=%s run_id=%s task=%s event=task_succeeded attempt=%d/%d",
                    dag_name, run_id, task.name, attempt, max_attempts,
                )
                attempts.append(TaskAttemptResult(attempt, TaskState.SUCCESS))
                return TaskResult(task.name, TaskState.SUCCESS, attempts)
