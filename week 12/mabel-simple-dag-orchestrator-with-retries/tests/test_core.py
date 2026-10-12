import random
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mini_orchestrator.dag import DAG, Task, RetryableError, NonRetryableError, classify_error, TaskState
from mini_orchestrator.runner import DAGRunner, compute_backoff_seconds
from mini_orchestrator.scheduler import Scheduler
from datetime import datetime, timezone, timedelta


def test_classify_error():
    assert classify_error(NonRetryableError("bad config")) == "non_retryable"
    assert classify_error(RetryableError("timeout")) == "retryable"
    assert classify_error(ValueError("unexpected bug")) == "retryable"  # unknown -> retryable by default


def test_backoff_is_exponential_and_jittered():
    r = random.Random(42)
    d1 = compute_backoff_seconds(1, retry_delay_seconds=60, max_retry_delay_seconds=3600, rand=r)
    d2 = compute_backoff_seconds(2, retry_delay_seconds=60, max_retry_delay_seconds=3600, rand=r)
    d3 = compute_backoff_seconds(3, retry_delay_seconds=60, max_retry_delay_seconds=3600, rand=r)
    # base delays (ignoring jitter) should double: 60, 120, 240
    assert 60 <= d1 <= 90
    assert 120 <= d2 <= 150
    assert 240 <= d3 <= 270


def test_backoff_respects_cap():
    r = random.Random(1)
    d = compute_backoff_seconds(10, retry_delay_seconds=60, max_retry_delay_seconds=300, rand=r)
    assert d <= 330  # capped base (300) + max jitter (30)


def test_task_eventually_succeeds_after_retries():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RetryableError("transient")

    task = Task(name="t", func=flaky, retries=3, retry_delay_seconds=0.01)
    dag = DAG(name="d", schedule="* * * * *", tasks=[task])
    runner = DAGRunner(sleep_fn=lambda s: None)
    result = runner.run(dag, run_id="test1")
    assert result.task_results[0].final_state == TaskState.SUCCESS
    assert calls["n"] == 3


def test_task_fails_permanent_after_exhausting_retries():
    def always_fails():
        raise RetryableError("still down")

    task = Task(name="t", func=always_fails, retries=2, retry_delay_seconds=0.01)
    dag = DAG(name="d", schedule="* * * * *", tasks=[task])
    runner = DAGRunner(sleep_fn=lambda s: None)
    result = runner.run(dag, run_id="test2")
    assert result.task_results[0].final_state == TaskState.FAILED_PERMANENT
    assert len(result.task_results[0].attempts) == 3  # 1 initial + 2 retries


def test_non_retryable_error_skips_retries_entirely():
    calls = {"n": 0}

    def broken():
        calls["n"] += 1
        raise NonRetryableError("bad config")

    task = Task(name="t", func=broken, retries=5, retry_delay_seconds=0.01)
    dag = DAG(name="d", schedule="* * * * *", tasks=[task])
    runner = DAGRunner(sleep_fn=lambda s: None)
    result = runner.run(dag, run_id="test3")
    assert result.task_results[0].final_state == TaskState.FAILED_PERMANENT
    assert calls["n"] == 1  # no retries attempted


def test_overlap_protection_skips_second_trigger():
    dag = DAG(name="d", schedule="* * * * *", tasks=[Task(name="t", func=lambda: None)])
    assert dag.try_acquire_run_slot() is True
    # Second acquire while first is "in progress" should fail.
    assert dag.try_acquire_run_slot() is False
    dag.release_run_slot()
    assert dag.try_acquire_run_slot() is True


def test_scheduler_does_not_fire_on_baseline_tick():
    fired = []

    def fake_run(dag, run_id):
        fired.append(dag.name)
        from mini_orchestrator.dag import DAGRunState
        return None

    dag = DAG(name="d", schedule="*/2 * * * *", tasks=[Task(name="t", func=lambda: None)])
    sched = Scheduler(check_interval_seconds=1)
    sched.register(dag)
    sched.tick()  # first tick just establishes baseline, should not fire
    assert dag._last_checked_at is not None


def test_scheduler_fires_when_cron_time_has_passed():
    dag = DAG(name="d", schedule="*/2 * * * *", tasks=[Task(name="t", func=lambda: None)])
    sched = Scheduler(check_interval_seconds=1)
    sched.register(dag)

    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = {"t": base}

    def fake_clock(tz):
        return clock["t"]

    sched.clock_fn = fake_clock
    sched.tick()  # baseline at 00:00:00
    clock["t"] = base + timedelta(minutes=2, seconds=1)  # now past 00:02:00 fire time
    sched.tick()
    assert dag._last_triggered_at is not None


def test_scheduler_skips_overlapping_trigger_and_logs_it(caplog):
    import time as time_module

    def slow_task():
        time_module.sleep(0.3)

    dag = DAG(name="d", schedule="*/2 * * * *", tasks=[Task(name="t", func=slow_task)])
    sched = Scheduler(check_interval_seconds=1)
    sched.register(dag)

    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    clock = {"t": base}
    sched.clock_fn = lambda tz: clock["t"]

    sched.tick()  # baseline
    clock["t"] = base + timedelta(minutes=2, seconds=1)
    with caplog.at_level("INFO"):
        sched.tick()  # triggers, spawns background thread that sleeps 0.3s
        clock["t"] = base + timedelta(minutes=4, seconds=1)
        sched.tick()  # should see overlap since first run still sleeping
    assert any("skipped_overlap" in r.message for r in caplog.records)
    time_module.sleep(0.5)  # let background thread finish before test exits
