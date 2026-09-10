"""
flaky_demo_dag.py - a task that fails twice, then succeeds on the 3rd try.
Demonstrates retry + exponential backoff. Not on a schedule - run manually.
"""

from mini_orchestrator import dag, task

_attempts = {"count": 0}


def flaky_task():
    _attempts["count"] += 1
    if _attempts["count"] < 3:
        raise ConnectionError(f"simulated flaky network (attempt {_attempts['count']})")
    print("      finally succeeded!")


@dag(name="flaky_demo", schedule=None)
def flaky_demo():
    # small retry_delay_seconds so the demo doesn't take forever to watch
    task("flaky", run=flaky_task, retries=3, retry_delay_seconds=5)
