"""
always_fails_dag.py - a task that always fails, to demonstrate retries
being exhausted and the task ending in failed_permanent.
"""

from mini_orchestrator import dag, task


def doomed_task():
    raise TimeoutError("downstream service is permanently down (simulated)")


@dag(name="always_fails", schedule=None)
def always_fails():
    task("doomed", run=doomed_task, retries=2, retry_delay_seconds=3)
