"""
demo_scheduled_dag.py - a lightweight DAG for demonstrating the scheduler.
Runs every 2 minutes (*/2 * * * *) - used to prove the scheduler triggers
DAGs on their own, unattended, over a sustained period.
"""

from datetime import datetime, timezone
from mini_orchestrator import dag, task


def say_hello():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"      demo task ran at {now} UTC")


@dag(name="demo_scheduled", schedule="*/2 * * * *")
def demo_scheduled():
    task("hello", run=say_hello, retries=1, retry_delay_seconds=10)
