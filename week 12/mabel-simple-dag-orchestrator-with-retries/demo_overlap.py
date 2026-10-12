"""
Standalone proof of the "no overlapping runs" requirement: a DAG with a
task that takes longer than the schedule interval. The second due-time
arrives while the first run is still executing, so it must be skipped
and logged rather than started concurrently.
"""

import os
import time
from datetime import datetime, timedelta, timezone

from mini_orchestrator.dag import DAG, Task
from mini_orchestrator.logging_config import configure_logging
from mini_orchestrator.scheduler import Scheduler

os.makedirs("logs", exist_ok=True)
configure_logging(log_file="logs/scheduler_overlap_demo.log")


def slow_task():
    print("slow_task: starting a 3-second unit of work")
    time.sleep(3)
    print("slow_task: done")


dag = DAG(
    name="overlap_demo",
    schedule="*/2 * * * *",  # every 2 minutes, but the task takes 3s (simulated as "slow")
    tasks=[Task(name="slow", func=slow_task, retries=0)],
)

scheduler = Scheduler(check_interval_seconds=0.01)
scheduler.register(dag)

sim_time = {"t": datetime.now(timezone.utc).replace(second=0, microsecond=0)}
scheduler.clock_fn = lambda tz: sim_time["t"]

print("=== Overlap-protection demo ===")
scheduler.tick()  # baseline
sim_time["t"] += timedelta(minutes=2, seconds=1)
scheduler.tick()  # triggers run #1 (background thread will sleep 3s)
time.sleep(0.5)    # run #1 is now definitely "in progress"
sim_time["t"] += timedelta(minutes=2, seconds=1)
scheduler.tick()  # due again, but run #1 still in progress -> should skip
time.sleep(4)      # let run #1 finish
print("=== Done. See logs/scheduler_overlap_demo.log ===")
