"""
Time-compressed demo of the "definition of done" scenario:

    a DAG scheduled */2 * * * * triggers every 2 minutes for 10 minutes
    straight without manual intervention

This script proves the scheduling *logic* is correct by injecting a
fake clock that advances 2 minutes of simulated time per real tick,
instead of making you wait 10 real minutes. Task execution itself
(including real retry backoff sleeps) still runs for real -- only the
cron "what time is it" check is simulated.

For the actual submission, run the real thing instead:
    python -m mini_orchestrator scheduler start
and let it sit for >= 10 minutes with dags/demo_dag.py registered.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

from mini_orchestrator.logging_config import configure_logging
from mini_orchestrator.registry import discover_dags
from mini_orchestrator.scheduler import Scheduler

os.makedirs("logs", exist_ok=True)
configure_logging(log_file="logs/scheduler_demo.log")

dags = discover_dags("dags")
scheduler = Scheduler(check_interval_seconds=0.01)  # real sleep between ticks is tiny
scheduler.register_all(dags)

# Simulated clock: starts now, advances 2 minutes + a few seconds each
# time tick() asks for the time, so every tick lands past the next
# */2 * * * * boundary.
sim_time = {"t": datetime.now(timezone.utc).replace(second=0, microsecond=0)}


def fake_clock(tz_name):
    return sim_time["t"]


scheduler.clock_fn = fake_clock

print("=== Time-compressed demo: simulating 10 minutes in a few seconds ===")
scheduler.tick()  # baseline tick
for i in range(5):  # 5 * 2min = 10 simulated minutes
    sim_time["t"] = sim_time["t"] + timedelta(minutes=2, seconds=1)
    scheduler.tick()
    time.sleep(0.2)  # tiny real pause so background run threads can start/log

# Give the last background DAG run threads a moment to finish logging
# (they may still be in a retry backoff sleep).
time.sleep(2)
print("=== Demo complete. See logs/scheduler_demo.log ===")
