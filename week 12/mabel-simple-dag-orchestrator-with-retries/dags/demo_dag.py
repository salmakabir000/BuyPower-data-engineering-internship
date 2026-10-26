"""
Demo DAG for the week's "definition of done" checklist:
scheduled */2 * * * * so it triggers every 2 minutes.

`flaky_extract` fails about 40% of the time with a RetryableError to
show backoff-and-retry in the logs. `always_succeeds` demonstrates a
normal task running after a retried one succeeds.
"""

import random

from mini_orchestrator.dag import DAG, Task, RetryableError

_rand = random.Random()


def flaky_extract():
    if _rand.random() < 0.4:
        raise RetryableError("upstream API timed out")
    print("extract: got 128 rows")


def always_succeeds():
    print("load: wrote 128 rows to warehouse")


demo_dag = DAG(
    name="demo_every_2_min",
    schedule="*/2 * * * *",
    tasks=[
        Task(name="extract", func=flaky_extract, retries=3, retry_delay_seconds=10),
        Task(name="load", func=always_succeeds, retries=3, retry_delay_seconds=10),
    ],
)
