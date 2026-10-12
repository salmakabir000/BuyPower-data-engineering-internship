"""
Optional second DAG with no network dependency, purely so `status` and
`logs` have something realistic to show without depending on external
API availability. Not part of the Week-3-ETL deliverable — crypto_etl
in dags.py is the real one.

Run with: MINI_ORCH_DAGS_MODULE=demo_dags mini_orchestrator run demo_etl
"""

import random
import time

from mini_orchestrator import dag, task


def extract():
    time.sleep(random.uniform(0.05, 0.2))


def transform():
    time.sleep(random.uniform(0.05, 0.2))
    if random.random() < 0.3:
        raise ValueError("simulated transform error: unexpected null in source row")


def load():
    time.sleep(random.uniform(0.05, 0.2))


@dag(name="demo_etl", schedule="0 * * * *")
def demo_etl():
    e = task("extract", run=extract)
    t = task("transform", run=transform, depends_on=[e])
    task("load", run=load, depends_on=[t])
