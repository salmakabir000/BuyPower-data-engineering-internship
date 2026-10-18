"""
yaml_pipelines_dag.py - runs config-driven YAML pipelines through the
mini_orchestrator scheduler (Weeks 12-14).

Each DAG here is deliberately thin. It contains no pipeline logic at all,
just two tasks that call the config_pipeline CLI:

    validate  ->  python -m config_pipeline validate <yaml>
    run       ->  python -m config_pipeline run <yaml>

`run` only starts if `validate` passed, so a broken YAML is caught before
any network call is made.

Why a subprocess instead of importing the engine directly:
  - the pipeline runs in its own working directory (this project's root), so
    the relative paths inside the YAMLs (data/crypto.db etc.) resolve
    correctly no matter where the scheduler was started from
  - the scheduler runs DAGs in background threads, and changing the working
    directory inside a thread would affect the whole process
  - the exit code gives a clean pass/fail signal for the scheduler's retries

Retries come from the Week 14 scheduler for free. A config mistake ("Config
error" / "Invalid") is raised as NonRetryableError so it fails immediately,
while a network failure is retried with exponential backoff and jitter.

Run from the orchestrator folder, pointing at this dags folder:
    python3 -m mini_orchestrator --dags-dir ../../week15/pozhar-config-pipeline/dags list
"""

import subprocess
import sys
from pathlib import Path

from mini_orchestrator import dag, task
from mini_orchestrator.retry import NonRetryableError

# the config-pipeline project (this file lives in <project>/dags/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _cli(command, yaml_path):
    """Returns a task function that runs `config_pipeline <command> <yaml_path>`."""

    def _run():
        result = subprocess.run(
            [sys.executable, "-m", "config_pipeline", command, yaml_path],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        for line in result.stdout.splitlines():
            print(f"      {line}")
        if result.returncode == 0:
            return

        stderr_lines = result.stderr.strip().splitlines()
        message = stderr_lines[-1] if stderr_lines else f"exited with code {result.returncode}"
        if message.startswith(("Config error", "Invalid")):
            raise NonRetryableError(message)  # a bad YAML won't fix itself
        raise RuntimeError(message)  # likely transient (network, rate limit)

    return _run


def yaml_pipeline_tasks(yaml_path, retries=3, retry_delay_seconds=30):
    """Adds the validate -> run task pair for one YAML pipeline to the DAG
    currently being defined."""
    check = task("validate", run=_cli("validate", yaml_path), retries=0)
    task(
        "run",
        run=_cli("run", yaml_path),
        depends_on=[check],
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
    )


@dag(name="crypto_pipeline", schedule="0 * * * *")  # hourly
def crypto_pipeline():
    yaml_pipeline_tasks("pipelines/crypto.yml")


@dag(name="weather_pipeline", schedule="*/2 * * * *")  # every 2 min: demo cadence
def weather_pipeline():
    yaml_pipeline_tasks("pipelines/weather.yml")


@dag(name="csv_ingest_pipeline", schedule=None)  # manual runs only
def csv_ingest_pipeline():
    yaml_pipeline_tasks("pipelines/csv_ingest.yml")
