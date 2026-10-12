"""
Demo DAG showing a non-retryable error: it fails once and immediately
goes to failed_permanent without burning through retries/backoff,
because the error is a config problem retrying can't fix.
"""

from mini_orchestrator.dag import DAG, Task, NonRetryableError


def broken_task():
    raise NonRetryableError("required config key 'API_KEY' is missing")


bad_config_dag = DAG(
    name="bad_config_example",
    schedule="0 3 * * *",  # 3am daily -- won't fire during the demo window
    tasks=[Task(name="setup", func=broken_task, retries=3, retry_delay_seconds=10)],
)
