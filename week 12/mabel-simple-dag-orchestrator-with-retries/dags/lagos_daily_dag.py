"""
Stretch goal demo: a DAG whose cron is interpreted in Africa/Lagos
local time rather than UTC. "0 9 * * MON-FRI" here means 9am WAT
(UTC+1), not 9am UTC.
"""

from mini_orchestrator.dag import DAG, Task


def send_morning_report():
    print("sending 9am WAT morning report")


lagos_daily_dag = DAG(
    name="lagos_morning_report",
    schedule="0 9 * * MON-FRI",
    timezone="Africa/Lagos",
    tasks=[Task(name="send_report", func=send_morning_report)],
)
