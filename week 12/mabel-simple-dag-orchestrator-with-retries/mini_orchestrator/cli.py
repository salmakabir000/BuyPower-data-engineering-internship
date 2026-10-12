"""
CLI entrypoint.

Usage:
    python -m mini_orchestrator scheduler start
    python -m mini_orchestrator scheduler start --dags-dir dags --check-interval 30
"""

from __future__ import annotations

import argparse
import os

from .logging_config import configure_logging
from .registry import discover_dags
from .scheduler import Scheduler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mini_orchestrator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scheduler_parser = subparsers.add_parser("scheduler", help="Scheduler commands")
    scheduler_sub = scheduler_parser.add_subparsers(dest="scheduler_command", required=True)

    start_parser = scheduler_sub.add_parser("start", help="Start the scheduler (foreground)")
    start_parser.add_argument("--dags-dir", default="dags", help="Directory containing DAG definitions")
    start_parser.add_argument("--check-interval", type=float, default=30.0,
                               help="Seconds between scheduling checks")
    start_parser.add_argument("--log-file", default="logs/scheduler.log")
    start_parser.add_argument("--max-ticks", type=int, default=None,
                               help="Stop after N ticks (for demos/tests); default: run forever")

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "scheduler" and args.scheduler_command == "start":
        os.makedirs(os.path.dirname(args.log_file) or ".", exist_ok=True)
        configure_logging(log_file=args.log_file)

        dags = discover_dags(args.dags_dir)
        scheduler = Scheduler(check_interval_seconds=args.check_interval)
        scheduler.register_all(dags)
        scheduler.start(max_ticks=args.max_ticks)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
