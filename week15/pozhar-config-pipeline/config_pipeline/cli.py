"""
cli.py - command-line interface.

Usage:
    config_pipeline run <path/to/pipeline.yml>
    config_pipeline validate <path/to/pipeline.yml>
"""

import argparse
import sys

from .engine import load_yaml, validate_config, run_pipeline
from .registry import ConfigError


def cmd_run(args):
    try:
        run_pipeline(args.file)
    except ConfigError as e:
        print(f"Config error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Pipeline failed while running: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_validate(args):
    try:
        config = load_yaml(args.file)
        validate_config(config, source_path=args.file)
    except ConfigError as e:
        print(f"Invalid: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Valid: '{args.file}' is a well-formed pipeline config.")


def main():
    parser = argparse.ArgumentParser(prog="config_pipeline", description="Config-driven pipeline engine.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_p = subparsers.add_parser("run", help="Run a pipeline defined in a YAML file")
    run_p.add_argument("file")
    run_p.set_defaults(func=cmd_run)

    validate_p = subparsers.add_parser("validate", help="Check a pipeline's YAML without running it")
    validate_p.add_argument("file")
    validate_p.set_defaults(func=cmd_validate)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
