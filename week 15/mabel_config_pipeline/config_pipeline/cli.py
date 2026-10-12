"""Command line: `config_pipeline run <file>` and `config_pipeline validate <file>`."""
from __future__ import annotations

import argparse
import logging
import sys

from .base import ConfigError, PipelineError
from .engine import run_file, validate_file


def _print_config_errors(path: str, exc: ConfigError) -> None:
    n = len(exc.errors)
    print(f"✗ {path} is invalid ({n} problem{'s' if n != 1 else ''}):", file=sys.stderr)
    for err in exc.errors:
        print(f"  - {err}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="config_pipeline", description="Run YAML-defined data pipelines.")
    sub = parser.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", help="validate, then run a pipeline")
    run_p.add_argument("file")
    val_p = sub.add_parser("validate", help="check a pipeline file without running it")
    val_p.add_argument("file")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    try:
        if args.command == "validate":
            cfg = validate_file(args.file)
            print(f"✓ {args.file} is valid (pipeline '{cfg['name']}')")
            return 0
        stats = run_file(args.file)
        print(f"✓ {stats['pipeline']}: read {stats['rows_read']}, wrote {stats['rows_written']} rows")
        return 0
    except ConfigError as exc:
        _print_config_errors(args.file, exc)
        return 2
    except PipelineError as exc:
        print(f"✗ pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
