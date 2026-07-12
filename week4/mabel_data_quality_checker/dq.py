#!/usr/bin/env python3
"""
dq.py — a tiny data quality checker (a mini Great Expectations / Soda).

Usage:
    python dq.py checks/coin_prices.yml

    # Compare mode — catch a silently-broken upstream extract by comparing
    # row counts against a previous run's config:
    python dq.py checks/today.yml --compare checks/yesterday.yml --max-drop-pct 10

Reads a YAML config describing a dataset (SQLite table or Parquet file) plus
a list of declarative checks, runs each check with pandas, prints a readable
report, and exits 1 if any *critical* check failed (warnings never fail the
build — that's the whole point of the severity split).
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import yaml


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------

def load_config(path: str) -> dict:
    """Read and parse the YAML config file describing the dataset + checks."""
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    for required_key in ("dataset", "source", "checks"):
        if required_key not in config:
            raise ValueError(f"Config is missing required top-level key: '{required_key}'")
    if not isinstance(config["checks"], list):
        raise ValueError("'checks' must be a list")

    return config


def load_dataframe(source: dict) -> pd.DataFrame:
    """
    Load the target dataset into a pandas DataFrame based on the 'source'
    block of the config.
        type: sqlite  -> requires path + table
        type: parquet -> requires path
    """
    source_type = source.get("type")

    if source_type == "sqlite":
        db_path = source.get("path")
        table = source.get("table")
        if not db_path or not table:
            raise ValueError("sqlite source requires both 'path' and 'table'")
        if not Path(db_path).exists():
            raise FileNotFoundError(f"SQLite database not found: {db_path}")
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        return df

    elif source_type == "parquet":
        path = source.get("path")
        if not path:
            raise ValueError("parquet source requires 'path'")
        if not Path(path).exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")
        return pd.read_parquet(path)

    else:
        raise ValueError(
            f"Unsupported source type: {source_type!r}. Supported types: 'sqlite', 'parquet'"
        )


# --------------------------------------------------------------------------
# Individual check implementations
#
# Each check function takes (df, check_config) and returns a dict with at
# least: passed (bool), failure_count (int), sample_failures (list).
# row_count_min also returns a "detail" string used directly in the report.
# --------------------------------------------------------------------------

def check_not_null(df: pd.DataFrame, check: dict) -> dict:
    column = check["column"]
    _require_column(df, column)
    mask = df[column].isna()
    failure_count = int(mask.sum())
    # "nan" is the only informative sample here — no need to enumerate nulls
    samples = ["nan"] if failure_count else []
    return {"passed": failure_count == 0, "failure_count": failure_count, "sample_failures": samples}


def check_unique(df: pd.DataFrame, check: dict) -> dict:
    column = check["column"]
    _require_column(df, column)
    dup_mask = df[column].duplicated(keep=False) & df[column].notna()
    failure_count = int(dup_mask.sum())
    samples = df.loc[dup_mask, column].astype(str).unique().tolist()[:5]
    return {"passed": failure_count == 0, "failure_count": failure_count, "sample_failures": samples}


def check_in_set(df: pd.DataFrame, check: dict) -> dict:
    column = check["column"]
    _require_column(df, column)
    allowed = set(check.get("values", []))
    mask = df[column].notna() & ~df[column].isin(allowed)
    failure_count = int(mask.sum())
    samples = df.loc[mask, column].astype(str).unique().tolist()[:5]
    return {"passed": failure_count == 0, "failure_count": failure_count, "sample_failures": samples}


def check_range(df: pd.DataFrame, check: dict) -> dict:
    column = check["column"]
    _require_column(df, column)
    min_val = check.get("min")
    max_val = check.get("max")

    numeric = pd.to_numeric(df[column], errors="coerce")
    non_null = numeric.notna()
    below = numeric < min_val if min_val is not None else pd.Series(False, index=df.index)
    above = numeric > max_val if max_val is not None else pd.Series(False, index=df.index)
    fail_mask = non_null & (below | above)

    failure_count = int(fail_mask.sum())
    samples = df.loc[fail_mask, column].astype(str).unique().tolist()[:5]
    return {"passed": failure_count == 0, "failure_count": failure_count, "sample_failures": samples}


def check_regex_match(df: pd.DataFrame, check: dict) -> dict:
    column = check["column"]
    _require_column(df, column)
    pattern = check.get("pattern")
    if not pattern:
        raise ValueError(f"regex_match check on column '{column}' requires a 'pattern'")
    compiled = re.compile(pattern)

    series = df[column].astype(str)
    mask = df[column].notna() & ~series.apply(lambda v: bool(compiled.match(v)))
    failure_count = int(mask.sum())
    samples = df.loc[mask, column].astype(str).unique().tolist()[:5]
    return {"passed": failure_count == 0, "failure_count": failure_count, "sample_failures": samples}


def check_row_count_min(df: pd.DataFrame, check: dict) -> dict:
    min_rows = check.get("value")
    if min_rows is None:
        raise ValueError("row_count_min check requires a 'value'")
    actual = len(df)
    passed = actual >= min_rows
    detail = f"{actual:,} >= {min_rows:,}" if passed else f"{actual:,} < {min_rows:,}"
    return {
        "passed": passed,
        "failure_count": 0 if passed else 1,
        "sample_failures": [],
        "detail": detail,
    }


CHECK_REGISTRY = {
    "not_null": check_not_null,
    "unique": check_unique,
    "in_set": check_in_set,
    "range": check_range,
    "regex_match": check_regex_match,
    "row_count_min": check_row_count_min,
}


def _require_column(df: pd.DataFrame, column: str):
    if column not in df.columns:
        raise ValueError(
            f"Column '{column}' not found in dataset. Available columns: {list(df.columns)}"
        )


# --------------------------------------------------------------------------
# Running checks + reporting
# --------------------------------------------------------------------------

def run_checks(df: pd.DataFrame, checks: list) -> list:
    """Run every check in the config against the DataFrame and return result dicts."""
    results = []
    for check in checks:
        check_type = check.get("type")
        if check_type not in CHECK_REGISTRY:
            raise ValueError(
                f"Unknown check type: {check_type!r}. Supported types: {sorted(CHECK_REGISTRY.keys())}"
            )

        severity = check.get("severity", "warning")
        if severity not in ("critical", "warning"):
            raise ValueError(f"Invalid severity {severity!r}. Must be 'critical' or 'warning'.")

        fn = CHECK_REGISTRY[check_type]
        try:
            outcome = fn(df, check)
        except Exception as e:
            # A check that errors out (bad column, bad config) is itself a
            # critical-style failure signal — silent errors are exactly what
            # this tool exists to prevent.
            outcome = {"passed": False, "failure_count": 1, "sample_failures": [], "detail": f"ERROR: {e}"}

        results.append({
            "check": check,
            "type": check_type,
            "severity": severity,
            "column": check.get("column"),
            **outcome,
        })
    return results


def format_report(dataset_name: str, total_rows: int, results: list) -> tuple:
    """Returns (report_text, has_critical_failure)."""
    lines = []
    lines.append(f"Data Quality Report — {dataset_name}")
    lines.append(f"Total rows checked: {total_rows:,}")
    lines.append("")

    for r in results:
        symbol = "✓" if r["passed"] else "✗"
        label = r["column"] if r["column"] else "row count"
        type_name = r["type"]

        if r["type"] == "row_count_min":
            status = "passed" if r["passed"] else "failed"
            lines.append(f"{symbol} {label:<15} {type_name:<15} {status} ({r['detail']})")
        elif r["passed"]:
            lines.append(f"{symbol} {label:<15} {type_name:<15} 0 failures")
        else:
            sev_tag = f" ({r['severity']})"
            lines.append(f"{symbol} {label:<15} {type_name:<15} {r['failure_count']} failures{sev_tag}")
            if r.get("detail") and str(r["detail"]).startswith("ERROR"):
                lines.append(f"    {r['detail']}")
            elif r.get("sample_failures"):
                sample_str = ", ".join(str(v) for v in r["sample_failures"])
                lines.append(f"    Sample failing values: {sample_str}")

    critical_failures = [r for r in results if not r["passed"] and r["severity"] == "critical"]
    warnings = [r for r in results if not r["passed"] and r["severity"] == "warning"]

    lines.append("")
    lines.append(f"Result: {len(warnings)} warning(s), {len(critical_failures)} critical failure(s)")

    return "\n".join(lines), len(critical_failures) > 0


# --------------------------------------------------------------------------
# Compare mode (row-count drop detection between two runs / sources)
# --------------------------------------------------------------------------

def run_compare(current_config: dict, previous_config_path: str, max_drop_pct: float) -> tuple:
    """
    Loads the dataset pointed to by `previous_config_path`, compares its row
    count against the current config's dataset, and flags a silent-shrink
    problem (e.g. an upstream extract that quietly returned far fewer rows).
    Returns (report_text, failed: bool).
    """
    previous_config = load_config(previous_config_path)
    previous_df = load_dataframe(previous_config["source"])
    current_df = load_dataframe(current_config["source"])

    prev_count = len(previous_df)
    curr_count = len(current_df)

    if prev_count == 0:
        drop_pct = 0.0 if curr_count == 0 else -100.0  # can't shrink from zero
    else:
        drop_pct = ((prev_count - curr_count) / prev_count) * 100

    failed = drop_pct > max_drop_pct

    lines = []
    lines.append(f"Row Count Comparison — {current_config['dataset']} vs {previous_config['dataset']}")
    lines.append(f"Previous rows: {prev_count:,}")
    lines.append(f"Current rows:  {curr_count:,}")
    if drop_pct > 0:
        lines.append(f"Change: -{drop_pct:.1f}% (threshold: {max_drop_pct}%)")
    else:
        lines.append(f"Change: +{abs(drop_pct):.1f}% (threshold: {max_drop_pct}%)")

    symbol = "✗" if failed else "✓"
    verdict = "FAILED — row count dropped more than allowed" if failed else "OK"
    lines.append(f"{symbol} {verdict}")

    return "\n".join(lines), failed


# --------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run declarative data quality checks against a dataset.")
    parser.add_argument("config", help="Path to the YAML config file describing the dataset and checks.")
    parser.add_argument(
        "--compare",
        metavar="PREVIOUS_CONFIG",
        help="Path to a previous config to compare row counts against (volume/drop check).",
    )
    parser.add_argument(
        "--max-drop-pct",
        type=float,
        default=10.0,
        help="Max allowed %% drop in row count vs --compare target before failing (default: 10).",
    )
    args = parser.parse_args()

    try:
        config = load_config(args.config)
        df = load_dataframe(config["source"])
        results = run_checks(df, config["checks"])
        report, has_critical_failure = format_report(config["dataset"], len(df), results)
        print(report)

        exit_code = 1 if has_critical_failure else 0

        if args.compare:
            print()
            compare_report, compare_failed = run_compare(config, args.compare, args.max_drop_pct)
            print(compare_report)
            if compare_failed:
                exit_code = 1

        sys.exit(exit_code)
    except Exception as e:
        print(f"dq.py error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
