"""
scd.py — Slowly Changing Dimension handlers.

Two functions:
    apply_scd1(conn, target_table, source_df, business_key, change_cols)
    apply_scd2(conn, target_table, source_df, business_key, change_cols, run_ts)

Both take a psycopg2 connection, a target table name, a pandas DataFrame of
the latest source snapshot, the name of the business/natural key column, and
the list of columns whose changes should be tracked.

Design notes
------------
* We commit once per call (one snapshot load = one transaction). If anything
  raises, the whole snapshot's changes are rolled back — you never end up
  with half a day's changes applied.
* "Change detection" is a straight Python-side comparison of change_cols
  between the source row and the current target row. NULL-safe: None == None
  is treated as "unchanged".
* apply_scd2 enforces the invariant "exactly one is_current=TRUE row per
  business key" by always closing the old current row (valid_to=run_ts,
  is_current=false) in the same transaction as inserting the new one.
"""
from __future__ import annotations

from typing import Iterable, Sequence
import psycopg2.extras


def _row_changed(source_row: dict, target_row: dict, change_cols: Sequence[str]) -> bool:
    """NULL-safe comparison of change_cols between a source and target row."""
    for col in change_cols:
        if source_row.get(col) != target_row.get(col):
            return True
    return False


def apply_scd1(
    conn,
    target_table: str,
    source_df,
    business_key: str,
    change_cols: Iterable[str],
) -> dict:
    """
    SCD Type 1: overwrite in place. No history.

    For each row in source_df:
      - if business_key exists in target_table -> UPDATE change_cols in place
      - if it doesn't exist -> INSERT a new row

    Assumes target_table has a column matching `business_key` as its
    primary/unique key, plus one column per entry in change_cols, plus a
    `last_updated` TIMESTAMPTZ column (see sql/schema.sql).

    Returns a dict with counts: {"inserted": n, "updated": n, "unchanged": n}
    """
    change_cols = list(change_cols)
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for _, row in source_df.iterrows():
            row = row.to_dict()
            bk_value = row[business_key]

            cur.execute(
                f"SELECT * FROM {target_table} WHERE {business_key} = %s",
                (bk_value,),
            )
            existing = cur.fetchone()

            if existing is None:
                cols = [business_key] + change_cols
                col_list = ", ".join(cols + ["last_updated"])
                placeholder_list = ", ".join(["%s"] * len(cols) + ["now()"])
                values = [row[business_key]] + [row[c] for c in change_cols]
                cur.execute(
                    f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholder_list})",
                    tuple(values),
                )
                counts["inserted"] += 1
            else:
                if _row_changed(row, existing, change_cols):
                    set_clause = ", ".join(f"{c} = %s" for c in change_cols)
                    cur.execute(
                        f"UPDATE {target_table} "
                        f"SET {set_clause}, last_updated = now() "
                        f"WHERE {business_key} = %s",
                        tuple(row[c] for c in change_cols) + (bk_value,),
                    )
                    counts["updated"] += 1
                else:
                    counts["unchanged"] += 1

    conn.commit()
    return counts


def apply_scd4(
    conn,
    current_table: str,
    history_table: str,
    source_df,
    business_key: str,
    change_cols: Iterable[str],
    run_ts,
) -> dict:
    """
    STRETCH GOAL — SCD Type 4: a small "current" table (one row per business
    key, always up to date, fast to join) plus a separate append-only
    "history" table that records a full snapshot of the row every time it
    changes (or is first seen).

    For each row in source_df:
      - business_key not in current_table -> INSERT into current_table,
            AND append a row to history_table (recorded_at=run_ts)
      - business_key in current_table and any change_cols differ ->
            UPDATE current_table in place (overwrite, like Type 1),
            AND append a new row to history_table (recorded_at=run_ts)
      - business_key in current_table and nothing changed -> no-op on both
            tables

    Unlike Type 2, the current table never grows extra rows for old
    versions — it behaves like Type 1. All the history lives in
    history_table instead, as a full-row append log rather than
    valid_from/valid_to ranges. This makes "give me the current value" a
    trivial single-row lookup, at the cost of point-in-time range queries
    being a little more work (you scan history_table ordered by
    recorded_at instead of filtering valid_from/valid_to).

    Returns a dict with counts: {"inserted": n, "changed": n, "unchanged": n}
    """
    change_cols = list(change_cols)
    counts = {"inserted": 0, "changed": 0, "unchanged": 0}
    all_cols = [business_key] + change_cols

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for _, row in source_df.iterrows():
            row = row.to_dict()
            bk_value = row[business_key]

            cur.execute(
                f"SELECT * FROM {current_table} WHERE {business_key} = %s",
                (bk_value,),
            )
            existing = cur.fetchone()

            if existing is None:
                col_list = ", ".join(all_cols + ["last_updated"])
                placeholder_list = ", ".join(["%s"] * len(all_cols) + ["%s"])
                cur.execute(
                    f"INSERT INTO {current_table} ({col_list}) VALUES ({placeholder_list})",
                    tuple(row[c] for c in all_cols) + (run_ts,),
                )
                hist_col_list = ", ".join(all_cols + ["recorded_at"])
                hist_placeholders = ", ".join(["%s"] * len(all_cols) + ["%s"])
                cur.execute(
                    f"INSERT INTO {history_table} ({hist_col_list}) VALUES ({hist_placeholders})",
                    tuple(row[c] for c in all_cols) + (run_ts,),
                )
                counts["inserted"] += 1
            elif _row_changed(row, existing, change_cols):
                set_clause = ", ".join(f"{c} = %s" for c in change_cols)
                cur.execute(
                    f"UPDATE {current_table} "
                    f"SET {set_clause}, last_updated = %s "
                    f"WHERE {business_key} = %s",
                    tuple(row[c] for c in change_cols) + (run_ts, bk_value),
                )
                hist_col_list = ", ".join(all_cols + ["recorded_at"])
                hist_placeholders = ", ".join(["%s"] * len(all_cols) + ["%s"])
                cur.execute(
                    f"INSERT INTO {history_table} ({hist_col_list}) VALUES ({hist_placeholders})",
                    tuple(row[c] for c in all_cols) + (run_ts,),
                )
                counts["changed"] += 1
            else:
                counts["unchanged"] += 1

    conn.commit()
    return counts


def apply_scd2(
    conn,
    target_table: str,
    source_df,
    business_key: str,
    change_cols: Iterable[str],
    run_ts,
    surrogate_key_col: str = "customer_sk",
) -> dict:
    """
    SCD Type 2: preserve history via valid_from / valid_to / is_current.

    For each row in source_df:
      - business_key not in target -> INSERT new row
            (valid_from=run_ts, valid_to=NULL, is_current=true)
      - business_key in target, current row differs on any change_cols ->
            UPDATE current row: valid_to=run_ts, is_current=false
            INSERT new row: valid_from=run_ts, valid_to=NULL, is_current=true
      - business_key in target, nothing changed -> no-op

    Assumes target_table has columns: business_key, change_cols...,
    valid_from, valid_to, is_current (see sql/schema.sql for dim_customer).

    Returns a dict with counts: {"inserted": n, "changed": n, "unchanged": n}
    """
    change_cols = list(change_cols)
    counts = {"inserted": 0, "changed": 0, "unchanged": 0}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for _, row in source_df.iterrows():
            row = row.to_dict()
            bk_value = row[business_key]

            cur.execute(
                f"SELECT * FROM {target_table} "
                f"WHERE {business_key} = %s AND is_current = TRUE",
                (bk_value,),
            )
            current = cur.fetchone()

            all_cols = [business_key] + change_cols

            if current is None:
                col_list = ", ".join(all_cols + ["valid_from", "valid_to", "is_current"])
                placeholders = ", ".join(["%s"] * len(all_cols) + ["%s", "NULL", "TRUE"])
                cur.execute(
                    f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})",
                    tuple(row[c] for c in all_cols) + (run_ts,),
                )
                counts["inserted"] += 1
                continue

            if _row_changed(row, current, change_cols):
                cur.execute(
                    f"UPDATE {target_table} "
                    f"SET valid_to = %s, is_current = FALSE "
                    f"WHERE {surrogate_key_col} = %s",
                    (run_ts, current[surrogate_key_col]),
                )
                col_list = ", ".join(all_cols + ["valid_from", "valid_to", "is_current"])
                placeholders = ", ".join(["%s"] * len(all_cols) + ["%s", "NULL", "TRUE"])
                cur.execute(
                    f"INSERT INTO {target_table} ({col_list}) VALUES ({placeholders})",
                    tuple(row[c] for c in all_cols) + (run_ts,),
                )
                counts["changed"] += 1
            else:
                counts["unchanged"] += 1

    conn.commit()
    return counts
