"""
cdc_tracker.py

A polling-based Change Data Capture (CDC) service for the `customers` table.

Every POLL_INTERVAL_SECONDS, it:
  1. Reads the last watermark (updated_at, id) from last_run.json.
  2. Queries for every row whose (updated_at, id) is greater than the watermark.
  3. Classifies each row as INSERT, UPDATE, or DELETE.
  4. Appends one JSON object per event to events.jsonl (JSON Lines format).
  5. Advances the watermark to the last row in the batch and persists it.

Restart-safety: because the watermark is written to disk after every batch,
killing and restarting this process picks up exactly where it left off —
it will never re-emit an already-processed row, and it will never skip a
row that arrived while the process was down.

Run:
    python cdc_tracker.py
"""

import json
import os
import time
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

DB_DSN = os.environ.get(
    "CDC_DB_DSN",
    "host=localhost port=5432 dbname=postgres user=postgres password=password",
)

POLL_INTERVAL_SECONDS = 30
WATERMARK_FILE = "last_run.json"
EVENTS_FILE = "events.jsonl"

# Watermark used on first-ever run, before any state exists on disk.
# Starting at the epoch means the very first poll will emit every existing row
# as an INSERT — that's the correct behavior for a brand-new tracker doing an
# initial "backfill" of the table.
DEFAULT_WATERMARK = {"updated_at": "1970-01-01T00:00:00+00:00", "id": 0}


def load_watermark():
    if not os.path.exists(WATERMARK_FILE):
        return dict(DEFAULT_WATERMARK)
    with open(WATERMARK_FILE, "r") as f:
        return json.load(f)


def save_watermark(watermark):
    # Write-to-temp-then-rename so a crash mid-write never corrupts the file
    # or leaves it half-written.
    tmp_path = WATERMARK_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(watermark, f)
    os.replace(tmp_path, WATERMARK_FILE)


def classify_row(row):
    """
    Decide whether a row represents an INSERT, UPDATE, or DELETE, using the
    soft-delete pattern described in the exercise.

    Order matters: a deleted row could also technically satisfy the "insert"
    or "update" checks, so DELETE must be checked first.
    """
    if row["deleted_at"] is not None:
        return "DELETE"
    if row["created_at"] == row["updated_at"]:
        return "INSERT"
    return "UPDATE"


def row_to_json_safe(row):
    """Convert datetime/other non-JSON-native types to ISO strings."""
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def build_event(op, row):
    now_ts = datetime.now(timezone.utc).isoformat()
    data = row_to_json_safe(row)

    if op == "UPDATE":
        # A polling-based tracker never captured the *previous* row version —
        # it only ever sees the current state at poll time. Recording `before`
        # as null (rather than guessing) is an honest representation of that
        # limitation. See README for how log-based CDC solves this.
        return {
            "op": "UPDATE",
            "table": "customers",
            "ts": now_ts,
            "before": None,
            "after": data,
        }
    else:  # INSERT or DELETE
        return {
            "op": op,
            "table": "customers",
            "ts": now_ts,
            "data": data,
        }


def append_events(events):
    with open(EVENTS_FILE, "a") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def poll_once(conn, watermark):
    query = """
        SELECT id, name, email, city, created_at, updated_at, deleted_at
        FROM customers
        WHERE (updated_at, id) > (%s, %s)
        ORDER BY updated_at ASC, id ASC
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (watermark["updated_at"], watermark["id"]))
        rows = cur.fetchall()

    if not rows:
        return watermark, 0

    events = []
    for row in rows:
        op = classify_row(row)
        events.append(build_event(op, row))

    append_events(events)

    last_row = rows[-1]
    new_watermark = {
        "updated_at": last_row["updated_at"].isoformat(),
        "id": last_row["id"],
    }
    save_watermark(new_watermark)

    return new_watermark, len(rows)


def main():
    print("CDC tracker starting.")
    watermark = load_watermark()
    print(f"Resuming from watermark: {watermark}")

    with psycopg.connect(DB_DSN, autocommit=True) as conn:
        while True:
            try:
                watermark, count = poll_once(conn, watermark)
                if count:
                    print(f"[{datetime.now(timezone.utc).isoformat()}] "
                          f"Processed {count} change(s). Watermark -> {watermark}")
                else:
                    print(f"[{datetime.now(timezone.utc).isoformat()}] No changes.")
            except Exception as e:
                # A production version would distinguish transient errors
                # (retry) from fatal ones (alert + exit), and would not
                # silently swallow repeated failures.
                print(f"[ERROR] Poll failed: {e}")

            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
