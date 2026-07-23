import psycopg2
import json
import time
import os
from datetime import datetime, timezone

WATERMARK_FILE = "last_run.json"
EVENTS_FILE = "events.jsonl"
POLL_INTERVAL = 30

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user="postgres",
        password="password"
    )

def load_watermark():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            data = json.load(f)
            return data.get("last_updated_at", "1970-01-01T00:00:00+00:00")
    return "1970-01-01T00:00:00+00:00"

def save_watermark(ts):
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_updated_at": ts}, f)

def detect_op(row):
    if row["deleted_at"] is not None:
        return "DELETE"
    elif row["created_at"] == row["updated_at"]:
        return "INSERT"
    else:
        return "UPDATE"

def process_batch(watermark):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, email, city, created_at, updated_at, deleted_at
        FROM customers
        WHERE updated_at > %s
        ORDER BY updated_at ASC
    """, (watermark,))

    rows = cur.fetchall()
    cols = ["id", "name", "email", "city", "created_at", "updated_at", "deleted_at"]

    new_watermark = watermark
    events = []

    for row in rows:
        data = dict(zip(cols, row))
        for k, v in data.items():
            if hasattr(v, 'isoformat'):
                data[k] = v.isoformat()

        op = detect_op(data)
        event = {
            "op": op,
            "table": "customers",
            "ts": data["updated_at"],
            "data": data
        }
        if op == "UPDATE":
            event["before"] = None
            event["after"] = data
            del event["data"]

        events.append(event)
        new_watermark = data["updated_at"]

    conn.close()

    if events:
        with open(EVENTS_FILE, "a") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
        print(f"[{datetime.now()}] Processed {len(events)} events")

    return new_watermark

def main():
    print("CDC Tracker starting...")
    watermark = load_watermark()
    print(f"Starting from watermark: {watermark}")

    while True:
        try:
            new_watermark = process_batch(watermark)
            if new_watermark != watermark:
                save_watermark(new_watermark)
                watermark = new_watermark
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
