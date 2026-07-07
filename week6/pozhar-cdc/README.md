# CDC Tracker — Change Data Capture

## What it does
A polling-based CDC service that tracks every INSERT, UPDATE 
and DELETE on a Postgres customers table and writes events 
to a JSON Lines file.

## How it works
1. Connects to Postgres every 30 seconds
2. Queries rows where updated_at > last watermark
3. Detects operation type:
   - INSERT: created_at == updated_at
   - UPDATE: updated_at > created_at AND deleted_at IS NULL
   - DELETE: deleted_at IS NOT NULL (soft delete)
4. Writes events to events.jsonl
5. Updates watermark so restarts pick up where they left off

## How to run
python3 mutate.py &
python3 cdc_tracker.py

## Polling CDC vs Debezium (Log-based CDC)

| Feature | Polling CDC (ours) | Debezium (log-based) |
|---------|-------------------|----------------------|
| How it works | Queries DB every N seconds | Reads Postgres transaction log |
| Hard deletes | Cannot detect | Detects everything |
| Latency | 30 second delay | Near real-time |
| DB load | Adds query load | Minimal load |
| Setup | Simple | Complex |

## 3 Limitations of Polling CDC
1. Cannot detect hard deletes (DELETE FROM table WHERE id=1)
   — row is gone, nothing to query
2. Schema changes break the query — adding/removing columns 
   causes errors
3. High throughput tables — if 10,000 rows change in 30 seconds,
   you may miss events or overwhelm memory

## Why polling CDC is inadequate for high-throughput tables
Polling reads the database on a fixed schedule. If thousands 
of rows change between polls, the query returns a massive 
result set that can crash memory. There is also a latency 
gap — changes made 1 second after a poll won't be seen for 
another 29 seconds. Finally, hard deletes are invisible to 
polling since the row no longer exists to be queried.
