# CDC Tracker (Polling-Based)

A minimal polling-based Change Data Capture pipeline for a Postgres `customers`
table. It watches for inserts, updates, and soft-deletes and writes each one
as a structured event to a JSON Lines file — the same shape of problem tools
like Debezium solve, but solved the simple, brute-force way first so the
tradeoffs are easy to feel rather than just read about.

## Files

| File | Purpose |
|---|---|
| `schema.sql` | Table definition for `customers` (includes `created_at`, `updated_at`, `deleted_at`) |
| `seed.py` | One-time script: creates the table and inserts 20 starter rows |
| `mutator.py` | Simulates an app: every 60s does a random insert, update, or soft-delete |
| `cdc_tracker.py` | The CDC service: polls every 30s, classifies changes, writes events |
| `last_run.json` | Watermark file (created automatically) — `{"updated_at": ..., "id": ...}` |
| `events.jsonl` | Output stream of change events (created automatically) |

## Setup

**1. Start Postgres:**

```bash
docker run --name pg-cdc -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres:16
```

**2. Install dependencies:**

```bash
pip install -r requirements.txt
```

**3. Seed the table** (creates schema + 20 starter rows):

```bash
python seed.py
```

**4. Run the mutator in the background** (simulates live traffic):

```bash
python mutator.py &
```

**5. Run the tracker:**

```bash
python cdc_tracker.py
```

Watch `events.jsonl` grow:

```bash
tail -f events.jsonl
```

If you need a different connection string, set `CDC_DB_DSN`, e.g.:

```bash
export CDC_DB_DSN="host=localhost port=5432 dbname=postgres user=postgres password=password"
```

## How it works

**Watermark.** Rather than tracking only `updated_at`, the watermark is the
pair `(updated_at, id)`. A single-column timestamp watermark has a subtle bug:
if two rows are updated within the same microsecond (very possible on a busy
table, or on databases with lower timestamp resolution), a `WHERE updated_at >
last_ts` query can skip one of them. Ordering and filtering by `(updated_at,
id)` — since `id` is unique and monotonic — makes the comparison unambiguous
even when timestamps collide.

**Classification.** Every row returned by the poll is classified using the
soft-delete convention described in the exercise:

1. `deleted_at IS NOT NULL` → **DELETE** (checked first — a deleted row would
   otherwise also match the INSERT/UPDATE checks)
2. `created_at == updated_at` → **INSERT** (both default to `NOW()`, and
   Postgres evaluates `NOW()` once per transaction, so on a true insert they
   land on the exact same value)
3. otherwise → **UPDATE**

**Restart-safety.** The watermark is written to disk (atomically, via
write-temp-then-rename) immediately after each batch is processed. On
restart, the tracker reads `last_run.json` and resumes from exactly that
point — it will neither re-emit a processed row nor skip one that arrived
while it was down.

## Polling CDC vs. Log-Based CDC (Debezium)

| | Polling (this project) | Log-based (Debezium / wal2json / pgoutput) |
|---|---|---|
| **Source of truth** | Re-queries the table itself | Reads the database's write-ahead log (WAL) directly |
| **Detects hard deletes** | No — a hard `DELETE` leaves no row to query | Yes — the WAL records the delete event even though the row is gone |
| **Captures "before" state** | No — only ever sees current state at poll time | Yes — WAL entries include before/after images for updates |
| **Latency** | Bounded by poll interval (here, 30s) | Near-real-time (milliseconds) |
| **Load on source DB** | A query every interval, against every "changed since" row | Reads a log stream; no repeated scanning queries |
| **Depends on app discipline** | Yes — requires every writer to correctly maintain `updated_at`/`deleted_at` | No — the WAL captures every write regardless of how it was made |
| **Ordering guarantees** | Good if watermark uses a tie-breaker; fragile otherwise | Strict, transaction-consistent ordering from the log |
| **Schema change visibility** | Not detected at all — `SELECT *` silently adopts whatever columns exist | Explicit DDL events are captured and can trigger schema evolution logic |

## Limitations of polling CDC

1. **Hard deletes are invisible.** This design only works because deletes are
   *soft* (a `deleted_at` flag). If any process ever runs a real `DELETE FROM
   customers WHERE ...`, that row simply vanishes with no trace — the tracker
   has no way to know it ever existed, let alone that it was removed.

2. **No visibility into schema changes.** If a column is added, dropped, or
   renamed, `SELECT *` just silently reflects the new shape. There's no
   `ALTER TABLE` event, no warning, and no way to distinguish "the schema
   changed" from "this is just how the table has always looked" downstream.

3. **Throughput and completeness tradeoffs.** On a high-throughput table,
   many changes can happen to the *same row* between two polls — only the
   latest state is ever seen, so intermediate updates are silently lost
   (there is no history of "it changed from A to B to C," only "it is now
   C"). Scaling the poll interval down to compensate increases load on the
   source database and still can't reach true real-time latency.

4. **Correctness depends entirely on application discipline.** The whole
   scheme relies on every writer correctly setting `updated_at` (and
   `deleted_at` for soft deletes). A single ad-hoc `UPDATE` run directly in
   `psql` without touching `updated_at` would be completely invisible to this
   tracker — log-based CDC has no such dependency, since it reads what
   actually happened at the storage engine level.

## Stretch goal: what would change with logical replication (wal2json / pgoutput)

I didn't build this, but here's the redesign:

- **Replace polling with a replication slot.** Instead of a `SELECT` loop, the
  tracker would create a Postgres logical replication slot (`pg_create_logical_replication_slot`)
  using the `wal2json` or built-in `pgoutput` plugin, and open a persistent
  replication connection. Postgres then *pushes* changes to the client as
  they commit, rather than the client pulling on a timer.
- **No more `updated_at`/`deleted_at` convention needed.** The WAL entry for
  an `UPDATE` already contains both the old and new row (with `REPLICA
  IDENTITY FULL` set on the table), so `before` would no longer need to be
  `null` — it would be populated for real. Hard deletes would show up as
  native `DELETE` WAL events too, closing limitation #1 above.
- **Watermark becomes an LSN, not a timestamp.** Instead of `last_run.json`
  storing `{"updated_at", "id"}`, it would store the last confirmed Log
  Sequence Number (LSN), and the tracker would periodically call
  `pg_replication_slot_advance` / send `standby status update` messages so
  Postgres knows it's safe to reclaim old WAL segments.
- **Failure handling changes shape.** If the tracker is down for a long time,
  WAL accumulates on the Postgres server (it can't be discarded until the
  slot confirms it's been consumed) — so I'd need monitoring on replication
  slot lag/disk usage, which isn't a concern at all with polling.
- **Schema changes become events too**, if using a tool like Debezium on top
  of the same slot — `ALTER TABLE` statements can be surfaced explicitly
  instead of silently changing what `SELECT *` returns.

## Why is polling CDC inadequate for high-throughput tables?

At high write volume, many changes can land on the same row between two poll
cycles, so the tracker only ever sees the final state and silently drops every
intermediate change — there's no way to reconstruct "what actually happened"
from a snapshot diff. Shrinking the poll interval to reduce that data loss
just trades it for constant, expensive re-scanning of the source table, which
adds real load and still can't reach true real-time latency. Log-based CDC
sidesteps both problems at once because it reads the database's own
transaction log, so every committed change is captured exactly once, in
order, with zero dependency on how the application happened to write it.
