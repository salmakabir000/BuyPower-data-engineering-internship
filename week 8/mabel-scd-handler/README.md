# Slowly Changing Dimension Handler

A small Python module (`scd.py`) that implements Slowly Changing Dimension
(SCD) handling against Postgres, plus a runnable demo that loads three
daily customer snapshots and proves point-in-time accuracy.

Implements:
- **Type 1** (`apply_scd1`) — overwrite in place, no history.
- **Type 2** (`apply_scd2`) — full history via `valid_from` / `valid_to` /
  `is_current`.
- **Type 4** (`apply_scd4`, stretch goal) — a "current" table (Type-1
  style) plus a separate append-only history table of full row snapshots.

## Project layout

```
scd-handler/
├── scd.py                     # apply_scd1, apply_scd2, apply_scd4
├── generate_snapshots.py      # builds data/day1.csv, day2.csv, day3.csv
├── run_snapshots.py           # loads the 3 snapshots through all 3 SCD types
├── sql/schema.sql             # DDL for dim_customer, dim_customer_scd1,
│                               # dim_customer_current, dim_customer_history
├── data/                      # generated CSV snapshots (day1/2/3)
└── tests/test_point_in_time.py  # proves correctness end to end
```

## Setup

```bash
pip install psycopg2-binary pandas

createdb scd_demo
psql -d scd_demo -f sql/schema.sql
```

## Run

```bash
python3 generate_snapshots.py   # writes data/day1.csv, day2.csv, day3.csv
python3 run_snapshots.py        # loads all three days through SCD1/2/4
python3 tests/test_point_in_time.py
```

Expected test output:

```
PASS: customer 42's city as of Day 1 = Lagos, as of Day 2 = Abuja
PASS: exactly one is_current=TRUE row per customer_id
PASS: customer 42 has exactly the Lagos(closed) -> Abuja(open) history

All tests passed.
```

## The test query that proves point-in-time accuracy

This is the "what was customer 42's city on Day 2?" query from the
assignment, using only `valid_from`/`valid_to`:

```sql
SELECT city FROM dim_customer
WHERE customer_id = 42
  AND valid_from <= '2026-09-13 12:00:00+00'
  AND (valid_to IS NULL OR valid_to > '2026-09-13 12:00:00+00');
```

Result: `Abuja`. Ask the same question with a Day 1 timestamp instead and
it correctly returns `Lagos` — the closed-out row is still there, just no
longer `is_current`.

Real result from `dim_customer` after loading all three days (customers
42, 43, 44 change; 46 is added on Day 3):

```
 customer_sk | customer_id |     name     |     city      |       valid_from       |        valid_to        | is_current
-------------+-------------+--------------+---------------+------------------------+------------------------+------------
           3 |          42 | Ada Lovelace | Lagos         | 2026-09-12 02:00:00+00 | 2026-09-13 02:00:00+00 | f
           6 |          42 | Ada Lovelace | Abuja         | 2026-09-13 02:00:00+00 |                        | t
           4 |          43 | Tunde Bakare | Ibadan        | 2026-09-12 02:00:00+00 | 2026-09-13 02:00:00+00 | f
           7 |          43 | Tunde Bakare | Ibadan        | 2026-09-13 02:00:00+00 |                        | t
           5 |          44 | Ngozi Eze    | Enugu         | 2026-09-12 02:00:00+00 | 2026-09-14 02:00:00+00 | f
           9 |          44 | Ngozi Eze    | Port Harcourt | 2026-09-14 02:00:00+00 |                        | t
          10 |          46 | Femi Adigun  | Abuja         | 2026-09-14 02:00:00+00 |                        | t
```

(Customer 43's `city` doesn't change on Day 2 — only its email does — so
you'll see two rows for 43 too: the change detection is column-by-column
across all of `change_cols`, not just city, and a new version row is
created whenever *any* tracked column changes, even if the one you're
staring at didn't.)

## How change detection works

`change_cols` is the list of columns you care about tracking (here:
`name`, `email`, `city`). For every source row, `scd.py` fetches the
current target row for that business key and does a plain Python
comparison, column by column, treating `None == None` as "unchanged" (a
NULL-safe comparison — Postgres's own `!=` treats `NULL != NULL` as
unknown, which would misfire here). If **any** change_col differs, the row
counts as changed, and Type 1 overwrites it while Type 2 versions it.

## Comparison of SCD Types 1, 2, 3, 4, 6

| Type | What it does | History kept? | Schema shape | When to use | Trade-offs |
|---|---|---|---|---|---|
| **Type 1** | Overwrite the old value in place | None — old value is gone | Same columns as source; no versioning columns | Corrections, typo fixes, attributes where history genuinely doesn't matter (e.g. fixing a misspelled name) | Simplest possible; but any report that joined on the old value in the past will silently "change" retroactively |
| **Type 2** | Add a new row for every change; close out the old one | Full history, one row per version | Adds `valid_from`, `valid_to`, `is_current` (or a version number) and a surrogate key | The default choice in Kimball-style warehouses — you need to know what was true as of any past date (e.g. "what territory was this rep in when they made that sale?") | Table grows over time; every fact table join must pick the *right* version (as-of, not just "the row"), which needs care |
| **Type 3** | Add a new column for the *previous* value (e.g. `previous_city`, `current_city`) | Only one prior value, not full history | Same row, extra `previous_X` column(s) | You only ever need to compare "before vs. after" for one attribute, not arbitrary point-in-time (e.g. "what was the last region before the last reorg?") | Cheap and simple, but can't answer "what was it 3 changes ago" — each new change overwrites the "previous" slot |
| **Type 4** | Keep only the current value in the main table; log every change to a separate history table | Full history, but off to the side | Two tables: `current` (Type-1 shape) + `history` (append-only, full row per change) | High-traffic dimensions where most queries only need "current" and you don't want every join to filter `is_current`; history is rare-access | Two tables to maintain; point-in-time queries need a join/lookup into history instead of a simple range filter on the main table |
| **Type 6** | Hybrid of 1 + 2 + 3: Type 2 row versioning, *plus* a Type 1 "current value" column copied onto every historical row, *plus* optionally Type 3's previous-value column | Full history (like Type 2) with easy "what's it now" lookups from any row | Type 2 columns (`valid_from`/`valid_to`/`is_current`) plus a `current_X` column kept in sync across *all* rows for that business key | You need both "what was true then" and "what's true now" answerable from the *same* row without a second lookup — common in BI tools that want to filter historical facts by a dimension's *current* attribute | Most complex to maintain — every Type 1-style update has to fan out and update the `current_X` column on *every* historical row for that key, not just the current one |

This project implements Types 1, 2, and 4 in `scd.py`. Types 3 and 6 aren't
built here, but the table above is enough to reason about when you'd reach
for them.

## Definition of done — status

- [x] Both `apply_scd1` and `apply_scd2` work on the three test snapshots
- [x] Type 2 correctly produces history rows (see table above)
- [x] At any point in time, exactly one row per `customer_id` has
      `is_current = true` (enforced both by the load logic and by a
      partial unique index: `uq_dim_customer_current`)
- [x] A test script proves point-in-time accuracy
      (`tests/test_point_in_time.py`)
- [x] README includes the Type 1/2/3/4/6 comparison table
- [x] Stretch goal: Type 4 implemented (`apply_scd4`, `dim_customer_current` +
      `dim_customer_history`)
