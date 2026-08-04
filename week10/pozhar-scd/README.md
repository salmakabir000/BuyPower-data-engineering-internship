# Slowly Changing Dimension (SCD) Handler

## What is a Slowly Changing Dimension?
Dimensions change over time — a customer moves cities, a product 
gets renamed. How you handle these changes is a major data 
warehouse design decision. This project implements SCD Type 1 
(overwrite) and Type 2 (preserve history).

## SCD Type 1 vs Type 2

### Type 1 — Overwrite
Updates the existing row in place. No history is kept — only 
the current value survives.

Example: Ada moves from Lagos to Abuja.
Result: 1 row, city = Abuja. Lagos is gone forever.

### Type 2 — Preserve History
Closes the old row (sets valid_to and is_current = false) and 
inserts a new row with the updated values.

Example: Ada moves from Lagos to Abuja.
Result: 2 rows:
- customer_sk 3: Lagos, valid_from=Day1, valid_to=Day2, is_current=false
- customer_sk 6: Abuja, valid_from=Day2, valid_to=NULL, is_current=true

## How Change Detection Works
For each row in the new snapshot:
1. Look up the current row for that business_key (customer_id)
2. If no current row exists → INSERT as new
3. If a current row exists, compare tracked columns (name, email, city)
4. If any tracked column differs → close old row, insert new row
5. If nothing changed → do nothing (no unnecessary rows created)

## Point-in-Time Query Proof
Tested: "What was Ada Lovelace's city on Day 2?"

Query filters WHERE valid_from <= point_in_time AND 
(valid_to IS NULL OR valid_to > point_in_time)

Result: Day 1 (midday) → Lagos. Day 2 → Abuja. Confirmed correct.

## SCD Types Comparison

| Type | Description | History Kept | Use Case |
|------|-------------|---------------|----------|
| Type 1 | Overwrite in place | No | Correcting typos, data you never need to audit |
| Type 2 | New row per change with valid_from/valid_to | Yes, full history | Tracking meaningful business changes (address, status) |
| Type 3 | Add a "previous_value" column | Only 1 prior value | When you only care about the immediately previous state |
| Type 4 | Separate history table, current table stays small | Yes, in a separate table | High-change dimensions where the current table must stay fast |
| Type 6 | Combines Type 1 + 2 + 3 (hybrid) | Yes, plus a quick-access current value | Complex reporting needs both history and fast current lookups |

## How to run
1. Start Postgres: sudo docker start pg-cdc
2. Create tables (see schema in setup)
3. Run: python3 load_snapshots.py
4. Run: python3 test_point_in_time.py
