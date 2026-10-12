# A tiny data quality checker

A small, dependency-light tool (pandas + PyYAML) that runs declarative data
quality checks against a SQLite table or a Parquet file, prints a
readable report, and exits non-zero if a **critical** check fails — so it
can be dropped straight into a pipeline as a gate.

## Why this exists

Pipelines don't crash when data quality degrades — they keep running and
quietly hand you garbage. A column goes 50% null, a join drops half your
rows, a value drifts outside a sane range — nothing errors, nothing pages
anyone. This tool is a minimal version of what tools like Great
Expectations, Soda, and Monte Carlo do: turn "what does good data look
like" into a YAML file that gets checked every run.

## Install

```bash
pip install pandas pyyaml pyarrow   # pyarrow only needed for Parquet sources
```

## Usage

```bash
python dq.py checks/coin_prices.yml
```

Exit code `0` = no critical failures (there may still be warnings printed).
Exit code `1` = at least one critical check failed. Use this in CI:

```bash
python dq.py checks/coin_prices.yml || echo "pipeline should stop here"
```

### Compare mode (row-count / volume check)

Catches a silently-broken upstream extract — e.g. yesterday's snapshot had
1,250 rows, today's has 40, and nothing else "errored":

```bash
python dq.py checks/today.yml --compare checks/yesterday.yml --max-drop-pct 10
```

This loads the dataset each config points to, compares row counts, and
exits `1` if the drop exceeds `--max-drop-pct` (default 10%).

## Supported checks

| type            | applies to        | required keys              | optional keys      |
|-----------------|--------------------|-----------------------------|---------------------|
| `not_null`      | one column         | `column`                   | —                    |
| `unique`        | one column         | `column`                   | —                    |
| `in_set`        | one column         | `column`, `values`         | —                    |
| `range`         | one numeric column | `column`                   | `min`, `max`         |
| `regex_match`   | one string column  | `column`, `pattern`        | —                    |
| `row_count_min` | whole dataset      | `value`                    | —                    |

Every check also takes `severity: critical` or `severity: warning`
(defaults to `warning` if omitted). Only `critical` failures flip the exit
code to 1.

## Config file structure

```yaml
dataset: <name shown in the report header>
source:
  type: sqlite | parquet
  path: <file path>
  table: <table name>        # sqlite only
checks:
  - column: <col name>       # omit for row_count_min
    type: not_null | unique | in_set | range | regex_match | row_count_min
    severity: critical | warning
    # + check-specific keys (values / min / max / pattern / value)
```

## Example 1 — SQLite source (`checks/coin_prices.yml`)

```yaml
dataset: coin_prices
source:
  type: sqlite
  path: crypto.db
  table: coin_prices

checks:
  - column: id
    type: not_null
    severity: critical

  - column: id
    type: unique
    severity: critical

  - column: current_price
    type: range
    min: 0
    max: 10000000
    severity: warning

  - column: symbol
    type: regex_match
    pattern: "^[a-z0-9]+$"
    severity: warning

  - column: symbol
    type: in_set
    values: [btc, eth, usdt, bnb, sol, xrp, ada, doge, dot, avax]
    severity: warning

  - type: row_count_min
    value: 100
    severity: critical
```

Sample run against the test database in this repo (see "Bug I found" below):

```
$ python dq.py checks/coin_prices.yml
Data Quality Report — coin_prices
Total rows checked: 1,250

✗ id             not_null        3 failures (critical)
    Sample failing values: nan
✓ id             unique          0 failures
✗ current_price  range           1 failures (warning)
    Sample failing values: 99999999.0
✗ symbol         regex_match     6 failures (warning)
    Sample failing values: DOGE, AVAX, XRP, BNB
✗ symbol         in_set          6 failures (warning)
    Sample failing values: DOGE, AVAX, XRP, BNB
✓ row count      row_count_min   passed (1,250 >= 100)

Result: 3 warning(s), 1 critical failure(s)
Exit code: 1
```

## Example 2 — Parquet source, clean dataset (`checks/coin_prices_clean_parquet.yml`)

```yaml
dataset: coin_prices (parquet, clean)
source:
  type: parquet
  path: coin_prices_clean.parquet

checks:
  - column: id
    type: not_null
    severity: critical

  - column: id
    type: unique
    severity: critical

  - column: symbol
    type: in_set
    values: [btc, eth]
    severity: warning

  - type: row_count_min
    value: 50
    severity: critical
```

```
$ python dq.py checks/coin_prices_clean_parquet.yml
Data Quality Report — coin_prices (parquet, clean)
Total rows checked: 200

✓ id             not_null        0 failures
✓ id             unique          0 failures
✓ symbol         in_set          0 failures
✓ row count      row_count_min   passed (200 >= 50)

Result: 0 warning(s), 0 critical failure(s)
Exit code: 0
```

## Bug found by running this against my own data

The test `crypto.db` in this repo stands in for a Week 3-style
`coin_prices` table, seeded with a few realistic bugs on purpose so the
checker had something real to catch:

- **3 rows had a `NULL` id** — a `not_null` critical check caught this
  immediately (exit code 1). In a real pipeline, a null primary key
  usually means a bad merge/upsert step lost track of a row's identity —
  worth checking the ingestion job's dedupe or ID-assignment logic before
  it reaches this table.
- **6 rows had an uppercase symbol** (`DOGE` instead of `doge`) — caught
  by both `regex_match` and `in_set` as warnings. This is the classic
  "join looked fine, but two systems disagree on casing" bug: it won't
  break anything obviously, but it will silently create duplicate
  "different" symbols in any `GROUP BY symbol` downstream, undercounting
  or overcounting per-coin metrics.
- **1 row had a price of 99,999,999** — way outside the `range` check's
  max of 10,000,000, caught as a warning. Real-world equivalent: a bad
  scrape, a unit mismatch (e.g. some upstream source in a different
  currency or scale), or a join that fanned out and multiplied a value.

None of these would throw an exception anywhere in a normal pipeline —
they'd just quietly sit in the table until a stakeholder noticed a metric
looked wrong. That's the whole point of this tool.

## Design notes / how the checks work

- `not_null`: counts `NaN`/`None` in the column.
- `unique`: flags every row involved in a duplicate (not just the 2nd+
  occurrence) — nulls are excluded here since `not_null` already reports
  those separately.
- `in_set`: flags any non-null value not present in the given list.
- `range`: coerces the column to numeric; anything that fails to parse as
  a number *and* anything outside `[min, max]` both count as failures.
- `regex_match`: applies `re.match` (anchors at the start of the string;
  use `^...$` in your pattern to anchor both ends) to each non-null value.
- `row_count_min`: simplest check — just `len(df) >= value`.

## Files in this repo

```
dq.py                                  # the library/CLI
checks/coin_prices.yml                 # example 1 (sqlite, has bugs)
checks/coin_prices_clean_parquet.yml   # example 2 (parquet, clean)
checks/coin_prices_broken.yml          # used to demo row_count_min failing
crypto.db                              # sample sqlite db with seeded bugs
crypto_small.db                        # sample "broken extract" for --compare demo
coin_prices_clean.parquet              # sample clean parquet file
```

## Stretch goal status

`dq_compare` is implemented via `--compare`: point it at a previous
config's `source`, set `--max-drop-pct`, and it flags (and exits 1 if)
the current dataset's row count dropped by more than that percentage
compared to the previous one.
