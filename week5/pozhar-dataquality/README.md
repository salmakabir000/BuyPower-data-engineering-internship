# Data Quality Checker (dq.py)

## What it does
A Python library that loads a YAML config file describing 
data quality rules and runs them against a dataset, 
producing a readable report.

## Supported checks
- not_null: column must have no nulls
- unique: column values must be unique
- in_set: column values must be in a given list
- range: numeric column must be between min and max
- regex_match: string column must match a pattern
- row_count_min: dataset must have at least N rows

## How to run
python3 dq.py checks/coin_prices.yml

## Exit codes
- 0: all checks passed or only warnings
- 1: at least one critical check failed

## Example YAML config 1 — coin_prices
dataset: coin_prices
source:
  type: sqlite
  path: crypto.db
  table: coin_prices
checks:
  - column: id
    type: not_null
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
  - type: row_count_min
    value: 100
    severity: critical

## Example YAML config 2 — parquet file
dataset: taxi_trips
source:
  type: parquet
  path: yellow_tripdata_2024-01.parquet
checks:
  - column: fare_amount
    type: range
    min: 0
    max: 1000
    severity: critical
  - column: payment_type
    type: in_set
    values: [1, 2, 3, 4, 5, 6]
    severity: warning
  - type: row_count_min
    value: 1000000
    severity: critical

## Bug found in Week 4 data
The symbol column contains Chinese characters (币安人生) 
and non-standard symbols that fail the regex pattern 
^[a-z0-9]+$. This suggests CoinGecko returns some coins 
with non-ASCII symbols that need cleaning before storage.
