# crypto_etl — Simple CoinGecko → SQLite ETL

A single-file Python ETL that pulls the top cryptocurrencies from the
[CoinGecko public API](https://www.coingecko.com/en/api/documentation) and
stores price snapshots in a local SQLite database.

---

## Requirements

Python 3.10+ and one third-party library:

```bash
pip install requests
```

No API key is required for the CoinGecko free tier.

---

## How to run

### Default — fetch top 250 coins and write to `crypto.db`

```bash
python crypto_etl.py
```

Output:
```
[EXTRACT] Fetching top 250 coins from CoinGecko...
[EXTRACT] Got 250 coins from API.
[TRANSFORM] Cleaning and adding ingested_at...
[TRANSFORM] 250 rows ready for load.
[LOAD] Writing to crypto.db...
Loaded 250 rows. Total rows in coin_prices: 250.
```

### Fetch only the top N coins

```bash
python crypto_etl.py --top 50
```

### Dry run — extract and transform, skip the DB write

```bash
python crypto_etl.py --dry-run
```

Useful for testing that the API is reachable without touching the database.

### Only write coins updated after a given date

```bash
python crypto_etl.py --since 2024-06-01
```

---

## Database schema

File: `crypto.db` (created automatically in the current directory)  
Table: `coin_prices`

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT | CoinGecko coin ID, e.g. `bitcoin` |
| `symbol` | TEXT | Ticker symbol, e.g. `btc` |
| `name` | TEXT | Display name |
| `current_price` | REAL | USD price at ingestion time |
| `market_cap` | REAL | USD market cap |
| `total_volume` | REAL | 24-hour trading volume (USD) |
| `price_change_percentage_24h` | REAL | % change over last 24 hours |
| `last_updated` | TEXT | Timestamp from CoinGecko |
| `ingested_at` | TEXT | UTC timestamp, rounded to the minute (composite PK) |

**Primary key:** `(id, ingested_at)`

---

## What is idempotency and why does it matter?

**Idempotency** means running the same operation multiple times produces
exactly the same result as running it once — no duplicates, no data loss.

In data pipelines this matters because:

- **Retries are normal.** Network hiccups, rate limits, and server errors
  mean scripts get re-run. Without idempotency, every retry duplicates rows.
- **Backfills.** You often need to re-ingest a time window after fixing a
  bug. An idempotent pipeline lets you do this safely.
- **Orchestrators assume it.** Tools like Airflow will automatically retry
  failed tasks. Your pipeline must handle that gracefully.

### How this script achieves idempotency

1. **`ingested_at` is rounded to the nearest minute.** Running the script
   twice in the same minute produces the exact same `ingested_at` value for
   every row.
2. **`INSERT OR IGNORE`** on the composite primary key `(id, ingested_at)`.
   If a row already exists for that coin at that minute, the database silently
   skips it instead of raising an error or creating a duplicate.

---

## Useful SQL queries

### Open the database

```bash
sqlite3 crypto.db
```

### Which coin had the biggest 24-hour price drop in the last week?

```sql
SELECT
    name,
    symbol,
    MIN(price_change_percentage_24h) AS worst_drop_pct,
    ingested_at
FROM coin_prices
WHERE ingested_at >= datetime('now', '-7 days')
GROUP BY id
ORDER BY worst_drop_pct ASC
LIMIT 10;
```

### How many snapshots have been collected?

```sql
SELECT COUNT(DISTINCT ingested_at) AS snapshots,
       COUNT(*)                    AS total_rows
FROM coin_prices;
```

### Top 10 coins by market cap in the latest snapshot

```sql
SELECT name, symbol, current_price, market_cap
FROM coin_prices
WHERE ingested_at = (SELECT MAX(ingested_at) FROM coin_prices)
ORDER BY market_cap DESC
LIMIT 10;
```

---

## Rate limiting

The script handles HTTP 429 (Too Many Requests) automatically. It will retry
up to 3 times with increasing back-off delays (5 s → 15 s → 30 s). If all
three attempts fail, it raises an error with a clear message.
