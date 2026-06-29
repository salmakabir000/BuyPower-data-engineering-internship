# Crypto ETL Pipeline

## What it does
Extracts the top 250 cryptocurrencies by market cap from 
CoinGecko API, transforms the data and loads it into a 
SQLite database as price snapshots.

## What is Idempotency?
Idempotency means running the script multiple times produces 
the same result without duplicates. In data pipelines this is 
critical because scripts can fail and need to be rerun. 

We achieve this by using a composite primary key on (id, ingested_at) 
rounded to the nearest minute, and INSERT OR IGNORE so the same 
data is never written twice in the same minute.

## How to run
python3 crypto_etl.py
python3 crypto_etl.py --dry-run
python3 crypto_etl.py --top 10

## Flags
- --dry-run: extract and transform only, no database write
- --top N: fetch only top N coins (default 250)

## SQL query to find biggest 24hr price drop in last week
SELECT id, name, MIN(price_change_percentage_24h) as biggest_drop
FROM coin_prices
WHERE ingested_at >= datetime('now', '-7 days')
GROUP BY id, name
ORDER BY biggest_drop ASC
LIMIT 1;
