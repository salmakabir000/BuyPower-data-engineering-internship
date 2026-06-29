# Crypto ETL

This project extracts cryptocurrency market data from the CoinGecko API, transforms it, and loads it into a SQLite database.

## Features

* Extracts top cryptocurrencies by market cap
* Stores data in SQLite
* Supports --dry-run
* Supports --top N
* Handles API rate limits with retries
* Prevents duplicate records

## Idempotency

Idempotency means running the same pipeline multiple times should not create duplicate records.

In this project, ingested_at is rounded to the minute and the table uses a composite primary key of (id, ingested_at). Together with INSERT OR IGNORE, this prevents duplicate rows when the script is run multiple times within the same minute.

## Example Commands

python crypto_etl.py

python crypto_etl.py --dry-run

python crypto_etl.py --top 10

## SQL Query

Find the coin with the largest 24-hour price drop:

SELECT name, price_change_percentage_24h
FROM coin_prices
ORDER BY price_change_percentage_24h ASC
LIMIT 1;
