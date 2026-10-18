# Crypto ETL Pipeline

## Overview

This project is a simple ETL (Extract, Transform, Load) pipeline built with Python. It retrieves cryptocurrency market data from the CoinGecko API, transforms the data into a cleaner format, and stores it in a SQLite database.

The script also supports limiting the number of coins retrieved, filtering records by date, and running without loading data into the database.

---

## Features

* Fetches cryptocurrency market data from the CoinGecko API
* Supports pagination when retrieving large datasets
* Keeps only the required fields during transformation
* Adds an ingestion timestamp to each record
* Loads data into a SQLite database
* Prevents duplicate records using a composite primary key
* Supports command-line arguments for:

  * limiting the number of coins (`--top`)
  * filtering by update date (`--since`)
  * skipping database loading (`--dry-run`)

---

## Requirements

* Python 3.10+
* requests

Install the required package:

```bash
pip install requests
```

---

## Project Structure

```text
.
├── crypto_etl.py
├── crypto.db
└── README.md
```

---

## Database Schema

The data is stored in a table called `coin_prices`.

| Column                      | Type    |
| --------------------------- | ------- |
| id                          | TEXT    |
| symbol                      | TEXT    |
| name                        | TEXT    |
| current_price               | REAL    |
| market_cap                  | INTEGER |
| total_volume                | INTEGER |
| price_change_percentage_24h | REAL    |
| ingested_at                 | TEXT    |

The table uses a composite primary key:

```text
(id, ingested_at)
```

This implement idempotency by preventing duplicate records for the same ingestion time.

---

## Usage

Run the script with the default settings:

```bash
python crypto_etl.py
```

Retrieve only the top 50 cryptocurrencies:

```bash
python crypto_etl.py --top 50
```

Run without loading data into SQLite:

```bash
python crypto_etl.py --dry-run
```

Filter records using the last updated date:

```bash
python crypto_etl.py --since 2026-06-28
```

Combine multiple options:

```bash
python crypto_etl.py --top 100 --since 2026-06-28 --dry-run
```

---

## Example Output

```text
Extracted 50 rows
Transformed 49 rows
Skipping database load.
```

or

```text
Extracted 250 rows
Transformed 250 rows
Loaded 250 rows.
Total rows in coin_prices: 250
```

---

## ETL Workflow

### Extract

* Retrieves cryptocurrency market data from the CoinGecko API.
* Supports pagination for requests larger than 100 records.

### Transform

* Selects only the required fields.
* Adds an `ingested_at` timestamp.
* Optionally filters records using the `--since` argument if provided.

### Load

* Creates the SQLite database and table if they do not already exist.
* Inserts records using `INSERT OR IGNORE`.
* Commits the transaction and reports the total number of rows stored.

---

## SQL Query

The query to find the cryptocurrency with the largest 24-hour price drop recorded in the last seven days.

```sql
SELECT id, name, price_change_percentage_24h, ingested_at
FROM coin_prices
WHERE ingested_at >= datetime('now', '-7 days')
ORDER BY price_change_percentage_24h ASC
LIMIT 1;
```

---
## Notes

* The script stores data in a local SQLite database named `crypto.db`.
* Duplicate records are ignored using the composite primary key.
* The `--dry-run` option performs extraction and transformation without writing to the database.
