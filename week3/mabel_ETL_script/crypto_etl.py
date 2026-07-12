#!/usr/bin/env python3
"""
crypto_etl.py — Simple ETL: CoinGecko → SQLite

Extract the top N cryptocurrencies by market cap, transform the fields,
and load them into a local SQLite database (crypto.db).

Usage:
    python crypto_etl.py                   # fetch top 250, write to DB
    python crypto_etl.py --top 50          # only top 50 coins
    python crypto_etl.py --dry-run         # extract & transform, skip DB write
    python crypto_etl.py --since 2024-01-01  # only write coins updated after date
"""

import argparse
import sqlite3
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
DB_PATH = "crypto.db"
PAGE_SIZE = 100          # CoinGecko max per_page
MAX_RETRIES = 3
RETRY_BACKOFF = [5, 15, 30]   # seconds to wait on each retry attempt

KEEP_FIELDS = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_percentage_24h",
    "last_updated",
]


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def fetch_page(page: int, per_page: int) -> list[dict]:
    """Fetch one page from the CoinGecko /coins/markets endpoint with retry."""
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(API_URL, params=params, timeout=15)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF[attempt]
                print(f"  Rate limited (429). Waiting {wait}s before retry {attempt + 1}/{MAX_RETRIES}...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                print(f"  Request error: {exc}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {exc}") from exc
    return []


def extract(top_n: int) -> list[dict]:
    """Paginate through the API and return raw coin records."""
    coins = []
    page = 1
    while len(coins) < top_n:
        batch_size = min(PAGE_SIZE, top_n - len(coins))
        print(f"  Fetching page {page} ({batch_size} coins)...")
        batch = fetch_page(page, batch_size)
        if not batch:
            break
        coins.extend(batch)
        page += 1
        # Be a polite API citizen — small delay between pages
        if len(coins) < top_n:
            time.sleep(1)
    return coins[:top_n]


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def round_to_minute(dt: datetime) -> str:
    """Round a datetime down to the nearest minute (for idempotency)."""
    return dt.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:00")


def transform(raw_coins: list[dict], since_date: str | None = None) -> list[dict]:
    """Keep only the required fields and add ingested_at."""
    ingested_at = round_to_minute(datetime.now(timezone.utc))
    rows = []
    for coin in raw_coins:
        # --since filter
        if since_date:
            last_updated = coin.get("last_updated", "")
            if last_updated and last_updated[:10] < since_date:
                continue

        row = {field: coin.get(field) for field in KEEP_FIELDS}
        row["ingested_at"] = ingested_at
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS coin_prices (
    id                          TEXT,
    symbol                      TEXT,
    name                        TEXT,
    current_price               REAL,
    market_cap                  REAL,
    total_volume                REAL,
    price_change_percentage_24h REAL,
    last_updated                TEXT,
    ingested_at                 TEXT,
    PRIMARY KEY (id, ingested_at)
);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO coin_prices
    (id, symbol, name, current_price, market_cap, total_volume,
     price_change_percentage_24h, last_updated, ingested_at)
VALUES
    (:id, :symbol, :name, :current_price, :market_cap, :total_volume,
     :price_change_percentage_24h, :last_updated, :ingested_at);
"""


def load(rows: list[dict]) -> int:
    """Write rows to SQLite and return the total row count in the table."""
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.executescript(CREATE_TABLE_SQL)
        cur.executemany(INSERT_SQL, rows)
        con.commit()
        cur.execute("SELECT COUNT(*) FROM coin_prices;")
        total = cur.fetchone()[0]
    finally:
        con.close()
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CoinGecko → SQLite ETL")
    parser.add_argument(
        "--top",
        type=int,
        default=250,
        metavar="N",
        help="Fetch only the top N coins by market cap (default: 250).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform but do NOT write to the database.",
    )
    parser.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        default=None,
        help="Only write coins whose last_updated field is after this date.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    print(f"[EXTRACT] Fetching top {args.top} coins from CoinGecko...")
    raw = extract(args.top)
    print(f"[EXTRACT] Got {len(raw)} coins from API.")

    print("[TRANSFORM] Cleaning and adding ingested_at...")
    rows = transform(raw, since_date=args.since)
    print(f"[TRANSFORM] {len(rows)} rows ready for load.")

    if args.dry_run:
        print("[DRY-RUN] Skipping database write. First 3 rows:")
        for r in rows[:3]:
            print(" ", r)
        return

    print(f"[LOAD] Writing to {DB_PATH}...")
    total = load(rows)
    print(f"Loaded {len(rows)} rows. Total rows in coin_prices: {total:,}.")


if __name__ == "__main__":
    main()
