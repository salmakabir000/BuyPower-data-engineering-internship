import argparse
import requests
import sqlite3
import time
from datetime import datetime

def fetch_page(page, per_page=100):
    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page
    }

    for attempt in range(3):
        response = requests.get(url, params=params)

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            wait = 2 ** attempt
            print(f"Rate limited. Waiting {wait} seconds...")
            time.sleep(wait)
        else:
            response.raise_for_status()

    raise Exception("Failed after 3 retries")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--top", type=int, default=250)

    args = parser.parse_args()

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    
    coins = []

    pages = (args.top + 99) // 100

    for page in range(1, pages + 1):
        coins.extend(fetch_page(page))

    coins = coins[:args.top]

    records = []

    for coin in coins:
        records.append((
            coin["id"],
            coin["symbol"],
            coin["name"],
            coin["current_price"],
            coin["market_cap"],
            coin["total_volume"],
            coin["price_change_percentage_24h"],
            timestamp
        ))

    if args.dry_run:
        print(f"Dry run complete. Would load {len(records)} rows.")
        return

    conn = sqlite3.connect("crypto.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS coin_prices (
        id TEXT,
        symbol TEXT,
        name TEXT,
        current_price REAL,
        market_cap REAL,
        total_volume REAL,
        price_change_percentage_24h REAL,
        ingested_at TEXT,
        PRIMARY KEY (id, ingested_at)
    )
    """)

    cursor.executemany("""
    INSERT OR IGNORE INTO coin_prices (
        id,
        symbol,
        name,
        current_price,
        market_cap,
        total_volume,
        price_change_percentage_24h,
        ingested_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)

    conn.commit()

    total_rows = cursor.execute(
        "SELECT COUNT(*) FROM coin_prices"
    ).fetchone()[0]

    print(
        f"Loaded {len(records)} rows. "
        f"Total rows in coin_prices: {total_rows}"
    )

    conn.close()


if __name__ == "__main__":
    main()