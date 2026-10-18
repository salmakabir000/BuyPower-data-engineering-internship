import argparse
import requests
from datetime import datetime, timezone
import sqlite3



parser = argparse.ArgumentParser()

parser.add_argument(
    "--top",
    type=int,
    default=250
)

parser.add_argument(
    "--dry-run",
    action="store_true"
)

parser.add_argument(
    "--since",
    type=str,
    default=None
)

args = parser.parse_args()



def extract_data(top):

    url = "https://api.coingecko.com/api/v3/coins/markets"

    all_coins = []

    pages_needed = (top + 99) // 100

    for page in range(1, pages_needed + 1):

        params = {
            "vs_currency": "usd",
            "per_page": 100,
            "page": page
        }

        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return []

        all_coins.extend(response.json())

    return all_coins[:top]


def transform_data(data, since):

    transformed_data = []

    ingested_at = datetime.now(timezone.utc).replace(
        second=0,
        microsecond=0
    )

    since_date = None

    if since:
        since_date = datetime.fromisoformat(since).replace(
            tzinfo=timezone.utc
        )

    for coin in data:
        coin_updated = datetime.fromisoformat(
            coin["last_updated"].replace("Z", "+00:00")
        )

        if since_date and coin_updated <= since_date:
            continue
        clean_coin = {
            "id": coin["id"],
            "symbol": coin["symbol"],
            "name": coin["name"],
            "current_price": coin["current_price"],
            "market_cap": coin["market_cap"],
            "total_volume": coin["total_volume"],
            "price_change_percentage_24h": coin["price_change_percentage_24h"],
            "ingested_at": ingested_at
        }

        transformed_data.append(clean_coin)

    return transformed_data


def load_data(transformed_data):

    connection = sqlite3.connect("crypto.db")
    cursor = connection.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS coin_prices (
        id TEXT,
        symbol TEXT,
        name TEXT,
        current_price REAL,
        market_cap INTEGER,
        total_volume INTEGER,
        price_change_percentage_24h REAL,
        ingested_at TEXT,
        PRIMARY KEY (id, ingested_at)
    )
    """)

    for coin in transformed_data:

        cursor.execute("""
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
        """,
        (
            coin["id"],
            coin["symbol"],
            coin["name"],
            coin["current_price"],
            coin["market_cap"],
            coin["total_volume"],
            coin["price_change_percentage_24h"],
            coin["ingested_at"]
        ))

    connection.commit()

    cursor.execute(""" SELECT COUNT(*) FROM coin_prices """)
    total_rows = cursor.fetchone()[0]

    print(f"Loaded {len(transformed_data)} rows.")
    print(f"Total rows in coin_prices: {total_rows}")

    connection.close()

raw_data = extract_data(args.top)

transformed_data = transform_data(raw_data, args.since)

print(f"Extracted {len(raw_data)} rows")
print(f"Transformed {len(transformed_data)} rows")


if args.dry_run:
    print("Skipping database load.")
else:
    load_data(transformed_data)
