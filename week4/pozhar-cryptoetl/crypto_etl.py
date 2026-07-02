import requests
import sqlite3
import click
from datetime import datetime, timezone, timedelta
import time

API_URL = "https://api.coingecko.com/api/v3/coins/markets"

def fetch_coins(top_n=250):
    coins = []
    per_page = 100
    pages = (top_n + per_page - 1) // per_page

    for page in range(1, pages + 1):
        retries = 3
        while retries > 0:
            response = requests.get(API_URL, params={
                'vs_currency': 'usd',
                'per_page': min(per_page, top_n - len(coins)),
                'page': page
            })
            if response.status_code == 429:
                print("Rate limited, retrying in 60 seconds...")
                time.sleep(60)
                retries -= 1
            elif response.status_code == 200:
                coins.extend(response.json())
                break
            else:
                print(f"Error: {response.status_code}")
                break
        time.sleep(1)

    return coins[:top_n]

def transform(coins):
    ingested_at = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ingested_at = ingested_at.strftime("%Y-%m-%d %H:%M:00")

    records = []
    for coin in coins:
        records.append((
            coin['id'],
            coin['symbol'],
            coin['name'],
            coin['current_price'],
            coin['market_cap'],
            coin['total_volume'],
            coin['price_change_percentage_24h'],
            ingested_at
        ))
    return records

def load(records, db_path='crypto.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
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
    ''')

    cursor.executemany('''
        INSERT OR IGNORE INTO coin_prices 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', records)

    conn.commit()
    total = cursor.execute('SELECT COUNT(*) FROM coin_prices').fetchone()[0]
    conn.close()
    return total

@click.command()
@click.option('--dry-run', is_flag=True, help='Extract and transform only, no database write')
@click.option('--top', default=250, help='Number of top coins to fetch')
def main(dry_run, top):
    print(f"Fetching top {top} coins...")
    coins = fetch_coins(top)
    records = transform(coins)

    if dry_run:
        print(f"Dry run: {len(records)} records extracted and transformed")
        for r in records[:3]:
            print(r)
        return

    total = load(records)
    print(f"Loaded {len(records)} rows. Total rows in coin_prices: {total}")

if __name__ == '__main__':
    main()
