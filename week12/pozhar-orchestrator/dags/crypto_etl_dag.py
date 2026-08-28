"""
crypto_etl_dag.py - wraps the Week 4 CoinGecko ETL as a mini_orchestrator DAG.

The extract/transform/load functions below are copied directly from
week4/pozhar-cryptoetl/crypto_etl.py, unchanged. The only new part is the
`context` dict pattern: since a Task's run() function takes no arguments,
each task reads its input from `context` (filled in by the previous task)
and writes its own output back into `context` for the next task to use.
"""

import time
import sqlite3
from datetime import datetime, timezone

import requests

from mini_orchestrator import dag, task

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


def transform_coins(coins):
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


def load_records(records, db_path='crypto.db'):
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


@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():
    # context carries data between tasks, since task.run() takes no args
    context = {}

    def do_extract():
        context["coins"] = fetch_coins(top_n=50)
        print(f"      extracted {len(context['coins'])} coins")

    def do_transform():
        context["records"] = transform_coins(context["coins"])
        print(f"      transformed {len(context['records'])} records")

    def do_load():
        context["total"] = load_records(context["records"])
        print(f"      loaded. total rows in coin_prices: {context['total']}")

    extract = task("extract", run=do_extract)
    transform = task("transform", run=do_transform, depends_on=[extract])
    load = task("load", run=do_load, depends_on=[transform])
