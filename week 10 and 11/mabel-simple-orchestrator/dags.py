"""
Registers the crypto_etl DAG so the CLI can find it (see MINI_ORCH_DAGS_MODULE
in cli.py — this file is the default `dags` module it imports).

This mirrors a Week-3-style CoinGecko ETL: extract raw prices, transform
into a flat row shape, load into a local SQLite "warehouse", then run a
quality check. If your actual Week 3 ETL lives in its own module with its
own extract/transform/load functions, just import those functions here
instead of the ones defined below and keep the @dag block as-is.
"""

import json
import sqlite3
from pathlib import Path

import requests

from mini_orchestrator import dag, task

DATA_DIR = Path.home() / ".mini_orchestrator" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

RAW_PATH = DATA_DIR / "crypto_raw.json"
TRANSFORMED_PATH = DATA_DIR / "crypto_transformed.json"
WAREHOUSE_PATH = DATA_DIR / "warehouse.db"

COINS = ["bitcoin", "ethereum", "solana"]


def extract_crypto():
    resp = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": ",".join(COINS),
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        },
        timeout=10,
    )
    resp.raise_for_status()
    RAW_PATH.write_text(json.dumps(resp.json()))


def transform_crypto():
    raw = json.loads(RAW_PATH.read_text())
    rows = [
        {
            "coin": coin,
            "price_usd": values.get("usd"),
            "change_24h": values.get("usd_24h_change"),
        }
        for coin, values in raw.items()
    ]
    TRANSFORMED_PATH.write_text(json.dumps(rows))


def load_to_warehouse():
    rows = json.loads(TRANSFORMED_PATH.read_text())
    conn = sqlite3.connect(WAREHOUSE_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_prices (
            coin TEXT,
            price_usd REAL,
            change_24h REAL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.executemany(
        "INSERT INTO crypto_prices (coin, price_usd, change_24h) VALUES (:coin, :price_usd, :change_24h)",
        rows,
    )
    conn.commit()
    conn.close()


def run_dq_checks():
    rows = json.loads(TRANSFORMED_PATH.read_text())
    if not rows:
        raise ValueError("No rows produced by transform step")
    for row in rows:
        if row["price_usd"] is None or row["price_usd"] <= 0:
            raise ValueError(f"Invalid price for {row['coin']}: {row['price_usd']}")


@dag(name="crypto_etl", schedule="0 * * * *")
def crypto_etl():
    extract = task("extract", run=extract_crypto)
    transform = task("transform", run=transform_crypto, depends_on=[extract])
    load = task("load", run=load_to_warehouse, depends_on=[transform])
    task("quality_check", run=run_dq_checks, depends_on=[load])
