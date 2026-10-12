
import datetime as dt
import os

import pandas as pd
import psycopg2

from scd import apply_scd1, apply_scd2, apply_scd4

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
BUSINESS_KEY = "customer_id"
CHANGE_COLS = ["name", "email", "city"]

RUN_TS = {
    "day1.csv": dt.datetime(2026, 9, 12, 2, 0, tzinfo=dt.timezone.utc),
    "day2.csv": dt.datetime(2026, 9, 13, 2, 0, tzinfo=dt.timezone.utc),
    "day3.csv": dt.datetime(2026, 9, 14, 2, 0, tzinfo=dt.timezone.utc),
}


def get_conn():
    return psycopg2.connect(
        host="localhost",
        dbname="scd_demo",
        user="postgres",
        password="postgres",
    )


def main():
    conn = get_conn()
    for filename in ["day1.csv", "day2.csv", "day3.csv"]:
        path = os.path.join(DATA_DIR, filename)
        df = pd.read_csv(path)
        run_ts = RUN_TS[filename]

        scd1_counts = apply_scd1(
            conn, "dim_customer_scd1", df, BUSINESS_KEY, CHANGE_COLS
        )
        scd2_counts = apply_scd2(
            conn, "dim_customer", df, BUSINESS_KEY, CHANGE_COLS, run_ts
        )
        scd4_counts = apply_scd4(
            conn,
            "dim_customer_current",
            "dim_customer_history",
            df,
            BUSINESS_KEY,
            CHANGE_COLS,
            run_ts,
        )

        print(f"[{filename}] SCD1 -> {scd1_counts}")
        print(f"[{filename}] SCD2 -> {scd2_counts}")
        print(f"[{filename}] SCD4 -> {scd4_counts}")

    conn.close()


if __name__ == "__main__":
    main()
