import pandas as pd
import psycopg2
from datetime import datetime, timezone
from scd import apply_scd1, apply_scd2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)

change_cols = ['name', 'email', 'city']

# Simulate 3 different days with different timestamps
day1_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
day2_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
day3_ts = datetime(2026, 1, 3, tzinfo=timezone.utc)

print("=== Loading Day 1 ===")
df1 = pd.read_csv("day1.csv")
apply_scd2(conn, "dim_customer", df1, "customer_id", change_cols, day1_ts)

print("\n=== Loading Day 2 ===")
df2 = pd.read_csv("day2.csv")
apply_scd2(conn, "dim_customer", df2, "customer_id", change_cols, day2_ts)

print("\n=== Loading Day 3 ===")
df3 = pd.read_csv("day3.csv")
apply_scd2(conn, "dim_customer", df3, "customer_id", change_cols, day3_ts)

conn.close()
print("\nAll snapshots loaded!")
