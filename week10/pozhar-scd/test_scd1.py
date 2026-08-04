import pandas as pd
import psycopg2
from scd import apply_scd1

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)

change_cols = ['name', 'email', 'city']

print("=== Loading Day 1 (Type 1) ===")
df1 = pd.read_csv("day1.csv")
apply_scd1(conn, "dim_customer_type1", df1, "customer_id", change_cols)

print("\n=== Loading Day 2 (Type 1) ===")
df2 = pd.read_csv("day2.csv")
apply_scd1(conn, "dim_customer_type1", df2, "customer_id", change_cols)

print("\n=== Loading Day 3 (Type 1) ===")
df3 = pd.read_csv("day3.csv")
apply_scd1(conn, "dim_customer_type1", df3, "customer_id", change_cols)

conn.close()
print("\nDone! Notice: only ONE row per customer, no history kept.")
