"""
seed.py

One-time seeding script. Creates the customers table (if missing) and inserts
a batch of starter rows so the mutator and tracker have something to work with.

Usage:
    python seed.py
"""

import os
import random

import psycopg

DB_DSN = os.environ.get(
    "CDC_DB_DSN",
    "host=localhost port=5432 dbname=postgres user=postgres password=password",
)

FIRST_NAMES = ["Ada", "Bola", "Chinedu", "Amara", "Femi", "Ngozi", "Tunde", "Yemi", "Zainab", "Kunle"]
LAST_NAMES = ["Okafor", "Suleiman", "Adeyemi", "Balogun", "Eze", "Mohammed", "Okoro", "Bello", "Nwosu", "Abubakar"]
CITIES = ["Kano", "Lagos", "Abuja", "Ibadan", "Port Harcourt", "Enugu", "Kaduna"]

NUM_SEED_ROWS = 20


def make_customer():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@example.com"
    city = random.choice(CITIES)
    return name, email, city


def main():
    with open("schema.sql") as f:
        schema_sql = f.read()

    with psycopg.connect(DB_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            print("Schema ensured.")

            for _ in range(NUM_SEED_ROWS):
                name, email, city = make_customer()
                # created_at and updated_at both default to NOW() and are evaluated
                # once per statement/transaction in Postgres, so they land on the
                # exact same value -> this row will correctly classify as an INSERT.
                cur.execute(
                    "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)",
                    (name, email, city),
                )

            print(f"Inserted {NUM_SEED_ROWS} seed rows.")


if __name__ == "__main__":
    main()
