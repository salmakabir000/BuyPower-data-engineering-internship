import psycopg2
import random
import time
from datetime import datetime, timezone

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)
conn.autocommit = True
cur = conn.cursor()

names = ["Frank", "Lois", "Hope", "Peace", "Noah", "Noda", "Steven", "Kumai"]
cities = ["Lagos", "Abuja", "London", "New York", "Paris", "Tokyo", "Dubai"]

def random_insert():
    name = random.choice(names)
    email = f"{name.lower()}_{random.randint(1,999)}@email.com"
    city = random.choice(cities)
    cur.execute(
        "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)",
        (name, email, city)
    )
    print(f"[{datetime.now()}] INSERT: {name}")

def random_update():
    cur.execute("SELECT id FROM customers WHERE deleted_at IS NULL ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if row:
        new_city = random.choice(cities)
        cur.execute(
            "UPDATE customers SET city=%s, updated_at=NOW() WHERE id=%s",
            (new_city, row[0])
        )
        print(f"[{datetime.now()}] UPDATE: id={row[0]} city={new_city}")

def random_delete():
    cur.execute("SELECT id FROM customers WHERE deleted_at IS NULL ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if row:
        cur.execute(
            "UPDATE customers SET deleted_at=NOW(), updated_at=NOW() WHERE id=%s",
            (row[0],)
        )
        print(f"[{datetime.now()}] SOFT DELETE: id={row[0]}")

print("Starting mutations every 30 seconds...")
while True:
    action = random.choice([random_insert, random_insert, random_update, random_delete])
    action()
    time.sleep(30)

