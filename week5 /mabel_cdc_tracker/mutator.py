import os
import random
import time

import psycopg

DB_DSN = os.environ.get(
    "CDC_DB_DSN",
    "host=localhost port=5432 dbname=postgres user=postgres password=password",
)

INTERVAL_SECONDS = 60

FIRST_NAMES = ["Ada", "Bola", "Chinedu", "Amara", "Femi", "Ngozi", "Tunde", "Yemi", "Zainab", "Kunle"]
LAST_NAMES = ["Okafor", "Suleiman", "Adeyemi", "Balogun", "Eze", "Mohammed", "Okoro", "Bello", "Nwosu", "Abubakar"]
CITIES = ["Kano", "Lagos", "Abuja", "Ibadan", "Port Harcourt", "Enugu", "Kaduna"]


def do_insert(cur):
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 9999)}@example.com"
    city = random.choice(CITIES)
    cur.execute(
        "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s) RETURNING id",
        (name, email, city),
    )
    new_id = cur.fetchone()[0]
    print(f"[mutator] INSERT id={new_id} name={name!r} city={city!r}")


def do_update(cur):
    cur.execute(
        "SELECT id FROM customers WHERE deleted_at IS NULL ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        print("[mutator] UPDATE skipped: no live rows")
        return
    target_id = row[0]
    new_city = random.choice(CITIES)
    cur.execute(
        "UPDATE customers SET city = %s, updated_at = NOW() WHERE id = %s",
        (new_city, target_id),
    )
    print(f"[mutator] UPDATE id={target_id} -> city={new_city!r}")


def do_delete(cur):
    cur.execute(
        "SELECT id FROM customers WHERE deleted_at IS NULL ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        print("[mutator] DELETE skipped: no live rows")
        return
    target_id = row[0]
    cur.execute(
        "UPDATE customers SET deleted_at = NOW(), updated_at = NOW() WHERE id = %s",
        (target_id,),
    )
    print(f"[mutator] SOFT DELETE id={target_id}")


def main():
    print(f"Mutator starting. Mutating every {INTERVAL_SECONDS}s. Ctrl+C to stop.")
    with psycopg.connect(DB_DSN, autocommit=True) as conn:
        while True:
            with conn.cursor() as cur:
                action = random.choices(
                    [do_insert, do_update, do_delete],
                    weights=[0.4, 0.4, 0.2],  # inserts/updates more common than deletes
                    k=1,
                )[0]
                action(cur)
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
