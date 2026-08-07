import psycopg2
from datetime import datetime, timezone

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)
cur = conn.cursor()

def get_customer_as_of(customer_id, point_in_time):
    """Point-in-time query: what did this customer look like at a given moment?"""
    cur.execute("""
        SELECT customer_id, name, email, city, valid_from, valid_to
        FROM dim_customer
        WHERE customer_id = %s
        AND valid_from <= %s
        AND (valid_to IS NULL OR valid_to > %s)
    """, (customer_id, point_in_time, point_in_time))
    return cur.fetchone()

# Test: What was Ada Lovelace's city on Day 2?
day2 = datetime(2026, 1, 2, tzinfo=timezone.utc)
result = get_customer_as_of(3, day2)

print("=== Point-in-Time Test ===")
print(f"Query: What was customer_id=3 (Ada Lovelace) like on {day2}?")
print(f"Result: {result}")
print(f"\nAda's city on Day 2 was: {result[3]}")

# Bonus: check on Day 1 too (before the change)
day1 = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
result_day1 = get_customer_as_of(3, day1)
print(f"\nAda's city on Day 1 (midday) was: {result_day1[3]}")

conn.close()
