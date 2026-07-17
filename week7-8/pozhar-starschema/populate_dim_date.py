import psycopg2
import calendar
from datetime import date, timedelta

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)
conn.autocommit = True
cur = conn.cursor()

def populate_dim_date(start_year=2020, end_year=2030):
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    current = start
    count = 0

    print(f"Populating dim_date from {start_year} to {end_year}...")

    while current <= end:
        date_sk = int(current.strftime("%Y%m%d"))
        cur.execute("""
            INSERT INTO dim_date (date_sk, date, year, quarter, month, month_name, 
                                  day, day_of_week, day_name, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_sk) DO NOTHING
        """, (
            date_sk,
            current,
            current.year,
            (current.month - 1) // 3 + 1,
            current.month,
            calendar.month_name[current.month],
            current.day,
            current.weekday(),
            calendar.day_name[current.weekday()],
            current.weekday() >= 5
        ))
        current += timedelta(days=1)
        count += 1

    print(f"Done! Inserted {count:,} dates")

if __name__ == '__main__':
    populate_dim_date()
