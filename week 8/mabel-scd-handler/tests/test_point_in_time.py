import datetime as dt
import os
import sys

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def get_conn():
    return psycopg2.connect(
        host="localhost",
        dbname="scd_demo",
        user="postgres",
        password="postgres",
    )


def city_as_of(conn, customer_id: int, as_of: dt.datetime):
    """
    Point-in-time lookup: the version of the row that was valid at `as_of`.
    valid_to IS NULL means "still open" (i.e. still current at that time).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT city FROM dim_customer
            WHERE customer_id = %s
              AND valid_from <= %s
              AND (valid_to IS NULL OR valid_to > %s)
            """,
            (customer_id, as_of, as_of),
        )
        row = cur.fetchone()
        return row["city"] if row else None


def test_point_in_time_city(conn):
    # Day 2 run_ts is 2026-09-13T02:00:00Z. Ask "as of Day 2, right after
    # that load" — anything after day2's run_ts and before day3's.
    as_of_day2 = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.timezone.utc)
    city = city_as_of(conn, 42, as_of_day2)
    assert city == "Abuja", f"Expected Abuja on Day 2, got {city}"

    # Same customer, as of Day 1 (before the move) should still be Lagos.
    as_of_day1 = dt.datetime(2026, 9, 12, 12, 0, tzinfo=dt.timezone.utc)
    city = city_as_of(conn, 42, as_of_day1)
    assert city == "Lagos", f"Expected Lagos on Day 1, got {city}"

    print("PASS: customer 42's city as of Day 1 = Lagos, as of Day 2 = Abuja")


def test_exactly_one_current_row_per_customer(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT customer_id, COUNT(*)
            FROM dim_customer
            WHERE is_current = TRUE
            GROUP BY customer_id
            HAVING COUNT(*) <> 1
            """
        )
        offenders = cur.fetchall()
    assert offenders == [], f"customers with != 1 current row: {offenders}"
    print("PASS: exactly one is_current=TRUE row per customer_id")


def test_history_row_shape_for_customer_42(conn):
    """Matches the example in the assignment: two rows for customer 42,
    Lagos closed on day2, Abuja open-ended."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT city, valid_to, is_current FROM dim_customer "
            "WHERE customer_id = 42 ORDER BY valid_from"
        )
        rows = cur.fetchall()
    assert len(rows) == 2, f"expected 2 history rows for customer 42, got {len(rows)}"
    assert rows[0]["city"] == "Lagos" and rows[0]["is_current"] is False
    assert rows[0]["valid_to"] is not None
    assert rows[1]["city"] == "Abuja" and rows[1]["is_current"] is True
    assert rows[1]["valid_to"] is None
    print("PASS: customer 42 has exactly the Lagos(closed) -> Abuja(open) history")


if __name__ == "__main__":
    conn = get_conn()
    try:
        test_point_in_time_city(conn)
        test_exactly_one_current_row_per_customer(conn)
        test_history_row_shape_for_customer_42(conn)
    finally:
        conn.close()
    print("\nAll tests passed.")
