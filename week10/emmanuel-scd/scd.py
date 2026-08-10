import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from psycopg2 import sql


# PostgreSQL connection settings
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "scd_db"
DB_USER = "postgres"
DB_PASSWORD = os.getenv("SCD_DB_PASSWORD")


def get_connection():
    """Create a connection to PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def create_target_table(conn, target_table):
    """Create the SCD target table if it does not already exist."""

    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            customer_sk SERIAL PRIMARY KEY,
            customer_id INT NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            city TEXT,
            valid_from TIMESTAMPTZ NOT NULL,
            valid_to TIMESTAMPTZ,
            is_current BOOLEAN NOT NULL DEFAULT TRUE
        );
    """).format(sql.Identifier(target_table))

    with conn.cursor() as cur:
        cur.execute(query)

    conn.commit()


def apply_scd1(conn, target_table, source_df, business_key, change_cols):
    """
    Apply Slowly Changing Dimension Type 1.

    Existing records are updated in place.
    New records are inserted.
    No history is kept.
    """

    with conn.cursor() as cur:

        for _, row in source_df.iterrows():

            business_value = row[business_key]

            # Check whether the business key already exists
            check_query = sql.SQL("""
                SELECT customer_sk
                FROM {}
                WHERE {} = %s
                LIMIT 1;
            """).format(
                sql.Identifier(target_table),
                sql.Identifier(business_key)
            )

            cur.execute(check_query, (business_value,))
            existing = cur.fetchone()

            if existing:
                # Update changed columns in place
                set_parts = []
                values = []

                for column in change_cols:
                    set_parts.append(
                        sql.SQL("{} = %s").format(sql.Identifier(column))
                    )
                    values.append(row[column])

                if set_parts:
                    update_query = sql.SQL("""
                        UPDATE {}
                        SET {}
                        WHERE {} = %s;
                    """).format(
                        sql.Identifier(target_table),
                        sql.SQL(", ").join(set_parts),
                        sql.Identifier(business_key)
                    )

                    values.append(business_value)
                    cur.execute(update_query, values)

            else:
                # Insert new record
                columns = list(source_df.columns) + [
                    "valid_from",
                    "valid_to",
                    "is_current"
                ]

                values = [row[column] for column in source_df.columns]
                values.extend([datetime.now(), None, True])

                column_sql = sql.SQL(", ").join(
                    sql.Identifier(column) for column in columns
                )

                placeholders = sql.SQL(", ").join(
                    sql.Placeholder() for _ in columns
                )

                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    VALUES ({});
                """).format(
                    sql.Identifier(target_table),
                    column_sql,
                    placeholders
                )

                cur.execute(insert_query, values)

    conn.commit()


def values_are_different(old_value, new_value):
    """Compare two values safely, including missing values."""

    if pd.isna(old_value) and pd.isna(new_value):
        return False

    if pd.isna(old_value) or pd.isna(new_value):
        return True

    return old_value != new_value


def apply_scd2(
    conn,
    target_table,
    source_df,
    business_key,
    change_cols,
    run_ts
):
    """
    Apply Slowly Changing Dimension Type 2.

    Existing historical records are preserved.

    When a tracked column changes:
    1. Close the current record.
    2. Set valid_to.
    3. Set is_current to FALSE.
    4. Insert the new version.
    """

    with conn.cursor() as cur:

        for _, row in source_df.iterrows():

            business_value = row[business_key]

            # Find the current version
            current_query = sql.SQL("""
                SELECT customer_sk, {}
                FROM {}
                WHERE {} = %s
                  AND is_current = TRUE
                LIMIT 1;
            """).format(
                sql.SQL(", ").join(
                    sql.Identifier(column)
                    for column in change_cols
                ),
                sql.Identifier(target_table),
                sql.Identifier(business_key)
            )

            cur.execute(current_query, (business_value,))
            current = cur.fetchone()

            # -------------------------------------------------
            # Customer does not exist
            # -------------------------------------------------
            if current is None:

                columns = list(source_df.columns) + [
                    "valid_from",
                    "valid_to",
                    "is_current"
                ]

                values = [row[column] for column in source_df.columns]
                values.extend([run_ts, None, True])

                column_sql = sql.SQL(", ").join(
                    sql.Identifier(column) for column in columns
                )

                placeholders = sql.SQL(", ").join(
                    sql.Placeholder() for _ in columns
                )

                insert_query = sql.SQL("""
                    INSERT INTO {} ({})
                    VALUES ({});
                """).format(
                    sql.Identifier(target_table),
                    column_sql,
                    placeholders
                )

                cur.execute(insert_query, values)

            else:
                # -------------------------------------------------
                # Customer exists - check for changes
                # -------------------------------------------------

                current_values = current[1:]

                changed = False

                for old_value, column in zip(
                    current_values,
                    change_cols
                ):
                    if values_are_different(
                        old_value,
                        row[column]
                    ):
                        changed = True
                        break

                # -------------------------------------------------
                # Something changed
                # -------------------------------------------------

                if changed:

                    customer_sk = current[0]

                    # Close old version
                    close_query = sql.SQL("""
                        UPDATE {}
                        SET valid_to = %s,
                            is_current = FALSE
                        WHERE customer_sk = %s;
                    """).format(
                        sql.Identifier(target_table)
                    )

                    cur.execute(
                        close_query,
                        (run_ts, customer_sk)
                    )

                    # Insert new version
                    columns = list(source_df.columns) + [
                        "valid_from",
                        "valid_to",
                        "is_current"
                    ]

                    values = [row[column] for column in source_df.columns]
                    values.extend([run_ts, None, True])

                    column_sql = sql.SQL(", ").join(
                        sql.Identifier(column)
                        for column in columns
                    )

                    placeholders = sql.SQL(", ").join(
                        sql.Placeholder()
                        for _ in columns
                    )

                    insert_query = sql.SQL("""
                        INSERT INTO {} ({})
                        VALUES ({});
                    """).format(
                        sql.Identifier(target_table),
                        column_sql,
                        placeholders
                    )

                    cur.execute(insert_query, values)

                # If nothing changed, do nothing.

    conn.commit()


def create_database_tables(conn):
    """Create separate tables for testing SCD Type 1 and Type 2."""

    create_target_table(conn, "dim_customer_scd1")
    create_target_table(conn, "dim_customer")


def load_snapshot(filename):
    """Load a daily CSV snapshot."""

    return pd.read_csv(filename)


def point_in_time_query(conn, target_table, customer_id, check_time):
    """
    Return the customer's record at a particular point in time.
    """

    query = sql.SQL("""
        SELECT customer_id, name, email, city,
               valid_from, valid_to, is_current
        FROM {}
        WHERE customer_id = %s
          AND valid_from <= %s
          AND (valid_to IS NULL OR valid_to > %s)
        ORDER BY valid_from DESC
        LIMIT 1;
    """).format(
        sql.Identifier(target_table)
    )

    with conn.cursor() as cur:
        cur.execute(
            query,
            (customer_id, check_time, check_time)
        )
        return cur.fetchone()


def main():

    print("Loading daily snapshots...")

    day1 = load_snapshot("source_day1.csv")
    day2 = load_snapshot("source_day2.csv")
    day3 = load_snapshot("source_day3.csv")

    print("Day 1 rows:", len(day1))
    print("Day 2 rows:", len(day2))
    print("Day 3 rows:", len(day3))

    conn = get_connection()

    try:

        create_database_tables(conn)

        change_cols = ["name", "email", "city"]

        # ==========================================
        # SCD TYPE 1
        # ==========================================

        print("\nRunning SCD Type 1...")

        apply_scd1(
            conn,
            "dim_customer_scd1",
            day1,
            "customer_id",
            change_cols
        )

        apply_scd1(
            conn,
            "dim_customer_scd1",
            day2,
            "customer_id",
            change_cols
        )

        apply_scd1(
            conn,
            "dim_customer_scd1",
            day3,
            "customer_id",
            change_cols
        )

        print("SCD Type 1 completed.")

        # ==========================================
        # SCD TYPE 2
        # ==========================================

        print("\nRunning SCD Type 2...")

        day1_ts = datetime(2026, 6, 1, 9, 0, 0)
        day2_ts = datetime(2026, 6, 2, 9, 0, 0)
        day3_ts = datetime(2026, 6, 3, 9, 0, 0)

        apply_scd2(
            conn,
            "dim_customer",
            day1,
            "customer_id",
            change_cols,
            day1_ts
        )

        apply_scd2(
            conn,
            "dim_customer",
            day2,
            "customer_id",
            change_cols,
            day2_ts
        )

        apply_scd2(
            conn,
            "dim_customer",
            day3,
            "customer_id",
            change_cols,
            day3_ts
        )

        print("SCD Type 2 completed.")

        # ==========================================
        # POINT-IN-TIME TEST
        # ==========================================

        print("\nPoint-in-time test:")

        result = point_in_time_query(
            conn,
            "dim_customer",
            102,
            day2_ts
        )

        print(result)

        # ==========================================
        # DISPLAY SCD2 TABLE
        # ==========================================

        with conn.cursor() as cur:

            cur.execute("""
                SELECT customer_sk,
                       customer_id,
                       name,
                       email,
                       city,
                       valid_from,
                       valid_to,
                       is_current
                FROM dim_customer
                ORDER BY customer_id, valid_from;
            """)

            rows = cur.fetchall()

            print("\n========== SCD TYPE 2 TABLE ==========")

            for row in rows:
                print(row)

    finally:
        conn.close()

    print("\nAssignment processing completed.")


if __name__ == "__main__":
    main()