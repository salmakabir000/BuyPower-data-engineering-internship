import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json


def get_connection():
    connection_string = os.getenv("PG_CONN")

    if not connection_string:
        raise EnvironmentError("PG_CONN environment variable is not set")

    return psycopg2.connect(connection_string)


def init_db():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dlq_records (
                    id BIGSERIAL PRIMARY KEY,
                    original_message TEXT NOT NULL,
                    error_type VARCHAR(255) NOT NULL,
                    error_message TEXT,
                    consumer VARCHAR(255),
                    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    attempt_count INTEGER DEFAULT 1,
                    status VARCHAR(50) DEFAULT 'pending',
                    is_poison BOOLEAN DEFAULT FALSE
                )
            """)

        connection.commit()
    finally:
        connection.close()


def save_dlq_record(record):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dlq_records (
                    original_message,
                    error_type,
                    error_message,
                    consumer,
                    failed_at,
                    attempt_count,
                    is_poison
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                record.get("original_message", ""),
                record.get("error_type", "UnknownError"),
                record.get("error_message", ""),
                record.get("consumer", "word_counter"),
                record.get("failed_at"),
                record.get("attempt_count", 1),
                record.get("is_poison", False)
            ))

            record_id = cursor.fetchone()[0]

        connection.commit()
        return record_id

    finally:
        connection.close()


def list_records(limit=20):
    connection = get_connection()

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT * FROM dlq_records
                ORDER BY failed_at DESC
                LIMIT %s
            """, (limit,))

            return cursor.fetchall()

    finally:
        connection.close()


def get_record(record_id):
    connection = get_connection()

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM dlq_records WHERE id = %s",
                (record_id,)
            )

            return cursor.fetchone()

    finally:
        connection.close()


def update_record_status(record_id, status, is_poison=False):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE dlq_records
                SET status = %s, is_poison = %s
                WHERE id = %s
            """, (status, is_poison, record_id))

        connection.commit()

    finally:
        connection.close()