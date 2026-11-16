from kafka import KafkaProducer
import argparse
import psycopg
from datetime import datetime, timedelta, timezone

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "wordcounts",
    "user": "wordcount",
    "password": "wordcount_dev_password",
}

KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER]
)

def list_records(limit):
    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT dlq_id, error_type, error_message,
                       original_topic, failed_at, status
                FROM dlq_records
                ORDER BY failed_at DESC
                LIMIT %s
                """,
                (limit,),
            )

            records = cursor.fetchall()

            for record in records:
                print(f"\nDLQ ID: {record[0]}")
                print(f"Error: {record[1]}")
                print(f"Message: {record[2]}")
                print(f"Topic: {record[3]}")
                print(f"Failed at: {record[4]}")
                print(f"Status: {record[5]}")
                print("-" * 50)


def show_record(dlq_id):
    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM dlq_records
                WHERE dlq_id = %s
                """,
                (dlq_id,),
            )

            record = cursor.fetchone()

            if record is None:
                print(f"No DLQ record found with ID {dlq_id}")
                return

            columns = [column.name for column in cursor.description]

            for name, value in zip(columns, record):
                if name == "original_message":
                    value = bytes(value)

                print(f"{name}: {value}")


def replay_record(dlq_id):
    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT original_message, original_topic,
                           replay_count, status
                    FROM dlq_records
                    WHERE dlq_id = %s
                    FOR UPDATE
                    """,
                    (dlq_id,),
                )

                record = cursor.fetchone()

                if record is None:
                    print(f"DLQ record {dlq_id} not found.")
                    return

                original_message, topic, replay_count, status = record

                if status != "pending":
                    print(f"Cannot replay a record with status: {status}")
                    return

                if replay_count >= 3:
                    cursor.execute(
                        """
                        UPDATE dlq_records
                        SET status = 'poison'
                        WHERE dlq_id = %s
                        """,
                        (dlq_id,),
                    )
                    print("Replay limit reached. Record marked poison.")
                    return

                next_attempt = replay_count + 1

                producer.send(
                    topic,
                    value=bytes(original_message),
                    headers=[
                        ("dlq_id", str(dlq_id).encode("utf-8")),
                        ("replay_count", str(next_attempt).encode("utf-8")),
                    ],
                ).get(timeout=10)

                cursor.execute(
                    """
                    UPDATE dlq_records
                    SET status = 'replaying'
                    WHERE dlq_id = %s
                    """,
                    (dlq_id,),
                )

                print(f"Replay attempt {next_attempt} sent for DLQ record {dlq_id}.")

def discard_record(dlq_id):
    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE dlq_records
                SET status = 'discarded'
                WHERE dlq_id = %s
                  AND status = 'pending'
                RETURNING dlq_id
                """,
                (dlq_id,),
            )

            if cursor.fetchone():
                print(f"DLQ record {dlq_id} discarded.")
            else:
                print(
                    f"Record {dlq_id} not found or cannot be discarded."
                )

def parse_duration(value):
    units = {
        "m": "minutes",
        "h": "hours",
        "d": "days",
    }

    number = int(value[:-1])
    unit = value[-1]

    if unit not in units or number <= 0:
        raise ValueError("Use a duration like 30m, 1h, or 2d.")

    return timedelta(**{units[unit]: number})


def replay_all(since):
    duration = parse_duration(since)
    cutoff = datetime.now(timezone.utc) - duration

    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT dlq_id
                FROM dlq_records
                WHERE status = 'pending'
                  AND replay_count < 3
                  AND failed_at >= %s
                ORDER BY failed_at
                """,
                (cutoff,),
            )

            records = cursor.fetchall()

    print(f"Found {len(records)} pending records to replay.")

    for record in records:
        replay_record(record[0])


def main():
    parser = argparse.ArgumentParser(
        description="Dead Letter Queue management CLI"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--limit", type=int, default=20)

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("dlq_id", type=int)
    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("dlq_id", nargs="?",type=int,)
    replay_parser.add_argument("--all", action="store_true",)
    replay_parser.add_argument("--since", default="1h",)
    discard_parser = subparsers.add_parser("discard")
    discard_parser.add_argument("dlq_id", type=int)


    args = parser.parse_args()

    if args.command == "list":
        list_records(args.limit)

    elif args.command == "show":
        show_record(args.dlq_id)

    elif args.command == "replay":
        if args.all and args.dlq_id is not None:
            parser.error("Use either a DLQ ID or --all, not both.")

        if args.all:
            replay_all(args.since)
        elif args.dlq_id is not None:
            replay_record(args.dlq_id)
        else:
            parser.error("Provide a DLQ ID or use --all.")

    elif args.command == "discard":
        discard_record(args.dlq_id)

if __name__ == "__main__":
    try:
        main()
    finally:
        producer.close()
