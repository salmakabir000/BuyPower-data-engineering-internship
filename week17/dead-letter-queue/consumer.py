import base64
import json
import re
from collections import Counter
from datetime import datetime, timezone, timedelta
import time
import psycopg
from kafka import KafkaConsumer, KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "sentences.test"
WINDOW_SECONDS = 30

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "wordcounts",
    "user": "wordcount",
    "password": "wordcount_dev_password",
}

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    group_id="word-count-dlq-test-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)

dlq_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

def send_to_dlq(connection, message, error, attempt_count=1):
    original_bytes = message.value
    failed_at = datetime.now(timezone.utc)

    dlq_record = {
        "original_message": base64.b64encode(original_bytes).decode("ascii"),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "consumer": "word-count-dlq-group",
        "failed_at": failed_at.isoformat(),
        "attempt_count": attempt_count,
        "original_topic": message.topic,
        "original_partition": message.partition,
        "original_offset": message.offset,
    }

    # Publish to Redpanda.
    dlq_producer.send(
        "sentences.dlq",
        value=dlq_record,
    ).get(timeout=10)

    # Save a copy in PostgreSQL.
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO dlq_records (
                    original_message,
                    error_type,
                    error_message,
                    consumer,
                    failed_at,
                    attempt_count,
                    original_topic,
                    original_partition,
                    original_offset
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    original_bytes,
                    dlq_record["error_type"],
                    dlq_record["error_message"],
                    dlq_record["consumer"],
                    failed_at,
                    attempt_count,
                    message.topic,
                    message.partition,
                    message.offset,
                ),
            )

def get_replay_info(message):
    headers = dict(message.headers or [])

    dlq_id = headers.get("dlq_id")
    replay_count = headers.get("replay_count")

    if dlq_id is None or replay_count is None:
        return None, None

    return int(dlq_id.decode("utf-8")), int(replay_count.decode("utf-8"))


def mark_replay_success(connection, dlq_id, replay_count):
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE dlq_records
                SET replay_count = %s,
                    status = 'replayed'
                WHERE dlq_id = %s
                """,
                (replay_count, dlq_id),
            )


def mark_replay_failure(connection, dlq_id, replay_count, error):
    new_status = "poison" if replay_count >= 3 else "pending"

    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE dlq_records
                SET replay_count = %s,
                    error_type = %s,
                    error_message = %s,
                    status = %s
                WHERE dlq_id = %s
                """,
                (
                    replay_count,
                    type(error).__name__,
                    str(error),
                    new_status,
                    dlq_id,
                ),
            )

def get_window_start(event_time):
    seconds = (event_time.second // WINDOW_SECONDS) * WINDOW_SECONDS
    return event_time.replace(second=seconds, microsecond=0)

def save_window(connection, window_start, counts):
    window_end = window_start + timedelta(seconds=WINDOW_SECONDS)

    with connection.cursor() as cursor:
        for word, count in counts.items():
            cursor.execute(
                """
                INSERT INTO word_counts_30s
                    (window_start, window_end, word, count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (window_start, word)
                DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    count = EXCLUDED.count
                """,
                (window_start, window_end, word, count),
            )

    connection.commit()
    print(f"Saved window {window_start} to {window_end}")

def process_with_retry(message, event, window_start):
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            with psycopg.connect(**DB_CONFIG) as connection:
                return process_message(
                    connection,
                    message,
                    event,
                    window_start,
                )

        except (psycopg.OperationalError, psycopg.InterfaceError) as error:
            if attempt == max_attempts:
                print("Database retries exhausted.")
                raise

            wait_time = 2 ** attempt

            print(
                f"Database error: {error}. "
                f"Retrying in {wait_time} seconds..."
            )

            time.sleep(wait_time)

def main():
    with psycopg.connect(**DB_CONFIG) as connection:
        print("Consumer started. Waiting for messages...")

        try:
            for message in consumer:
                try:
                    # Decode raw bytes and parse JSON.
                    event = json.loads(message.value.decode("utf-8"))

                    # Validate the message structure.
                    if not isinstance(event, dict):
                        raise ValueError("Message must be a JSON object")

                    if not isinstance(event.get("sentence"), str):
                        raise ValueError("Missing or invalid sentence field")

                    if not isinstance(event.get("ts"), str):
                        raise ValueError("Missing or invalid ts field")

                    # Parse event timestamp.
                    event_time = datetime.fromisoformat(event["ts"])

                    if event_time.tzinfo is None:
                        event_time = event_time.replace(
                            tzinfo=timezone.utc
                        )

                    window_start = get_window_start(event_time)

                    # Process the valid message.
                    replay_dlq_id, replay_count = get_replay_info(message)
                    processed = process_with_retry(
                        message,
                        event,
                        window_start,
                    )

                    if replay_dlq_id is not None:
                        mark_replay_success(
                            connection,
                            replay_dlq_id,
                            replay_count,
                        )
                    # Commit Kafka progress after database processing succeeds.
                    consumer.commit()

                    if processed:
                        print(
                            f"Processed partition {message.partition}, "
                            f"offset {message.offset}, "
                            f"window {window_start}"
                        )
                    else:
                        print(
                            f"Skipped duplicate message: "
                            f"partition {message.partition}, "
                            f"offset {message.offset}"
                        )

                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    KeyError,
                    TypeError,
                    ValueError,
                ) as error:

                    replay_dlq_id, replay_count = get_replay_info(message)

                    if replay_dlq_id is not None:
                        mark_replay_failure(
                            connection,
                            replay_dlq_id,
                            replay_count,
                            error,
                        )
                        print(
                            f"Replay failed for DLQ record {replay_dlq_id}. "
                            f"Attempt {replay_count}."
                        )
                    else:
                        send_to_dlq(connection, message, error)
                        print(
                            f"Sent failed message to DLQ: "
                            f"partition {message.partition}, "
                            f"offset {message.offset}, "
                            f"error {type(error).__name__}"
                        )

                    consumer.commit()

        except KeyboardInterrupt:
            print("\nStopping consumer...")

        finally:
            consumer.close()
            dlq_producer.close()



def process_message(connection, message, event, window_start):
    words = re.findall(r"\b[a-zA-Z]+\b", event["sentence"].lower())
    word_counts = Counter(words)

    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO processed_messages
                    (topic, partition_id, message_offset)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING message_offset
                """,
                (message.topic, message.partition, message.offset),
            )

            if cursor.fetchone() is None:
                return False

            window_end = window_start + timedelta(seconds=WINDOW_SECONDS)

            for word, count in word_counts.items():
                cursor.execute(
                    """
                    INSERT INTO word_counts_30s
                        (window_start, window_end, word, count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (window_start, word)
                    DO UPDATE SET
                        count = word_counts_30s.count + EXCLUDED.count
                    """,
                    (window_start, window_end, word, count),
                )

    return True

if __name__ == "__main__":
    main()
