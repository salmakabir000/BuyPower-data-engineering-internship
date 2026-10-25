import json
import re
from collections import Counter
from datetime import datetime, timezone, timedelta

import psycopg
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPIC = "sentences"
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
    group_id="word-count-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
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



def main():
    with psycopg.connect(**DB_CONFIG) as connection:
        print("Consumer started. Waiting for messages...")

        try:
            for message in consumer:
                event = message.value
                event_time = datetime.fromisoformat(event["ts"])

                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)

                window_start = get_window_start(event_time)

                processed = process_message(
                    connection,
                    message,
                    event,
                    window_start,
                )

                # Commit Kafka progress only after database processing succeeds.
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

        except KeyboardInterrupt:
            print("\nStopping consumer...")

        finally:
            consumer.close()




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
