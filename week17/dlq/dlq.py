import argparse
import json
import os

from confluent_kafka import Producer

from database import (
    init_db,
    list_records,
    get_record,
    update_record_status
)


DLQ_TOPIC = "sentences.dlq"
MAIN_TOPIC = "sentences"
MAX_ATTEMPTS = 3


def create_producer():
    return Producer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
    })


def list_messages():
    records = list_records()

    if not records:
        print("No failed messages found.")
        return

    for record in records:
        print(
            f"ID: {record['id']} | "
            f"Status: {record['status']} | "
            f"Error: {record['error_type']} | "
            f"Attempts: {record['attempt_count']}"
        )


def show_message(record_id):
    record = get_record(record_id)

    if not record:
        print("Message not found.")
        return

    print(json.dumps(dict(record), indent=4, default=str))


def replay_message(record_id):
    record = get_record(record_id)

    if not record:
        print("Message not found.")
        return

    if record["is_poison"]:
        print("This message is marked as poison and cannot be replayed.")
        return

    if record["attempt_count"] >= MAX_ATTEMPTS:
        print("Maximum replay attempts reached.")
        return

    producer = create_producer()

    producer.produce(
        MAIN_TOPIC,
        value=record["original_message"],
        callback=lambda error, message: print(
            f"Replay failed: {error}" if error
            else "Message successfully replayed."
        )
    )

    remaining = producer.flush()

    if remaining == 0:
        update_record_status(
            record_id,
            "replayed",
            False
        )
    else:
        print("Replay delivery is still pending.")


def discard_message(record_id):
    record = get_record(record_id)

    if not record:
        print("Message not found.")
        return

    update_record_status(
        record_id,
        "discarded",
        record["is_poison"]
    )

    print(f"Message {record_id} discarded.")


def main():
    parser = argparse.ArgumentParser(
        description="Dead Letter Queue management CLI"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    subparsers.add_parser("list")

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("id", type=int)

    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("id", type=int)

    discard_parser = subparsers.add_parser("discard")
    discard_parser.add_argument("id", type=int)

    args = parser.parse_args()

    init_db()

    if args.command == "list":
        list_messages()

    elif args.command == "show":
        show_message(args.id)

    elif args.command == "replay":
        replay_message(args.id)

    elif args.command == "discard":
        discard_message(args.id)


if __name__ == "__main__":
    main()