import json
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from database import init_db, save_dlq_record


DLQ_TOPIC = "sentences.dlq"
MAIN_TOPIC = "sentences"
MAX_ATTEMPTS = 3


def delivery_report(error, message):
    if error:
        print(f"DLQ delivery failed: {error}")
    else:
        print(f"Message sent to {message.topic()}")


def send_to_dlq(producer, original_message, error, attempt_count=1):
    envelope = {
        "original_message": original_message,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "consumer": "word_counter",
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "attempt_count": attempt_count,
        "is_poison": attempt_count >= MAX_ATTEMPTS
    }

    producer.produce(
        DLQ_TOPIC,
        key=str(datetime.now(timezone.utc).timestamp()),
        value=json.dumps(envelope),
        callback=delivery_report
    )

    remaining = producer.flush()

    if remaining != 0:
        raise RuntimeError("DLQ message delivery is still pending")

    save_dlq_record(envelope)
    return envelope


def process_message(message, producer):
    try:
        if message.error():
            raise RuntimeError(str(message.error()))

        raw_message = message.value().decode("utf-8")
        data = json.loads(raw_message)

        if "sentence" not in data:
            raise ValueError("Missing required field: sentence")

        sentence = data["sentence"]

        if not isinstance(sentence, str):
            raise ValueError("Sentence must be a string")

        if not sentence.strip():
            raise ValueError("Sentence cannot be empty")

        print(f"Processed sentence: {sentence}")
        return True

    except Exception as error:
        print(f"Message processing failed: {error}")

        try:
            original = message.value()
            if isinstance(original, bytes):
                original = original.decode("utf-8", errors="replace")

            send_to_dlq(producer, original, error)
            print("Failed message safely sent to DLQ.")
            return True

        except Exception as dlq_error:
            print(f"Could not save failed message: {dlq_error}")
            return False


def main():
    init_db()

    consumer = Consumer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        "group.id": "word-counter-dlq",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    producer = Producer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
    })

    consumer.subscribe([MAIN_TOPIC])

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            handled = process_message(message, producer)

            if handled:
                consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        print("Consumer stopped.")

    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()