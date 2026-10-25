import json
import random
import re
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

BOOK_URL = "https://www.gutenberg.org/files/11/11-0.txt"
TOPIC = "sentences"
KAFKA_SERVER = "localhost:9092"


def load_sentences():
    response = requests.get(BOOK_URL, timeout=30)
    response.raise_for_status()
    book_text = response.text

    start_marker = "CHAPTER I.\nDown the Rabbit-Hole"
    start_index = book_text.find(start_marker)

    if start_index == -1:
        raise ValueError("Could not find the beginning of the story.")

    book_text = book_text[start_index:]

    end_marker = "*** END OF THE PROJECT GUTENBERG EBOOK"
    book_text = book_text.split(end_marker, 1)[0]

    book_text = re.sub(r"\s+", " ", book_text).strip()
    sentences = re.split(r"(?<=[.!?])\s+", book_text)

    return [sentence.strip() for sentence in sentences if sentence.strip()]


def main():
    sentences = load_sentences()
    print(f"Loaded {len(sentences)} text segments.")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        for sentence in sentences:
            event = {
                "sentence": sentence,
                "ts": datetime.now(timezone.utc).isoformat(),
            }

            producer.send(TOPIC, value=event)
            print(f"Sent: {sentence[:80]}")

            time.sleep(random.uniform(0.1, 1.0))

        producer.flush()
        print("Finished streaming the book.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
