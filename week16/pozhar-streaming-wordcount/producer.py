"""
producer.py - streams a book to a Kafka topic, one sentence at a time.

Each message is JSON: {"sentence": "It was the best of times...", "ts": "2026-06-04T10:00:00Z"}
and the producer sleeps a random 0.1-1.0 seconds between sentences to
simulate data arriving in real time.

Usage:
    python3 producer.py                      # stream frankenstein.txt to topic 'sentences'
    python3 producer.py --dry-run --limit 5  # just print the first 5 sentences, no Kafka needed
"""

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, timezone


def load_sentences(path):
    """Reads a Project Gutenberg text file and returns a list of sentences."""
    with open(path, encoding="utf-8-sig") as f:
        text = f.read()

    # keep only the book itself: drop Gutenberg's license header and footer
    start = re.search(r"\*\*\* START OF .*?\*\*\*", text)
    end = re.search(r"\*\*\* END OF ", text)
    if start:
        text = text[start.end():]
    if end:
        text = text[: text.find("*** END OF ")]

    # the file wraps lines at ~70 characters, so join everything into one stream
    text = re.sub(r"\s+", " ", text).strip()

    # split after . ! ? (also when a closing quote follows the punctuation)
    # (only when the next word starts with a capital or an opening quote, so
    # dialogue tags like: "Are you well?" he asked.  stay in one sentence)
    parts = re.split(
        r"(?<=[.!?])\s+(?=[A-Z0-9\"\u201c\u2018'])|(?<=[.!?][\"\u201d\u2019])\s+(?=[A-Z0-9\"\u201c\u2018'])",
        text,
    )
    return [p.strip() for p in parts if re.search(r"\w", p)]


def make_message(sentence):
    return {
        "sentence": sentence,
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def main():
    parser = argparse.ArgumentParser(description="Stream a book to Kafka, sentence by sentence.")
    parser.add_argument("--file", default="frankenstein.txt")
    parser.add_argument("--topic", default="sentences")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--min-delay", type=float, default=0.1)
    parser.add_argument("--max-delay", type=float, default=1.0)
    parser.add_argument("--limit", type=int, default=None, help="stop after this many sentences")
    parser.add_argument("--dry-run", action="store_true", help="print sentences instead of sending them")
    args = parser.parse_args()

    sentences = load_sentences(args.file)
    print(f"Loaded {len(sentences)} sentences from {args.file}")
    if args.limit:
        sentences = sentences[: args.limit]

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError

        print(f"Connecting to Kafka at {args.bootstrap} (gives up after ~30s)...")
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except KafkaError:
            print(
                f"Couldn't reach Kafka at {args.bootstrap}. Is Redpanda running?\n"
                "Try: sudo docker start redpanda",
                file=sys.stderr,
            )
            sys.exit(1)

    sent = 0
    try:
        for sentence in sentences:
            message = make_message(sentence)
            if producer is None:
                print(json.dumps(message))
            else:
                # no key is set, so Kafka spreads messages across the topic's partitions
                metadata = producer.send(args.topic, message).get(timeout=10)
                print(f"partition={metadata.partition} offset={metadata.offset} | {sentence[:70]}")
            sent += 1
            time.sleep(random.uniform(args.min_delay, args.max_delay))
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        print(f"Done. Sent {sent} sentences.")


if __name__ == "__main__":
    main()
