"""
consumer.py - reads sentences from a Kafka topic and keeps live word counts.

For each message it lowercases the sentence, strips punctuation, splits it
into words, and adds them to a running count. Every 10 seconds it prints the
top 5 words seen so far.

Usage:
    python3 consumer.py                    # join consumer group 'wordcount'
    python3 consumer.py --group fresh1     # a new group re-reads the topic from the start
"""

import argparse
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime

# letters, with apostrophes allowed inside a word (don't, it's)
WORD_RE = re.compile(r"[a-z]+(?:'[a-z]+)*")


def tokenize(sentence):
    """Lowercase a sentence and return its words, with punctuation stripped."""
    text = sentence.lower().replace("\u2019", "'").replace("\u2018", "'")
    return WORD_RE.findall(text)


def print_report(counts, sentences, top_n, now, out=print):
    stamp = now().strftime("%H:%M:%S")
    if sentences == 0:
        out(f"[{stamp}] waiting for messages...")
        return
    out(f"[{stamp}] {sentences} sentences, {sum(counts.values())} words so far. Top {top_n}:")
    for rank, (word, count) in enumerate(counts.most_common(top_n), start=1):
        out(f"    {rank}. {word:<12} {count}")


def run(consumer, interval=10.0, top_n=5, clock=time.monotonic, now=datetime.now, out=print):
    """Reads messages until interrupted, printing a report every `interval` seconds."""
    counts = Counter()
    sentences = 0
    skipped = 0
    last_report = clock()

    try:
        while True:
            # poll() waits at most 1s, so the timer below still fires when no messages arrive
            batches = consumer.poll(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    try:
                        message = json.loads(record.value.decode("utf-8"))
                        words = tokenize(message["sentence"])
                    except (ValueError, KeyError, TypeError, AttributeError):
                        skipped += 1  # not valid JSON / missing 'sentence': skip it
                        continue
                    counts.update(words)
                    sentences += 1

            if clock() - last_report >= interval:
                print_report(counts, sentences, top_n, now, out)
                last_report = clock()
    except KeyboardInterrupt:
        out("\nStopping. Final counts:")
        print_report(counts, sentences, top_n, now, out)
        if skipped:
            out(f"(skipped {skipped} malformed messages)")


def main():
    parser = argparse.ArgumentParser(description="Live word counts from a Kafka topic.")
    parser.add_argument("--topic", default="sentences")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="wordcount", help="consumer group id")
    parser.add_argument("--interval", type=float, default=10.0, help="seconds between reports")
    parser.add_argument("--top", type=int, default=5, help="how many top words to show")
    args = parser.parse_args()

    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    print(f"Connecting to Kafka at {args.bootstrap} (gives up after ~30s)...")
    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap,
            group_id=args.group,
            auto_offset_reset="earliest",  # a brand-new group starts from the beginning
        )
    except KafkaError:
        print(
            f"Couldn't reach Kafka at {args.bootstrap}. Is Redpanda running?\n"
            "Try: sudo docker start redpanda",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Subscribed to '{args.topic}' as group '{args.group}'. Ctrl+C to stop.")
    try:
        run(consumer, interval=args.interval, top_n=args.top)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
