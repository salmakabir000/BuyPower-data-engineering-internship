"""
producer.py - streams a book to a Kafka topic, one sentence at a time.

Each message is JSON: {"sentence": "It was the best of times...", "ts": "2026-06-04T10:00:00Z"}
and the producer sleeps a random 0.1-1.0 seconds between sentences to
simulate data arriving in real time.

It can also send deliberately BAD messages, to test the dead letter queue:
    malformed      - JSON cut off halfway
    missing_field  - valid JSON, but no 'sentence' field
    non_utf8       - bytes that are not valid UTF-8 (latin-1 text)

Usage:
    python3 producer.py                           # stream frankenstein.txt to topic 'sentences'
    python3 producer.py --inject-bad 6            # send 6 bad messages first (2 of each kind), then stream
    python3 producer.py --bad-rate 0.05           # about 1 message in 20 is bad, mixed into the stream
    python3 producer.py --dry-run --limit 5       # just print the first 5 messages, no Kafka needed
"""

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, timezone

BAD_KINDS = ("malformed", "missing_field", "non_utf8")


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


def make_bad_message(kind, sentence):
    """Returns the raw BYTES of a deliberately broken message of the given kind."""
    good = make_message(sentence)
    if kind == "malformed":
        raw = json.dumps(good)
        return raw[: max(1, len(raw) // 2)].encode("utf-8")  # cut off halfway: never valid JSON
    if kind == "missing_field":
        return json.dumps({"text": sentence, "ts": good["ts"]}).encode("utf-8")  # no 'sentence'
    if kind == "non_utf8":
        raw = json.dumps({"sentence": sentence + " caf\u00e9", "ts": good["ts"]}, ensure_ascii=False)
        return raw.encode("latin-1", errors="replace")  # latin-1 'e-acute' is byte 0xE9: invalid UTF-8
    raise ValueError(f"unknown bad-message kind '{kind}' (choose from {', '.join(BAD_KINDS)})")


def serialize(value):
    """Dicts become JSON; raw bytes (our bad messages) are sent exactly as they are."""
    return value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description="Stream a book to Kafka, sentence by sentence.")
    parser.add_argument("--file", default="frankenstein.txt")
    parser.add_argument("--topic", default="sentences")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--min-delay", type=float, default=0.1)
    parser.add_argument("--max-delay", type=float, default=1.0)
    parser.add_argument("--limit", type=int, default=None, help="stop after this many sentences")
    parser.add_argument("--dry-run", action="store_true", help="print messages instead of sending them")
    parser.add_argument("--inject-bad", type=int, default=0, metavar="N",
                        help="send N bad messages first, cycling through the bad kinds")
    parser.add_argument("--bad-rate", type=float, default=0.0, metavar="P",
                        help="chance (0 to 1) that any streamed message is replaced by a bad one")
    parser.add_argument("--bad-kinds", default=",".join(BAD_KINDS),
                        help=f"which bad kinds to use (default: all of {', '.join(BAD_KINDS)})")
    parser.add_argument("--seed", type=int, default=None, help="make the random choices repeatable")
    args = parser.parse_args()

    kinds = [k.strip() for k in args.bad_kinds.split(",") if k.strip()]
    unknown = [k for k in kinds if k not in BAD_KINDS]
    if unknown or not kinds:
        parser.error(f"--bad-kinds must be a comma-separated list from: {', '.join(BAD_KINDS)}")
    if not 0 <= args.bad_rate <= 1:
        parser.error("--bad-rate must be between 0 and 1")
    rng = random.Random(args.seed)

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
            producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=serialize)
        except KafkaError:
            print(
                f"Couldn't reach Kafka at {args.bootstrap}. Is Redpanda running?\n"
                "Try: sudo docker start redpanda",
                file=sys.stderr,
            )
            sys.exit(1)

    def send(value, label, preview):
        if producer is None:
            print(f"{label}: {preview}")
        else:
            # no key is set, so Kafka spreads messages across the topic's partitions
            metadata = producer.send(args.topic, value).get(timeout=10)
            print(f"partition={metadata.partition} offset={metadata.offset} | {label}: {preview}")
        time.sleep(rng.uniform(args.min_delay, args.max_delay))

    sent = bad = 0
    try:
        for i in range(args.inject_bad):
            kind = kinds[i % len(kinds)]
            payload = make_bad_message(kind, sentences[i % len(sentences)])
            send(payload, f"BAD ({kind})", repr(payload[:60]))
            bad += 1
        for sentence in sentences:
            if args.bad_rate and rng.random() < args.bad_rate:
                kind = rng.choice(kinds)
                payload = make_bad_message(kind, sentence)
                send(payload, f"BAD ({kind})", repr(payload[:60]))
                bad += 1
            else:
                send(make_message(sentence), "ok", sentence[:60])
                sent += 1
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        print(f"Done. Streamed {sent} sentences, plus {bad} bad messages.")


if __name__ == "__main__":
    main()
